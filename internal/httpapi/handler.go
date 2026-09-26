package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/merak-max/auction-engine/internal/auction"
)

type Options struct {
	Run   func(context.Context, auction.Request) (auction.Result, error)
	Ready func(context.Context) error
	Token string
	Bids  map[string]int64 // server-side bids for the recommender bridge
}

// Handler is the local, unauthenticated in-memory handler used by tests.
func Handler(engine *auction.Engine) http.Handler {
	return NewHandler(Options{Run: func(_ context.Context, r auction.Request) (auction.Result, error) { return engine.Run(r) }})
}

type counters struct {
	filled, noFill, clientErrors, serverErrors atomic.Uint64
	latencies                                  [8]atomic.Uint64
	sumNanos                                   atomic.Uint64
}

var cutoffs = [...]time.Duration{500 * time.Microsecond, time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond}

func (m *counters) observe(duration time.Duration) {
	m.sumNanos.Add(uint64(duration.Nanoseconds()))
	for i, limit := range cutoffs {
		if duration <= limit {
			m.latencies[i].Add(1)
			return
		}
	}
	m.latencies[len(m.latencies)-1].Add(1)
}

func NewHandler(opts Options) http.Handler {
	if opts.Run == nil {
		panic("auction runner is required")
	}
	m := &counters{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if opts.Ready != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 250*time.Millisecond)
			defer cancel()
			if err := opts.Ready(ctx); err != nil {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(r, opts.Token) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintln(w, "# HELP auction_requests_total Completed auction requests by outcome.\n# TYPE auction_requests_total counter")
		fmt.Fprintf(w, "auction_requests_total{outcome=\"filled\"} %d\n", m.filled.Load())
		fmt.Fprintf(w, "auction_requests_total{outcome=\"no_fill\"} %d\n", m.noFill.Load())
		fmt.Fprintf(w, "auction_requests_total{outcome=\"client_error\"} %d\n", m.clientErrors.Load())
		fmt.Fprintf(w, "auction_requests_total{outcome=\"server_error\"} %d\n", m.serverErrors.Load())
		fmt.Fprintln(w, "# HELP auction_http_duration_seconds HTTP auction request duration.\n# TYPE auction_http_duration_seconds histogram")
		var count uint64
		labels := [...]string{"0.0005", "0.001", "0.002", "0.005", "0.01", "0.05", "0.1", "+Inf"}
		for i, label := range labels {
			count += m.latencies[i].Load()
			fmt.Fprintf(w, "auction_http_duration_seconds_bucket{le=%q} %d\n", label, count)
		}
		fmt.Fprintf(w, "auction_http_duration_seconds_sum %.9f\nauction_http_duration_seconds_count %d\n", float64(m.sumNanos.Load())/1e9, count)
	})
	mux.HandleFunc("POST /v1/auctions", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(r, opts.Token) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		started := time.Now()
		defer func() { m.observe(time.Since(started)) }()
		var req auction.Request
		if err := decode(w, r, &req); err != nil {
			m.clientErrors.Add(1)
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		result, err := opts.Run(r.Context(), req)
		if err != nil {
			serveError(w, m, err)
			return
		}
		recordResult(m, result)
		writeJSON(w, result)
	})
	mux.HandleFunc("POST /v1/auction-recommendations", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(r, opts.Token) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		started := time.Now()
		defer func() { m.observe(time.Since(started)) }()
		var wrapper struct {
			AuctionID              string          `json:"auction_id"`
			FloorMicros            int64           `json:"floor_micros"`
			RecommendationResponse json.RawMessage `json:"recommendation_response"`
		}
		if err := decode(w, r, &wrapper); err != nil {
			m.clientErrors.Add(1)
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		var upstream struct {
			Recommendations []json.RawMessage `json:"recommendations"`
		}
		if err := json.Unmarshal(wrapper.RecommendationResponse, &upstream); err != nil || len(upstream.Recommendations) == 0 || len(upstream.Recommendations) > 128 {
			m.clientErrors.Add(1)
			http.Error(w, "expected a filled recommendation_response with 1 to 128 recommendations", http.StatusBadRequest)
			return
		}
		req := auction.Request{AuctionID: wrapper.AuctionID, FloorMicros: wrapper.FloorMicros}
		byID := make(map[string]json.RawMessage, len(upstream.Recommendations))
		for _, raw := range upstream.Recommendations {
			var rec struct {
				Ad struct {
					CampaignID string `json:"campaign_id"`
				} `json:"ad"`
			}
			if err := json.Unmarshal(raw, &rec); err != nil || rec.Ad.CampaignID == "" || opts.Bids[rec.Ad.CampaignID] <= 0 || byID[rec.Ad.CampaignID] != nil {
				m.clientErrors.Add(1)
				http.Error(w, "every recommendation needs a distinct configured campaign_id and bid", http.StatusBadRequest)
				return
			}
			byID[rec.Ad.CampaignID] = raw
			req.Candidates = append(req.Candidates, auction.Candidate{CampaignID: rec.Ad.CampaignID, BidMicros: opts.Bids[rec.Ad.CampaignID]})
		}
		result, err := opts.Run(r.Context(), req)
		if err != nil {
			serveError(w, m, err)
			return
		}
		recordResult(m, result)
		writeJSON(w, struct {
			Auction        auction.Result  `json:"auction"`
			Recommendation json.RawMessage `json:"recommendation"`
		}{result, byID[result.WinnerID]})
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}

func authorized(r *http.Request, token string) bool {
	if token == "" { // development only: the CLI forbids unauthenticated non-loopback binds
		return true
	}
	value := r.Header.Get("Authorization")
	if !strings.HasPrefix(value, "Bearer ") {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(value, "Bearer ")), []byte(token)) == 1
}

func decode(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("unexpected content after request")
	}
	return nil
}

func recordResult(m *counters, result auction.Result) {
	if result.WinnerID == "" {
		m.noFill.Add(1)
	} else {
		m.filled.Add(1)
	}
}

func serveError(w http.ResponseWriter, m *counters, err error) {
	if errors.Is(err, auction.ErrInvalidRequest) {
		m.clientErrors.Add(1)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	m.serverErrors.Add(1)
	log.Printf("auction failed: %v", err)
	http.Error(w, "auction temporarily unavailable", http.StatusServiceUnavailable)
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
