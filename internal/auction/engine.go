// Package auction implements a single-slot, impression-priced second-price auction.
package auction

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	maxCandidates = 128
	maxMicros     = 1_000_000_000_000 // also keeps budget * seconds-per-day in int64
	daySeconds    = 86_400
	cacheSize     = 100_000
)

var ErrClockRegression = errors.New("UTC clock moved to a previous day")
var ErrInvalidRequest = errors.New("invalid auction request")

type Campaign struct {
	ID                string `json:"id"`
	DailyBudgetMicros int64  `json:"daily_budget_micros"`
	PacingBurstMicros int64  `json:"pacing_burst_micros"`
	BidMicros         int64  `json:"bid_micros,omitempty"`
}

type Config struct {
	Campaigns []Campaign `json:"campaigns"`
}

type Candidate struct {
	CampaignID string `json:"campaign_id"`
	BidMicros  int64  `json:"bid_micros"`
}

type Request struct {
	AuctionID   string      `json:"auction_id"`
	FloorMicros int64       `json:"floor_micros"`
	Candidates  []Candidate `json:"candidates"`
}

type Result struct {
	AuctionID           string `json:"auction_id"`
	WinnerID            string `json:"winner_id,omitempty"`
	WinningBidMicros    int64  `json:"winning_bid_micros,omitempty"`
	ClearingPriceMicros int64  `json:"clearing_price_micros,omitempty"`
	NoFillReason        string `json:"no_fill_reason,omitempty"`
}

type account struct {
	budget int64
	burst  int64
	spent  int64
}

type savedResult struct {
	request Request
	result  Result
}

// Engine holds one process's budget ledger. Its mutex covers eligibility,
// ranking, pricing and debiting so concurrent wins cannot overspend a budget.
type Engine struct {
	mu       sync.Mutex
	clock    func() time.Time
	day      string
	accounts map[string]*account
	seen     map[string]savedResult
	order    []string
	next     int
}

func New(cfg Config, clock func() time.Time) (*Engine, error) {
	if clock == nil {
		clock = time.Now
	}
	if len(cfg.Campaigns) == 0 {
		return nil, errors.New("at least one campaign is required")
	}
	e := &Engine{clock: clock, accounts: make(map[string]*account, len(cfg.Campaigns)), seen: make(map[string]savedResult)}
	for _, c := range cfg.Campaigns {
		if c.ID == "" || len(c.ID) > 128 {
			return nil, errors.New("campaign IDs must be 1 to 128 bytes")
		}
		if _, exists := e.accounts[c.ID]; exists {
			return nil, fmt.Errorf("duplicate campaign %q", c.ID)
		}
		if c.DailyBudgetMicros < 1 || c.DailyBudgetMicros > maxMicros || c.PacingBurstMicros < 0 || c.PacingBurstMicros > c.DailyBudgetMicros || c.BidMicros < 0 || c.BidMicros > maxMicros {
			return nil, fmt.Errorf("invalid budget or burst for campaign %q", c.ID)
		}
		e.accounts[c.ID] = &account{budget: c.DailyBudgetMicros, burst: c.PacingBurstMicros}
	}
	return e, nil
}

func (e *Engine) Run(req Request) (Result, error) {
	if err := validateRequest(req); err != nil {
		return Result{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	now := e.clock().UTC()
	day := now.Format("2006-01-02")
	if e.day != day {
		if e.day != "" && day < e.day {
			return Result{}, ErrClockRegression
		}
		e.day = day
		for _, a := range e.accounts {
			a.spent = 0
		}
		e.seen = make(map[string]savedResult)
		e.order = nil
		e.next = 0
	}
	if previous, found := e.seen[req.AuctionID]; found {
		if !sameRequest(previous.request, req) {
			return Result{}, fmt.Errorf("%w: auction_id already used for a different request", ErrInvalidRequest)
		}
		return previous.result, nil
	}

	// Seconds since UTC midnight; the burst supplies initial headroom. The
	// allowance rises linearly, but never exceeds the actual daily budget.
	seconds := int64(now.Hour()*3600 + now.Minute()*60 + now.Second())
	type bid struct {
		Candidate
		headroom int64
	}
	eligible := make([]bid, 0, len(req.Candidates))
	for _, c := range req.Candidates {
		a, found := e.accounts[c.CampaignID]
		if !found {
			return Result{}, fmt.Errorf("%w: unknown campaign_id %q", ErrInvalidRequest, c.CampaignID)
		}
		if c.BidMicros < req.FloorMicros {
			continue
		}
		allowed := min(a.budget, a.burst+a.budget*seconds/daySeconds)
		if a.spent <= allowed && allowed-a.spent >= req.FloorMicros {
			eligible = append(eligible, bid{c, allowed - a.spent})
		}
	}
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].BidMicros != eligible[j].BidMicros {
			return eligible[i].BidMicros > eligible[j].BidMicros
		}
		return eligible[i].CampaignID < eligible[j].CampaignID
	})

	result := Result{AuctionID: req.AuctionID, NoFillReason: "no_eligible_budget_or_bid"}
	for i, winner := range eligible {
		price := req.FloorMicros
		if i+1 < len(eligible) {
			price = max(price, eligible[i+1].BidMicros)
		}
		// A bidder that cannot pay the second price is removed; the next
		// bidder competes against the remaining, affordable bids.
		if price > winner.headroom {
			continue
		}
		e.accounts[winner.CampaignID].spent += price
		result = Result{AuctionID: req.AuctionID, WinnerID: winner.CampaignID, WinningBidMicros: winner.BidMicros, ClearingPriceMicros: price}
		break
	}
	e.remember(req, result)
	return result, nil
}

func validateRequest(req Request) error {
	if req.AuctionID == "" || len(req.AuctionID) > 128 {
		return fmt.Errorf("%w: auction_id must be 1 to 128 bytes", ErrInvalidRequest)
	}
	if req.FloorMicros < 0 || req.FloorMicros > maxMicros {
		return fmt.Errorf("%w: floor_micros out of range", ErrInvalidRequest)
	}
	if len(req.Candidates) == 0 || len(req.Candidates) > maxCandidates {
		return fmt.Errorf("%w: candidates must contain 1 to %d entries", ErrInvalidRequest, maxCandidates)
	}
	unique := make(map[string]bool, len(req.Candidates))
	for _, c := range req.Candidates {
		if c.BidMicros < 1 || c.BidMicros > maxMicros || unique[c.CampaignID] {
			return fmt.Errorf("%w: bid out of range or duplicate campaign_id", ErrInvalidRequest)
		}
		unique[c.CampaignID] = true
	}
	return nil
}

func sameRequest(a, b Request) bool {
	if a.AuctionID != b.AuctionID || a.FloorMicros != b.FloorMicros || len(a.Candidates) != len(b.Candidates) {
		return false
	}
	for i := range a.Candidates {
		if a.Candidates[i] != b.Candidates[i] {
			return false
		}
	}
	return true
}

func (e *Engine) remember(req Request, result Result) {
	// A fixed-size, per-day replay window bounds RAM. Entries removed from the
	// window are no longer idempotent; this is not a durable billing ledger.
	copyReq := req
	copyReq.Candidates = append([]Candidate(nil), req.Candidates...)
	if len(e.order) < cacheSize {
		e.order = append(e.order, req.AuctionID)
	} else {
		delete(e.seen, e.order[e.next])
		e.order[e.next] = req.AuctionID
		e.next = (e.next + 1) % cacheSize
	}
	e.seen[req.AuctionID] = savedResult{copyReq, result}
}

// SpentMicros reports this process's current UTC-day spend for a campaign.
func (e *Engine) SpentMicros(id string) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if a := e.accounts[id]; a != nil {
		return a.spent
	}
	return 0
}
