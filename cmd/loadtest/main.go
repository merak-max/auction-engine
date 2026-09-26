// loadtest measures closed-loop HTTP latency through complete response reads.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type report struct {
	Date        string  `json:"date_utc"`
	Label       string  `json:"label"`
	GoVersion   string  `json:"go_version"`
	Platform    string  `json:"platform"`
	CPUs        int     `json:"logical_cpus"`
	Requests    int     `json:"requests"`
	Warmup      int     `json:"warmup"`
	Concurrency int     `json:"concurrency"`
	Slots       int     `json:"slots"`
	Seconds     float64 `json:"elapsed_seconds"`
	RPS         float64 `json:"requests_per_second"`
	P50         float64 `json:"p50_ms"`
	P95         float64 `json:"p95_ms"`
	P99         float64 `json:"p99_ms"`
	Max         float64 `json:"max_ms"`
	Failures    int64   `json:"failures"`
	NoFills     int64   `json:"no_fills"`
}

func main() {
	url := flag.String("url", "http://127.0.0.1:8788/v1/auctions", "comma-separated auction endpoints (round robin)")
	requests := flag.Int("requests", 100000, "measured requests")
	warmup := flag.Int("warmup", 2000, "unmeasured warmup requests")
	concurrency := flag.Int("concurrency", 8, "simultaneous clients (closed-loop)")
	slots := flag.Int("slots", 1, "requested slots (1..3)")
	output := flag.String("output", "", "write machine-readable benchmark report")
	label := flag.String("label", "local", "workload label for report")
	maxP99 := flag.Duration("max-p99", 0, "optional SLO gate; fail if p99 is at or above this duration")
	flag.Parse()
	urls := strings.Split(*url, ",")
	if *requests <= 0 || *warmup < 0 || *concurrency <= 0 || *slots < 1 || *slots > 3 {
		fmt.Fprintln(os.Stderr, "invalid request/warmup/concurrency/slots argument")
		os.Exit(2)
	}
	transport := &http.Transport{MaxIdleConns: *concurrency, MaxIdleConnsPerHost: *concurrency, MaxConnsPerHost: *concurrency}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	stamp := time.Now().UnixNano()
	run := func(count, offset int) ([]time.Duration, time.Duration, int64, int64) {
		latencies := make([]time.Duration, count)
		var next, failures, noFills atomic.Int64
		var wg sync.WaitGroup
		started := time.Now()
		for range *concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(next.Add(1) - 1)
					if i >= count {
						return
					}
					id := fmt.Sprintf("load-%d-%d", stamp, offset+i)
					body := []byte(fmt.Sprintf(`{"auction_id":%q,"slots":%d,"floor_micros":500,"candidates":[{"campaign_id":"atlas","bid_micros":1800},{"campaign_id":"boreal","bid_micros":1500},{"campaign_id":"cedar","bid_micros":1100}]}`, id, *slots))
					req, err := http.NewRequest(http.MethodPost, strings.TrimSpace(urls[(offset+i)%len(urls)]), bytes.NewReader(body))
					if err != nil {
						failures.Add(1)
						continue
					}
					req.Header.Set("Content-Type", "application/json")
					if token := os.Getenv("AUCTION_TOKEN"); token != "" {
						req.Header.Set("Authorization", "Bearer "+token)
					}
					start := time.Now()
					resp, err := client.Do(req)
					if err != nil {
						latencies[i] = time.Since(start)
						failures.Add(1)
						continue
					}
					data, readErr := io.ReadAll(resp.Body)
					_ = resp.Body.Close()
					latencies[i] = time.Since(start)
					if readErr != nil || resp.StatusCode != 200 {
						failures.Add(1)
						continue
					}
					var result struct {
						AuctionID string            `json:"auction_id"`
						WinnerID  string            `json:"winner_id"`
						NoFill    string            `json:"no_fill_reason"`
						Winners   []json.RawMessage `json:"winners"`
					}
					if err = json.Unmarshal(data, &result); err != nil || result.AuctionID != id {
						failures.Add(1)
						continue
					}
					if result.NoFill != "" {
						noFills.Add(1)
					} else if result.WinnerID == "" || (*slots > 1 && len(result.Winners) != *slots) {
						failures.Add(1)
					}
				}
			}()
		}
		wg.Wait()
		return latencies, time.Since(started), failures.Load(), noFills.Load()
	}
	_, _, warmErrors, warmNoFills := run(*warmup, 0)
	if warmErrors+warmNoFills != 0 {
		fmt.Fprintf(os.Stderr, "warmup failed: errors=%d no_fills=%d\n", warmErrors, warmNoFills)
		os.Exit(1)
	}
	latencies, duration, failures, noFills := run(*requests, *warmup)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(p int) time.Duration { return latencies[(len(latencies)*p+99)/100-1] }
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	r := report{Date: time.Now().UTC().Format(time.RFC3339), Label: *label, GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, CPUs: runtime.NumCPU(), Requests: *requests, Warmup: *warmup, Concurrency: *concurrency, Slots: *slots, Seconds: duration.Seconds(), RPS: float64(*requests) / duration.Seconds(), P50: ms(percentile(50)), P95: ms(percentile(95)), P99: ms(percentile(99)), Max: ms(latencies[len(latencies)-1]), Failures: failures, NoFills: noFills}
	encoded, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(encoded))
	if *output != "" {
		if err := os.WriteFile(*output, append(encoded, '\n'), 0644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if failures+noFills != 0 || (*maxP99 > 0 && percentile(99) >= *maxP99) {
		fmt.Fprintln(os.Stderr, "load test failed its error/fill/SLO gate")
		os.Exit(1)
	}
}
