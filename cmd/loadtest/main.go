// loadtest measures end-to-end loopback HTTP latency, including response reads.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:8788/v1/auctions", "auction endpoint")
	requests := flag.Int("requests", 100_000, "measured requests")
	warmup := flag.Int("warmup", 2_000, "unmeasured warmup requests")
	concurrency := flag.Int("concurrency", 32, "simultaneous clients")
	flag.Parse()
	if *requests <= 0 || *warmup < 0 || *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "requests/concurrency must be positive and warmup nonnegative")
		os.Exit(2)
	}
	transport := &http.Transport{MaxIdleConns: *concurrency, MaxIdleConnsPerHost: *concurrency, MaxConnsPerHost: *concurrency}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	stamp := time.Now().UnixNano()
	var failures, noFills atomic.Int64
	latencies := make([]time.Duration, *requests)
	jobs := make(chan int, *concurrency)
	var wg sync.WaitGroup
	for range *concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				// Every auction has a unique ID: the load test measures real debits,
				// not quick responses from the idempotency cache.
				body := []byte(fmt.Sprintf(`{"auction_id":"load-%d-%d","floor_micros":500,"candidates":[{"campaign_id":"atlas","bid_micros":1800},{"campaign_id":"boreal","bid_micros":1500},{"campaign_id":"cedar","bid_micros":1100}]}`, stamp, n))
				req, err := http.NewRequest(http.MethodPost, *url, bytes.NewReader(body))
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
					failures.Add(1)
					continue
				}
				data, readErr := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				elapsed := time.Since(start)
				if readErr != nil || resp.StatusCode != http.StatusOK {
					failures.Add(1)
					continue
				}
				if bytes.Contains(data, []byte(`"no_fill_reason"`)) {
					noFills.Add(1)
				}
				if n >= *warmup {
					latencies[n-*warmup] = elapsed
				}
			}
		}()
	}
	start := time.Now()
	for n := range *warmup + *requests {
		jobs <- n
	}
	close(jobs)
	wg.Wait()
	duration := time.Since(start)
	if failures.Load() != 0 || noFills.Load() != 0 {
		fmt.Fprintf(os.Stderr, "invalid load test: failures=%d no_fills=%d (check server budgets and config)\n", failures.Load(), noFills.Load())
		os.Exit(1)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p int) time.Duration {
		index := (len(latencies)*p+99)/100 - 1
		return latencies[index]
	}
	fmt.Printf("requests=%d warmup=%d concurrency=%d elapsed=%s throughput=%.0f req/s\n", *requests, *warmup, *concurrency, duration.Round(time.Millisecond), float64(*warmup+*requests)/duration.Seconds())
	fmt.Printf("HTTP latency: p50=%s p95=%s p99=%s max=%s; failures=0 no_fills=0\n", pct(50), pct(95), pct(99), latencies[len(latencies)-1])
}
