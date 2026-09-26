package auction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Run with AUCTION_TEST_DATABASE_URL pointed at a disposable PostgreSQL DB.
func TestPostgresConcurrentAccountsAndReplay(t *testing.T) {
	dsn := os.Getenv("AUCTION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set AUCTION_TEST_DATABASE_URL to test shared PostgreSQL reservations")
	}
	ctx := context.Background()
	dbA, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	dbB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()
	dbA.SetMaxOpenConns(8)
	dbB.SetMaxOpenConns(8)
	id := fmt.Sprintf("test-%d", time.Now().UnixNano())
	cfg := Config{Campaigns: []Campaign{{ID: id, DailyBudgetMicros: 100, PacingBurstMicros: 100}}}
	a, err := NewPostgresStore(dbA, cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewPostgresStore(dbB, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Init(ctx); err != nil {
		t.Fatal(err)
	}
	batchA, batchB := NewBatcher(a), NewBatcher(b)
	defer batchA.Close()
	defer batchB.Close()
	defer func() {
		_, _ = dbA.ExecContext(ctx, `DELETE FROM auction_replays WHERE auction_id LIKE $1`, id+"%")
		_, _ = dbA.ExecContext(ctx, `DELETE FROM auction_budget_accounts WHERE campaign_id=$1`, id)
	}()
	request := Request{AuctionID: id + "-replay", FloorMicros: 5, Candidates: []Candidate{{CampaignID: id, BidMicros: 10}}}
	var replayWG sync.WaitGroup
	for _, store := range []*Batcher{batchA, batchB} {
		replayWG.Add(1)
		go func() {
			defer replayWG.Done()
			got, err := store.Run(ctx, request)
			if err != nil || got.WinnerID != id || got.ClearingPriceMicros != 5 {
				t.Errorf("cross-process replay: %+v %v", got, err)
			}
		}()
	}
	replayWG.Wait()
	if _, err := b.Run(ctx, Request{AuctionID: request.AuctionID, FloorMicros: 8, Candidates: request.Candidates}); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("conflicting replay should fail: %v", err)
	}
	var wg sync.WaitGroup
	var wins atomic.Int64
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store := batchA
			if i%2 == 0 {
				store = batchB
			}
			got, err := store.Run(ctx, Request{AuctionID: fmt.Sprintf("%s-%d", id, i), FloorMicros: 5, Candidates: request.Candidates})
			if err != nil {
				t.Errorf("concurrent auction: %v", err)
			} else if got.WinnerID != "" {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	var spent int64
	if err := dbA.QueryRowContext(ctx, `SELECT spent_micros FROM auction_budget_accounts WHERE campaign_id=$1`, id).Scan(&spent); err != nil {
		t.Fatal(err)
	}
	if spent != 100 || wins.Load() != 19 {
		t.Fatalf("shared ledger: spent=%d additional_wins=%d; want 100 and 19", spent, wins.Load())
	}
	// One invalid auction must not roll back neighboring requests in a commit batch.
	reqs := []Request{
		{AuctionID: id + "-valid1", FloorMicros: 0, Candidates: []Candidate{{id, 10}}},
		{AuctionID: id + "-invalid", FloorMicros: 0, Candidates: []Candidate{{"not-a-campaign", 10}}},
		{AuctionID: id + "-valid2", FloorMicros: 0, Candidates: []Candidate{{id, 10}}},
	}
	outcomes, err := a.runBatch(ctx, reqs)
	if err != nil || len(outcomes) != 3 {
		t.Fatalf("batch failed: %+v %v", outcomes, err)
	}
	if outcomes[0].Result.WinnerID != id || outcomes[1].Code != "AU001" || outcomes[2].Result.WinnerID != id {
		t.Fatalf("batch isolation: %+v", outcomes)
	}
}
