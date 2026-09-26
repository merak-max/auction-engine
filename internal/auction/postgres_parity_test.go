package auction

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestPostgresMatchesMemory(t *testing.T) {
	dsn := os.Getenv("AUCTION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable PostgreSQL")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	prefix := fmt.Sprintf("parity-%d", time.Now().UnixNano())
	cfg := Config{}
	for i := 0; i < 5; i++ {
		cfg.Campaigns = append(cfg.Campaigns, Campaign{ID: fmt.Sprintf("%s-%d", prefix, i), DailyBudgetMicros: int64(50 + i*20), PacingBurstMicros: int64(50 + i*20)})
	}
	pg, err := NewPostgresStore(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = pg.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = db.ExecContext(ctx, `DELETE FROM auction_replays WHERE auction_id LIKE $1`, prefix+"%")
		for _, c := range cfg.Campaigns {
			_, _ = db.ExecContext(ctx, `DELETE FROM auction_budget_accounts WHERE campaign_id=$1`, c.ID)
		}
	}()
	mem := setup(t, nil, cfg.Campaigns...)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 150; i++ {
		req := Request{AuctionID: fmt.Sprintf("%s-%d", prefix, i), FloorMicros: int64(rng.Intn(5)), Slots: 1 + rng.Intn(3)}
		for _, n := range rng.Perm(5)[:1+rng.Intn(5)] {
			req.Candidates = append(req.Candidates, Candidate{cfg.Campaigns[n].ID, int64(1 + rng.Intn(30))})
		}
		want, err := mem.Run(req)
		if err != nil {
			t.Fatal(err)
		}
		got, err := pg.Run(ctx, req)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("request=%+v got=%+v want=%+v err=%v", req, got, want, err)
		}
		if i%10 == 0 {
			got, err = pg.Run(ctx, req)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("replay mismatch: %+v %v", got, err)
			}
		}
	}
	for _, c := range cfg.Campaigns {
		var spent int64
		if err = db.QueryRowContext(ctx, `SELECT spent_micros FROM auction_budget_accounts WHERE campaign_id=$1`, c.ID).Scan(&spent); err != nil {
			t.Fatal(err)
		}
		if spent != mem.SpentMicros(c.ID) {
			t.Fatalf("spend mismatch: %s DB=%d memory=%d", c.ID, spent, mem.SpentMicros(c.ID))
		}
	}
	// Reinitializing another process must preserve spend and replay history.
	pg2, _ := NewPostgresStore(db, cfg)
	if err = pg2.Init(ctx); err != nil {
		t.Fatal(err)
	}
	var spentBefore int64
	id := cfg.Campaigns[0].ID
	_ = db.QueryRowContext(ctx, `SELECT spent_micros FROM auction_budget_accounts WHERE campaign_id=$1`, id).Scan(&spentBefore)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = pg2.Run(canceled, Request{AuctionID: prefix + "-canceled", FloorMicros: 1, Candidates: []Candidate{{id, 10}}}); err == nil {
		t.Fatal("canceled request succeeded")
	}
	var spentAfter int64
	_ = db.QueryRowContext(ctx, `SELECT spent_micros FROM auction_budget_accounts WHERE campaign_id=$1`, id).Scan(&spentAfter)
	if spentAfter != spentBefore {
		t.Fatal("canceled request spent budget")
	}
	// An old UTC-day account is rolled over transactionally.
	_, err = db.ExecContext(ctx, `UPDATE auction_budget_accounts SET utc_day=(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date-1,spent_micros=daily_budget_micros WHERE campaign_id=$1`, id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pg2.Run(ctx, Request{AuctionID: prefix + "-rollover", FloorMicros: 1, Candidates: []Candidate{{id, 10}}})
	if err != nil || got.WinnerID != id {
		t.Fatalf("rollover: %+v %v", got, err)
	}
}
