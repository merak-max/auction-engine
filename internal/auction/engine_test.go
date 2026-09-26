package auction

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setup(t *testing.T, clock func() time.Time, campaigns ...Campaign) *Engine {
	t.Helper()
	e, err := New(Config{Campaigns: campaigns}, clock)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func fixedClock() time.Time { return time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC) }

func TestSecondPriceFloorTieAndReplay(t *testing.T) {
	e := setup(t, fixedClock,
		Campaign{"a", 100, 100, 0}, Campaign{"b", 100, 100, 0}, Campaign{"c", 100, 100, 0})
	req := Request{"one", 3, []Candidate{{"b", 10}, {"a", 10}, {"c", 6}}, 0}
	got, err := e.Run(req)
	if err != nil || got.WinnerID != "a" || got.ClearingPriceMicros != 10 || e.SpentMicros("a") != 10 {
		t.Fatalf("tie/second price: result=%+v err=%v", got, err)
	}
	replay, err := e.Run(req)
	if err != nil || !reflect.DeepEqual(replay, got) || e.SpentMicros("a") != 10 {
		t.Fatalf("replay charged twice: %+v, %v", replay, err)
	}
	if _, err := e.Run(Request{"one", 5, []Candidate{{"a", 10}}, 0}); err == nil {
		t.Fatal("conflicting replay accepted")
	}
	got, err = e.Run(Request{"two", 3, []Candidate{{"c", 6}}, 0})
	if err != nil || got.WinnerID != "c" || got.ClearingPriceMicros != 3 {
		t.Fatalf("single bidder should pay floor: %+v %v", got, err)
	}
}

func TestUnaffordableWinnerReauctioned(t *testing.T) {
	e := setup(t, fixedClock, Campaign{"a", 5, 5, 0}, Campaign{"b", 20, 20, 0}, Campaign{"c", 20, 20, 0})
	got, err := e.Run(Request{"one", 1, []Candidate{{"a", 10}, {"b", 8}, {"c", 4}}, 0})
	if err != nil || got.WinnerID != "b" || got.ClearingPriceMicros != 4 || e.SpentMicros("a") != 0 {
		t.Fatalf("winner should be reauctioned: %+v %v", got, err)
	}
}

func TestPacingAndUTCRollover(t *testing.T) {
	now := fixedClock()
	e := setup(t, func() time.Time { return now }, Campaign{"a", 100, 10, 0})
	for i := range 2 {
		got, err := e.Run(Request{AuctionID: string(rune('a' + i)), FloorMicros: 5, Candidates: []Candidate{{"a", 6}}})
		if err != nil || got.WinnerID != "a" {
			t.Fatalf("expected win: %+v %v", got, err)
		}
	}
	got, err := e.Run(Request{"c", 5, []Candidate{{"a", 6}}, 0})
	if err != nil || got.WinnerID != "" || e.SpentMicros("a") != 10 {
		t.Fatalf("pacing should pause delivery: %+v %v", got, err)
	}
	now = now.Add(12 * time.Hour)
	got, err = e.Run(Request{"d", 5, []Candidate{{"a", 6}}, 0})
	if err != nil || got.WinnerID != "a" {
		t.Fatalf("pacing should resume: %+v %v", got, err)
	}
	now = now.Add(12 * time.Hour)
	got, err = e.Run(Request{"e", 5, []Candidate{{"a", 6}}, 0})
	if err != nil || got.WinnerID != "a" || e.SpentMicros("a") != 5 {
		t.Fatalf("UTC day should reset ledger: %+v %v", got, err)
	}
	now = now.Add(-24 * time.Hour)
	if _, err := e.Run(Request{"f", 5, []Candidate{{"a", 6}}, 0}); err != ErrClockRegression {
		t.Fatalf("clock regression accepted: %v", err)
	}
}

func TestConcurrentBudgetNeverOverspends(t *testing.T) {
	e := setup(t, fixedClock, Campaign{"a", 1000, 1000, 0}, Campaign{"b", 1000, 1000, 0})
	var wg sync.WaitGroup
	var wins atomic.Int64
	for i := range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := time.Unix(0, int64(i)).Format(time.RFC3339Nano)
			got, err := e.Run(Request{id, 1, []Candidate{{"a", 10}, {"b", 5}}, 0})
			if err != nil {
				t.Errorf("auction: %v", err)
			} else if got.WinnerID != "" {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if e.SpentMicros("a") > 1000 || e.SpentMicros("b") > 1000 || e.SpentMicros("a")+e.SpentMicros("b") > 2000 || wins.Load() == 0 {
		t.Fatalf("overspend: a=%d b=%d wins=%d", e.SpentMicros("a"), e.SpentMicros("b"), wins.Load())
	}
}

func TestInvalidInputs(t *testing.T) {
	if _, err := New(Config{Campaigns: []Campaign{{"a", 10, 11, 0}}}, nil); err == nil {
		t.Fatal("invalid burst accepted")
	}
	e := setup(t, fixedClock, Campaign{"a", 10, 10, 0})
	for _, req := range []Request{
		{"", 1, []Candidate{{"a", 1}}, 0},
		{"x", -1, []Candidate{{"a", 1}}, 0},
		{"x", 1, []Candidate{{"a", 1}, {"a", 2}}, 0},
		{"x", 1, []Candidate{{"missing", 1}}, 0},
	} {
		if _, err := e.Run(req); err == nil {
			t.Fatalf("invalid request accepted: %+v", req)
		}
	}
}
