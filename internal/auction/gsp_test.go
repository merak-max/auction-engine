package auction

import (
	"reflect"
	"testing"
)

func TestGSPAtomicBatchAndRepricing(t *testing.T) {
	e := setup(t, fixedClock, Campaign{"a", 100, 100, 0}, Campaign{"b", 3, 3, 0}, Campaign{"c", 100, 100, 0}, Campaign{"d", 100, 100, 0})
	req := Request{AuctionID: "gsp", FloorMicros: 1, Slots: 3, Candidates: []Candidate{{"a", 10}, {"b", 8}, {"c", 6}, {"d", 2}}}
	result, err := e.Run(req)
	want := []Winner{{"a", 10, 6}, {"c", 6, 2}, {"d", 2, 1}}
	if err != nil || !reflect.DeepEqual(result.Winners, want) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if e.SpentMicros("b") != 0 {
		t.Fatal("excluded bidder charged")
	}
	result.Winners[0].ClearingPriceMicros = 999
	replay, err := e.Run(req)
	if err != nil || !reflect.DeepEqual(replay.Winners, want) || e.SpentMicros("a") != 6 {
		t.Fatal("replay changed or charged again")
	}
	before := e.SpentMicros("a")
	nofill, err := e.Run(Request{AuctionID: "short", FloorMicros: 1, Slots: 3, Candidates: []Candidate{{"a", 10}, {"c", 6}}})
	if err != nil || nofill.WinnerID != "" || e.SpentMicros("a") != before {
		t.Fatal("partial batch charged")
	}
}
