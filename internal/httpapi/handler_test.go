package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/merak-max/auction-engine/internal/auction"
)

func TestHandler(t *testing.T) {
	e, err := auction.New(auction.Config{Campaigns: []auction.Campaign{{ID: "a", DailyBudgetMicros: 100, PacingBurstMicros: 100}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := Handler(e)
	for _, tc := range []struct {
		method, path, body string
		want               int
	}{
		{"GET", "/healthz", "", 200},
		{"POST", "/v1/auctions", `{"auction_id":"1","floor_micros":2,"candidates":[{"campaign_id":"a","bid_micros":3}]}`, 200},
		{"POST", "/v1/auctions", `{"auction_id":"2","floor_micros":2,"candidates":[{"campaign_id":"a","bid_micros":3}],"typo":true}`, 400},
		{"POST", "/v1/auctions", `{} {}`, 400},
		{"POST", "/v1/auctions", strings.Repeat(" ", 65<<10), 400},
		{"GET", "/v1/auctions", "", 405},
	} {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, req)
		if resp.Code != tc.want {
			t.Errorf("%s %s: status=%d want=%d", tc.method, tc.path, resp.Code, tc.want)
		}
		if tc.want == http.StatusOK && tc.method == "POST" && !strings.Contains(resp.Body.String(), `"clearing_price_micros":2`) {
			t.Errorf("unexpected price: %s", resp.Body.String())
		}
	}
}

func TestAuthMetricsAndRecommendationBridge(t *testing.T) {
	e, err := auction.New(auction.Config{Campaigns: []auction.Campaign{
		{ID: "atlas", DailyBudgetMicros: 100, PacingBurstMicros: 100},
		{ID: "boreal", DailyBudgetMicros: 100, PacingBurstMicros: 100},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(Options{
		Token: "test-token",
		Bids:  map[string]int64{"atlas": 10, "boreal": 7},
		Run:   func(_ context.Context, req auction.Request) (auction.Result, error) { return e.Run(req) },
	})
	body := `{"auction_id":"bridge-1","floor_micros":3,"recommendation_response":{"schema_version":1,"sampled":true,"recommendations":[{"kind":"website","ad":{"campaign_id":"atlas","title":"A"}},{"kind":"website","ad":{"campaign_id":"boreal","title":"B"}}]}}`
	request := func(method, path, payload, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(payload))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, req)
		return resp
	}
	if got := request("POST", "/v1/auction-recommendations", body, ""); got.Code != 401 {
		t.Fatalf("unauthenticated bridge: %d", got.Code)
	}
	got := request("POST", "/v1/auction-recommendations", body, "test-token")
	if got.Code != 200 {
		t.Fatalf("bridge: status=%d body=%s", got.Code, got.Body.String())
	}
	var response struct {
		Auction        auction.Result  `json:"auction"`
		Recommendation json.RawMessage `json:"recommendation"`
	}
	if err := json.Unmarshal(got.Body.Bytes(), &response); err != nil || response.Auction.WinnerID != "atlas" || response.Auction.ClearingPriceMicros != 7 || !strings.Contains(string(response.Recommendation), `"title":"A"`) {
		t.Fatalf("bridge result: %+v %v", response, err)
	}
	if got := request("GET", "/metrics", "", ""); got.Code != 401 {
		t.Fatalf("unauthenticated metrics: %d", got.Code)
	}
	metrics := request("GET", "/metrics", "", "test-token")
	if metrics.Code != 200 || !strings.Contains(metrics.Body.String(), `auction_requests_total{outcome="filled"} 1`) || !strings.Contains(metrics.Body.String(), "auction_http_duration_seconds_bucket") {
		t.Fatalf("metrics: %d %s", metrics.Code, metrics.Body.String())
	}
}
