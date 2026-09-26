# Auction Engine

A standalone Go service for **single-slot second-price and multi-slot GSP
auctions** with paced daily budgets. Single-slot requests charge the winner
the greater of the floor or the next eligible bid. Two- and three-slot
requests price each slot against the next eligible bid (last slot pays the
floor); either all slots fill and are charged atomically, or none are. Direct
requests supply campaign bids; the recommender bridge uses configured bids.
Every clearing price is reserved against its campaign's daily budget. Money
is integer **micro-USD per impression** (`1,000,000` micros = `$1`).
The AlgoChat backend has an opt-in three-slot handoff, preserving its existing
three-card contract. The recommender's classifier and Thompson sampling remain
unchanged. This repo is independently runnable with synthetic campaign data.

## Architecture

```text
direct bids -> /v1/auctions    recommender JSON -> /v1/auction-recommendations
                     \          /
                auth + validation
                     |
          +----------+-----------+
          |                      |
     in-memory mutex       bounded commit queue
          |                      |
          |              PostgreSQL atomic batch
          +----------+-----------+
                     |
      UTC pacing -> rank -> second price (1 slot) / GSP (2-3 slots)
                     |
           reserve budget + replay auction ID
                     |
              JSON + Prometheus metrics
```

Pacing, ranking, pricing, reservation and replay run *inside* the chosen
mutex or database transaction, not after it commits.

In memory mode, a mutex covers checking allowance, ranking, pricing and
charging. PostgreSQL mode drains up to 32 already-queued requests into one
database call, takes ordered logical campaign locks and row locks, and commits
reservations together with replay entries. There is no artificial collection
delay. Each successful response waits for that synchronous transaction to
commit; `fsync`, `synchronous_commit` and `full_page_writes` remain enabled.
The pending queue is capped at 1,024 auctions; overload fails with HTTP 503.
Canonical lock ordering makes overlapping batches from multiple instances safe.
Client errors are isolated within the commit batch, not charged or silently
converted to fills. Both pricing implementations
use `min(budget, burst + budget * elapsed_UTC_seconds/86400)` as the paced
allowance. If a selected bidder cannot afford its clearing price, it is
removed and all slot prices are recomputed. Equal bids break by campaign ID.
A lone eligible bidder pays the floor. A no-fill reserves nothing. At UTC
midnight the spend ledger starts a new day. The memory replay cache resets;
PostgreSQL stores replay entries by UTC day. A clock regression to a previous
UTC day fails closed. Burst allows immediate delivery at midnight;
the remainder unlocks linearly over the day. Pacing is deterministic headroom,
not probabilistic throttling or a guarantee of even traffic.

## Run locally

Requires Go 1.25+. From this directory:

```sh
go test ./...
go test -race ./...
go run ./cmd/auction-server -config config/campaigns.json # in-memory demo
```

Set `AUCTION_TEST_DATABASE_URL` to a **disposable** PostgreSQL database to
include shared-ledger, batching, replay, rollover and randomized Go/SQL parity
tests. CI runs these with PostgreSQL 17, plus race detection and `go vet`.

The server listens on `127.0.0.1:8788` by default. In another terminal:

```sh
curl -sS http://127.0.0.1:8788/v1/auctions \
  -H 'Content-Type: application/json' \
  -d '{"auction_id":"example-1","floor_micros":500,"candidates":[{"campaign_id":"atlas","bid_micros":1800},{"campaign_id":"boreal","bid_micros":1500},{"campaign_id":"cedar","bid_micros":1100}]}'
```

Expected on a fresh budget: `atlas` wins and pays `1500` micros, not its
`1800`-micro bid. The response contains `auction_id`, `winner_id`,
`winning_bid_micros`, and `clearing_price_micros`. No-fill responses instead
contain `no_fill_reason`. `GET /healthz` responds with `ok`. Unknown campaign
IDs, duplicate candidates, conflicting reuse of an auction ID, unknown JSON
fields and malformed requests receive HTTP 400. The body limit is 64 KiB and
candidate limit is 128 per auction.

Omit `slots` (or set it to 1) for the original single-slot contract. Set
`slots: 3` for a three-winner batch. Multi-slot results additionally contain
`winners`, ordered by slot, with each winner's bid and clearing price. All
requested slots must fill; otherwise **no campaign is charged**. With bids
1800, 1500, 1100 and floor 500, three-slot prices are 1500, 1100, 500.
Slots are unweighted; there is no CTR or quality-score adjustment.

Campaigns, daily budgets, pacing bursts and optional bridge bids are in
`config/campaigns.json` (not reloaded at runtime). If you retry the *identical*
request with the same `auction_id` during its replay window, the result is
returned without another charge. Price `0` is possible when a floor of `0`
meets a lone bidder; use a positive floor if free impressions are not desired.

### Shared PostgreSQL ledger

Set `AUCTION_DATABASE_URL` to a PostgreSQL DSN before starting the server.
The server creates two namespaced tables, seeds missing campaign budgets and
checks that configured budgets match existing rows. It **never overwrites** an
existing policy silently. PostgreSQL mode keeps spend and auction-ID replay
across restarts and across servers sharing the same database. A daily rollover
is applied inside the first transaction for each campaign that day. For local
testing with a disposable database:

```sh
docker run --rm -d --name auction-demo-postgres \
  -e POSTGRES_PASSWORD=demo_only -e POSTGRES_DB=auction \
  -p 127.0.0.1:5433:5432 postgres:17-alpine
export AUCTION_DATABASE_URL='postgres://postgres:demo_only@127.0.0.1:5433/auction?sslmode=disable'
export AUCTION_TOKEN='replace-with-a-long-random-token'
go run ./cmd/auction-server -config config/campaigns.json
# Stop only your disposable test container when finished: docker stop auction-demo-postgres
```

`GET /readyz` checks the database (or returns OK for memory mode).
`GET /metrics` exports request outcomes and HTTP latency histogram buckets;
it requires the bearer token when `AUCTION_TOKEN` is set. Both POST endpoints
also require the bearer token when set. Without a token, the CLI refuses to
bind outside loopback. For network deployments use TLS at a trusted ingress,
rotate tokens, and do not expose the database DSN. Authenticated callers of
`/v1/auctions` supply bids and must be trusted; the recommender bridge below
uses server-configured bids instead.

### Recommender handoff

The existing recommender returns three Thompson-sampled recommendations, not
auction bids. A consumer calls it first, then sends its *filled* JSON response
to `POST /v1/auction-recommendations` with a stable `auction_id` and floor. The
bridge maps each `ad.campaign_id` to `bid_micros` in this server's config,
auctions only those preselected recommendations, and returns original creatives
in winning order. `slots: 3` preserves the complete three-card batch. The
single-slot `recommendation` field remains available for existing callers;
multi-slot callers use `recommendations` and `auction.winners`. It does not
auction the recommender's entire inventory or change its Thompson sampling.

```sh
curl -sS http://127.0.0.1:8788/v1/auction-recommendations \
  -H "Authorization: Bearer $AUCTION_TOKEN" -H 'Content-Type: application/json' \
  -d '{"auction_id":"handoff-1","slots":3,"floor_micros":500,"recommendation_response":{"schema_version":1,"sampled":true,"recommendations":[{"kind":"website","ad":{"campaign_id":"atlas","title":"Atlas"}},{"kind":"website","ad":{"campaign_id":"boreal","title":"Boreal"}},{"kind":"website","ad":{"campaign_id":"cedar","title":"Cedar"}}]}}'
```

Configure **every** served recommender campaign ID and bid before using this
route (the sample IDs are illustrative). Keep the candidate list identical on
retries; a changed list under the same auction ID is a conflict. The caller
must decide when to render and count a delivery—this bridge is not impression
verification or production tracking.

The consuming backend's opt-in settings are `ADS_AUCTION_URL` (base URL),
`ADS_AUCTION_TOKEN`, `ADS_AUCTION_FLOOR_MICROS`, and optional
`ADS_AUCTION_TIMEOUT_MS` (250 by default, maximum 1000). It keeps a stable
per-chat auction ID, verifies the complete winner list against the original
creatives and fails closed on errors—never falling back to unbudgeted ads.
With no URL configured, its existing behavior is unchanged. Real activation
requires reviewed campaign bids and budgets; the synthetic demo economics are
not applied to production advertisers.

## Measured load test

Run against the **benchmark config**: its large budgets ensure the run
continues exercising filled auctions rather than measuring the no-fill path.

```sh
go run ./cmd/auction-server -config config/benchmark.json
# in another terminal, from this directory:
go run ./cmd/loadtest -requests 100000 -warmup 2000 -concurrency 32
```

Historical in-memory, single-slot local runs on **2026-09-26**, Linux/aarch64,
**8 vCPUs**, Go **1.27.1**,
client and server on the **same host via loopback**:

| Run | Measured requests | Concurrent clients | Throughput¹ | p50 | p95 | p99 | Max | Errors / no-fills |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 100,000 | 32 | 78,228 req/s | 0.235 ms | 1.259 ms | **1.993 ms** | 8.680 ms | 0 / 0 |
| 2 | 100,000 | 32 | 78,862 req/s | 0.234 ms | 1.205 ms | **1.837 ms** | 13.387 ms | 0 / 0 |
| 3 (with auth/bridge code) | 100,000 | 32 | 76,362 req/s | 0.237 ms | 1.261 ms | **1.985 ms** | 7.133 ms | 0 / 0 |

¹ This earlier harness divided 102,000 requests (including 2,000 warmups) by
the full run time (1.304, 1.293 and 1.336 seconds respectively); latency
percentiles include only the 100,000 measured requests, from start of the
HTTP call through reading the response. Unlike the durable reports below,
these historical runs have no checked-in raw JSON and their throughput
method is not directly comparable to the current load generator. Requests
carry unique auction IDs and cause real budget reservations. The client and
server share this machine; these numbers **do not** include network hops,
other processes, persistence or a multi-region deployment. The design target
is p99 < 10 ms under this stated local workload; the measurement is not a
universal latency guarantee. Rerun the command on your hardware and report
hardware, concurrency, fill rate and percentile together.

### Durable-mode GSP SLO results

The current PostgreSQL path **meets p99 < 10 ms at eight clients** on this
same host with three-slot GSP, synchronous disk-backed commits, a valid bearer
token, and every request competing for the same three campaigns:

| API instances sharing one ledger | Measured requests | Clients total | Throughput | p50 | p95 | p99 | Errors / no-fills |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 50,000 | 8 | 1,757 req/s | 4.502 ms | 5.081 ms | **5.509 ms** | 0 / 0 |
| 2 | 50,000 | 8 | 1,399 req/s | 5.207 ms | 7.004 ms | **7.419 ms** | 0 / 0 |

These newer runs exclude 2,000 warmups from **both** throughput and latency.
Raw machine-generated reports and exact reproduction instructions are in
[benchmarks](benchmarks/README.md). The load generator can enforce the target
with `-max-p99 10ms`; errors and no-fills also fail the run. Results describe a
closed-loop workload and this hardware, not an unlimited-rate, WAN or p100 SLO.
Two instances demonstrate shared-ledger correctness, not linear scaling of
one hot campaign set. Higher concurrency needs a separate capacity measurement.

For comparison, the original unbatched client-side transaction implementation
measured 97.862 ms p99 at eight clients; a 16-client trial also timed out 16
requests. Moving pricing into one database call alone was insufficient.
Ordered logical campaign locks stabilized contention, and draining queued
auctions into synchronous commit batches amortized disk flushes. No durability
settings were disabled to reach the new numbers.

## Boundaries and next milestones

- **Implemented:** single-slot second price and atomic multi-slot GSP, UTC-day
  pacing, atomic memory or shared durable spend, bounded commit batching,
  deterministic ties, same-day replay, opt-in three-card backend integration,
  bearer auth, readiness, metrics, graceful shutdown, validation, race tests,
  PostgreSQL differential/contention tests and a reproducible HTTP SLO gate.
- **Deployment responsibilities:** provision reviewed bids/budgets and TLS,
  enable the backend settings, size capacity for real arrival patterns, and
  configure backups, replay retention, monitoring and reservation reconciliation
  before billing real advertisers. Demo data is never promoted automatically.

Do **not** run multiple memory-mode instances against the same campaigns: each
has its own spend ledger and a restart resets spend. Memory replay retains only
the latest 100,000 IDs per UTC day; after eviction or restart, a retried ID
can charge again. PostgreSQL mode shares a ledger and retains replay entries
until explicitly pruned; plan retention against your audit obligations.
Spending is booked at auction win (CPM/impression), not
on a delayed click (CPC), and there is no impression verification. These are
explicit prototype constraints, not production billing guarantees.

## License

MIT; see [LICENSE](LICENSE).
