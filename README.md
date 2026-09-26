# Auction Engine

A standalone Go service for a **single-slot, impression-priced
second-price auction**. Each request carries candidate campaigns and bids;
the service ranks eligible bids, charges the winner the greater of the floor or
the next eligible bid, and atomically reserves that charge against a paced daily
budget. Money is integer **micro-USD per impression** (`1,000,000` micros = `$1`).
This is a focused serving project with an explicit handoff for the AlgoChat
recommender; it does not alter or deploy that production service.

## Architecture

```text
POST /v1/auctions  OR  recommender JSON -> POST /v1/auction-recommendations
       |
       v
bearer auth + strict JSON + candidate validation
       |
       v
memory mutex OR PostgreSQL transaction (shared budget rows + durable replay)
       |
       v
UTC pacing -> rank -> second price / floor -> reserve + replay result
       |
       v
JSON response + low-cardinality Prometheus metrics
```

In memory mode, a mutex covers checking allowance, ranking, pricing and
charging. In PostgreSQL mode, a transaction locks participating budget rows
in stable ID order and commits the reservation with the replay entry. Both
use `min(budget, burst + budget * elapsed_UTC_seconds/86400)` as the paced
allowance. If the
highest bidder cannot afford the clearing price, it is removed and the next
bidder competes against the bids below it. Equal bids break by campaign ID.
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

Campaigns, daily budgets, pacing bursts and optional bridge bids are in
`config/campaigns.json` (not
reloaded at runtime). If you retry the *identical* request with the same
`auction_id` during its replay window, the result is returned without another
charge. Price `0` is possible when a floor of `0` meets a lone bidder; use a
positive floor if free impressions are not desired.

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
auction bids. A consumer can call it first, then send its *filled* JSON response
to `POST /v1/auction-recommendations` with a stable `auction_id` and floor. The
bridge maps each `ad.campaign_id` to `bid_micros` in this server's config,
auctions only those preselected recommendations, and returns the winning
original recommendation alongside its clearing price. It does not auction the
recommender's entire inventory, change its Thompson sampling, or make a live
production call to that service.

```sh
curl -sS http://127.0.0.1:8788/v1/auction-recommendations \
  -H "Authorization: Bearer $AUCTION_TOKEN" -H 'Content-Type: application/json' \
  -d '{"auction_id":"handoff-1","floor_micros":500,"recommendation_response":{"schema_version":1,"sampled":true,"recommendations":[{"kind":"website","ad":{"campaign_id":"atlas","title":"Atlas"}},{"kind":"website","ad":{"campaign_id":"boreal","title":"Boreal"}}]}}'
```

Configure **every** served recommender campaign ID and bid before using this
route (the sample IDs are illustrative). Keep the candidate list identical on
retries; a changed list under the same auction ID is a conflict. The caller
must decide when to render and count a delivery—this bridge is not impression
verification or production tracking.

## Measured load test

Run against the **benchmark config**: its large budgets ensure the run
continues exercising filled auctions rather than measuring the no-fill path.

```sh
go run ./cmd/auction-server -config config/benchmark.json
# in another terminal, from this directory:
go run ./cmd/loadtest -requests 100000 -warmup 2000 -concurrency 32
```

Three in-memory local runs on **2026-09-26**, Linux/aarch64, **8 vCPUs**, Go **1.27.1**,
client and server on the **same host via loopback**:

| Run | Measured requests | Concurrent clients | Throughput¹ | p50 | p95 | p99 | Max | Errors / no-fills |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 100,000 | 32 | 78,228 req/s | 0.235 ms | 1.259 ms | **1.993 ms** | 8.680 ms | 0 / 0 |
| 2 | 100,000 | 32 | 78,862 req/s | 0.234 ms | 1.205 ms | **1.837 ms** | 13.387 ms | 0 / 0 |
| 3 (with auth/bridge code) | 100,000 | 32 | 76,362 req/s | 0.237 ms | 1.261 ms | **1.985 ms** | 7.133 ms | 0 / 0 |

¹ Throughput divides 102,000 requests (including 2,000 warmups) by the full
run time (1.304, 1.293 and 1.336 seconds respectively); latency percentiles include only the 100,000 measured
requests, from start of the HTTP call through reading the response. Requests
carry unique auction IDs and cause real budget reservations. The client and
server share this machine; these numbers **do not** include network hops,
other processes, persistence or a multi-region deployment. The design target
is p99 < 10 ms under this stated local workload; the measurement is not a
universal latency guarantee. Rerun the command on your hardware and report
hardware, concurrency, fill rate and percentile together.

PostgreSQL **does not meet** that p99 target in this local setup. A separate
PostgreSQL 17 Alpine container on the same host, with the current server,
5,000 measured requests, 300 warmups, eight clients and the benchmark config,
returned **745 req/s**, p50 **4.089 ms**, p95 **12.901 ms**, p99 **97.862 ms**,
max **1.639 s**, and zero failures or no-fills. An earlier 10,000-request run
at 16 clients with a 3-second client timeout had **16 failures**; its
incomplete latency sample was discarded. The service serializes competing
reservations on the same campaign rows to protect budgets; do not conflate
the fast in-memory benchmark with durable-mode performance.

## Boundaries and next milestones

- **Implemented:** second-price pricing, UTC-day pacing, atomic per-process or
  PostgreSQL-shared spend, deterministic ties, same-day replay, recommender
  handoff, optional bearer auth, readiness, metrics, graceful shutdown,
  validation, race tests, PostgreSQL contention test and HTTP load tool.
- **Next for a deployment:** bring durable-mode p99 within the desired SLO,
  load-test real network topology, add targeting and integrate the handoff in
  an opted-in consuming backend. Decide on DB backups, replay retention,
  monitoring and impression verification before billing real advertisers.

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
