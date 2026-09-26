package auction

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// PostgresStore shares budget reservations and auction replay across processes.
// Call Init at startup; all servers for one campaign must use the same database.
type PostgresStore struct {
	db  *sql.DB
	cfg Config
}

func NewPostgresStore(db *sql.DB, cfg Config) (*PostgresStore, error) {
	if db == nil {
		return nil, errors.New("database is required")
	}
	if _, err := New(cfg, nil); err != nil {
		return nil, err
	}
	return &PostgresStore{db: db, cfg: cfg}, nil
}

func (s *PostgresStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *PostgresStore) Init(ctx context.Context) error {
	const accounts = `CREATE TABLE IF NOT EXISTS auction_budget_accounts (
		campaign_id TEXT PRIMARY KEY,
		daily_budget_micros BIGINT NOT NULL CHECK (daily_budget_micros BETWEEN 1 AND 1000000000000),
		pacing_burst_micros BIGINT NOT NULL CHECK (pacing_burst_micros BETWEEN 0 AND daily_budget_micros),
		spent_micros BIGINT NOT NULL DEFAULT 0 CHECK (spent_micros >= 0 AND spent_micros <= daily_budget_micros),
		utc_day DATE NOT NULL DEFAULT ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date)
	)`
	const replays = `CREATE TABLE IF NOT EXISTS auction_replays (
		utc_day DATE NOT NULL, auction_id TEXT NOT NULL,
		request_json JSONB NOT NULL, result_json JSONB NOT NULL,
		PRIMARY KEY (utc_day, auction_id)
	)`
	for _, statement := range []string{accounts, replays} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create auction tables: %w", err)
		}
	}
	for _, c := range s.cfg.Campaigns {
		_, err := s.db.ExecContext(ctx, `INSERT INTO auction_budget_accounts
			(campaign_id, daily_budget_micros, pacing_burst_micros) VALUES ($1, $2, $3)
			ON CONFLICT (campaign_id) DO NOTHING`, c.ID, c.DailyBudgetMicros, c.PacingBurstMicros)
		if err != nil {
			return fmt.Errorf("seed campaign %q: %w", c.ID, err)
		}
		var budget, burst int64
		if err := s.db.QueryRowContext(ctx, `SELECT daily_budget_micros, pacing_burst_micros
			FROM auction_budget_accounts WHERE campaign_id=$1`, c.ID).Scan(&budget, &burst); err != nil {
			return err
		}
		if budget != c.DailyBudgetMicros || burst != c.PacingBurstMicros {
			return fmt.Errorf("configured policy differs from ledger for %q; migrate budgets explicitly", c.ID)
		}
	}
	return nil
}

func (s *PostgresStore) Run(ctx context.Context, req Request) (Result, error) {
	if err := validateRequest(req); err != nil {
		return Result{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, fmt.Errorf("begin auction: %w", err)
	}
	defer tx.Rollback()
	var now time.Time
	if err := tx.QueryRowContext(ctx, `SELECT transaction_timestamp()`).Scan(&now); err != nil {
		return Result{}, fmt.Errorf("read database clock: %w", err)
	}
	now = now.UTC()
	day := now.Format("2006-01-02")
	// Serialize duplicate IDs across processes before touching any budgets.
	// Hash collisions only cause extra waiting; the actual identity is checked
	// against the composite key and original request in auction_replays.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, day+":"+req.AuctionID); err != nil {
		return Result{}, fmt.Errorf("lock auction ID: %w", err)
	}
	var original, encodedResult []byte
	err = tx.QueryRowContext(ctx, `SELECT request_json, result_json FROM auction_replays
		WHERE utc_day=$1 AND auction_id=$2`, day, req.AuctionID).Scan(&original, &encodedResult)
	if err == nil {
		var prior Request
		var result Result
		if err := json.Unmarshal(original, &prior); err != nil {
			return Result{}, err
		}
		if !sameRequest(prior, req) {
			return Result{}, fmt.Errorf("%w: auction_id already used for a different request", ErrInvalidRequest)
		}
		if err := json.Unmarshal(encodedResult, &result); err != nil {
			return Result{}, err
		}
		if err := tx.Commit(); err != nil {
			return Result{}, err
		}
		return result, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Result{}, fmt.Errorf("check auction replay: %w", err)
	}

	// Lock every participating account in a stable order; the price is based
	// on one consistent set of budgets. Locks are held through the debit and
	// replay write, so another process cannot overspend or see a partial win.
	ids := make([]string, 0, len(req.Candidates))
	for _, c := range req.Candidates {
		ids = append(ids, c.CampaignID)
	}
	sort.Strings(ids)
	loaded := Config{Campaigns: make([]Campaign, 0, len(ids))}
	spentByID := make(map[string]int64, len(ids))
	rows, err := tx.QueryContext(ctx, `SELECT campaign_id, daily_budget_micros, pacing_burst_micros,
		spent_micros, utc_day::text FROM auction_budget_accounts
		WHERE campaign_id=ANY($1::text[]) ORDER BY campaign_id FOR UPDATE`, ids)
	if err != nil {
		return Result{}, fmt.Errorf("lock auction budgets: %w", err)
	}
	type rollover struct{ id string }
	var resets []rollover
	for rows.Next() {
		var c Campaign
		var spent int64
		var accountDay string
		if err := rows.Scan(&c.ID, &c.DailyBudgetMicros, &c.PacingBurstMicros, &spent, &accountDay); err != nil {
			rows.Close()
			return Result{}, fmt.Errorf("read locked budget: %w", err)
		}
		if accountDay > day {
			rows.Close()
			return Result{}, ErrClockRegression
		}
		if accountDay < day {
			spent = 0
			resets = append(resets, rollover{c.ID})
		}
		loaded.Campaigns = append(loaded.Campaigns, c)
		spentByID[c.ID] = spent
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return Result{}, fmt.Errorf("scan budgets: %w", err)
	}
	rows.Close()
	if len(loaded.Campaigns) != len(ids) {
		for _, id := range ids {
			if _, ok := spentByID[id]; !ok {
				return Result{}, fmt.Errorf("%w: unknown campaign_id %q", ErrInvalidRequest, id)
			}
		}
	}
	for _, reset := range resets {
		if _, err := tx.ExecContext(ctx, `UPDATE auction_budget_accounts
			SET utc_day=$2, spent_micros=0 WHERE campaign_id=$1`, reset.id, day); err != nil {
			return Result{}, fmt.Errorf("reset UTC budget: %w", err)
		}
	}
	engine, err := New(loaded, func() time.Time { return now })
	if err != nil {
		return Result{}, err
	}
	engine.day = day
	for id, spent := range spentByID {
		engine.accounts[id].spent = spent
	}
	result, err := engine.Run(req)
	if err != nil {
		return Result{}, err
	}
	if result.WinnerID != "" {
		_, err = tx.ExecContext(ctx, `UPDATE auction_budget_accounts SET spent_micros=$2
			WHERE campaign_id=$1`, result.WinnerID, engine.SpentMicros(result.WinnerID))
		if err != nil {
			return Result{}, fmt.Errorf("reserve spend: %w", err)
		}
	}
	requestJSON, err := json.Marshal(req)
	if err != nil {
		return Result{}, err
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return Result{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO auction_replays
		(utc_day, auction_id, request_json, result_json) VALUES ($1, $2, $3, $4)`,
		day, req.AuctionID, requestJSON, resultJSON); err != nil {
		return Result{}, fmt.Errorf("save auction result: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Result{}, fmt.Errorf("commit auction: %w", err)
	}
	return result, nil
}
