package auction

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed postgres.sql
var schemaSQL string

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

// Init is a transactional, serialized, additive migration. Existing spend is never reset.
func (s *PostgresStore) Init(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(638492035196)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("migrate auction schema: %w", err)
	}
	for _, c := range s.cfg.Campaigns {
		_, err = tx.ExecContext(ctx, `INSERT INTO auction_budget_accounts(campaign_id,daily_budget_micros,pacing_burst_micros)
            VALUES($1,$2,$3) ON CONFLICT(campaign_id) DO NOTHING`, c.ID, c.DailyBudgetMicros, c.PacingBurstMicros)
		if err != nil {
			return err
		}
		var budget, burst int64
		if err = tx.QueryRowContext(ctx, `SELECT daily_budget_micros,pacing_burst_micros FROM auction_budget_accounts WHERE campaign_id=$1`, c.ID).Scan(&budget, &burst); err != nil {
			return err
		}
		if budget != c.DailyBudgetMicros || burst != c.PacingBurstMicros {
			return fmt.Errorf("configured policy differs from ledger for %q; migrate budgets explicitly", c.ID)
		}
	}
	return tx.Commit()
}

func (s *PostgresStore) Run(ctx context.Context, req Request) (Result, error) {
	if err := validateRequest(req); err != nil {
		return Result{}, err
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		return Result{}, err
	}
	var raw []byte
	// Autocommit completes the transaction before Scan returns successfully.
	err = s.db.QueryRowContext(ctx, `SELECT auction_run_v1($1::jsonb)`, encoded).Scan(&raw)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == "AU001" {
				return Result{}, fmt.Errorf("%w: %s", ErrInvalidRequest, pgErr.Message)
			}
			if pgErr.Code == "AU002" {
				return Result{}, ErrClockRegression
			}
		}
		return Result{}, fmt.Errorf("atomic auction: %w", err)
	}
	var result Result
	if err = json.Unmarshal(raw, &result); err != nil {
		return Result{}, err
	}
	return result, nil
}
