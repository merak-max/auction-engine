package auction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrOverloaded = errors.New("auction queue is full")

type batchOutcome struct {
	Result  Result `json:"result"`
	Code    string `json:"error_code"`
	Message string `json:"message"`
}
type batchReply struct {
	result Result
	err    error
}
type batchJob struct {
	ctx   context.Context
	req   Request
	reply chan batchReply
}

// Batcher amortizes synchronous commits. It drains only requests already queued,
// without an artificial batching delay. Separate processes still share DB locks.
type Batcher struct {
	store   *PostgresStore
	queue   chan batchJob
	ctx     context.Context
	cancel  context.CancelFunc
	stopped chan struct{}
	once    sync.Once
}

func NewBatcher(store *PostgresStore) *Batcher {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Batcher{store: store, queue: make(chan batchJob, 1024), ctx: ctx, cancel: cancel, stopped: make(chan struct{})}
	go b.loop()
	return b
}
func (b *Batcher) Close() { b.once.Do(b.cancel); <-b.stopped }
func (b *Batcher) Run(ctx context.Context, req Request) (Result, error) {
	if err := validateRequest(req); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	req.Candidates = append([]Candidate(nil), req.Candidates...)
	job := batchJob{ctx: ctx, req: req, reply: make(chan batchReply, 1)}
	select {
	case b.queue <- job:
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-b.ctx.Done():
		return Result{}, errors.New("auction store closed")
	default:
		return Result{}, ErrOverloaded
	}
	select {
	case r := <-job.reply:
		return r.result, r.err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-b.ctx.Done():
		return Result{}, errors.New("auction store closed")
	}
}
func (b *Batcher) loop() {
	defer close(b.stopped)
	for {
		var first batchJob
		select {
		case <-b.ctx.Done():
			return
		case first = <-b.queue:
		}
		jobs := []batchJob{first}
	collect:
		for len(jobs) < 32 {
			select {
			case job := <-b.queue:
				jobs = append(jobs, job)
			default:
				break collect
			}
		}
		active := jobs[:0]
		for _, job := range jobs {
			if err := job.ctx.Err(); err != nil {
				job.reply <- batchReply{err: err}
			} else {
				active = append(active, job)
			}
		}
		if len(active) == 0 {
			continue
		}
		reqs := make([]Request, len(active))
		for i, job := range active {
			reqs[i] = job.req
		}
		ctx, cancel := context.WithTimeout(b.ctx, 2*time.Second)
		outcomes, err := b.store.runBatch(ctx, reqs)
		cancel()
		for i, job := range active {
			if err != nil {
				job.reply <- batchReply{err: err}
				continue
			}
			outcome := outcomes[i]
			var itemErr error
			switch outcome.Code {
			case "AU001":
				itemErr = fmt.Errorf("%w: %s", ErrInvalidRequest, outcome.Message)
			case "AU002":
				itemErr = ErrClockRegression
			case "":
			default:
				itemErr = fmt.Errorf("unexpected database outcome %s", outcome.Code)
			}
			job.reply <- batchReply{result: outcome.Result, err: itemErr}
		}
	}
}
func (s *PostgresStore) runBatch(ctx context.Context, reqs []Request) ([]batchOutcome, error) {
	encoded, err := json.Marshal(reqs)
	if err != nil {
		return nil, err
	}
	var raw []byte
	if err = s.db.QueryRowContext(ctx, `SELECT auction_batch_v1($1::jsonb)`, encoded).Scan(&raw); err != nil {
		return nil, fmt.Errorf("commit auction batch: %w", err)
	}
	var results []batchOutcome
	if err = json.Unmarshal(raw, &results); err != nil {
		return nil, err
	}
	if len(results) != len(reqs) {
		return nil, errors.New("database returned incomplete batch")
	}
	return results, nil
}
