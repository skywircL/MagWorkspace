// Package memorycapture processes durable, private post-answer memory jobs.
// MySQL owns eligibility and commits; Redis carries reconstructible IDs only.
package memorycapture

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/observability"
	"magworkspace/internal/store"
	"magworkspace/internal/taskqueue"
)

type Options struct {
	Queue        *taskqueue.Queue
	Telemetry    *observability.Telemetry
	PollInterval time.Duration
	JobTimeout   time.Duration
}

type Service struct {
	db        *store.Store
	extractor agent.MemoryExtractor
	opts      Options
}

func NewService(db *store.Store, extractor agent.MemoryExtractor, opts Options) *Service {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 2 * time.Second
	}
	if opts.JobTimeout <= 0 || opts.JobTimeout > 30*time.Second {
		opts.JobTimeout = 30 * time.Second
	}
	return &Service{db: db, extractor: extractor, opts: opts}
}

// The queue owns consumption when configured. A Redis outage retains durable
// work for reconciliation; direct polling is only the no-Redis development path.
func (s *Service) Run(ctx context.Context) {
	if s.opts.Queue != nil {
		for ctx.Err() == nil {
			if err := s.opts.Queue.Start(ctx, s.ProcessDelivery); err == nil {
				break
			}
			slog.Warn("memory queue startup pending")
			if !pause(ctx, s.opts.PollInterval) {
				return
			}
		}
		defer s.opts.Queue.Shutdown()
		for ctx.Err() == nil {
			if err := s.DispatchOnce(ctx); err != nil && ctx.Err() == nil {
				s.opts.Telemetry.ObserveCount("memory.dispatch", "error", 1)
			}
			if !pause(ctx, s.opts.PollInterval) {
				return
			}
		}
		return
	}
	for ctx.Err() == nil {
		worked, err := s.ProcessOne(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Warn("memory capture pending", "code", "memory_capture_failed")
		}
		if !worked || err != nil {
			if !pause(ctx, s.opts.PollInterval) {
				return
			}
		}
	}
}

func pause(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// DispatchOnce republishes only committed, due deliveries. Enqueue success does
// not consume the SQL intent: queue loss or an unknown ACK remains recoverable.
func (s *Service) DispatchOnce(ctx context.Context) error {
	if s.opts.Queue == nil {
		return nil
	}
	deliveries, err := s.db.PendingMemoryCaptureDeliveries(ctx, 32)
	if err != nil {
		return err
	}
	for _, delivery := range deliveries {
		if err := s.opts.Queue.Enqueue(ctx, delivery.ID, delivery.JobID); err != nil {
			return err
		}
		if err := s.db.MarkMemoryCaptureDeliverySent(ctx, delivery); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) ProcessOne(ctx context.Context) (bool, error) {
	if s.opts.Queue != nil {
		return false, nil
	}
	ids, err := s.db.PendingMemoryCaptures(ctx, 32)
	if err != nil {
		return false, err
	}
	for _, id := range ids {
		job, claimed, err := s.db.ClaimMemoryCapture(ctx, id, s.opts.JobTimeout+5*time.Second)
		if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrConflict) {
			continue
		}
		if err != nil {
			return false, err
		}
		if !claimed {
			continue
		}
		processErr, persistErr := s.executeClaimed(ctx, job)
		if persistErr != nil {
			return true, persistErr
		}
		return true, processErr
	}
	return false, nil
}

// ACK after a durable success, cancellation or retry decision. Asynq retries
// transport/persistence failures, never the already-recorded model failure.
func (s *Service) ProcessDelivery(ctx context.Context, jobID, deliveryID string) error {
	job, claimed, err := s.db.ClaimMemoryCaptureDelivery(ctx, jobID, deliveryID, s.opts.JobTimeout+5*time.Second)
	if err != nil || !claimed {
		return err
	}
	_, persistErr := s.executeClaimed(ctx, job)
	return persistErr
}

func (s *Service) executeClaimed(ctx context.Context, job domain.MemoryCaptureJob) (error, error) {
	err := s.process(ctx, job)
	if err == nil {
		return nil, nil
	}
	// Shutdown leaves the fenced lease and delivery intact. Another worker can
	// reclaim after expiry; a canceled process cannot publish stale output.
	if ctx.Err() != nil {
		return err, ctx.Err()
	}
	return err, s.fail(job, err)
}

func (s *Service) process(ctx context.Context, job domain.MemoryCaptureJob) (resultErr error) {
	ctx, finish := s.opts.Telemetry.Start(ctx, "memory.capture")
	defer func() { finish(resultErr) }()
	valid, err := s.db.ValidateMemoryCapture(ctx, job)
	if err != nil {
		return err
	}
	if !valid {
		return nil
	}
	var candidates []domain.MemoryCandidate
	if job.Explicit != nil {
		candidates = []domain.MemoryCandidate{*job.Explicit}
	} else {
		if s.extractor == nil {
			return agent.ErrUnavailable
		}
		extractCtx, cancel := context.WithTimeout(ctx, s.opts.JobTimeout)
		extractCtx, end := s.opts.Telemetry.Start(extractCtx, "memory.extract")
		items, err := s.extractor.ExtractMemories(extractCtx, job.Query)
		end(err)
		cancel()
		if err != nil {
			return err
		}
		for _, item := range items {
			candidates = append(candidates, domain.MemoryCandidate{Kind: item.Kind, Content: item.Content, Topic: item.Topic, Importance: item.Importance, Evidence: item.Evidence})
		}
	}
	commitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return s.db.CompleteMemoryCapture(commitCtx, job, candidates)
}

func (s *Service) fail(job domain.MemoryCaptureJob, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if errors.Is(cause, agent.ErrExtractionBusy) {
		return s.db.DeferMemoryCaptureBusy(ctx, job)
	}
	code := "extraction_failed"
	if errors.Is(cause, context.Canceled) {
		code = "capture_cancelled"
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		code = "extraction_timeout"
	}
	if errors.Is(cause, agent.ErrUnavailable) {
		code = "extraction_unavailable"
	}
	if errors.Is(cause, domain.ErrInvalid) {
		code = "invalid_extraction"
	}
	return s.db.FailMemoryCapture(ctx, job, code)
}
