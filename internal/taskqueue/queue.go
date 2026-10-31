// Package taskqueue transports durable outbox deliveries through Redis.
// Tasks contain identifiers only; MySQL owns authorization, leases and outcomes.
package taskqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

var (
	ErrUnavailable   = errors.New("task queue unavailable")
	ErrInvalidConfig = errors.New("invalid task queue configuration")
	ErrInvalidTask   = errors.New("invalid task queue task")
	ErrClosed        = errors.New("task queue closed")
	ErrStarted       = errors.New("task queue already started")
	ErrHandler       = errors.New("task delivery failed")
	ErrHandlerPanic  = errors.New("task delivery panicked")
)

type payload struct {
	JobID      string `json:"job_id"`
	DeliveryID string `json:"delivery_id"`
}

// Queue is both a publisher and a worker for one explicit queue and task type.
// A new Queue must be constructed after Shutdown; Close releases its Redis pool.
type Queue struct {
	r         *redis.Client
	client    *asynq.Client
	inspector *asynq.Inspector
	name      string
	taskType  string
	workers   int
	timeout   time.Duration
	configErr error

	mu       sync.Mutex
	server   *asynq.Server
	cancel   context.CancelFunc
	stopping bool
	closed   bool
	done     chan struct{}
	closeOne sync.Once
	closeErr error
}

// New does not connect to Redis. Invalid configuration is reported by Start or
// Enqueue, so an invalid name cannot accidentally create or consume another queue.
// Queue names are bounded ASCII identifiers; task types use namespace:name with
// the same identifier characters, excluding Redis hash tags and key separators.
// timeout is the business execution budget; transport adds 30 seconds for final
// state persistence. Business retry policy belongs to the durable outbox.
func New(addr, password, queueName, taskType string, workers int, timeout time.Duration) *Queue {
	q := &Queue{
		name: queueName, taskType: taskType, workers: workers, timeout: timeout + 30*time.Second,
		done: make(chan struct{}),
	}
	host, port, addrErr := net.SplitHostPort(addr)
	portNumber, portErr := strconv.Atoi(port)
	if addrErr != nil || host == "" || portErr != nil || portNumber < 1 || portNumber > 65535 ||
		!validID(queueName) || !validTaskType(taskType) ||
		workers < 1 || workers > 64 || timeout <= 0 || timeout > 24*time.Hour {
		q.configErr = ErrInvalidConfig
	}
	q.r = redis.NewClient(&redis.Options{
		Addr: addr, Password: password, MaxRetries: -1,
		DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second,
		ContextTimeoutEnabled: true, PoolSize: max(8, min(workers+8, 72)),
	})
	q.client = asynq.NewClientFromRedisClient(q.r)
	q.inspector = asynq.NewInspectorFromRedisClient(q.r)
	return q
}

// Validate checks static configuration without requiring Redis availability.
func (q *Queue) Validate() error { return q.configErr }

func validID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, ch := range id {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' ||
			ch >= '0' && ch <= '9' || ch == '-' || ch == '_') {
			return false
		}
	}
	return true
}

func validTaskType(value string) bool {
	if len(value) > 128 {
		return false
	}
	namespace, name, ok := strings.Cut(value, ":")
	return ok && validID(namespace) && validID(name)
}

func (q *Queue) check(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if q.configErr != nil {
		return q.configErr
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	return nil
}

// Enqueue is idempotent while a delivery is pending, active or awaiting retry.
// Completed tasks are not retained: a lost acknowledgement can be redelivered,
// and the durable delivery claim must reject work already committed to MySQL.
// Reconciliation can revive an archived transport failure without deleting an
// active task. No Redis-only deduplication marker can permanently block recovery.
func (q *Queue) Enqueue(ctx context.Context, deliveryID, jobID string) error {
	if err := q.check(ctx); err != nil {
		return err
	}
	if !validID(deliveryID) || !validID(jobID) {
		return ErrInvalidTask
	}
	data, _ := json.Marshal(payload{JobID: jobID, DeliveryID: deliveryID})
	for range 2 {
		_, err := q.client.EnqueueContext(ctx, asynq.NewTask(q.taskType, data),
			asynq.Queue(q.name), asynq.TaskID(deliveryID), asynq.MaxRetry(5), asynq.Timeout(q.timeout))
		if err == nil {
			return nil
		}
		if !errors.Is(err, asynq.ErrTaskIDConflict) && !errors.Is(err, asynq.ErrDuplicateTask) {
			return safeTransportError(ctx)
		}
		existing, err := q.inspector.GetTaskInfo(q.name, deliveryID)
		if errors.Is(err, asynq.ErrTaskNotFound) || errors.Is(err, asynq.ErrQueueNotFound) {
			// Completion/removal raced the duplicate check; enqueue once more.
			continue
		}
		if err != nil {
			return safeTransportError(ctx)
		}
		if existing.Type != q.taskType || !bytes.Equal(existing.Payload, data) {
			return ErrInvalidTask
		}
		if existing.State != asynq.TaskStateArchived {
			return nil
		}
		if err := q.inspector.RunTask(q.name, deliveryID); err == nil {
			return nil
		}
		// Another reconciler may have revived the task, or a worker may already
		// have completed it. The next enqueue observes that state atomically.
	}
	return safeTransportError(ctx)
}

func safeTransportError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ErrUnavailable
}

// Start is non-blocking. It does not require Redis to be available at startup:
// Asynq reconnects, while the durable outbox retains undelivered work.
// The caller must call Shutdown after cancellation and Close before disposal.
func (q *Queue) Start(ctx context.Context, handle func(context.Context, string, string) error) error {
	if err := q.check(ctx); err != nil {
		return err
	}
	if handle == nil {
		return ErrInvalidConfig
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.stopping {
		return ErrClosed
	}
	if q.server != nil {
		return ErrStarted
	}
	base, cancel := context.WithCancel(ctx)
	server := asynq.NewServerFromRedisClient(q.r, asynq.Config{
		Concurrency: q.workers, Queues: map[string]int{q.name: 1},
		BaseContext:       func() context.Context { return base },
		TaskCheckInterval: 200 * time.Millisecond, DelayedTaskCheckInterval: time.Second,
		ShutdownTimeout: 5 * time.Second,
		RetryDelayFunc: func(n int, _ error, _ *asynq.Task) time.Duration {
			return time.Duration(1<<min(max(n, 0), 4)) * time.Second
		},
		Logger: safeLogger{}, LogLevel: asynq.WarnLevel,
	})
	if err := server.Start(asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		return q.handleTask(ctx, task, handle)
	})); err != nil {
		cancel()
		return ErrUnavailable
	}
	q.server, q.cancel = server, cancel
	return nil
}

func (q *Queue) handleTask(ctx context.Context, task *asynq.Task, handle func(context.Context, string, string) error) (err error) {
	// Recover before Asynq's own panic handler: both Redis LastErr and logs must
	// never contain handler error text, panic values, original content or keys.
	defer func() {
		if recover() != nil {
			err = ErrHandlerPanic
		}
	}()
	if task == nil || task.Type() != q.taskType || len(task.Payload()) > 512 {
		return fmt.Errorf("%w: %w", ErrInvalidTask, asynq.SkipRetry)
	}
	var p payload
	decoder := json.NewDecoder(bytes.NewReader(task.Payload()))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&p) != nil || !validID(p.JobID) || !validID(p.DeliveryID) {
		return fmt.Errorf("%w: %w", ErrInvalidTask, asynq.SkipRetry)
	}
	var tail any
	if decoder.Decode(&tail) != io.EOF {
		return fmt.Errorf("%w: %w", ErrInvalidTask, asynq.SkipRetry)
	}
	taskID, ok := asynq.GetTaskID(ctx)
	if !ok || taskID != p.DeliveryID {
		return fmt.Errorf("%w: %w", ErrInvalidTask, asynq.SkipRetry)
	}
	if err := handle(ctx, p.JobID, p.DeliveryID); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrHandler
	}
	return nil
}

// Shutdown stops fetching, cancels active handlers, and waits for Asynq to put
// unfinished deliveries back in Redis. Concurrent calls wait for the same stop.
func (q *Queue) Shutdown() {
	q.mu.Lock()
	if q.stopping {
		q.mu.Unlock()
		<-q.done
		return
	}
	q.stopping = true
	server, cancel := q.server, q.cancel
	q.mu.Unlock()
	if server != nil {
		server.Stop()
		cancel()
		server.Shutdown()
	}
	close(q.done)
}

func (q *Queue) Close() error {
	q.closeOne.Do(func() {
		q.Shutdown()
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		if q.r.Close() != nil {
			q.closeErr = ErrUnavailable
		}
	})
	return q.closeErr
}

// Asynq's messages may include raw Redis errors or task data. Log only a fixed
// category; callers expose durable state and bounded error codes separately.
type safeLogger struct{}

func (safeLogger) Debug(...any) {}
func (safeLogger) Info(...any)  {}
func (safeLogger) Warn(...any)  { slog.Warn("task queue warning", "code", "task_queue_warning") }
func (safeLogger) Error(...any) { slog.Error("task queue error", "code", "task_queue_error") }
func (safeLogger) Fatal(...any) { slog.Error("task queue fatal error", "code", "task_queue_fatal") }
