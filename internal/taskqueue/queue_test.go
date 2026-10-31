package taskqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

func TestTaskQueueValidation(t *testing.T) {
	queueName := "mw-test-validation"
	for name, q := range map[string]*Queue{
		"address":    New("redis://private-secret@host", "", queueName, "magworkspace:test", 1, time.Second),
		"queue name": New("localhost:6379", "", "other:{queue}", "magworkspace:test", 1, time.Second),
		"workers":    New("localhost:6379", "", queueName, "magworkspace:test", 0, time.Second),
		"timeout":    New("localhost:6379", "", queueName, "magworkspace:test", 1, 0),
	} {
		t.Run(name, func(t *testing.T) {
			defer q.Close()
			if err := q.Enqueue(context.Background(), "delivery", "job"); err != ErrInvalidConfig {
				t.Fatalf("configuration was not rejected safely: %v", err)
			}
			if err := q.Start(context.Background(), func(context.Context, string, string) error { return nil }); err != ErrInvalidConfig {
				t.Fatalf("worker accepted invalid configuration: %v", err)
			}
		})
	}
	q := New("localhost:6379", "", queueName, "magworkspace:test", 1, time.Second)
	defer q.Close()
	for _, id := range []string{"", "../document", "a b", "a:secret", strings.Repeat("x", 129), "正文"} {
		if err := q.Enqueue(context.Background(), id, "job"); err != ErrInvalidTask {
			t.Fatalf("delivery id was not rejected: %v", err)
		}
		if err := q.Enqueue(context.Background(), "delivery", id); err != ErrInvalidTask {
			t.Fatalf("job id was not rejected: %v", err)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q.Enqueue(canceled, "delivery", "job"); err != context.Canceled {
		t.Fatal(err)
	}
	if err := q.Start(context.Background(), nil); err != ErrInvalidConfig {
		t.Fatal(err)
	}
	q.Shutdown()
	if err := q.Start(context.Background(), func(context.Context, string, string) error { return nil }); err != ErrClosed {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(context.Background(), "delivery", "job"); err != ErrClosed {
		t.Fatal(err)
	}
}

func TestTaskQueueMalformedTasks(t *testing.T) {
	q := New("localhost:6379", "", "mw-test-malformed", "magworkspace:test", 1, time.Second)
	defer q.Close()
	for _, task := range []*asynq.Task{
		nil,
		asynq.NewTask("unknown", []byte("{}")),
		asynq.NewTask(q.taskType, []byte(`{"job_id":"job","delivery_id":"delivery","body":"private"}`)),
		asynq.NewTask(q.taskType, []byte(`{"job_id":"job","delivery_id":"delivery"} {}`)),
		asynq.NewTask(q.taskType, []byte(`{"job_id":"job","delivery_id":"bad:delivery"}`)),
		asynq.NewTask(q.taskType, []byte(`{"job_id":"job","delivery_id":"delivery"}`)),
		asynq.NewTask(q.taskType, []byte(strings.Repeat("x", 513))),
	} {
		err := q.handleTask(context.Background(), task, func(context.Context, string, string) error {
			t.Fatal("invalid payload invoked handler")
			return nil
		})
		if !errors.Is(err, asynq.SkipRetry) || !errors.Is(err, ErrInvalidTask) {
			t.Fatalf("invalid task was not archived: %v", err)
		}
	}
}

func TestTaskQueueNamespaceValidation(t *testing.T) {
	for _, name := range []string{"", "mw-memory:{capture}", "mw-memory:capture", "../capture", "with space", "中文", strings.Repeat("a", 129)} {
		q := New("localhost:6379", "", name, "magworkspace:memory-capture", 1, 30*time.Second)
		if err := q.Enqueue(context.Background(), "delivery", "job"); err != ErrInvalidConfig {
			t.Fatalf("unsafe queue name accepted: %q: %v", name, err)
		}
		q.Close()
	}
	for _, taskType := range []string{"", "memory", ":memory", "magworkspace:", "magworkspace:{memory}",
		"magworkspace:memory:other", "magworkspace:memory capture", "magworkspace:../memory", "magworkspace:" + strings.Repeat("a", 128)} {
		q := New("localhost:6379", "", "mw-memory-capture", taskType, 1, 30*time.Second)
		if err := q.Start(context.Background(), func(context.Context, string, string) error { return nil }); err != ErrInvalidConfig {
			t.Fatalf("unsafe task type accepted: %q: %v", taskType, err)
		}
		q.Close()
	}
	q := New("localhost:6379", "", "mw-memory-capture", "magworkspace:memory-capture", 1, 30*time.Second)
	defer q.Close()
	if err := q.Validate(); err != nil || q.timeout != time.Minute {
		t.Fatalf("memory task configuration or finalization budget rejected: timeout=%v error=%v", q.timeout, err)
	}
}

func redisQueue(t *testing.T, taskTypes ...string) *Queue {
	t.Helper()
	addr := os.Getenv("MAGWORKSPACE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("isolated Redis required")
	}
	entropy := make([]byte, 32)
	if _, err := rand.Read(entropy); err != nil {
		t.Fatal(err)
	}
	queueName := "mw-test-" + hex.EncodeToString(entropy)
	taskType := "magworkspace:test"
	if len(taskTypes) > 0 {
		taskType = taskTypes[0]
	}
	q := New(addr, "", queueName, taskType, 2, 5*time.Second)
	ctx := context.Background()
	if err := q.r.Ping(ctx).Err(); err != nil {
		q.Close()
		t.Fatalf("isolated Redis unavailable: %v", err)
	}
	t.Cleanup(func() {
		q.Close()
		// Only this test's random queue keys are removed. Never flush the
		// shared Redis used by other packages in the integration suite.
		r := redis.NewClient(&redis.Options{Addr: addr})
		defer r.Close()
		var cursor uint64
		for {
			keys, next, err := r.Scan(ctx, cursor, "asynq:{"+q.name+"}:*", 100).Result()
			if err != nil {
				t.Error("cleanup queue scan failed")
				break
			}
			if len(keys) > 0 {
				if err := r.Del(ctx, keys...).Err(); err != nil {
					t.Error("cleanup queue keys failed")
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		if err := r.SRem(ctx, "asynq:queues", q.name).Err(); err != nil {
			t.Error("cleanup queue registration failed")
		}
	})
	return q
}

func eventually(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("queue state did not converge")
}

func TestTaskQueueRedisDedupQueueAndPendingRestart(t *testing.T) {
	q := redisQueue(t)
	other := redisQueue(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := q.Enqueue(ctx, "delivery", "job"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := q.Enqueue(ctx, "delivery", "different-job"); err != ErrInvalidTask {
		t.Fatalf("delivery collision accepted: %v", err)
	}
	if err := other.Enqueue(ctx, "delivery", "other-job"); err != nil {
		t.Fatal(err)
	}
	info, err := q.inspector.GetQueueInfo(q.name)
	if err != nil || info.Pending != 1 {
		t.Fatalf("duplicate queue delivery: %+v %v", info, err)
	}
	// A fresh worker consumes a durable pending delivery after client restart.
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := New(os.Getenv("MAGWORKSPACE_TEST_REDIS_ADDR"), "", q.name, q.taskType, 2, 5*time.Second)
	defer restarted.Close()
	var calls atomic.Int32
	if err := restarted.Start(ctx, func(_ context.Context, jobID, deliveryID string) error {
		if jobID != "job" || deliveryID != "delivery" {
			t.Error("wrong delivery")
		}
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		_, err := restarted.inspector.GetTaskInfo(q.name, "delivery")
		return calls.Load() == 1 && errors.Is(err, asynq.ErrTaskNotFound)
	})
	if err := restarted.Start(ctx, func(context.Context, string, string) error { return nil }); err != ErrStarted {
		t.Fatal(err)
	}
	otherInfo, err := other.inspector.GetTaskInfo(other.name, "delivery")
	if err != nil || otherInfo.State != asynq.TaskStatePending {
		t.Fatalf("worker consumed a different queue: %+v %v", otherInfo, err)
	}
}

func TestTaskQueueRedisRetryPanicAndSafeErrors(t *testing.T) {
	q := redisQueue(t)
	ctx := context.Background()
	const private = "do-not-store-provider-secret-or-document"
	var calls atomic.Int32
	if err := q.Start(ctx, func(context.Context, string, string) error {
		switch calls.Add(1) {
		case 1:
			return fmt.Errorf("%s", private)
		case 2:
			panic(private)
		default:
			return nil
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, "retry-delivery", "job"); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		info, err := q.inspector.GetTaskInfo(q.name, "retry-delivery")
		return err == nil && info.State == asynq.TaskStateRetry && info.LastErr == ErrHandler.Error()
	})
	eventually(t, func() bool {
		info, err := q.inspector.GetTaskInfo(q.name, "retry-delivery")
		return err == nil && info.State == asynq.TaskStateRetry && info.LastErr == ErrHandlerPanic.Error()
	})
	eventually(t, func() bool {
		_, err := q.inspector.GetTaskInfo(q.name, "retry-delivery")
		return calls.Load() == 3 && errors.Is(err, asynq.ErrTaskNotFound)
	})
}

func TestTaskQueueRedisArchiveRepairAndLoss(t *testing.T) {
	q := redisQueue(t)
	ctx := context.Background()
	data, _ := json.Marshal(payload{JobID: "job", DeliveryID: "archived-delivery"})
	// Exhaust the transport retry budget, then simulate durable-outbox
	// reconciliation reviving the same delivery after its dependency recovers.
	_, err := q.client.EnqueueContext(ctx, asynq.NewTask(q.taskType, data),
		asynq.Queue(q.name), asynq.TaskID("archived-delivery"), asynq.MaxRetry(0))
	if err != nil {
		t.Fatal(err)
	}
	var recovered atomic.Bool
	var calls atomic.Int32
	if err := q.Start(ctx, func(context.Context, string, string) error {
		calls.Add(1)
		if !recovered.Load() {
			return errors.New("transient database failure")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		info, err := q.inspector.GetTaskInfo(q.name, "archived-delivery")
		return err == nil && info.State == asynq.TaskStateArchived
	})
	recovered.Store(true)
	if err := q.Enqueue(ctx, "archived-delivery", "job"); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		_, err := q.inspector.GetTaskInfo(q.name, "archived-delivery")
		return calls.Load() == 2 && errors.Is(err, asynq.ErrTaskNotFound)
	})
	q.Shutdown()
	if err := q.Enqueue(ctx, "lost-delivery", "job"); err != nil {
		t.Fatal(err)
	}
	info, err := q.inspector.GetTaskInfo(q.name, "lost-delivery")
	if err != nil || info.MaxRetry != 5 || info.Timeout != 35*time.Second {
		t.Fatalf("transport policy not applied: %+v %v", info, err)
	}
	if err := q.inspector.DeleteTask(q.name, "lost-delivery"); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, "lost-delivery", "job"); err != nil {
		t.Fatal(err)
	}
	info, err = q.inspector.GetTaskInfo(q.name, "lost-delivery")
	if err != nil || info.State != asynq.TaskStatePending {
		t.Fatalf("missing queue delivery could not be restored: %+v %v", info, err)
	}
}

func TestTaskQueueRedisShutdownRequeuesActive(t *testing.T) {
	q := redisQueue(t)
	ctx := context.Background()
	active := make(chan struct{})
	var initialCalls atomic.Int32
	if err := q.Start(ctx, func(ctx context.Context, _, _ string) error {
		initialCalls.Add(1)
		close(active)
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, "active-delivery", "job"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-active:
	case <-time.After(10 * time.Second):
		t.Fatal("task not started")
	}
	var concurrent sync.WaitGroup
	for range 8 {
		concurrent.Add(1)
		go func() {
			defer concurrent.Done()
			if err := q.Enqueue(ctx, "active-delivery", "job"); err != nil {
				t.Error(err)
			}
		}()
	}
	concurrent.Wait()
	info, err := q.inspector.GetTaskInfo(q.name, "active-delivery")
	if err != nil || info.State != asynq.TaskStateActive || initialCalls.Load() != 1 {
		t.Fatalf("deduplication disturbed active delivery: %+v %v", info, err)
	}
	for range 3 {
		concurrent.Add(1)
		go func() {
			defer concurrent.Done()
			q.Shutdown()
		}()
	}
	concurrent.Wait()
	info, err = q.inspector.GetTaskInfo(q.name, "active-delivery")
	if err != nil || (info.State != asynq.TaskStateRetry && info.State != asynq.TaskStatePending) {
		t.Fatalf("shutdown lost active delivery: %+v %v", info, err)
	}
	restarted := New(os.Getenv("MAGWORKSPACE_TEST_REDIS_ADDR"), "", q.name, q.taskType, 1, 5*time.Second)
	defer restarted.Close()
	var calls atomic.Int32
	if err := restarted.Start(ctx, func(_ context.Context, jobID, deliveryID string) error {
		if jobID != "job" || deliveryID != "active-delivery" {
			t.Error("wrong recovered delivery")
		}
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		_, err := restarted.inspector.GetTaskInfo(q.name, "active-delivery")
		return calls.Load() == 1 && errors.Is(err, asynq.ErrTaskNotFound)
	})
}

func TestTaskQueueRedisRejectsForgedDelivery(t *testing.T) {
	q := redisQueue(t)
	ctx := context.Background()
	var calls atomic.Int32
	if err := q.Start(ctx, func(context.Context, string, string) error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(payload{JobID: "job", DeliveryID: "different-delivery"})
	if _, err := q.client.EnqueueContext(ctx, asynq.NewTask(q.taskType, data),
		asynq.Queue(q.name), asynq.TaskID("actual-delivery")); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		info, err := q.inspector.GetTaskInfo(q.name, "actual-delivery")
		return err == nil && info.State == asynq.TaskStateArchived &&
			strings.Contains(info.LastErr, ErrInvalidTask.Error()) && info.Retried == 0
	})
	if calls.Load() != 0 {
		t.Fatal("mismatched transport identity invoked business handler")
	}
}

func TestTaskQueueRedisQueueAndTypeIsolation(t *testing.T) {
	index := redisQueue(t, "magworkspace:index")
	memory := redisQueue(t, "magworkspace:memory-capture")
	ctx := context.Background()
	if err := index.Enqueue(ctx, "same-delivery", "index-job"); err != nil {
		t.Fatal(err)
	}
	if err := memory.Enqueue(ctx, "same-delivery", "memory-job"); err != nil {
		t.Fatal(err)
	}
	// Even a misconfigured publisher sharing a queue cannot mistake another
	// task type for successful deduplication of its own delivery.
	wrongPublisher := New(os.Getenv("MAGWORKSPACE_TEST_REDIS_ADDR"), "", index.name, memory.taskType, 1, time.Second)
	defer wrongPublisher.Close()
	if err := wrongPublisher.Enqueue(ctx, "same-delivery", "index-job"); err != ErrInvalidTask {
		t.Fatalf("cross-type duplicate accepted: %v", err)
	}
	var indexCalls, memoryCalls atomic.Int32
	if err := index.Start(ctx, func(_ context.Context, job, delivery string) error {
		if job != "index-job" || delivery != "same-delivery" {
			t.Error("index handler received another queue or task type")
		}
		indexCalls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		_, err := index.inspector.GetTaskInfo(index.name, "same-delivery")
		return indexCalls.Load() == 1 && errors.Is(err, asynq.ErrTaskNotFound)
	})
	info, err := memory.inspector.GetTaskInfo(memory.name, "same-delivery")
	if err != nil || info.State != asynq.TaskStatePending || info.Type != memory.taskType {
		t.Fatalf("index worker consumed memory queue: %+v %v", info, err)
	}
	// Direct transport contamination must be archived without invoking the
	// business handler or burning transport retry attempts.
	data, _ := json.Marshal(payload{JobID: "memory-job", DeliveryID: "foreign-type"})
	if _, err := index.client.EnqueueContext(ctx, asynq.NewTask(memory.taskType, data),
		asynq.Queue(index.name), asynq.TaskID("foreign-type")); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		info, err := index.inspector.GetTaskInfo(index.name, "foreign-type")
		return err == nil && info.State == asynq.TaskStateArchived && info.Retried == 0 && strings.Contains(info.LastErr, ErrInvalidTask.Error())
	})
	if indexCalls.Load() != 1 {
		t.Fatal("foreign task type reached index handler")
	}
	if err := memory.Start(ctx, func(_ context.Context, job, delivery string) error {
		if job != "memory-job" || delivery != "same-delivery" {
			t.Error("memory handler received another queue or task type")
		}
		memoryCalls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		_, err := memory.inspector.GetTaskInfo(memory.name, "same-delivery")
		return memoryCalls.Load() == 1 && errors.Is(err, asynq.ErrTaskNotFound)
	})
}
