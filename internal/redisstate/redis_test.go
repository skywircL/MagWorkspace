package redisstate

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func testClient(t *testing.T) *Client {
	t.Helper()
	addr := os.Getenv("MAGWORKSPACE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("isolated Redis required")
	}
	c := New(addr, "", fmt.Sprintf("test:%d:", time.Now().UnixNano()))
	t.Cleanup(func() {
		keys, _ := c.r.Keys(context.Background(), c.prefix+"*").Result()
		if len(keys) > 0 {
			_ = c.r.Del(context.Background(), keys...).Err()
		}
		c.Close()
	})
	if err := c.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	return c
}
func TestRedisQueueDedupAndLoss(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	jobs := []domain.IndexJob{{ID: "one", CreatedAt: time.Now()}}
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Dispatch(ctx, "p", jobs); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var got atomic.Int32
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := c.Pop(ctx, "p")
			if err != nil {
				t.Error(err)
			}
			if id != "" {
				got.Add(1)
			}
		}()
	}
	wg.Wait()
	if got.Load() != 1 {
		t.Fatal("duplicate dispatch", got.Load())
	}
	// Recovery after pop-before-claim is the same durable-intent reconciliation.
	if err := c.Dispatch(ctx, "p", jobs); err != nil {
		t.Fatal(err)
	}
	if id, err := c.Pop(ctx, "p"); err != nil || id != "one" {
		t.Fatal(id, err)
	}
}
func TestRedisEventsBoundedReplayAndDedup(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	for i := uint64(1); i <= EventLimit+1; i++ {
		if err := c.AppendEvent(ctx, "run", 1, i, "message.delta", []byte(`{"delta":"x"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.AppendEvent(ctx, "run", 1, EventLimit+1, "message.delta", []byte(`{"delta":"x"}`)); err != nil {
		t.Fatal(err)
	}
	if err := c.AppendEvent(ctx, "run", 1, EventLimit+1, "message.delta", []byte(`{"delta":"changed"}`)); err == nil {
		t.Fatal("conflicting duplicate accepted")
	}
	if _, err := c.Events(ctx, "run", 1, 0); err != ErrHistory {
		t.Fatal("trimmed history", err)
	}
	rows, err := c.Events(ctx, "run", 1, EventLimit)
	if err != nil || len(rows) != 1 || rows[0].Sequence != EventLimit+1 {
		t.Fatal(rows, err)
	}
	if _, err := c.Events(ctx, "run", 2, 0); err != ErrHistory {
		t.Fatal("cross generation", err)
	}
	if ttl := c.r.TTL(ctx, c.eventKey("run", 1)).Val(); ttl <= 0 || ttl > stateTTL {
		t.Fatal("unbounded retention", ttl)
	}
	c.r.Del(ctx, c.eventKey("run", 1))
	if _, err := c.Events(ctx, "run", 1, 1); err != ErrHistory {
		t.Fatal("expired", err)
	}
}
func TestRedisProgressGenerationAndUnavailable(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	job := domain.IndexJob{ID: "one", Generation: "new", Status: "running"}
	c.PutProgress(ctx, job, "embedding", 4)
	if p := c.Progress(ctx, job); p == nil || p.Stage != "embedding" || p.Chunks != 4 {
		t.Fatal(p)
	}
	job.Generation = "old"
	if c.Progress(ctx, job) != nil {
		t.Fatal("stale progress")
	}
	job.Generation = "new"
	job.Status = "ready"
	if c.Progress(ctx, job) != nil {
		t.Fatal("progress overrides terminal")
	}
	c.Close()
	start := time.Now()
	if _, err := c.Pop(ctx, "p"); err != ErrUnavailable {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("unbounded outage")
	}
}
