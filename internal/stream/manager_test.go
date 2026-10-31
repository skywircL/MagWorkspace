package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func managers(t *testing.T, fn func(*testing.T, Manager)) {
	t.Run("memory", func(t *testing.T) { fn(t, NewMemory(DefaultTTL)) })
	t.Run("redis", func(t *testing.T) { fn(t, testRedis(t, DefaultTTL)) })
}
func testRedis(t *testing.T, ttl time.Duration) *Redis {
	t.Helper()
	addr := os.Getenv("MAGWORKSPACE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("isolated Redis required")
	}
	client := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
	prefix := fmt.Sprintf("stream-test:%d:", time.Now().UnixNano())
	t.Cleanup(func() {
		keys, _ := client.Keys(context.Background(), prefix+"*").Result()
		if len(keys) > 0 {
			_ = client.Del(context.Background(), keys...).Err()
		}
		client.Close()
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return NewRedis(client, prefix, ttl)
}

var testKey = Key{SessionID: "session", RunID: "run", Generation: 1}

func TestStreamReplayIsolationAndBounds(t *testing.T) {
	managers(t, func(t *testing.T, m Manager) {
		ctx := context.Background()
		k := testKey
		if _, _, err := m.GetEvents(ctx, k, 0); !errors.Is(err, ErrHistory) {
			t.Fatal(err)
		}
		for i := uint64(1); i <= EventLimit+1; i++ {
			if err := m.AppendEvent(ctx, k, Event{Sequence: i, Type: "message.delta", Data: json.RawMessage(`{"delta":"x"}`)}); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := m.GetEvents(ctx, k, 0); !errors.Is(err, ErrHistory) {
			t.Fatal("trim not detected", err)
		}
		a, n, err := m.GetEvents(ctx, k, EventLimit)
		if err != nil || n != EventLimit+1 || len(a) != 1 {
			t.Fatal(a, n, err)
		}
		b, n2, err := m.GetEvents(ctx, k, EventLimit)
		if err != nil || n != n2 || !reflect.DeepEqual(a, b) {
			t.Fatal("destructive read", err)
		}
		a[0].Data[0] = 'X'
		b, _, _ = m.GetEvents(ctx, k, EventLimit)
		if !json.Valid(b[0].Data) {
			t.Fatal("aliased cache")
		}
		for _, other := range []Key{{"other", "run", 1}, {"session", "other", 1}, {"session", "run", 2}} {
			if _, _, err = m.GetEvents(ctx, other, 0); !errors.Is(err, ErrHistory) {
				t.Fatal("cross scope", err)
			}
		}
		if _, _, err = m.GetEvents(ctx, k, EventLimit+2); !errors.Is(err, ErrHistory) {
			t.Fatal("future offset", err)
		}
		if err = m.AppendEvent(ctx, k, Event{Sequence: EventLimit + 3, Type: "x", Data: json.RawMessage(`{}`)}); !errors.Is(err, ErrHistory) {
			t.Fatal("gap", err)
		}
		if err = m.ResetEvents(ctx, k); err != nil {
			t.Fatal(err)
		}
		if err = m.AppendEvent(ctx, k, Event{Sequence: 1, Type: "new", Data: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		if _, _, err = m.GetEvents(ctx, k, 0); err == nil {
			t.Fatal("canceled read")
		}
	})
}
func TestStreamStopIdempotentAndIsolated(t *testing.T) {
	managers(t, func(t *testing.T, m Manager) {
		ctx := context.Background()
		k := testKey
		if requested, err := m.StopRequested(ctx, k); err != nil || requested {
			t.Fatal("unexpected stop", requested, err)
		}
		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				if err := m.RequestStop(ctx, k); err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		for range 2 {
			if requested, err := m.StopRequested(ctx, k); err != nil || !requested {
				t.Fatal("stop lost or consumed", requested, err)
			}
		}
		for _, other := range []Key{{"other", "run", 1}, {"session", "other", 1}, {"session", "run", 2}} {
			if requested, err := m.StopRequested(ctx, other); err != nil || requested {
				t.Fatal("stop crossed execution scope", requested, err)
			}
		}
		if _, _, err := m.GetEvents(ctx, k, 0); !errors.Is(err, ErrHistory) {
			t.Fatal("stop leaked into output", err)
		}
		if err := m.AppendEvent(ctx, k, Event{Sequence: 1, Type: "run.started", Data: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		if err := m.ResetEvents(ctx, k); err != nil {
			t.Fatal(err)
		}
		if requested, err := m.StopRequested(ctx, k); err != nil || !requested {
			t.Fatal("output reset removed stop", requested, err)
		}
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		if err := m.RequestStop(ctx, k); err == nil {
			t.Fatal("canceled stop write")
		}
		if _, err := m.StopRequested(ctx, k); err == nil {
			t.Fatal("canceled stop read")
		}
	})
}
func TestStreamLiveOwnershipAndGenerationCAS(t *testing.T) {
	managers(t, func(t *testing.T, m Manager) {
		ctx := context.Background()
		var wg sync.WaitGroup
		var won atomic.Int32
		for i := range 16 {
			wg.Go(func() {
				l := LiveRun{RunID: fmt.Sprint(i), Generation: 1, RequestID: "req"}
				if err := m.SetLiveRun(ctx, "session", l); err == nil {
					won.Add(1)
				} else if !errors.Is(err, ErrConflict) {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		if won.Load() != 1 {
			t.Fatal(won.Load())
		}
		old, err := m.GetLiveRun(ctx, "session")
		if err != nil {
			t.Fatal(err)
		}
		if err = m.SetLiveRun(ctx, "session", old); err != nil {
			t.Fatal("idempotent marker", err)
		}
		next := old
		next.Generation++
		if err = m.ClaimLiveRun(ctx, "session", old, next); err != nil {
			t.Fatal(err)
		}
		if err = m.ClearLiveRun(ctx, "session", old); err != nil {
			t.Fatal(err)
		}
		live, _ := m.GetLiveRun(ctx, "session")
		if live != next {
			t.Fatal("old cleanup removed successor", live)
		}
		if err = m.ClaimLiveRun(ctx, "session", old, old); !errors.Is(err, ErrConflict) {
			t.Fatal("stale takeover", err)
		}
		if err = m.ClearLiveRun(ctx, "session", next); err != nil {
			t.Fatal(err)
		}
		live, _ = m.GetLiveRun(ctx, "session")
		if live != (LiveRun{}) {
			t.Fatal(live)
		}
	})
}
func TestStreamExpiryAndRedisSharing(t *testing.T) {
	for _, kind := range []string{"memory", "redis"} {
		t.Run(kind, func(t *testing.T) {
			var m Manager
			ttl := 150 * time.Millisecond
			if kind == "memory" {
				m = NewMemory(ttl)
			} else {
				m = testRedis(t, ttl)
			}
			ctx := context.Background()
			k := testKey
			l := LiveRun{"run", 1, "req"}
			if err := m.SetLiveRun(ctx, k.SessionID, l); err != nil {
				t.Fatal(err)
			}
			if err := m.AppendEvent(ctx, k, Event{Sequence: 1, Type: "run.started", Data: json.RawMessage(`{}`)}); err != nil {
				t.Fatal(err)
			}
			if err := m.RequestStop(ctx, k); err != nil {
				t.Fatal(err)
			}
			if r, ok := m.(*Redis); ok {
				peer := NewRedis(r.client, r.prefix, ttl)
				rows, _, err := peer.GetEvents(ctx, k, 0)
				if len(rows) != 1 || err != nil {
					t.Fatal("peer read", rows, err)
				}
				if requested, err := peer.StopRequested(ctx, k); err != nil || !requested {
					t.Fatal("peer stop read", requested, err)
				}
				if typ := r.client.Type(ctx, r.keys(k)[0]).Val(); typ != "list" {
					t.Fatal(typ)
				}
			}
			time.Sleep(2 * ttl)
			if _, _, err := m.GetEvents(ctx, k, 0); !errors.Is(err, ErrHistory) {
				t.Fatal("event retained", err)
			}
			if requested, err := m.StopRequested(ctx, k); err != nil || requested {
				t.Fatal("stop retained", requested, err)
			}
			if live, err := m.GetLiveRun(ctx, k.SessionID); err != nil || live != (LiveRun{}) {
				t.Fatal("live retained", live, err)
			}
		})
	}
}
func TestStreamRejectsOversizeAndInvalidInputs(t *testing.T) {
	managers(t, func(t *testing.T, m Manager) {
		ctx := context.Background()
		k := testKey
		for _, e := range []Event{{Sequence: 0, Type: "x", Data: json.RawMessage(`{}`)}, {Sequence: 1, Type: "x\ninjected", Data: json.RawMessage(`{}`)}, {Sequence: 1, Type: "x", Data: json.RawMessage(`no`)}, {Sequence: 1, Type: "x", Data: json.RawMessage(`"` + strings.Repeat("x", maxEventBytes) + `"`)}} {
			if err := m.AppendEvent(ctx, k, e); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
		}
		for _, invalid := range []Key{{}, {"session", "run", 0}, {"session:other", "run", 1}} {
			if err := m.RequestStop(ctx, invalid); !errors.Is(err, ErrInvalid) {
				t.Fatal("invalid stop write", err)
			}
			if _, err := m.StopRequested(ctx, invalid); !errors.Is(err, ErrInvalid) {
				t.Fatal("invalid stop read", err)
			}
		}
	})
}

func TestStreamIdlePollingRefreshesOnlyMatchingLiveRun(t *testing.T) {
	managers(t, func(t *testing.T, m Manager) {
		ctx := context.Background()
		k := testKey
		l := LiveRun{"run", 1, "req"}
		if err := m.SetLiveRun(ctx, k.SessionID, l); err != nil {
			t.Fatal(err)
		}
		shorten := func() {
			switch backend := m.(type) {
			case *Memory:
				backend.mu.Lock()
				v := backend.live[k.SessionID]
				v.expires = time.Now().Add(time.Second)
				backend.live[k.SessionID] = v
				backend.mu.Unlock()
			case *Redis:
				if err := backend.client.PExpire(ctx, backend.liveKey(k.SessionID), time.Second).Err(); err != nil {
					t.Fatal(err)
				}
			}
		}
		remaining := func() time.Duration {
			switch backend := m.(type) {
			case *Memory:
				backend.mu.Lock()
				defer backend.mu.Unlock()
				return time.Until(backend.live[k.SessionID].expires)
			case *Redis:
				return backend.client.PTTL(ctx, backend.liveKey(k.SessionID)).Val()
			}
			return 0
		}
		shorten()
		if requested, err := m.StopRequested(ctx, k); err != nil || requested {
			t.Fatal(err)
		}
		if remaining() < time.Minute {
			t.Fatal("stop polling did not renew live marker")
		}
		successor := l
		successor.Generation = 2
		if err := m.ClaimLiveRun(ctx, k.SessionID, l, successor); err != nil {
			t.Fatal(err)
		}
		shorten()
		if requested, err := m.StopRequested(ctx, k); err != nil || requested {
			t.Fatal(err)
		}
		if remaining() > time.Second {
			t.Fatal("old generation renewed successor")
		}
	})
}
