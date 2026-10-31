package indexqueue

import (
	"context"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/taskqueue"
)

func TestIndexQueueProfileWrapper(t *testing.T) {
	profile := strings.Repeat("a", 64)
	if got := profileQueueName(profile); got != "mw-index-"+profile {
		t.Fatalf("index queue namespace changed: %q", got)
	}
	if taskType != "magworkspace:index" {
		t.Fatalf("index task type changed: %q", taskType)
	}
	q := New("localhost:6379", "", profile, 1, time.Second)
	defer q.Close()
	if err := q.Validate(); err != nil {
		t.Fatalf("valid index profile rejected: %v", err)
	}
	var shared *taskqueue.Queue = q
	if shared != q || ErrUnavailable != taskqueue.ErrUnavailable || ErrInvalidConfig != taskqueue.ErrInvalidConfig ||
		ErrInvalidTask != taskqueue.ErrInvalidTask || ErrClosed != taskqueue.ErrClosed || ErrStarted != taskqueue.ErrStarted ||
		ErrHandler != taskqueue.ErrHandler || ErrHandlerPanic != taskqueue.ErrHandlerPanic {
		t.Fatal("index wrapper changed shared queue or error identity")
	}
}

func TestIndexQueueInvalidProfilesCannotPublishOrConsume(t *testing.T) {
	for _, profile := range []string{"", "other:{queue}", "memory-capture", strings.Repeat("a", 63),
		strings.Repeat("a", 65), strings.Repeat("A", 64), strings.Repeat("g", 64)} {
		t.Run(profile, func(t *testing.T) {
			q := New("localhost:6379", "", profile, 1, time.Second)
			defer q.Close()
			if err := q.Validate(); err != ErrInvalidConfig {
				t.Fatalf("invalid profile accepted: %v", err)
			}
			if err := q.Enqueue(context.Background(), "delivery", "job"); err != ErrInvalidConfig {
				t.Fatalf("invalid profile could publish: %v", err)
			}
			if err := q.Start(context.Background(), func(context.Context, string, string) error { return nil }); err != ErrInvalidConfig {
				t.Fatalf("invalid profile could consume: %v", err)
			}
		})
	}
}
