// Package indexqueue scopes the shared task transport to an immutable index profile.
package indexqueue

import (
	"encoding/hex"
	"time"

	"magworkspace/internal/taskqueue"
)

// Queue keeps the index transport API while sharing its implementation with
// other durable outbox workers.
type Queue = taskqueue.Queue

var (
	ErrUnavailable   = taskqueue.ErrUnavailable
	ErrInvalidConfig = taskqueue.ErrInvalidConfig
	ErrInvalidTask   = taskqueue.ErrInvalidTask
	ErrClosed        = taskqueue.ErrClosed
	ErrStarted       = taskqueue.ErrStarted
	ErrHandler       = taskqueue.ErrHandler
	ErrHandlerPanic  = taskqueue.ErrHandlerPanic
)

const taskType = "magworkspace:index"

// New preserves the existing profile queue name and wire task type. Invalid
// profiles produce an invalid queue without connecting to Redis.
func New(addr, password, profile string, workers int, timeout time.Duration) *Queue {
	return taskqueue.New(addr, password, profileQueueName(profile), taskType, workers, timeout)
}

func profileQueueName(profile string) string {
	value, err := hex.DecodeString(profile)
	if err != nil || len(value) != 32 || hex.EncodeToString(value) != profile {
		return ""
	}
	return "mw-index-" + profile
}
