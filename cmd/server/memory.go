package main

import (
	"errors"
	"os"
	"time"

	"magworkspace/internal/memorycapture"
	"magworkspace/internal/taskqueue"
)

// Memory has its own bounded consumer, independent of document indexing.
func configureMemoryCapture() (memorycapture.Options, error) {
	opts := memorycapture.Options{JobTimeout: 30 * time.Second, PollInterval: 2 * time.Second}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		queue := taskqueue.New(addr, os.Getenv("REDIS_PASSWORD"), "mw-memory-capture", "magworkspace:memory-capture", 1, opts.JobTimeout)
		if err := queue.Validate(); err != nil {
			queue.Close()
			return opts, errors.New("invalid Redis memory queue configuration")
		}
		opts.Queue = queue
	}
	return opts, nil
}
