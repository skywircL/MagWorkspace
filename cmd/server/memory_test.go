package main

import (
	"testing"
	"time"
)

func TestConfigureMemoryCapture(t *testing.T) {
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("REDIS_PASSWORD", "fixture-secret")
	opts, err := configureMemoryCapture()
	if err != nil || opts.Queue != nil || opts.JobTimeout != 30*time.Second || opts.PollInterval != 2*time.Second {
		t.Fatal("missing local polling defaults", err)
	}
	t.Setenv("DOCUMENT_INDEXING_ENABLED", "false")
	t.Setenv("REDIS_ADDR", "127.0.0.1:1")
	opts, err = configureMemoryCapture()
	if err != nil || opts.Queue == nil {
		t.Fatal("memory queue depends on document indexing or live Redis", err)
	}
	defer opts.Queue.Close()
	if err := opts.Queue.Validate(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REDIS_ADDR", "redis://fixture-secret@invalid")
	opts, err = configureMemoryCapture()
	if err == nil || err.Error() != "invalid Redis memory queue configuration" || opts.Queue != nil {
		t.Fatal("invalid queue configuration did not fail safely")
	}
}
