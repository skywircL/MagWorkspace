package indexing

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"magworkspace/internal/domain"
)

type cleanupStore interface {
	RequeueRetiredCleanup(context.Context, string, ...string) error
	ClaimIndexCleanup(context.Context, []string, int, time.Duration) ([]domain.IndexCleanup, error)
	CompleteIndexCleanup(context.Context, string, string) error
	DeferIndexCleanup(context.Context, string, string, string, time.Duration) error
}

// Cleaner owns retired vectors independently from new indexing or retrieval.
// Storage IDs resolve only to explicitly configured clients; there is no
// fallback to the active indexing endpoint for an unknown binding.
type Cleaner struct {
	db         cleanupStore
	stores     map[string]SearchStore
	storageIDs []string
}

func NewCleaner(db cleanupStore, stores []SearchStore) *Cleaner {
	c := &Cleaner{db: db, stores: make(map[string]SearchStore)}
	for _, client := range stores {
		if client != nil && hexID(client.StorageID(), 64) {
			c.stores[client.StorageID()] = client
		}
	}
	for id := range c.stores {
		c.storageIDs = append(c.storageIDs, id)
	}
	sort.Strings(c.storageIDs)
	return c
}

func (c *Cleaner) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := c.CleanupOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("index cleanup pending", "code", cleanupErrorCode(err))
		}
		if !pause(ctx, 30*time.Second) {
			return
		}
	}
}

func (c *Cleaner) CleanupOnce(ctx context.Context) error {
	if err := c.db.RequeueRetiredCleanup(ctx, "index", c.storageIDs...); err != nil {
		return err
	}
	var firstErr error
	for range 16 {
		// Claim immediately before I/O, so earlier slow deletes cannot exhaust
		// a later item's lease. A crashed worker is reclaimed after 30 seconds.
		items, err := c.db.ClaimIndexCleanup(ctx, c.storageIDs, 1, 30*time.Second)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			break
		}
		item := items[0]
		client := c.stores[item.StorageID]
		if client == nil {
			err = &Failure{Code: "storage_binding_unavailable"}
		} else {
			cleanCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			err = client.DeleteGeneration(cleanCtx, item.ProfileID, item.Generation)
			cancel()
		}
		// Persist the outcome even if shutdown cancelled the provider call.
		persistCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err == nil {
			err = c.db.CompleteIndexCleanup(persistCtx, item.ID, item.ClaimToken)
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			delay := 30 * time.Second
			if client == nil {
				delay = 5 * time.Minute
			}
			if deferred := c.db.DeferIndexCleanup(persistCtx, item.ID, item.ClaimToken, cleanupErrorCode(err), delay); deferred != nil {
				cancel()
				return deferred
			}
		}
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return firstErr
}

func cleanupErrorCode(err error) string {
	switch code := ErrorCode(err); code {
	case "storage_binding_unavailable", "qdrant_invalid", "qdrant_unavailable", "index_invalid", "index_unavailable", "processing_timeout", "processing_canceled":
		return code
	default:
		return "cleanup_failed"
	}
}
