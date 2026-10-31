package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/domain"
)

func TestMySQLIndexCleanupDeferredHeadAllowsProgressAndPersists(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	profile := testIndexProfile()
	firstGeneration := strings.Repeat("1", 32)
	secondGeneration := strings.Repeat("2", 32)
	must(t, s.QueueIndexCleanup(ctx, profile.StorageID, profile.ID, firstGeneration))
	must(t, s.QueueIndexCleanup(ctx, profile.StorageID, profile.ID, secondGeneration))
	_, err := s.pool.ExecContext(ctx, `UPDATE index_cleanup SET not_before=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 2 SECOND) WHERE generation=?`, firstGeneration)
	must(t, err)
	expireTestIndexCleanup(t, s, secondGeneration)
	pending, err := s.PendingIndexCleanup(ctx, 1)
	must(t, err)
	if len(pending) != 1 || pending[0].Generation != firstGeneration {
		t.Fatalf("expected first cleanup at queue head: %+v", pending)
	}
	claimed, err := s.ClaimIndexCleanup(ctx, []string{profile.StorageID}, 1, time.Minute)
	must(t, err)
	firstID, firstToken := claimed[0].ID, claimed[0].ClaimToken
	must(t, s.DeferIndexCleanup(ctx, firstID, firstToken, "qdrant_unavailable", 30*time.Second))
	var deferredUntil time.Time
	var persistedID string
	must(t, s.pool.QueryRowContext(ctx, `SELECT id,not_before FROM index_cleanup WHERE generation=?`, firstGeneration).Scan(&persistedID, &deferredUntil))
	if persistedID != firstID {
		t.Fatal("deferring cleanup changed its acknowledgement token")
	}
	pending, err = s.PendingIndexCleanup(ctx, 1)
	must(t, err)
	if len(pending) != 1 || pending[0].Generation != secondGeneration {
		t.Fatalf("failed head blocked an eligible generation: %+v", pending)
	}
	claimed, err = s.ClaimIndexCleanup(ctx, []string{profile.StorageID}, 1, time.Minute)
	must(t, err)
	must(t, s.CompleteIndexCleanup(ctx, claimed[0].ID, claimed[0].ClaimToken))
	s.Close()
	reopened, err := Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	var afterRestart time.Time
	must(t, reopened.pool.QueryRowContext(ctx, `SELECT not_before FROM index_cleanup WHERE id=?`, firstID).Scan(&afterRestart))
	if !afterRestart.Equal(deferredUntil) {
		t.Fatal("restart lost the cleanup retry deadline")
	}
	pending, err = reopened.PendingIndexCleanup(ctx, 1)
	must(t, err)
	if len(pending) != 0 {
		t.Fatalf("deferred cleanup ran before its retry deadline: %+v", pending)
	}
	expireTestIndexCleanup(t, reopened, firstGeneration)
	pending, err = reopened.PendingIndexCleanup(ctx, 1)
	must(t, err)
	if len(pending) != 1 || pending[0].ID != firstID {
		t.Fatalf("deferred cleanup did not become eligible again: %+v", pending)
	}
}

func TestMySQLIndexCleanupLateFailureCannotPostponeNewRequest(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	profile := testIndexProfile()
	generation := strings.Repeat("3", 32)
	must(t, s.QueueIndexCleanup(ctx, profile.StorageID, profile.ID, generation))
	expireTestIndexCleanup(t, s, generation)
	pending, err := s.PendingIndexCleanup(ctx, 1)
	must(t, err)
	if len(pending) != 1 {
		t.Fatalf("missing cleanup request: %+v", pending)
	}
	claimed, err := s.ClaimIndexCleanup(ctx, []string{profile.StorageID}, 1, time.Minute)
	must(t, err)
	oldID, oldToken := claimed[0].ID, claimed[0].ClaimToken
	must(t, s.QueueIndexCleanup(ctx, profile.StorageID, profile.ID, generation))
	var newID string
	var protectedUntil time.Time
	must(t, s.pool.QueryRowContext(ctx, `SELECT id,not_before FROM index_cleanup WHERE generation=?`, generation).Scan(&newID, &protectedUntil))
	if newID == oldID {
		t.Fatal("requeue reused the previous acknowledgement token")
	}
	must(t, s.DeferIndexCleanup(ctx, oldID, oldToken, "qdrant_unavailable", time.Hour))
	must(t, s.CompleteIndexCleanup(ctx, oldID, oldToken))
	var currentID string
	var currentProtection time.Time
	must(t, s.pool.QueryRowContext(ctx, `SELECT id,not_before FROM index_cleanup WHERE generation=?`, generation).Scan(&currentID, &currentProtection))
	if currentID != newID || !currentProtection.Equal(protectedUntil) {
		t.Fatal("stale cleanup failure or acknowledgement modified the newer request")
	}
	must(t, s.DeferIndexCleanup(ctx, newID, oldToken, "qdrant_unavailable", time.Second))
	must(t, s.pool.QueryRowContext(ctx, `SELECT not_before FROM index_cleanup WHERE id=?`, newID).Scan(&currentProtection))
	if !currentProtection.Equal(protectedUntil) {
		t.Fatal("short retry delay shortened the existing cleanup protection")
	}
	for _, delay := range []time.Duration{0, -time.Second, time.Nanosecond} {
		wantError(t, s.DeferIndexCleanup(ctx, newID, oldToken, "qdrant_unavailable", delay), domain.ErrInvalid)
	}
	wantError(t, s.DeferIndexCleanup(ctx, "invalid", oldToken, "qdrant_unavailable", time.Second), domain.ErrInvalid)
	must(t, s.DeferIndexCleanup(ctx, strings.Repeat("f", 32), oldToken, "qdrant_unavailable", time.Second))
}
