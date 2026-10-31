package store

import (
	"context"
	"testing"
	"time"
)

func TestMySQLMemoryCaptureBusyRetainsBudgetAndAtomicDelivery(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "capture-busy")
	_, _, job := completeCaptureRun(t, s, u.ID, w.ID, "I prefer concise answers")
	original := captureDelivery(t, s, job.ID)
	_, err := s.pool.ExecContext(ctx, `CREATE TRIGGER capture_busy_failure BEFORE UPDATE ON memory_capture_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced busy failure'`)
	must(t, err)
	if s.DeferMemoryCaptureBusy(ctx, job) == nil {
		t.Fatal("busy outbox failure was ignored")
	}
	captureStatus(t, s, job, "running", 0)
	if captureDelivery(t, s, job.ID) != original {
		t.Fatal("failed deferral changed delivery")
	}
	_, err = s.pool.ExecContext(ctx, `DROP TRIGGER capture_busy_failure`)
	must(t, err)
	for i := 0; i < 4; i++ {
		prior := captureDelivery(t, s, job.ID)
		must(t, s.DeferMemoryCaptureBusy(ctx, job))
		captureStatus(t, s, job, "queued", 0)
		next := captureDelivery(t, s, job.ID)
		var attempts int
		var delayed bool
		must(t, s.pool.QueryRowContext(ctx, `SELECT attempt,next_attempt_at>UTC_TIMESTAMP(6) FROM memory_capture_jobs WHERE run_id=?`, job.ID).Scan(&attempts, &delayed))
		if attempts != 0 || !delayed || next.ID == prior.ID {
			t.Fatal("admission refusal consumed budget or lost fenced backoff")
		}
		must(t, s.DeferMemoryCaptureBusy(ctx, job))
		if captureDelivery(t, s, job.ID) != next {
			t.Fatal("duplicate busy receipt reset delivery")
		}
		_, claimed, err := s.ClaimMemoryCaptureDelivery(ctx, next.JobID, next.ID, time.Minute)
		must(t, err)
		if claimed {
			t.Fatal("busy deferral bypassed its cooldown")
		}
		captureDeliveryDue(t, s, job.ID)
		current := claimCaptureDelivery(t, s, next)
		must(t, s.DeferMemoryCaptureBusy(ctx, job))
		captureStatus(t, s, current, "running", 0)
		if current.Attempt != 1 || current.Generation <= job.Generation {
			t.Fatal("busy retry did not retain fresh execution fencing")
		}
		job = current
	}
	// Ordinary failures still consume the original three-attempt budget.
	for attempt := 1; attempt <= 3; attempt++ {
		if job.Attempt != attempt {
			t.Fatal("failed model budget changed")
		}
		must(t, s.FailMemoryCapture(ctx, job, "extraction_failed"))
		if attempt < 3 {
			job = claimCaptureDelivery(t, s, captureDeliveryDue(t, s, job.ID))
		}
	}
	captureStatus(t, s, job, "failed", 0)
	assertCaptureOutboxCount(t, s, job.ID, 0)
	must(t, s.DeferMemoryCaptureBusy(ctx, job))
	assertCaptureOutboxCount(t, s, job.ID, 0)
}

func TestMySQLMemoryCaptureBusyCannotReviveInvalidatedClaim(t *testing.T) {
	for _, action := range []string{"clear", "disable", "revoke", "expired"} {
		t.Run(action, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := context.Background()
			owner, w := testUser(t, s, "busy-owner")
			member, _ := testUser(t, s, "busy-member")
			_, err := s.AddMember(ctx, owner.ID, w.ID, member.Email, "viewer")
			must(t, err)
			_, _, job := completeCaptureRun(t, s, member.ID, w.ID, "I prefer concise answers")
			before := captureDelivery(t, s, job.ID)
			switch action {
			case "clear":
				must(t, s.ClearMemories(ctx, member.ID, w.ID))
			case "disable":
				no := false
				_, err = s.MemorySettings(ctx, member.ID, w.ID, &no)
				must(t, err)
			case "revoke":
				must(t, s.DeleteMember(ctx, owner.ID, w.ID, member.ID))
			case "expired":
				_, err = s.pool.ExecContext(ctx, `UPDATE memory_capture_jobs SET lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE run_id=?`, job.ID)
				must(t, err)
			}
			must(t, s.DeferMemoryCaptureBusy(ctx, job))
			if action == "expired" {
				if captureDelivery(t, s, job.ID) != before {
					t.Fatal("expired busy result rotated delivery")
				}
				var attempts int
				must(t, s.pool.QueryRowContext(ctx, `SELECT attempt FROM memory_capture_jobs WHERE run_id=?`, job.ID).Scan(&attempts))
				if attempts != 1 {
					t.Fatal("expired busy result refunded unknown attempt")
				}
			} else {
				assertCaptureOutboxCount(t, s, job.ID, 0)
			}
		})
	}
}
