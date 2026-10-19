package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"magworkspace/internal/domain"
)

// Every test owns a randomly named database. The configured database and all
// pre-existing databases are untouched; the test account needs CREATE DATABASE.
func testStore(t *testing.T) (*Store, string) {
	t.Helper()
	dsn := os.Getenv("MAGWORKSPACE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MySQL integration skipped: MAGWORKSPACE_TEST_DATABASE_URL is not set")
	}
	cfg, err := mysql.ParseDSN(dsn)
	must(t, err)
	cfg.DBName = ""
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	must(t, err)
	admin.SetMaxOpenConns(2)
	suffix, err := newID()
	must(t, err)
	name := "mw_test_" + suffix
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err = admin.ExecContext(ctx, "CREATE DATABASE `"+name+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		admin.Close()
		t.Fatalf("create isolated test database: %v", err)
	}
	cfg.DBName = name
	isolated := cfg.FormatDSN()
	var st *Store
	t.Cleanup(func() {
		if st != nil {
			st.Close()
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(cleanupCtx, "DROP DATABASE `"+name+"`"); err != nil {
			t.Errorf("drop owned test database: %v", err)
		}
		admin.Close()
	})
	st, err = Open(ctx, isolated)
	must(t, err)
	must(t, st.Migrate(ctx))
	return st, isolated
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func wantError(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}
func testUser(t *testing.T, s *Store, name string) (domain.User, domain.Workspace) {
	t.Helper()
	u, w, err := s.CreateUser(context.Background(), name, name+"@example.com", "opaque-password-hash")
	must(t, err)
	return u, w
}
func testAuth(t *testing.T, userID string) domain.AuthSession {
	t.Helper()
	id, err := newID()
	must(t, err)
	now := time.Now().UTC()
	return domain.AuthSession{ID: id, UserID: userID, AccessHash: "a-" + id, RefreshHash: "r-" + id, AccessExpiresAt: now.Add(time.Hour), RefreshExpiresAt: now.Add(24 * time.Hour)}
}
func expireTestRun(t *testing.T, s *Store, id string) {
	t.Helper()
	_, err := s.pool.ExecContext(context.Background(), `UPDATE runs SET lease_expires_at=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 SECOND) WHERE id=?`, id)
	must(t, err)
}

func TestMySQLMigrationsAndReconnect(t *testing.T) {
	s, dsn := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "alice")
	session, err := s.CreateSession(ctx, u.ID, w.ID, "persistent chat")
	must(t, err)
	run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, session.ID, "remember me", time.Minute)
	must(t, err)
	must(t, s.FinishRun(ctx, u.ID, w.ID, session.ID, run.ID, "remembered", "test"))
	s.Close()
	reopened, err := Open(ctx, dsn)
	must(t, err)
	defer reopened.Close()
	must(t, reopened.Migrate(ctx))
	loaded, err := reopened.User(ctx, u.ID)
	must(t, err)
	if loaded.Email != u.Email {
		t.Fatalf("user lost after reconnect: %+v", loaded)
	}
	workspace, err := reopened.Workspace(ctx, u.ID, w.ID)
	must(t, err)
	if workspace.Role != "owner" {
		t.Fatalf("owner lost: %+v", workspace)
	}
	messages, total, err := reopened.Messages(ctx, u.ID, w.ID, session.ID, 20, 0)
	must(t, err)
	if total != 2 || len(messages) != 2 || messages[0].Content != "remember me" || messages[1].Content != "remembered" || messages[1].Status != "completed" {
		t.Fatalf("messages after reconnect: total %d, %+v", total, messages)
	}
	_, err = reopened.pool.ExecContext(ctx, `UPDATE schema_version SET version=? WHERE singleton=1`, len(migrationFiles)+1)
	must(t, err)
	if err = reopened.Migrate(ctx); err == nil {
		t.Fatal("newer schema accepted")
	}
	var version int
	must(t, reopened.pool.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE singleton=1`).Scan(&version))
	if version != len(migrationFiles)+1 {
		t.Fatal("newer schema downgraded")
	}
	_, err = reopened.pool.ExecContext(ctx, `UPDATE schema_version SET version=? WHERE singleton=1`, len(migrationFiles))
	must(t, err)
	// Recover the version marker after a simulated interrupted initial migration.
	_, err = reopened.pool.ExecContext(ctx, `DELETE FROM schema_version`)
	must(t, err)
	must(t, reopened.Migrate(ctx))
	loaded, err = reopened.User(ctx, u.ID)
	must(t, err)
	if loaded.ID != u.ID {
		t.Fatal("resumable migration lost data")
	}
}

func TestMySQLSignupAndRefreshTransactions(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() { <-start; _, _, err := s.CreateUser(ctx, "Alice", "ALICE@example.com", "hash"); results <- err }()
	}
	close(start)
	successes, conflicts := 0, 0
	for range 2 {
		err := <-results
		if err == nil {
			successes++
		} else if errors.Is(err, domain.ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent signup results %d success %d conflict", successes, conflicts)
	}
	var users, workspaces, members int
	must(t, s.pool.QueryRowContext(ctx, `SELECT count(*) FROM users`).Scan(&users))
	must(t, s.pool.QueryRowContext(ctx, `SELECT count(*) FROM workspaces`).Scan(&workspaces))
	must(t, s.pool.QueryRowContext(ctx, `SELECT count(*) FROM workspace_members`).Scan(&members))
	if users != 1 || workspaces != 1 || members != 1 {
		t.Fatalf("partial signup rows: users %d workspaces %d members %d", users, workspaces, members)
	}
	u, hash, err := s.UserByEmail(ctx, "Alice@example.com")
	must(t, err)
	if hash != "hash" {
		t.Fatal("password hash missing")
	}
	auth := testAuth(t, u.ID)
	must(t, s.CreateAuth(ctx, auth))
	_, err = s.Authenticate(ctx, auth.AccessHash)
	must(t, err)
	next1, next2 := testAuth(t, "forged-user"), testAuth(t, "forged-user")
	type refreshed struct {
		a   domain.AuthSession
		err error
	}
	refreshes := make(chan refreshed, 2)
	start = make(chan struct{})
	for _, next := range []domain.AuthSession{next1, next2} {
		go func(next domain.AuthSession) {
			<-start
			a, err := s.RotateAuth(ctx, auth.RefreshHash, next)
			refreshes <- refreshed{a, err}
		}(next)
	}
	close(start)
	var winner domain.AuthSession
	successes = 0
	for range 2 {
		v := <-refreshes
		if v.err == nil {
			winner = v.a
			successes++
		} else {
			wantError(t, v.err, domain.ErrUnauthorized)
		}
	}
	if successes != 1 || winner.ID != auth.ID || winner.UserID != u.ID {
		t.Fatalf("refresh identity or concurrency failed: %+v, success %d", winner, successes)
	}
	_, err = s.Authenticate(ctx, auth.AccessHash)
	wantError(t, err, domain.ErrUnauthorized)
	_, err = s.RotateAuth(ctx, auth.RefreshHash, testAuth(t, u.ID))
	wantError(t, err, domain.ErrUnauthorized)
	_, err = s.Authenticate(ctx, winner.AccessHash)
	must(t, err)
	must(t, s.DeleteAuth(ctx, winner.ID))
	_, err = s.Authenticate(ctx, winner.AccessHash)
	wantError(t, err, domain.ErrUnauthorized)
	expired := testAuth(t, u.ID)
	expired.AccessExpiresAt = time.Now().Add(-time.Hour)
	expired.RefreshExpiresAt = time.Now().Add(-time.Minute)
	must(t, s.CreateAuth(ctx, expired))
	_, err = s.Authenticate(ctx, expired.AccessHash)
	wantError(t, err, domain.ErrUnauthorized)
	_, err = s.RotateAuth(ctx, expired.RefreshHash, testAuth(t, u.ID))
	wantError(t, err, domain.ErrUnauthorized)
}

func TestMySQLMembershipOwnerInvariant(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	alice, w := testUser(t, s, "alice")
	bob, _ := testUser(t, s, "bob")
	carol, _ := testUser(t, s, "carol")
	dave, _ := testUser(t, s, "dave")
	_, err := s.AddMember(ctx, alice.ID, w.ID, bob.Email, "owner")
	must(t, err)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, u := range []domain.User{alice, bob} {
		go func(id string) { <-start; results <- s.UpdateMember(ctx, id, w.ID, id, "viewer") }(u.ID)
	}
	close(start)
	success := 0
	for range 2 {
		err := <-results
		if err == nil {
			success++
		} else {
			wantError(t, err, domain.ErrConflict)
		}
	}
	if success != 1 {
		t.Fatalf("both owners removed or both blocked: %d successes", success)
	}
	members, err := s.Members(ctx, alice.ID, w.ID)
	must(t, err)
	owner := ""
	owners := 0
	for _, m := range members {
		if m.Role == "owner" {
			owner = m.UserID
			owners++
		}
	}
	if owners != 1 {
		t.Fatalf("owner invariant broken: %+v", members)
	}
	wantError(t, s.DeleteMember(ctx, owner, w.ID, owner), domain.ErrConflict)
	_, err = s.AddMember(ctx, owner, w.ID, carol.Email, "admin")
	must(t, err)
	_, err = s.AddMember(ctx, carol.ID, w.ID, dave.Email, "owner")
	wantError(t, err, domain.ErrForbidden)
	wantError(t, s.UpdateMember(ctx, carol.ID, w.ID, owner, "viewer"), domain.ErrForbidden)
	_, err = s.AddMember(ctx, carol.ID, w.ID, dave.Email, "contributor")
	must(t, err)
	wantError(t, s.UpdateMember(ctx, carol.ID, w.ID, dave.ID, "admin"), domain.ErrForbidden)
	_, err = s.UpdateWorkspace(ctx, dave.ID, w.ID, "not allowed", "")
	wantError(t, err, domain.ErrForbidden)
	updated, err := s.UpdateWorkspace(ctx, carol.ID, w.ID, "renamed", "description")
	must(t, err)
	if updated.Name != "renamed" || updated.Role != "admin" {
		t.Fatalf("workspace update: %+v", updated)
	}
	session, err := s.CreateSession(ctx, dave.ID, w.ID, "running member")
	must(t, err)
	run, _, role, err := s.BeginRun(ctx, dave.ID, w.ID, session.ID, "work", time.Minute)
	must(t, err)
	if role != "contributor" {
		t.Fatal("role not authoritative")
	}
	wantError(t, s.DeleteMember(ctx, owner, w.ID, dave.ID), domain.ErrConflict)
	wantError(t, s.UpdateMember(ctx, owner, w.ID, dave.ID, "viewer"), domain.ErrConflict)
	wantError(t, s.DeleteWorkspace(ctx, owner, w.ID), domain.ErrConflict)
	must(t, s.FailRun(ctx, dave.ID, w.ID, session.ID, run.ID, "canceled"))
	must(t, s.UpdateMember(ctx, carol.ID, w.ID, dave.ID, "viewer"))
	run, _, role, err = s.BeginRun(ctx, dave.ID, w.ID, session.ID, "read", time.Minute)
	must(t, err)
	if role != "viewer" {
		t.Fatal("stale membership role")
	}
	must(t, s.FailRun(ctx, dave.ID, w.ID, session.ID, run.ID, "canceled"))
	must(t, s.DeleteMember(ctx, carol.ID, w.ID, dave.ID))
	_, err = s.Session(ctx, dave.ID, w.ID, session.ID)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.AddMember(ctx, owner, w.ID, dave.Email, "viewer")
	must(t, err)
	_, err = s.Session(ctx, dave.ID, w.ID, session.ID)
	must(t, err)
	must(t, s.DeleteWorkspace(ctx, owner, w.ID))
	var sessions int
	must(t, s.pool.QueryRowContext(ctx, `SELECT count(*) FROM sessions WHERE workspace_id=?`, w.ID).Scan(&sessions))
	if sessions != 0 {
		t.Fatal("workspace did not cascade sessions")
	}
}

func TestMySQLPrivateSessionAndRunLifecycle(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	alice, w := testUser(t, s, "alice")
	bob, _ := testUser(t, s, "bob")
	_, err := s.AddMember(ctx, alice.ID, w.ID, bob.Email, "owner")
	must(t, err)
	other, err := s.CreateWorkspace(ctx, alice.ID, "other", "")
	must(t, err)
	session, err := s.CreateSession(ctx, alice.ID, w.ID, "private")
	must(t, err)
	run, history, role, err := s.BeginRun(ctx, alice.ID, w.ID, session.ID, "first", time.Minute)
	must(t, err)
	if len(history) != 0 || role != "owner" || run.Status != "running" {
		t.Fatalf("begin: %+v history %+v role %s", run, history, role)
	}
	_, err = s.Session(ctx, bob.ID, w.ID, session.ID)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.Session(ctx, alice.ID, other.ID, session.ID)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.UpdateSession(ctx, bob.ID, w.ID, session.ID, "hijack")
	wantError(t, err, domain.ErrNotFound)
	wantError(t, s.DeleteSession(ctx, bob.ID, w.ID, session.ID), domain.ErrNotFound)
	_, _, err = s.Messages(ctx, bob.ID, w.ID, session.ID, 10, 0)
	wantError(t, err, domain.ErrNotFound)
	_, err = s.Run(ctx, bob.ID, w.ID, session.ID, run.ID)
	wantError(t, err, domain.ErrNotFound)
	_, _, _, err = s.BeginRun(ctx, bob.ID, w.ID, session.ID, "hijack", time.Minute)
	wantError(t, err, domain.ErrNotFound)
	wantError(t, s.FinishRun(ctx, bob.ID, w.ID, session.ID, run.ID, "hijack", "test"), domain.ErrNotFound)
	wantError(t, s.FailRun(ctx, bob.ID, w.ID, session.ID, run.ID, "hijack"), domain.ErrNotFound)
	list, total, err := s.Sessions(ctx, bob.ID, w.ID, 10, 0)
	must(t, err)
	if total != 0 || len(list) != 0 {
		t.Fatal("private sessions leaked in list")
	}
	wantError(t, s.DeleteSession(ctx, alice.ID, w.ID, session.ID), domain.ErrConflict)
	must(t, s.FinishRun(ctx, alice.ID, w.ID, session.ID, run.ID, "answer", "test"))
	wantError(t, s.FinishRun(ctx, alice.ID, w.ID, session.ID, run.ID, "duplicate", "test"), domain.ErrConflict)
	wantError(t, s.FailRun(ctx, alice.ID, w.ID, session.ID, run.ID, "late"), domain.ErrConflict)
	failed, history, _, err := s.BeginRun(ctx, alice.ID, w.ID, session.ID, "failed query", time.Minute)
	must(t, err)
	if len(history) != 2 || history[0].Content != "first" || history[1].Content != "answer" {
		t.Fatalf("completed history: %+v", history)
	}
	must(t, s.FailRun(ctx, alice.ID, w.ID, session.ID, failed.ID, "bridge_failed"))
	stale, history, _, err := s.BeginRun(ctx, alice.ID, w.ID, session.ID, "interrupted", time.Minute)
	must(t, err)
	if len(history) != 2 {
		t.Fatal("failed turn entered context")
	}
	expireTestRun(t, s, stale.ID)
	next, history, _, err := s.BeginRun(ctx, alice.ID, w.ID, session.ID, "after restart", time.Minute)
	must(t, err)
	if len(history) != 2 {
		t.Fatal("expired turn entered context")
	}
	old, err := s.Run(ctx, alice.ID, w.ID, session.ID, stale.ID)
	must(t, err)
	if old.Status != "failed" || old.ErrorCode != "lease_expired" {
		t.Fatalf("stale run: %+v", old)
	}
	wantError(t, s.FinishRun(ctx, alice.ID, w.ID, session.ID, stale.ID, "late result", "test"), domain.ErrConflict)
	expireTestRun(t, s, next.ID)
	wantError(t, s.FinishRun(ctx, alice.ID, w.ID, session.ID, next.ID, "too late", "test"), domain.ErrConflict)
	old, err = s.Run(ctx, alice.ID, w.ID, session.ID, next.ID)
	must(t, err)
	if old.Status != "failed" {
		t.Fatal("expired finish left run active")
	}
	messages, total, err := s.Messages(ctx, alice.ID, w.ID, session.ID, 20, 0)
	must(t, err)
	if total != 5 || len(messages) != 5 {
		t.Fatalf("message retention: %d %+v", total, messages)
	}
	for _, m := range messages[2:] {
		if m.Status != "failed" {
			t.Fatalf("failed status not propagated: %+v", m)
		}
	}
	renamed, err := s.UpdateSession(ctx, alice.ID, w.ID, session.ID, "renamed")
	must(t, err)
	if renamed.Title != "renamed" {
		t.Fatal("rename did not persist")
	}
	must(t, s.DeleteSession(ctx, alice.ID, w.ID, session.ID))
	_, err = s.Run(ctx, alice.ID, w.ID, session.ID, run.ID)
	wantError(t, err, domain.ErrNotFound)
	var count int
	must(t, s.pool.QueryRowContext(ctx, `SELECT count(*) FROM messages WHERE session_id=?`, session.ID).Scan(&count))
	if count != 0 {
		t.Fatal("session deletion left messages")
	}
}

func TestMySQLRunConcurrencyAndHistoryBound(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "alice")
	session, err := s.CreateSession(ctx, u.ID, w.ID, "concurrent")
	must(t, err)
	type admission struct {
		run domain.Run
		err error
	}
	start := make(chan struct{})
	results := make(chan admission, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, session.ID, "one at a time", time.Minute)
			results <- admission{run, err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var active domain.Run
	success := 0
	for v := range results {
		if v.err == nil {
			active = v.run
			success++
		} else {
			wantError(t, v.err, domain.ErrConflict)
		}
	}
	if success != 1 {
		t.Fatalf("admitted %d simultaneous runs", success)
	}
	second, err := s.CreateSession(ctx, u.ID, w.ID, "independent")
	must(t, err)
	other, _, _, err := s.BeginRun(ctx, u.ID, w.ID, second.ID, "parallel session", time.Minute)
	must(t, err)
	// The database also enforces exclusivity if an accidental caller bypasses admission.
	id, err := newID()
	must(t, err)
	_, err = s.pool.ExecContext(ctx, `INSERT INTO runs(id,session_id,status,lease_expires_at) VALUES(?,?,'running',DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 MINUTE))`, id, session.ID)
	wantError(t, dbError(err), domain.ErrConflict)
	must(t, s.FailRun(ctx, u.ID, w.ID, session.ID, active.ID, "canceled"))
	must(t, s.FailRun(ctx, u.ID, w.ID, second.ID, other.ID, "canceled"))
	// Exercise the persisted 100-turn hydration limit through the real state machine.
	for i := range 101 {
		run, _, _, err := s.BeginRun(ctx, u.ID, w.ID, session.ID, fmt.Sprintf("q%d", i), time.Minute)
		must(t, err)
		must(t, s.FinishRun(ctx, u.ID, w.ID, session.ID, run.ID, fmt.Sprintf("a%d", i), "test"))
	}
	run, history, _, err := s.BeginRun(ctx, u.ID, w.ID, session.ID, "new query excluded", time.Minute)
	must(t, err)
	if len(history) != 200 || history[0].Content != "q1" || history[1].Content != "a1" || history[199].Content != "a100" {
		t.Fatalf("history bound or order: len %d first %+v last %+v", len(history), history[0], history[len(history)-1])
	}
	_, total, err := s.Messages(ctx, u.ID, w.ID, session.ID, 10, 0)
	must(t, err)
	if total != 204 {
		t.Fatalf("history truncation deleted stored messages: %d", total)
	}
	must(t, s.FailRun(ctx, u.ID, w.ID, session.ID, run.ID, "canceled"))
}

func TestMySQLConcurrentWorkspacePatch(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	u, w := testUser(t, s, "alice")
	name, description := "new name", "new description"
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() { <-start; _, err := s.PatchWorkspace(ctx, u.ID, w.ID, &name, nil); results <- err }()
	go func() { <-start; _, err := s.PatchWorkspace(ctx, u.ID, w.ID, nil, &description); results <- err }()
	close(start)
	must(t, <-results)
	must(t, <-results)
	updated, err := s.Workspace(ctx, u.ID, w.ID)
	must(t, err)
	if updated.Name != name || updated.Description != description {
		t.Fatalf("concurrent patches overwrote disjoint fields: %+v", updated)
	}
}

func TestMySQLPollingExpiresInterruptedRuns(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	alice, w := testUser(t, s, "alice")
	bob, _ := testUser(t, s, "bob")
	_, err := s.AddMember(ctx, alice.ID, w.ID, bob.Email, "owner")
	must(t, err)
	for _, poll := range []string{"messages", "run"} {
		session, err := s.CreateSession(ctx, alice.ID, w.ID, poll)
		must(t, err)
		run, _, _, err := s.BeginRun(ctx, alice.ID, w.ID, session.ID, "killed process", time.Minute)
		must(t, err)
		expireTestRun(t, s, run.ID)
		_, err = s.Run(ctx, bob.ID, w.ID, session.ID, run.ID)
		wantError(t, err, domain.ErrNotFound)
		_, _, err = s.Messages(ctx, bob.ID, w.ID, session.ID, 10, 0)
		wantError(t, err, domain.ErrNotFound)
		var status string
		must(t, s.pool.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?`, run.ID).Scan(&status))
		if status != "running" {
			t.Fatal("unauthorized polling mutated run")
		}
		if poll == "messages" {
			messages, total, err := s.Messages(ctx, alice.ID, w.ID, session.ID, 10, 0)
			must(t, err)
			if total != 1 || messages[0].Status != "failed" {
				t.Fatalf("messages poll left expired run active: %+v", messages)
			}
		}
		expired, err := s.Run(ctx, alice.ID, w.ID, session.ID, run.ID)
		must(t, err)
		if expired.Status != "failed" || expired.ErrorCode != "lease_expired" || expired.FinishedAt == nil {
			t.Fatalf("run poll left expired run active: %+v", expired)
		}
	}
}
