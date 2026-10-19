package httpapi_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"magworkspace/internal/agent"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
)

func seedContextTurns(t *testing.T, fixture *integrationFixture, owner integrationAuth, session domain.Session, count int, text string) {
	t.Helper()
	for i := range count {
		run, _, _, err := fixture.store.BeginRun(context.Background(), owner.User.ID, session.WorkspaceID, session.ID, fmt.Sprintf("seed-%d-%s", i, text), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.FinishRun(context.Background(), owner.User.ID, session.WorkspaceID, session.ID, run.ID, fmt.Sprintf("reply-%d-%s", i, text), "demo"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHTTPContextHistoryRetainsMoreThan20AuthorizedTurnsAndProtectsOldSources(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "contexthistory")
	a := x.upload(t, owner, "旧文档 PRIVATE_OLD_SOURCE。")
	x.process(t)
	b := x.upload(t, owner, "新文档 CURRENT_SOURCE。")
	x.process(t)
	session := x.session(t, owner, owner.Workspaces[0].ID, "长历史与来源")
	x.agent.set(func(ctx context.Context, request agent.Request) (agent.Result, error) {
		evidence, err := acquireTestEvidence(ctx, request)
		if err != nil || len(evidence) != 1 || evidence[0].DocumentID != a.Document.ID {
			return agent.Result{}, errors.New("failed to acquire original source")
		}
		return agent.Result{RunID: request.RunID, Mode: "demo", Answer: "PRIVATE_OLD_SOURCE without a citation"}, nil
	})
	x.request(t, http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "旧文档", "document_ids": []string{a.Document.ID}}, 201)
	// Seed independent completed turns so their source bindings do not inherit A.
	seedContextTurns(t, x.integrationFixture, owner, session, 25, strings.Repeat("界", 500))
	x.agent.set(func(_ context.Context, request agent.Request) (agent.Result, error) {
		if !request.HistoryWindowed || len(request.History) != 52 {
			return agent.Result{}, fmt.Errorf("long authorized window lost older complete turns: %d", len(request.History))
		}
		if !strings.Contains(request.History[1].Content, "PRIVATE_OLD_SOURCE") {
			return agent.Result{}, errors.New("authorized old source was dropped by the old 20-turn limit")
		}
		return agent.Result{RunID: request.RunID, Mode: "demo", Answer: "authorized current answer"}, nil
	})
	x.request(t, http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "全部范围继续"}, 201)
	x.agent.set(func(_ context.Context, request agent.Request) (agent.Result, error) {
		if len(request.History) != 50 {
			return agent.Result{}, fmt.Errorf("narrower scope lost independent turns or retained source-derived turns: %d", len(request.History))
		}
		for _, message := range request.History {
			if strings.Contains(message.Content, "PRIVATE_OLD_SOURCE") || message.Content == "authorized current answer" {
				return agent.Result{}, errors.New("old direct/inherited source escaped narrower scope")
			}
		}
		return agent.Result{RunID: request.RunID, Mode: "demo", Answer: "B-only answer"}, nil
	})
	x.request(t, http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "缩小范围", "document_ids": []string{b.Document.ID}}, 201)
	x.request(t, http.MethodDelete, documentPath(a.Document), owner.Token, nil, 204)
	x.agent.set(func(_ context.Context, request agent.Request) (agent.Result, error) {
		for _, message := range request.History {
			if strings.Contains(message.Content, "PRIVATE_OLD_SOURCE") || message.Content == "authorized current answer" {
				return agent.Result{}, errors.New("deleted old source escaped history validation")
			}
		}
		return agent.Result{RunID: request.RunID, Mode: "demo", Answer: "deleted source remains unavailable"}, nil
	})
	x.request(t, http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "删除后继续"}, 201)
	stored, _, err := x.store.Messages(context.Background(), owner.User.ID, session.WorkspaceID, session.ID, 100, 0)
	if err != nil || !strings.Contains(stored[1].Content, "PRIVATE_OLD_SOURCE") {
		t.Fatal("model hydration changed durable history")
	}
}

func TestHTTPContextHistoryOmitsCompletePairsWithoutTruncatingDurableMessages(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner := f.register(t, "contextbytes")
	session := f.session(t, owner, owner.Workspaces[0].ID, "有界历史")
	seedContextTurns(t, f, owner, session, 10, strings.Repeat("a", 60000))
	f.agent.set(func(_ context.Context, request agent.Request) (agent.Result, error) {
		if !request.HistoryWindowed || len(request.History) != 8 || !strings.HasPrefix(request.History[0].Content, "seed-6-") {
			return agent.Result{}, fmt.Errorf("expected four newest complete turns within 512 KiB, got %d", len(request.History))
		}
		for i, message := range request.History {
			if len(message.Content) < 60000 || (i%2 == 0 && message.Role != "user") || (i%2 == 1 && message.Role != "assistant") {
				return agent.Result{}, errors.New("history pair was split or text was shortened")
			}
		}
		return agent.Result{RunID: request.RunID, Mode: "demo", Answer: "bounded"}, nil
	})
	f.request(t, http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "continue"}, 201)
	stored, total, err := f.store.Messages(context.Background(), owner.User.ID, session.WorkspaceID, session.ID, 100, 0)
	if err != nil || total != 22 || len(stored[0].Content) < 60000 {
		t.Fatal("history selection altered persisted turns")
	}
}

func TestHTTPContextHistoryBindsOnlySelectedRunsAcrossBudgetGaps(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "contextselectedsources")
	a := x.upload(t, owner, "甲住宿标准 A。")
	x.process(t)
	b := x.upload(t, owner, "乙住宿标准 B。")
	x.process(t)
	session := x.session(t, owner, owner.Workspaces[0].ID, "非连续历史来源")
	x.agent.set(func(ctx context.Context, request agent.Request) (agent.Result, error) {
		evidence, err := acquireTestEvidence(ctx, request)
		if err != nil || len(evidence) != 1 || evidence[0].DocumentID != b.Document.ID {
			return agent.Result{}, errors.New("old small turn failed to acquire B")
		}
		return agent.Result{RunID: request.RunID, Mode: "demo", Answer: "OLD_SMALL_B"}, nil
	})
	x.request(t, http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "small B question", "document_ids": []string{b.Document.ID}}, 201)
	x.agent.set(func(ctx context.Context, request agent.Request) (agent.Result, error) {
		evidence, err := acquireTestEvidence(ctx, request)
		if err != nil || len(evidence) != 1 || evidence[0].DocumentID != a.Document.ID {
			return agent.Result{}, errors.New("large turn failed to acquire A")
		}
		return agent.Result{RunID: request.RunID, Mode: "demo", Answer: "SKIPPED_LARGE_A" + strings.Repeat("a", 60000)}, nil
	})
	x.request(t, http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]any{"content": strings.Repeat("q", 60000), "document_ids": []string{a.Document.ID}}, 201)
	seedContextTurns(t, x.integrationFixture, owner, session, 4, strings.Repeat("z", 60000))
	x.agent.set(func(ctx context.Context, request agent.Request) (agent.Result, error) {
		if len(request.History) != 10 || request.History[1].Content != "OLD_SMALL_B" {
			return agent.Result{}, fmt.Errorf("expected four newest large turns and the old small B turn, got %d messages", len(request.History))
		}
		for _, message := range request.History {
			if strings.Contains(message.Content, "SKIPPED_LARGE_A") {
				return agent.Result{}, errors.New("large A-derived pair exceeded the encoded history budget")
			}
		}
		// A was not sent to the model. Its deletion must not invalidate this
		// turn through a stale positional binding to the old contiguous tail.
		if err := x.store.DeleteDocument(ctx, owner.User.ID, session.WorkspaceID, a.Document.ID); err != nil {
			return agent.Result{}, err
		}
		return agent.Result{RunID: request.RunID, Mode: "demo", Answer: "only selected history sources were bound"}, nil
	})
	x.request(t, http.MethodPost, sessionPath(session)+"/messages", owner.Token, map[string]any{"content": "continue bounded history"}, 201)
}
