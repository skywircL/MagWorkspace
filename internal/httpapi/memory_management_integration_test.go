package httpapi_test

import (
	"encoding/json"
	"magworkspace/internal/domain"
	"magworkspace/internal/httpapi"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHTTPMemoryManagementUpdateSettingsExportClearIsolation(t *testing.T) {
	f := newIntegrationFixture(t, httpapi.Config{AllowRegistration: true})
	owner := f.register(t, "manage-owner")
	other := f.register(t, "manage-other")
	wid := owner.Workspaces[0].ID
	f.request(t, "POST", workspacePath(wid)+"/members", owner.Token, map[string]string{"email": other.User.Email, "role": "viewer"}, 201)
	base := memoryPath(wid)
	settings := workspacePath(wid) + "/memory-settings"
	input := map[string]any{"kind": "fact", "content": "owner private content", "topic": "database"}
	item := integrationData[domain.Memory](t, f.request(t, "POST", base, owner.Token, input, 201))
	f.request(t, "PUT", base+"/"+item.ID, other.Token, input, 404)
	f.request(t, "PUT", settings, owner.Token, map[string]any{}, 400)
	f.request(t, "PUT", settings, owner.Token, map[string]any{"enabled": nil}, 400)
	f.request(t, "PUT", settings, owner.Token, map[string]any{"enabled": false, "user_id": other.User.ID}, 400)
	f.request(t, "PUT", settings, owner.Token, map[string]any{"enabled": false}, 200)
	f.restart(t)
	own := integrationData[domain.MemorySettings](t, f.request(t, "GET", settings, owner.Token, nil, 200))
	theirs := integrationData[domain.MemorySettings](t, f.request(t, "GET", settings, other.Token, nil, 200))
	if own.Enabled || !theirs.Enabled {
		t.Fatal("private settings isolation/restart")
	}
	input["content"] = "owner updated content"
	updated := integrationData[domain.Memory](t, f.request(t, "PUT", base+"/"+item.ID, owner.Token, input, 200))
	if updated.ID == item.ID {
		t.Fatal("edit overwrote history")
	}
	f.request(t, "PUT", base+"/"+item.ID, owner.Token, input, 409)
	all := integrationData[[]domain.Memory](t, f.request(t, "GET", base+"/export", owner.Token, nil, 200))
	if len(all) != 2 {
		t.Fatal("export missing history")
	}
	theirsItems := integrationData[[]domain.Memory](t, f.request(t, "GET", base+"/export", other.Token, nil, 200))
	if len(theirsItems) != 0 {
		t.Fatal("export leaked other user")
	}
	f.request(t, "DELETE", base, owner.Token, map[string]any{"confirm": false}, 400)
	f.request(t, "DELETE", base, other.Token, map[string]any{"confirm": true}, 204)
	f.request(t, "DELETE", base, owner.Token, map[string]any{"confirm": true}, 204)
	f.request(t, "POST", base, owner.Token, input, 409)
	f.request(t, "PUT", base+"/"+updated.ID, owner.Token, input, 409)
	f.request(t, "DELETE", workspacePath(wid)+"/members/"+other.User.ID, owner.Token, nil, 204)
	for _, p := range []string{settings, base + "/export"} {
		f.request(t, "GET", p, other.Token, nil, 404)
	}
	f.request(t, "PUT", settings, other.Token, map[string]any{"enabled": true}, 404)
	f.request(t, "DELETE", base, other.Token, map[string]any{"confirm": true}, 404)
}

func TestHTTPMemoryFastAPIManagementRecallAndRestart(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "memory-http-management")
	wid := owner.Workspaces[0].ID
	original := "数据库使用 ＭＹＳＱＬ 8.4"
	replacement := "数据库使用 ＭＹＳＱＬ 9.0"
	item := integrationData[domain.Memory](t, x.request(t, "POST", memoryPath(wid), owner.Token, map[string]any{"kind": "fact", "content": original}, 201))
	var phase atomic.Int32
	var calls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := harnessHTTPModelInput(t, r)
		calls.Add(1)
		visible, _ := json.Marshal(payload.Messages)
		text := string(visible)
		stage := phase.Load()
		wantOriginal := stage == 0
		wantNew := stage == 2
		if strings.Contains(text, original) != wantOriginal || strings.Contains(text, replacement) != wantNew {
			t.Errorf("memory injection phase=%d original=%v replacement=%v", stage, strings.Contains(text, original), strings.Contains(text, replacement))
		}
		if len(payload.Messages) > 0 && payload.Messages[len(payload.Messages)-1].Role == "tool" {
			result := payload.Messages[len(payload.Messages)-1].Content
			if strings.Contains(result, original) != wantOriginal || strings.Contains(result, replacement) != wantNew {
				t.Errorf("search_memory stale at phase %d", stage)
			}
			harnessHTTPModelReply(w, payload, "已完成记忆检查。", "", "")
		} else {
			harnessHTTPModelReply(w, payload, "", "search_memory", `{"query":"mysql"}`)
		}
	}))
	defer provider.Close()
	callback := "http://" + harnessHTTPAddress(t)
	service := newHarnessHTTPService(t, provider.URL, callback)
	client := harnessHTTPClient(t, service.url, callback)
	installHarnessHTTPClient(x, client)
	for stage := 0; stage < 4; stage++ {
		phase.Store(int32(stage))
		if stage == 1 {
			x.request(t, "PUT", workspacePath(wid)+"/memory-settings", owner.Token, map[string]bool{"enabled": false}, 200)
		}
		if stage == 2 {
			x.request(t, "PUT", memoryPath(wid)+"/"+item.ID, owner.Token, map[string]any{"kind": "fact", "content": replacement}, 200)
			x.request(t, "PUT", workspacePath(wid)+"/memory-settings", owner.Token, map[string]bool{"enabled": true}, 200)
			service.stop()
			service.start()
			x.restart(t)
			installHarnessHTTPClient(x, client)
		}
		if stage == 3 {
			x.request(t, "DELETE", memoryPath(wid), owner.Token, map[string]bool{"confirm": true}, 204)
		}
		session := x.session(t, owner, wid, "memory lifecycle")
		if stage%2 == 0 {
			x.request(t, "POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "mysql"}, 201)
		} else {
			harnessHTTPCompletedSSE(t, x, sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "mysql"})
		}
	}
	if calls.Load() != 8 {
		t.Fatalf("expected all memory search callbacks: %d", calls.Load())
	}
}
