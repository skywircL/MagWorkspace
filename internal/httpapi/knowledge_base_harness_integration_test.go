package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"magworkspace/internal/domain"
)

// This uses the real HTTP API, MySQL, FastAPI, Harness and callback handlers.
// Only the model provider and external object/index services are controlled fixtures.
func TestHTTPKnowledgeBaseHarnessSelectionAndApprovedCreation(t *testing.T) {
	x := newIndexFixture(t)
	owner := x.register(t, "kb-harness-owner")
	wid := owner.Workspaces[0].ID
	target := createHTTPKnowledgeBase(t, x, owner, "旅行知识库")
	other := createHTTPKnowledgeBase(t, x, owner, "其他资料")
	selected := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, target.ID, "知识库住宿规则：每晚 500 元。")
	x.process(t)
	excluded := uploadHTTPKnowledgeBase(t, x, owner.Token, wid, other.ID, "知识库 OTHER_LIBRARY_SECRET")
	x.process(t)
	session := x.session(t, owner, wid, "指定知识库工具与审批")
	const approvedContent = "知识库旅行摘要：仅保存已审批的全文。\n每晚500元。"
	var models atomic.Int32
	var citation atomic.Value
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := harnessHTTPModelInput(t, r)
		if queryUnderstandingRequest(payload) {
			harnessHTTPModelReply(w, payload, "not-json", "", "")
			return
		}
		step := models.Add(1)
		for _, message := range payload.Messages {
			if strings.Contains(message.Content, "OTHER_LIBRARY_SECRET") || strings.Contains(message.Content, excluded.Document.ID) {
				t.Error("non-selected knowledge base document entered model context")
			}
			if strings.Contains(message.Content, harnessHTTPTestKey) || strings.Contains(message.Content, owner.User.ID) {
				t.Error("model context exposed service capability or caller identity")
			}
		}
		readTool := func(name string, dst any) bool {
			for i := len(payload.Messages) - 1; i >= 0; i-- {
				message := payload.Messages[i]
				if message.Role != "tool" || message.ToolCallID != "http-"+name {
					continue
				}
				if err := json.Unmarshal([]byte(message.Content), dst); err != nil {
					t.Errorf("model step %d did not receive successful %s JSON: %s", step, name, message.Content)
					return false
				}
				return true
			}
			t.Errorf("model step %d omitted tool result %s", step, name)
			return false
		}
		switch step {
		case 1:
			harnessHTTPModelReply(w, payload, "", "list_knowledge_bases", `{"limit":20}`)
		case 2:
			var result struct {
				KnowledgeBases []httpKnowledgeBase `json:"knowledge_bases"`
			}
			readTool("list_knowledge_bases", &result)
			found := ""
			for _, kb := range result.KnowledgeBases {
				if kb.Name == target.Name {
					found = kb.ID
				}
			}
			if found != target.ID {
				t.Error("knowledge base discovery did not return chosen library")
			}
			raw, _ := json.Marshal(map[string]any{"knowledge_base_ids": []string{found}})
			harnessHTTPModelReply(w, payload, "", "list_documents", string(raw))
		case 3:
			var result struct {
				Documents []struct {
					Document domain.Document `json:"document"`
				} `json:"documents"`
			}
			if readTool("list_documents", &result) && (len(result.Documents) != 1 || result.Documents[0].Document.ID != selected.Document.ID || result.Documents[0].Document.KnowledgeBaseID != target.ID) {
				t.Error("directory tool did not preserve selected knowledge base and attribution")
			}
			raw, _ := json.Marshal(map[string]any{"query": "知识库", "mode": "keyword", "knowledge_base_ids": []string{target.ID}})
			harnessHTTPModelReply(w, payload, "", "search_knowledge", string(raw))
		case 4:
			var result struct {
				Evidence []struct {
					ID         string `json:"evidence_id"`
					DocumentID string `json:"document_id"`
					Content    string `json:"content"`
				} `json:"evidence"`
			}
			if readTool("search_knowledge", &result) && len(result.Evidence) == 1 {
				if result.Evidence[0].DocumentID != selected.Document.ID || !strings.Contains(result.Evidence[0].Content, "500") {
					t.Error("search crossed the selected library")
				}
				citation.Store(result.Evidence[0].ID)
			} else {
				t.Error("selected knowledge base search did not return one verified passage")
			}
			raw, _ := json.Marshal(map[string]string{"document_id": selected.Document.ID})
			harnessHTTPModelReply(w, payload, "", "read_document", string(raw))
		case 5:
			var result struct {
				Document domain.Document `json:"document"`
			}
			if readTool("read_document", &result) && (result.Document.ID != selected.Document.ID || result.Document.KnowledgeBaseID != target.ID) {
				t.Error("document reading lost selected knowledge base attribution")
			}
			harnessHTTPModelReply(w, payload, "", "search_knowledge", `{"query":"知识库","knowledge_base_ids":[]}`)
		case 6:
			var result struct {
				Evidence []any `json:"evidence"`
			}
			if readTool("search_knowledge", &result) && (result.Evidence == nil || len(result.Evidence) != 0) {
				t.Error("explicit empty knowledge base scope did not remain empty")
			}
			raw, _ := json.Marshal(map[string]string{"title": "旅行摘要", "filename": "travel-summary.md", "content": approvedContent, "knowledge_base_id": target.ID})
			harnessHTTPModelReply(w, payload, "", "create_document", string(raw))
		case 7:
			var result struct {
				Document domain.Document `json:"document"`
			}
			if readTool("create_document", &result) && (result.Document.ID == "" || result.Document.KnowledgeBaseID != target.ID) {
				t.Error("approved creation used a different destination")
			}
			id, _ := citation.Load().(string)
			if id == "" {
				t.Error("current-run source citation was lost")
			}
			harnessHTTPModelReply(w, payload, "已将旅行摘要保存至指定知识库。[E:"+id+"]", "", "")
		default:
			t.Errorf("unexpected model replay: %d", step)
			http.Error(w, "unexpected model replay", http.StatusBadRequest)
		}
	}))
	t.Cleanup(provider.Close)
	callback := "http://" + harnessHTTPAddress(t)
	service := newHarnessHTTPService(t, provider.URL, callback)
	client := harnessHTTPClient(t, service.url, callback)
	installHarnessHTTPClient(x, client)
	response := x.call("POST", sessionPath(session)+"/messages", owner.Token, map[string]string{"content": "选择旅行知识库，查阅住宿规则，并在同一知识库创建旅行摘要，等待我批准。"})
	pending := integrationData[promptHTTPResult](t, checkIntegrationResponse(t, response, http.StatusAccepted))
	assertJournalResponsePrivate(t, response.body)
	if pending.Run.Status != "suspended" || pending.Prompt.Kind != "tool_approval" || pending.Prompt.Approval == nil || pending.Prompt.Approval.Tool != "create_document" || pending.Prompt.Approval.KnowledgeBaseID != target.ID {
		t.Fatalf("create did not bind its knowledge base into approval: %+v", pending)
	}
	_, before, err := x.store.KnowledgeBaseDocuments(context.Background(), owner.User.ID, wid, target.ID, 20, 0)
	if err != nil || before != 1 || models.Load() != 6 {
		t.Fatalf("create ran before approval or unexpected model count: documents=%d calls=%d error=%v", before, models.Load(), err)
	}
	service.stop()
	service.start()
	path := promptPath(session, pending.Run.ID, pending.Prompt.ID)
	completed := harnessHTTPCompletedSSE(t, x, path+"/approve", owner.Token, map[string]bool{"approved": true})
	if completed.Run.ID != pending.Run.ID || !strings.Contains(completed.Answer, "指定知识库") {
		t.Fatal("approval resume lost original run or final answer")
	}
	x.request(t, "POST", path+"/approve", owner.Token, map[string]bool{"approved": true}, http.StatusConflict)
	receipt := integrationData[domain.RunPrompt](t, x.request(t, "GET", path, owner.Token, nil, http.StatusOK))
	if receipt.Approval == nil || receipt.Approval.Result == nil || receipt.Approval.Result.Document.KnowledgeBaseID != target.ID {
		t.Fatal("durable receipt did not retain created document's knowledge base")
	}
	write := receipt.Approval.Result
	_, content, err := x.store.DocumentContent(context.Background(), owner.User.ID, wid, write.Document.ID, write.Version.ID)
	if err != nil || string(content) != approvedContent {
		t.Fatalf("approved source bytes were not preserved: %v", err)
	}
	_, targetTotal, err := x.store.KnowledgeBaseDocuments(context.Background(), owner.User.ID, wid, target.ID, 20, 0)
	if err != nil || targetTotal != 2 {
		t.Fatalf("selected knowledge base must receive exactly one document: total=%d err=%v", targetTotal, err)
	}
	_, otherTotal, err := x.store.KnowledgeBaseDocuments(context.Background(), owner.User.ID, wid, other.ID, 20, 0)
	if err != nil || otherTotal != 1 {
		t.Fatalf("other knowledge base changed: total=%d err=%v", otherTotal, err)
	}
	messages := integrationData[[]domain.Message](t, x.request(t, "GET", sessionPath(session)+"/messages", owner.Token, nil, http.StatusOK))
	if len(messages) != 2 || models.Load() != 7 {
		t.Fatalf("approval replayed the run: messages=%d model_calls=%d", len(messages), models.Load())
	}
}
