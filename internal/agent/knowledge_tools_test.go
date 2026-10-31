package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

type knowledgeModelRequest struct {
	Tools []struct {
		Function struct {
			Name       string         `json:"name"`
			Parameters map[string]any `json:"parameters"`
		} `json:"function"`
	} `json:"tools"`
	Messages []struct {
		Role    string `json:"role"`
		ID      string `json:"tool_call_id"`
		Content string `json:"content"`
	} `json:"messages"`
}

func knowledgeModelPayload(t *testing.T, r *http.Request) knowledgeModelRequest {
	t.Helper()
	var payload knowledgeModelRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		t.Error(err)
	}
	return payload
}

func knowledgeReply(w http.ResponseWriter, stream bool, calls []any) {
	if stream {
		if len(calls) > 0 {
			for i, call := range calls {
				call.(map[string]any)["index"] = i
			}
			sendStream(w, providerDelta(map[string]any{"tool_calls": calls}, "tool_calls")+"data: [DONE]\n\n")
		} else {
			sendStream(w, providerChunk("已完成知识库检查。", "stop")+"data: [DONE]\n\n")
		}
		return
	}
	message := map[string]any{"role": "assistant", "content": "已完成知识库检查。"}
	finish := "stop"
	if len(calls) > 0 {
		message["content"], message["tool_calls"], finish = "", calls, "tool_calls"
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": message, "finish_reason": finish}}})
}

func knowledgeCall(id, name, args string) any {
	return map[string]any{"id": id, "type": "function", "function": map[string]any{"name": name, "arguments": args}}
}

func runKnowledgeHarness(t *testing.T, client *ProcessClient, req Request, stream bool) (Result, error) {
	t.Helper()
	if !stream {
		return client.Run(context.Background(), req)
	}
	return client.Stream(context.Background(), req, func(e Event) error {
		if strings.Contains(e.Delta, "tool_capabilities") || strings.Contains(e.Delta, "created_by") {
			t.Error("private callback data escaped into answer deltas")
		}
		return nil
	})
}

func TestHarnessKnowledgeToolsJSONAndSSE(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			req := retrievalRequest()
			kbID := strings.Repeat("d", 32)
			evidence := evidenceForTest(req)
			names := []string{"list_knowledge_bases", "search_knowledge", "list_documents", "read_document", "search_knowledge", "list_documents"}
			arguments := []string{`{"limit":1,"offset":0}`, `{"query":"住宿标准","knowledge_base_ids":["` + kbID + `"]}`, `{"knowledge_base_ids":["` + kbID + `"],"limit":1}`, `{"document_id":"` + strings.Repeat("6", 32) + `"}`, `{"query":"住宿标准","knowledge_base_ids":[]}`, `{"knowledge_base_ids":[]}`}
			allowed := []string{"list_knowledge_bases", "search_knowledge", "list_documents", "read_document", "ask_user", "create_document"}
			metadata := func() map[string]any {
				return map[string]any{
					"document": map[string]any{"id": evidence["document_id"], "workspace_id": req.WorkspaceID, "knowledge_base_id": kbID, "title": "差旅制度", "current_version_id": evidence["version_id"], "created_by": req.UserID},
					"version":  map[string]any{"id": evidence["version_id"], "document_id": evidence["document_id"], "filename": "rules.md"},
					"index":    map[string]any{"id": evidence["index_id"], "document_id": evidence["document_id"], "version_id": evidence["version_id"], "status": "ready"},
				}
			}
			var callbacks, discovery, models atomic.Int32
			req.ToolHandler = func(_ context.Context, name string, raw json.RawMessage) (any, error) {
				if name == "tool_capabilities" {
					discovery.Add(1)
					if string(raw) != "{}" {
						t.Error("capability discovery supplied model arguments")
					}
					return map[string]any{"tools": allowed}, nil
				}
				n := int(callbacks.Add(1)) - 1
				if n >= len(names) {
					t.Error("unexpected extra callback")
					return nil, errors.New("unexpected callback")
				}
				var got, want any
				_ = json.Unmarshal(raw, &got)
				_ = json.Unmarshal([]byte(arguments[n]), &want)
				if name != names[n] || !reflect.DeepEqual(got, want) {
					t.Errorf("callback %d: %s %s", n, name, raw)
				}
				switch name {
				case "list_knowledge_bases":
					return map[string]any{"knowledge_bases": []any{map[string]any{"id": kbID, "workspace_id": req.WorkspaceID, "name": "旅行资料", "description": "住宿规则", "is_default": false, "created_by": req.UserID}}, "offset": 0, "limit": 1, "has_more": false}, nil
				case "search_knowledge":
					if n == 4 {
						return map[string]any{"evidence": []any{}}, nil
					}
					return map[string]any{"evidence": []any{evidence}}, nil
				case "list_documents":
					if n == 5 {
						return map[string]any{"documents": []any{}, "offset": 0, "limit": 10, "has_more": false}, nil
					}
					return map[string]any{"documents": []any{metadata()}, "offset": 0, "limit": 1, "has_more": false}, nil
				case "read_document":
					result := metadata()
					result["evidence"] = []any{evidence}
					return result, nil
				}
				return nil, errors.New("unexpected callback")
			}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload := knowledgeModelPayload(t, r)
				got := map[string]bool{}
				for _, tool := range payload.Tools {
					got[tool.Function.Name] = true
					props, _ := tool.Function.Parameters["properties"].(map[string]any)
					if (tool.Function.Name == "search_knowledge" || tool.Function.Name == "list_documents") && props["knowledge_base_ids"] == nil {
						t.Errorf("missing KB scope schema for %s", tool.Function.Name)
					}
					if tool.Function.Name == "create_document" && props["knowledge_base_id"] == nil {
						t.Error("missing create destination schema")
					}
				}
				if len(got) != len(allowed) {
					t.Errorf("unexpected model tool set %v", got)
				}
				for _, name := range allowed {
					if !got[name] {
						t.Errorf("missing allowed tool %s", name)
					}
				}
				for _, message := range payload.Messages {
					if strings.Contains(message.Content, req.UserID) || strings.Contains(message.Content, `"created_by"`) {
						t.Error("source metadata exposed caller identity")
					}
					if message.Role != "tool" {
						continue
					}
					var result map[string]any
					if err := json.Unmarshal([]byte(message.Content), &result); err != nil {
						t.Errorf("invalid tool JSON for %s: %s", message.ID, message.Content)
						continue
					}
					switch message.ID {
					case "kb0":
						if !strings.Contains(message.Content, "旅行资料") || !strings.Contains(message.Content, kbID) {
							t.Error("knowledge base not visible")
						}
					case "kb2", "kb3":
						if !strings.Contains(message.Content, `"knowledge_base_id"`) || !strings.Contains(message.Content, kbID) {
							t.Error("document lost knowledge base attribution")
						}
					case "kb4":
						if len(result["evidence"].([]any)) != 0 {
							t.Error("empty KB scope produced evidence")
						}
					case "kb5":
						if len(result["documents"].([]any)) != 0 {
							t.Error("empty KB scope produced documents")
						}
					}
				}
				n := int(models.Add(1)) - 1
				if n < len(names) {
					knowledgeReply(w, stream, []any{knowledgeCall(fmt.Sprintf("kb%d", n), names[n], arguments[n])})
				} else {
					knowledgeReply(w, stream, nil)
				}
			}))
			defer provider.Close()
			result, err := runKnowledgeHarness(t, mustClient(t, liveStreamConfig(t, provider.URL)), req, stream)
			if err != nil || result.Status != "completed" || callbacks.Load() != 6 || discovery.Load() != 1 || models.Load() != 7 {
				t.Fatalf("KB pipeline failed: err=%v status=%s callbacks=%d discovery=%d models=%d", err, result.Status, callbacks.Load(), discovery.Load(), models.Load())
			}
		})
	}
}

func TestHarnessKnowledgeCapabilitiesDenyGuessedCalls(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			req := retrievalRequest()
			var discovery, unexpected, models atomic.Int32
			req.ToolHandler = func(_ context.Context, name string, _ json.RawMessage) (any, error) {
				if name == "tool_capabilities" {
					discovery.Add(1)
					return map[string]any{"tools": []string{}}, nil
				}
				unexpected.Add(1)
				return nil, errors.New("forbidden callback")
			}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload := knowledgeModelPayload(t, r)
				if len(payload.Tools) != 0 {
					t.Error("empty capability set advertised tools")
				}
				if models.Add(1) == 1 {
					knowledgeReply(w, stream, []any{knowledgeCall("disabled-read", "list_documents", `{}`), knowledgeCall("disabled-question", "ask_user", `{"question":"enable tools?"}`), knowledgeCall("private-discovery", "tool_capabilities", `{}`), knowledgeCall("private-restore", "restore_evidence", `{}`), knowledgeCall("disabled-memory", "search_memory", `{"query":"private"}`)})
					return
				}
				count := 0
				for _, message := range payload.Messages {
					if message.Role == "tool" {
						count++
						if !strings.Contains(message.Content, "Error:") {
							t.Errorf("guessed tool did not fail: %s", message.Content)
						}
					}
				}
				if count != 5 {
					t.Errorf("missing denial results: %d", count)
				}
				knowledgeReply(w, stream, nil)
			}))
			defer provider.Close()
			result, err := runKnowledgeHarness(t, mustClient(t, liveStreamConfig(t, provider.URL)), req, stream)
			if err != nil || result.Status != "completed" || unexpected.Load() != 0 || discovery.Load() != 1 || models.Load() != 2 {
				t.Fatalf("capability denial failed: %v status=%s callbacks=%d discovery=%d models=%d", err, result.Status, unexpected.Load(), discovery.Load(), models.Load())
			}
		})
	}
}

func TestHarnessKnowledgeCapabilitiesRejectInvalidManifest(t *testing.T) {
	for name, manifest := range map[string]any{
		"internal":        map[string]any{"tools": []string{"tool_capabilities"}},
		"unknown":         map[string]any{"tools": []string{"shell_exec"}},
		"duplicate":       map[string]any{"tools": []string{"list_documents", "list_documents"}},
		"null":            map[string]any{"tools": nil},
		"extra authority": map[string]any{"tools": []string{"list_documents"}, "role": "owner"},
	} {
		t.Run(name, func(t *testing.T) {
			req := retrievalRequest()
			var discovery, models atomic.Int32
			req.ToolHandler = func(_ context.Context, name string, _ json.RawMessage) (any, error) {
				if name != "tool_capabilities" {
					t.Error("untrusted capability executed a tool")
				}
				discovery.Add(1)
				return manifest, nil
			}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { models.Add(1); knowledgeReply(w, false, nil) }))
			defer provider.Close()
			_, err := runKnowledgeHarness(t, mustClient(t, liveStreamConfig(t, provider.URL)), req, false)
			if !errors.Is(err, ErrFailed) || models.Load() != 0 || discovery.Load() != 1 {
				t.Fatalf("invalid capabilities did not fail closed: %v models=%d discovery=%d", err, models.Load(), discovery.Load())
			}
		})
	}
}

func TestHarnessKnowledgeCapabilitiesRefreshOnResume(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			req := retrievalRequest()
			var discovery, unexpected, models atomic.Int32
			req.ToolHandler = func(_ context.Context, name string, _ json.RawMessage) (any, error) {
				switch name {
				case "tool_capabilities":
					if discovery.Add(1) == 1 {
						return map[string]any{"tools": []string{"ask_user", "list_knowledge_bases"}}, nil
					}
					return map[string]any{"tools": []string{"ask_user"}}, nil
				case "restore_evidence":
					return map[string]any{"evidence": []any{}}, nil
				default:
					unexpected.Add(1)
					return nil, errors.New("revoked tool executed")
				}
			}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload := knowledgeModelPayload(t, r)
				if models.Add(1) == 1 {
					knowledgeReply(w, stream, []any{knowledgeCall("question", "ask_user", `{"question":"确认浏览知识库？"}`), knowledgeCall("pending-kb", "list_knowledge_bases", `{}`)})
					return
				}
				if len(payload.Tools) != 1 || payload.Tools[0].Function.Name != "ask_user" {
					t.Error("resume reused old capabilities")
				}
				denied := false
				for _, message := range payload.Messages {
					if message.ID == "pending-kb" && strings.Contains(message.Content, "Error:") {
						denied = true
					}
				}
				if !denied {
					t.Error("pending revoked tool was not denied")
				}
				knowledgeReply(w, stream, nil)
			}))
			defer provider.Close()
			client := mustClient(t, liveStreamConfig(t, provider.URL))
			first, err := runKnowledgeHarness(t, client, req, stream)
			if err != nil || first.Prompt == nil || first.Status != "suspended" {
				t.Fatalf("suspend failed: %v %+v", err, first)
			}
			req.Resume = answerPrompt(t, req, first.Prompt, "确认")
			result, err := runKnowledgeHarness(t, client, req, stream)
			if err != nil || result.Status != "completed" || discovery.Load() != 2 || unexpected.Load() != 0 || models.Load() != 2 {
				t.Fatalf("revoked pending tool executed on resume: %v status=%s discovery=%d tools=%d models=%d", err, result.Status, discovery.Load(), unexpected.Load(), models.Load())
			}
		})
	}
}

func TestHarnessKnowledgeToolsRejectInvalidScopes(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			req := retrievalRequest()
			var discovery, unexpected, models atomic.Int32
			req.ToolHandler = func(_ context.Context, name string, _ json.RawMessage) (any, error) {
				if name == "tool_capabilities" {
					discovery.Add(1)
					return map[string]any{"tools": []string{"list_knowledge_bases", "search_knowledge", "list_documents", "create_document"}}, nil
				}
				unexpected.Add(1)
				return nil, errors.New("invalid arguments reached callback")
			}
			invalid := []any{
				knowledgeCall("null", "search_knowledge", `{"query":"住宿标准","knowledge_base_ids":null}`),
				knowledgeCall("upper", "search_knowledge", `{"query":"住宿标准","knowledge_base_ids":["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"]}`),
				knowledgeCall("short", "list_documents", `{"knowledge_base_ids":["a"]}`),
				knowledgeCall("duplicate", "list_documents", `{"knowledge_base_ids":["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]}`),
				knowledgeCall("limit", "list_knowledge_bases", `{"limit":21}`),
				knowledgeCall("offset", "list_knowledge_bases", `{"offset":-1}`),
				knowledgeCall("identity", "list_knowledge_bases", `{"workspace_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`),
				knowledgeCall("many", "list_documents", `{"knowledge_base_ids":[`+strings.TrimSuffix(strings.Repeat(`"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",`, 21), ",")+`]}`),
			}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload := knowledgeModelPayload(t, r)
				if models.Add(1) == 1 {
					knowledgeReply(w, stream, invalid)
					return
				}
				count := 0
				for _, message := range payload.Messages {
					if message.Role == "tool" {
						count++
						if !strings.Contains(message.Content, "Error:") {
							t.Errorf("invalid scope succeeded: %s", message.Content)
						}
					}
				}
				if count != len(invalid) {
					t.Errorf("missing invalid argument results: %d", count)
				}
				knowledgeReply(w, stream, nil)
			}))
			defer provider.Close()
			result, err := runKnowledgeHarness(t, mustClient(t, liveStreamConfig(t, provider.URL)), req, stream)
			if err != nil || result.Status != "completed" || unexpected.Load() != 0 || discovery.Load() != 1 || models.Load() != 2 {
				t.Fatalf("invalid arguments reached callback: %v callbacks=%d", err, unexpected.Load())
			}
		})
	}
}

func TestHarnessKnowledgeCreateApprovalBindsDestination(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			req := retrievalRequest()
			kbID := strings.Repeat("d", 32)
			var unexpected atomic.Int32
			req.ToolHandler = func(_ context.Context, name string, _ json.RawMessage) (any, error) {
				if name == "tool_capabilities" {
					return map[string]any{"tools": []string{"create_document"}}, nil
				}
				unexpected.Add(1)
				return nil, errors.New("unapproved mutation reached callback")
			}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = knowledgeModelPayload(t, r)
				knowledgeReply(w, stream, []any{knowledgeCall("create-in-kb", "create_document", `{"title":"旅行摘要","filename":"summary.md","content":"住宿规则","knowledge_base_id":"`+kbID+`"}`)})
			}))
			defer provider.Close()
			result, err := runKnowledgeHarness(t, mustClient(t, liveStreamConfig(t, provider.URL)), req, stream)
			if err != nil || result.Status != "suspended" || result.Prompt == nil || result.Prompt.Kind != "tool_approval" || unexpected.Load() != 0 {
				t.Fatalf("create did not require approval: %v %+v callbacks=%d", err, result, unexpected.Load())
			}
			var approval struct {
				Tool      string `json:"tool"`
				Arguments struct {
					KnowledgeBaseID string `json:"knowledge_base_id"`
				} `json:"arguments"`
			}
			if json.Unmarshal(result.Prompt.Request, &approval) != nil || approval.Tool != "create_document" || approval.Arguments.KnowledgeBaseID != kbID {
				t.Fatal("approval did not bind requested knowledge base")
			}
		})
	}
}

func TestHarnessKnowledgeBaseDirectoryBoundsDescriptions(t *testing.T) {
	for label, description := range map[string]string{"controls": strings.Repeat("\n", 2000), "long ASCII": strings.Repeat("a", 2000)} {
		t.Run(label, func(t *testing.T) {
			req := retrievalRequest()
			name := strings.Repeat(`"`, 255)
			items := make([]any, 20)
			for i := range items {
				items[i] = map[string]any{"id": fmt.Sprintf("%032x", i+16), "name": name, "description": description, "is_default": false}
			}
			var models, lists atomic.Int32
			req.ToolHandler = func(_ context.Context, tool string, raw json.RawMessage) (any, error) {
				switch tool {
				case "tool_capabilities":
					return map[string]any{"tools": []string{"list_knowledge_bases"}}, nil
				case "list_knowledge_bases":
					lists.Add(1)
					var args struct {
						Limit int `json:"limit"`
					}
					if json.Unmarshal(raw, &args) != nil || args.Limit != 20 {
						t.Error("directory request did not preserve page size")
					}
					return map[string]any{"knowledge_bases": items, "offset": 0, "limit": 20, "has_more": false}, nil
				default:
					t.Errorf("unexpected callback: %s", tool)
					return nil, errors.New("unexpected tool")
				}
			}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload := knowledgeModelPayload(t, r)
				if models.Add(1) == 1 {
					knowledgeReply(w, false, []any{knowledgeCall("large-directory", "list_knowledge_bases", `{"limit":20}`)})
					return
				}
				found := false
				for _, message := range payload.Messages {
					if message.Role != "tool" || message.ID != "large-directory" {
						continue
					}
					found = true
					var result struct {
						KnowledgeBases []struct {
							ID, Name, Description string
							Excerpt               bool `json:"description_is_excerpt"`
						} `json:"knowledge_bases"`
					}
					if err := json.Unmarshal([]byte(message.Content), &result); err != nil {
						t.Errorf("bounded directory is not complete JSON: %s", message.Content)
						continue
					}
					if len(message.Content) > 14000 || len(result.KnowledgeBases) != 20 {
						t.Errorf("directory exceeded display limit or lost entries: bytes=%d entries=%d", len(message.Content), len(result.KnowledgeBases))
					}
					for i, item := range result.KnowledgeBases {
						if item.ID != fmt.Sprintf("%032x", i+16) || item.Name != name || !item.Excerpt || len(item.Description) >= len(description) {
							t.Errorf("directory entry %d lost identity/name or omission marker", i)
						}
					}
				}
				if !found {
					t.Error("bounded directory result was missing")
				}
				knowledgeReply(w, false, nil)
			}))
			defer provider.Close()
			result, err := runKnowledgeHarness(t, mustClient(t, liveStreamConfig(t, provider.URL)), req, false)
			if err != nil || result.Status != "completed" || lists.Load() != 1 || models.Load() != 2 {
				t.Fatalf("large directory could not be read: %v status=%s callbacks=%d models=%d", err, result.Status, lists.Load(), models.Load())
			}
		})
	}
}
