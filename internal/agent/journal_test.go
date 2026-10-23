package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestJournalTransportClosedCapabilityAndSchema(t *testing.T) {
	called := 0
	handler := journalHTTPHandler(strings.Repeat("a", 64), func(ctx context.Context, operation string, arguments json.RawMessage) (any, error) {
		called++
		return map[string]any{"records": []any{}}, nil
	})
	for _, tc := range []struct {
		name, method, path, auth, body string
		status                         int
	}{
		{"missing capability", "POST", "/journal", "", `{"operation":"load","arguments":{}}`, 401},
		{"wrong capability", "POST", "/journal", "Bearer " + strings.Repeat("b", 64), `{"operation":"load","arguments":{}}`, 401},
		{"wrong path", "POST", "/tools", "Bearer " + strings.Repeat("a", 64), `{"operation":"load","arguments":{}}`, 400},
		{"query", "POST", "/journal?run=other", "Bearer " + strings.Repeat("a", 64), `{"operation":"load","arguments":{}}`, 400},
		{"get", "GET", "/journal", "Bearer " + strings.Repeat("a", 64), `{"operation":"load","arguments":{}}`, 400},
		{"identity", "POST", "/journal", "Bearer " + strings.Repeat("a", 64), `{"operation":"load","arguments":{"run_id":"other"}}`, 400},
		{"model tool", "POST", "/journal", "Bearer " + strings.Repeat("a", 64), `{"operation":"search_knowledge","arguments":{}}`, 400},
		{"extra field", "POST", "/journal", "Bearer " + strings.Repeat("a", 64), `{"operation":"load","arguments":{},"caller":"other"}`, 400},
		{"duplicate", "POST", "/journal", "Bearer " + strings.Repeat("a", 64), `{"operation":"load","operation":"append","arguments":{}}`, 400},
		{"good", "POST", "/journal", "Bearer " + strings.Repeat("a", 64), `{"operation":"load","arguments":{}}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			r.Header.Set("Authorization", tc.auth)
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	if called != 1 {
		t.Fatalf("invalid requests reached authority: %d", called)
	}
}

func TestJournalTransportDoesNotExposeHandlerFailures(t *testing.T) {
	for _, panicHandler := range []bool{false, true} {
		handler := journalHTTPHandler(strings.Repeat("a", 64), func(context.Context, string, json.RawMessage) (any, error) {
			if panicHandler {
				panic("private document data")
			}
			return nil, errors.New("private document data")
		})
		r := httptest.NewRequest("POST", "/journal", strings.NewReader(`{"operation":"load","arguments":{}}`))
		r.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 64))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != http.StatusForbidden || strings.Contains(w.Body.String(), "private") {
			t.Fatalf("unsanitized failure: %d %s", w.Code, w.Body.String())
		}
	}
}

func TestJournalArgumentRecordEnvelope(t *testing.T) {
	good := `{"record":{"version":1,"seq":1,"kind":"created","data":{},"prev_hash":"","digest":"` + strings.Repeat("a", 64) + `"}}`
	if !validJournalArguments("append", json.RawMessage(good)) {
		t.Fatal("valid envelope rejected")
	}
	for _, bad := range []string{strings.Replace(good, `"seq":1`, `"seq":0`, 1), strings.Replace(good, `"version":1`, `"version":2`, 1), strings.Replace(good, `"data":{}`, `"data":null`, 1), strings.Replace(good, `"kind":"created"`, `"kind":""`, 1), `{"record":{}}`, `{"record":{},"path":"/private"}`} {
		if validJournalArguments("append", json.RawMessage(bad)) {
			t.Fatal("invalid envelope accepted")
		}
	}
}

func TestRecoveryRequiresPrivatePersistenceAndTools(t *testing.T) {
	request := retrievalRequest()
	request.Recover = true
	if validateRequest(request) == nil {
		t.Fatal("recovery without authority accepted")
	}
	request.JournalHandler = func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }
	if validateRequest(request) == nil {
		t.Fatal("recovery without tools accepted")
	}
	request.ToolHandler = ToolHandler(request.JournalHandler)
	if err := validateRequest(request); err != nil {
		t.Fatal(err)
	}
}

// memoryJournal exercises the real subprocess protocol. Raw envelopes preserve
// Python JSON number spellings, including integral-valued floats, across restart.
type memoryJournal struct {
	mu         sync.Mutex
	records    []json.RawMessage
	checkpoint json.RawMessage
}

func (j *memoryJournal) handle(_ context.Context, operation string, raw json.RawMessage) (any, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var args map[string]json.RawMessage
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, err
	}
	switch operation {
	case "load":
		return map[string]any{"records": append([]json.RawMessage{}, j.records...)}, nil
	case "append":
		var record struct {
			Seq int `json:"seq"`
		}
		if err := json.Unmarshal(args["record"], &record); err != nil {
			return nil, err
		}
		if record.Seq != len(j.records)+1 {
			return nil, errors.New("stale journal")
		}
		j.records = append(j.records, append(json.RawMessage{}, args["record"]...))
		return j.records[len(j.records)-1], nil
	case "checkpoint_read":
		if j.checkpoint == nil {
			return nil, nil
		}
		return j.checkpoint, nil
	case "checkpoint_write":
		j.checkpoint = append(json.RawMessage{}, args["data"]...)
		return map[string]any{}, nil
	}
	return nil, errors.New("unknown operation")
}

func TestHarnessJournalTerminalRecoveryUsesCommittedAnswer(t *testing.T) {
	client := mustClient(t, harnessConfig(t))
	request := retrievalRequest()
	j := &memoryJournal{}
	request.JournalHandler = j.handle
	// Authenticated restore is mandatory even for cached final answers. This
	// text-only first run produces no evidence and needs no provider network.
	first, err := client.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(j.records) < 2 {
		t.Fatal("run facts were not committed")
	}
	request.Recover = true
	request.ToolHandler = testToolCapabilities(func(_ context.Context, tool string, _ json.RawMessage) (any, error) {
		if tool != "restore_evidence" {
			t.Fatalf("unexpected tool during recovery: %s", tool)
		}
		return map[string]any{"evidence": []any{}}, nil
	})
	recovered, err := client.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Answer != first.Answer {
		t.Fatal("recovery regenerated the final answer")
	}
	var events []Event
	streamed, err := client.Stream(context.Background(), request, func(event Event) error { events = append(events, event); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if streamed.Answer != first.Answer || len(events) != 0 {
		t.Fatal("cached terminal recovery emitted new model output")
	}
}

func TestJournalTransportCapabilityCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	endpoint, stop, err := startJournalServer(ctx, func(context.Context, string, json.RawMessage) (any, error) {
		return map[string]any{"records": []any{}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest("POST", endpoint.URL, strings.NewReader(`{"operation":"load","arguments":{}}`))
	request.Header.Set("Authorization", "Bearer "+endpoint.Token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	cancel()
	stop()
	if _, err := http.DefaultClient.Do(request); err == nil {
		t.Fatal("closed capability still reachable")
	}
}

func TestHarnessJournalQuestionRecoveryAfterCommittedAnswer(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			request := retrievalRequest()
			journal := &memoryJournal{}
			var loseAnswerReceipt atomic.Bool
			request.JournalHandler = func(ctx context.Context, operation string, arguments json.RawMessage) (any, error) {
				result, err := journal.handle(ctx, operation, arguments)
				if err == nil && operation == "append" {
					var appended struct {
						Record struct {
							Kind string `json:"kind"`
						} `json:"record"`
					}
					_ = json.Unmarshal(arguments, &appended)
					if appended.Record.Kind == "tool_finished" && loseAnswerReceipt.Swap(false) {
						return nil, errors.New("simulated lost commit receipt")
					}
				}
				return result, err
			}
			request.ToolHandler = testToolCapabilities(func(_ context.Context, tool string, _ json.RawMessage) (any, error) {
				if tool != "restore_evidence" {
					t.Errorf("unexpected tool: %s", tool)
				}
				return map[string]any{"evidence": []any{}}, nil
			})
			var calls atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if calls.Add(1) == 1 {
					call := map[string]any{"id": "question", "type": "function", "function": map[string]any{"name": "ask_user", "arguments": `{"question":"选择城市？","options":["北京","上海"]}`}}
					if stream {
						call["index"] = 0
						sendStream(w, providerDelta(map[string]any{"tool_calls": []any{call}}, "tool_calls")+"data: [DONE]\n\n")
					} else {
						_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "", "tool_calls": []any{call}}, "finish_reason": "tool_calls"}}})
					}
					return
				}
				if !strings.Contains(string(body), "北京") {
					t.Error("committed answer absent after recovery")
				}
				if stream {
					sendStream(w, providerChunk("已选择北京。", "stop")+"data: [DONE]\n\n")
				} else {
					_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "已选择北京。"}, "finish_reason": "stop"}}})
				}
			}))
			defer provider.Close()
			client := mustClient(t, liveStreamConfig(t, provider.URL))
			run := func() (Result, error) {
				if stream {
					return client.Stream(context.Background(), request, func(Event) error { return nil })
				}
				return client.Run(context.Background(), request)
			}
			first, err := run()
			if err != nil || first.Prompt == nil {
				t.Fatalf("suspend %+v %v", first, err)
			}
			request.Resume = answerPrompt(t, request, first.Prompt, "北京")
			loseAnswerReceipt.Store(true)
			if _, err := run(); err == nil {
				t.Fatal("lost receipt did not interrupt execution")
			}
			if calls.Load() != 1 {
				t.Fatal("model ran after the uncertain commit")
			}
			request.Recover = true
			result, err := run()
			if err != nil || result.Answer != "已选择北京。" || calls.Load() != 2 {
				t.Fatalf("recovered %+v %v model=%d", result, err, calls.Load())
			}
		})
	}
}

func TestJournalPythonAdapterIntegrityAndBoundary(t *testing.T) {
	client := mustClient(t, harnessConfig(t))
	script := `
import sys
sys.path.insert(0, sys.argv[1])
from persistence import RemoteJournal, RemoteRunStore, validate_endpoint, validate_state, copy_json
from magent_harness.file_store import PersistenceError, CorruptJournalError
from magent_harness import Caller, Role
from bridge import parse_request
import json
endpoint = {'url': 'http://127.0.0.1:1234/journal', 'token': 'a'*64}
class Journal(RemoteJournal):
    def _call(self, op, args):
        if op == 'load': return {'records': []}
        if op == 'append': return copy_json(args['record'])
        if op == 'checkpoint_read': return None
        return {}
with Journal(endpoint) as journal:
    journal.append('created', {'n': 1.0})
    assert journal.seq == 1 and journal.read_checkpoint() is None
    record = journal.records[0]
class Reload(Journal):
    def _call(self, op, args): return {'records': [record]}
with Reload(endpoint) as journal: assert journal.records[0]['data']['n'] == 1.0
record['data']['n'] = 2.0
try:
    with Reload(endpoint): pass
except CorruptJournalError: pass
else: raise AssertionError('corrupt fact accepted')
for url in ['https://127.0.0.1:1234/journal', 'http://localhost:1234/journal', 'http://127.0.0.1:1234/tools', 'http://127.0.0.1:1234/journal?x=1']:
    try: validate_endpoint({'url': url, 'token': 'a'*64})
    except ValueError: pass
    else: raise AssertionError('unsafe endpoint accepted')
store = RemoteRunStore(endpoint, {'workspace_id': 'w', 'user_id': 'u', 'run_id': 'r'}, None)
try: store.journal(Caller('other', 'u', Role.OWNER), 'r')
except PersistenceError: pass
else: raise AssertionError('cross identity accepted')
request = {'workspace_id':'w','user_id':'u','run_id':'r','session_id':'s','role':'owner','query':'test','history':[]}
for patch in [{'recover': True}, {'recover': 'true'}, {'generation': 9}, {'recover': True, 'persistence': endpoint}]:
    try: parse_request(json.dumps({**request, **patch}).encode())
    except ValueError: pass
    else: raise AssertionError('unauthenticated recovery accepted')
from types import SimpleNamespace
from dataclasses import replace
from magent_harness import AccessLevel, ResourceScope, ToolPolicy
state = SimpleNamespace(run_id='r',session_id='s',query='test',caller=Caller('w','u',Role.OWNER),scope=ResourceScope('w',None),policy=ToolPolicy(AccessLevel.READ_ONLY,frozenset({'list_documents'})),status='running')
validate_state(request, state)
for changes in [{'query':'other'},{'session_id':'other'},{'document_ids':[]},{'workspace_id':'other'}]:
    try: validate_state({**request, **changes}, state)
    except ValueError: pass
    else: raise AssertionError('backend identity or scope mismatch accepted')
state.policy = ToolPolicy(AccessLevel.AUTO_WRITE, frozenset({'list_documents'}))
try: validate_state(request, state)
except ValueError: pass
else: raise AssertionError('persisted write policy accepted')
`
	cmd := exec.Command(client.config.Python, "-S", "-B", "-c", script, filepath.Dir(client.config.Script))
	cmd.Env = client.env
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Python adapter validation: %v %s", err, output)
	}
}

func TestHarnessJournalRecoverUnpublishedSecondQuestion(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			request := retrievalRequest()
			journal := &memoryJournal{}
			var loseQuestionReceipt atomic.Bool
			request.JournalHandler = func(ctx context.Context, operation string, arguments json.RawMessage) (any, error) {
				result, err := journal.handle(ctx, operation, arguments)
				if err == nil && operation == "append" {
					var appended struct {
						Record struct {
							Kind string `json:"kind"`
						} `json:"record"`
					}
					_ = json.Unmarshal(arguments, &appended)
					if appended.Record.Kind == "suspended" && loseQuestionReceipt.Swap(false) {
						return nil, errors.New("lost new question before Go publication")
					}
				}
				return result, err
			}
			request.ToolHandler = testToolCapabilities(func(_ context.Context, tool string, _ json.RawMessage) (any, error) {
				if tool != "restore_evidence" {
					t.Errorf("unexpected tool %s", tool)
				}
				return map[string]any{"evidence": []any{}}, nil
			})
			var calls atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				n := calls.Add(1)
				if n <= 2 {
					arguments := `{"question":"国内还是海外？","options":["国内","海外"]}`
					if n == 2 {
						arguments = `{"question":"哪个城市？","options":["北京","上海"]}`
					}
					call := map[string]any{"id": fmt.Sprintf("question-%d", n), "type": "function", "function": map[string]any{"name": "ask_user", "arguments": arguments}}
					if stream {
						call["index"] = 0
						sendStream(w, providerDelta(map[string]any{"tool_calls": []any{call}}, "tool_calls")+"data: [DONE]\n\n")
					} else {
						_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "", "tool_calls": []any{call}}, "finish_reason": "tool_calls"}}})
					}
					return
				}
				if n != 3 || strings.Count(string(body), "The user answered: 国内") != 1 || strings.Count(string(body), "The user answered: 北京") != 1 {
					t.Errorf("committed human answers were lost or duplicated: model calls=%d", n)
				}
				if stream {
					sendStream(w, providerChunk("国内北京已确认。", "stop")+"data: [DONE]\n\n")
				} else {
					_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "国内北京已确认。"}, "finish_reason": "stop"}}})
				}
			}))
			defer provider.Close()
			client := mustClient(t, liveStreamConfig(t, provider.URL))
			run := func() (Result, error) {
				if stream {
					return client.Stream(context.Background(), request, func(Event) error { return nil })
				}
				return client.Run(context.Background(), request)
			}
			first, err := run()
			if err != nil || first.Prompt == nil {
				t.Fatalf("first question %+v %v", first, err)
			}
			answer := answerPrompt(t, request, first.Prompt, "国内")
			request.Resume = answer
			loseQuestionReceipt.Store(true)
			if _, err := run(); err == nil || calls.Load() != 2 {
				t.Fatalf("second question did not interrupt at publication: %v calls=%d", err, calls.Load())
			}
			request.Recover = true
			for _, mismatch := range []string{"id", "answer"} {
				var raw map[string]any
				_ = json.Unmarshal(answer.Prompt, &raw)
				if mismatch == "id" {
					raw["id"] = strings.Repeat("f", 32)
				} else {
					raw["response"] = map[string]string{"answer": "海外"}
				}
				encoded, _ := json.Marshal(raw)
				request.Resume = &ResumeRequest{Prompt: encoded}
				if _, err := run(); err == nil || calls.Load() != 2 {
					t.Fatal("unknown or changed historical answer reached recovery")
				}
			}
			request.Resume = answer
			second, err := run()
			if err != nil || second.Prompt == nil || second.Prompt.ID == first.Prompt.ID || second.Prompt.Question != "哪个城市？" || calls.Load() != 2 {
				t.Fatalf("unpublished second question not recovered: %+v %v calls=%d", second, err, calls.Load())
			}
			request.Recover = false
			request.Resume = answerPrompt(t, request, second.Prompt, "北京")
			final, err := run()
			if err != nil || final.Answer != "国内北京已确认。" || calls.Load() != 3 {
				t.Fatalf("second question failed to resume: %+v %v", final, err)
			}
			journal.mu.Lock()
			defer journal.mu.Unlock()
			finished := map[string]int{}
			for _, raw := range journal.records {
				var record struct {
					Kind string `json:"kind"`
					Data struct {
						Record struct {
							ID string `json:"id"`
						} `json:"record"`
					} `json:"data"`
				}
				_ = json.Unmarshal(raw, &record)
				if record.Kind == "tool_finished" {
					finished[record.Data.Record.ID]++
				}
			}
			if finished["question-1"] != 1 || finished["question-2"] != 1 {
				t.Fatalf("human answer execution facts repeated: %v", finished)
			}
		})
	}
}
