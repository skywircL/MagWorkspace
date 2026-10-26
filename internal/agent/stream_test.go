package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func providerChunk(text string, finish any) string {
	return providerDelta(map[string]any{"content": text}, finish)
}

func providerDelta(delta map[string]any, finish any) string {
	value := map[string]any{
		"id": "completion-1", "object": "chat.completion.chunk",
		"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}},
	}
	data, _ := json.Marshal(value)
	return "data: " + string(data) + "\n\n"
}

func sendStream(w http.ResponseWriter, data string) {
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = io.WriteString(w, data)
	w.(http.Flusher).Flush()
}

func liveStreamConfig(t *testing.T, url string) Config {
	t.Helper()
	config := harnessConfig(t)
	config.Mode, config.BaseURL, config.APIKey, config.Model = "live", url, "test-stream-key", "test-model"
	return config
}

func TestStreamDeliversActualDeltaBeforeProviderCompletion(t *testing.T) {
	firstDelta := make(chan struct{}, 1)
	finishProvider := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if string(body["stream"]) != "true" || body["tools"] != nil || r.Header.Get("Authorization") != "Bearer test-stream-key" {
			t.Error("streaming provider must request text-only authenticated SSE")
		}
		sendStream(w, ": provider heartbeat\n\n"+providerDelta(map[string]any{"content": "你好", "reasoning_content": "PRIVATE_SUCCESS_REASONING"}, nil))
		select {
		case <-finishProvider:
			sendStream(w, providerChunk("，世界", nil)+providerChunk("", "stop")+"data: [DONE]\n\n")
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := mustClient(t, liveStreamConfig(t, server.URL))
	request := requestForTest()
	request.History = []Message{{Role: "user", Content: "first"}, {Role: "assistant", Content: "answer"}}
	var events []Event
	type outcome struct {
		result Result
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := client.Stream(ctx, request, func(event Event) error {
			events = append(events, event)
			if event.Type == "message.delta" {
				select {
				case firstDelta <- struct{}{}:
				default:
				}
			}
			return nil
		})
		done <- outcome{result, err}
	}()
	select {
	case <-firstDelta:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("first delta was buffered until provider completion")
	}
	select {
	case early := <-done:
		t.Fatalf("Stream completed before provider completion: %v", early.err)
	default:
	}
	close(finishProvider)
	result := <-done
	if result.err != nil || result.result.Answer != "你好，世界" || len(result.result.Messages) != 4 {
		t.Fatalf("unexpected completed stream: %+v %v", result.result, result.err)
	}
	if len(events) != 3 || events[0].Type != "message.start" || events[1].Delta != "你好" || events[2].Delta != "，世界" {
		t.Fatalf("unexpected incremental events: %+v", events)
	}
	for _, event := range events {
		if event.RunID != request.RunID || event.SessionID != request.SessionID || event.Round != 1 {
			t.Fatalf("event not bound to this run: %+v", event)
		}
	}
	encoded, err := json.Marshal(struct {
		Events []Event
		Result Result
	}{events, result.result})
	if err != nil || strings.Contains(string(encoded), "PRIVATE_SUCCESS_REASONING") || strings.Contains(string(encoded), "response_id") {
		t.Fatalf("native reasoning or response identity escaped the public protocol: %v", err)
	}
}

func TestStreamDemoAndRunRemainCompatible(t *testing.T) {
	client := mustClient(t, harnessConfig(t))
	var text strings.Builder
	result, err := client.Stream(context.Background(), requestForTest(), func(event Event) error {
		if event.Type == "message.delta" {
			text.WriteString(event.Delta)
		}
		return nil
	})
	if err != nil || result.Mode != "demo" || !strings.Contains(result.Answer, "未调用真实模型") || text.String() != result.Answer {
		t.Fatalf("demo stream mismatch: %+v %v", result, err)
	}
	plain, err := client.Run(context.Background(), requestForTest())
	if err != nil || plain.Answer != result.Answer {
		t.Fatalf("Run JSON protocol regressed: %+v %v", plain, err)
	}
}

func TestStreamCancellationAndCallbackErrorsCloseProvider(t *testing.T) {
	for _, callbackFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("callback_error=%t", callbackFails), func(t *testing.T) {
			closed := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				sendStream(w, providerChunk("first", nil))
				select {
				case <-r.Context().Done():
					closed <- struct{}{}
				case <-time.After(3 * time.Second):
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			callbackErr := errors.New("sensitive-callback-detail")
			_, err := mustClient(t, liveStreamConfig(t, server.URL)).Stream(ctx, requestForTest(), func(event Event) error {
				if event.Type == "message.delta" {
					if callbackFails {
						return callbackErr
					}
					cancel()
				}
				return nil
			})
			want := error(context.Canceled)
			if callbackFails {
				want = callbackErr
			}
			if !errors.Is(err, ErrFailed) || !errors.Is(err, want) || strings.Contains(err.Error(), "sensitive-callback-detail") {
				t.Fatalf("expected sanitized cancellation, got %v", err)
			}
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Fatal("canceled stream left provider connection open")
			}
		})
	}
}

func TestStreamMalformedOrIncompleteUpstreamNeverCommits(t *testing.T) {
	cases := map[string]string{
		"EOF after delta":       providerChunk("partial", nil),
		"EOF after finish":      providerChunk("partial", "stop"),
		"DONE without finish":   providerChunk("partial", nil) + "data: [DONE]\n\n",
		"malformed JSON":        "data: {secret-provider-body\n\n",
		"provider error":        "data: {\"error\":{\"message\":\"secret-provider-body\"}}\n\n",
		"tool call":             "data: {\"id\":\"completion-1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"function\":{\"name\":\"read_file\"}}]},\"finish_reason\":null}]}\n\n",
		"wrong role":            "data: {\"id\":\"completion-1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"system\"},\"finish_reason\":null}]}\n\n",
		"bad delta type":        "data: {\"id\":\"completion-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":12},\"finish_reason\":null}]}\n\n",
		"delta after finish":    providerChunk("a", "stop") + providerChunk("b", nil) + "data: [DONE]\n\n",
		"oversized event":       ":" + strings.Repeat("x", 1<<20) + "\n\n" + providerChunk("must not complete", "stop") + "data: [DONE]\n\n",
		"oversized generation":  providerChunk(strings.Repeat("x", maxMessageBytes+1), "stop") + "data: [DONE]\n\n",
		"unterminated SSE line": "data: [DONE]",
		"oversized response":    strings.Repeat(":"+strings.Repeat("x", 4093)+"\n\n", 4097) + providerChunk("must not complete", "stop") + "data: [DONE]\n\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				sendStream(w, body)
			}))
			defer server.Close()
			result, err := mustClient(t, liveStreamConfig(t, server.URL)).Stream(context.Background(), requestForTest(), func(Event) error { return nil })
			if !errors.Is(err, ErrFailed) || result.RunID != "" || strings.Contains(err.Error(), "secret-provider-body") || calls.Load() != 1 {
				t.Fatalf("invalid stream must fail once without a result: %+v %v calls=%d", result, err, calls.Load())
			}
		})
	}
}

func TestStreamNativeProviderMetadataDoesNotControlRunIdentity(t *testing.T) {
	first := strings.Replace(providerChunk("native ", nil), `"id":"completion-1",`, "", 1)
	last := strings.Replace(providerChunk("parser", "stop"), "completion-1", "provider-changed-id", 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sendStream(w, first+last+"data: [DONE]\n\n")
	}))
	defer server.Close()
	request := requestForTest()
	var events []Event
	result, err := mustClient(t, liveStreamConfig(t, server.URL)).Stream(context.Background(), request, func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil || result.RunID != request.RunID || result.Answer != "native parser" || len(events) != 3 {
		t.Fatalf("native parser metadata contract changed: result=%+v events=%+v err=%v", result, events, err)
	}
	for _, event := range events {
		if event.RunID != request.RunID || event.SessionID != request.SessionID || event.Round != 1 {
			t.Fatalf("provider metadata replaced authoritative identity: %+v", event)
		}
	}
}

func TestStreamNativeDiscardAndUnapprovedToolsNeverCommit(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "tool-must-not-run")
	arguments, err := json.Marshal(map[string]string{"path": marker, "content": "PRIVATE_TOOL_ARGUMENTS"})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		body        string
		wantDiscard bool
	}{
		"content filter": {
			body:        providerChunk("uncommitted draft", nil) + providerChunk("", "content_filter") + "data: [DONE]\n\n",
			wantDiscard: true,
		},
		"tool call and reasoning": {
			body: providerDelta(map[string]any{"content": "uncommitted draft", "reasoning_content": "PRIVATE_REASONING"}, nil) +
				providerDelta(map[string]any{"tool_calls": []any{map[string]any{
					"index": 0, "id": "private-tool-id", "type": "function",
					"function": map[string]any{"name": "write_file", "arguments": string(arguments)},
				}}}, "tool_calls") + "data: [DONE]\n\n",
			wantDiscard: true,
		},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var body map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if _, ok := body["tools"]; ok {
					t.Error("text-only bridge advertised tools")
				}
				sendStream(w, test.body)
			}))
			defer server.Close()
			var events []Event
			result, err := mustClient(t, liveStreamConfig(t, server.URL)).Stream(context.Background(), requestForTest(), func(event Event) error {
				events = append(events, event)
				return nil
			})
			if !errors.Is(err, ErrFailed) || result.RunID != "" || result.Answer != "" || calls.Load() != 1 {
				t.Fatalf("rejected native response returned a result or continued: result=%+v events=%+v err=%v calls=%d", result, events, err, calls.Load())
			}
			if len(events) < 2 || events[0].Type != "message.start" || events[1].Type != "message.delta" || events[1].Delta != "uncommitted draft" {
				t.Fatalf("test did not observe native provisional text: %+v", events)
			}
			if test.wantDiscard && (len(events) != 3 || events[2].Type != "message.discard" || events[2].Delta != "" || events[2].Round != 1) {
				t.Fatalf("discarded text was not invalidated before failure: %+v", events)
			}
			encoded, marshalErr := json.Marshal(events)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			for _, private := range []string{"PRIVATE_REASONING", "PRIVATE_TOOL_ARGUMENTS", "private-tool-id", "write_file", marker} {
				if strings.Contains(string(encoded), private) || strings.Contains(err.Error(), private) {
					t.Fatalf("private model or tool metadata escaped the native bridge: %s", private)
				}
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatal("unapproved model-generated tool was executed")
			}
		})
	}
}

func TestStreamNoRetryOnProviderFailure(t *testing.T) {
	var redirected atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Store(true)
	}))
	defer target.Close()
	for _, status := range []int{http.StatusTooManyRequests, http.StatusTemporaryRedirect} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", target.URL)
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "private-provider-error")
			}))
			defer server.Close()
			_, err := mustClient(t, liveStreamConfig(t, server.URL)).Stream(context.Background(), requestForTest(), func(Event) error { return nil })
			if !errors.Is(err, ErrFailed) || calls.Load() != 1 || strings.Contains(err.Error(), "private-provider-error") {
				t.Fatalf("stream unexpectedly retried or exposed error: %v calls=%d", err, calls.Load())
			}
		})
	}
	if redirected.Load() {
		t.Fatal("native streaming provider redirect received a request or credentials")
	}
}

func TestStreamInternalCompactionIsNotForwarded(t *testing.T) {
	var summaries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Stream bool `json:"stream"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if !body.Stream {
			summaries.Add(1)
			_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"INTERNAL_CHECKPOINT_SECRET"},"finish_reason":"stop"}]}`)
			return
		}
		sendStream(w, providerChunk("public answer", "stop")+"data: [DONE]\n\n")
	}))
	defer server.Close()
	request := requestForTest()
	for i := range 24 {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		request.History = append(request.History, Message{Role: role, Content: strings.Repeat("界", 5000)})
	}
	var events []Event
	result, err := mustClient(t, liveStreamConfig(t, server.URL)).Stream(context.Background(), request, func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil || summaries.Load() == 0 || result.Answer != "public answer" || len(events) != 2 || events[0].Type != "message.start" || events[0].Round != 1 || events[1].Type != "message.delta" || events[1].Delta != "public answer" {
		t.Fatalf("summary was skipped or leaked: summaries=%d events=%+v error=%v", summaries.Load(), events, err)
	}
}

func TestStreamOversizedSummaryClosesBodyAndMarksOmittedHistory(t *testing.T) {
	request := requestForTest()
	for i := range 24 {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		request.History = append(request.History, Message{Role: role, Content: strings.Repeat("界", 5000)})
	}
	var summaries atomic.Int32
	var answers atomic.Int32
	summaryClosed := make(chan struct{}, 1)
	stopProvider := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Stream   bool      `json:"stream"`
			Messages []Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if !body.Stream {
			summaries.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", fmt.Sprint(4<<20))
			// Leave the declared body unfinished. The bounded summary reader must
			// close it after 2 MiB + 1 bytes instead of waiting for an EOF.
			_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"PRIVATE_OVERSIZED_SUMMARY`+strings.Repeat("x", (2<<20)+4096))
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
				select {
				case summaryClosed <- struct{}{}:
				default:
				}
			case <-stopProvider:
			}
			return
		}
		answers.Add(1)
		encoded, _ := json.Marshal(body.Messages)
		if len(body.Messages) >= len(request.History)+2 || !strings.Contains(string(encoded), "Older conversation was omitted") || strings.Contains(string(encoded), "PRIVATE_OVERSIZED_SUMMARY") {
			t.Error("unavailable summary did not safely mark omitted model context")
		}
		if body.Messages[len(body.Messages)-1].Content != request.Query {
			t.Error("summary failure lost the current query")
		}
		sendStream(w, providerChunk("answer after summary failure", "stop")+"data: [DONE]\n\n")
	}))
	defer server.Close()
	defer close(stopProvider)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var events []Event
	result, err := mustClient(t, liveStreamConfig(t, server.URL)).Stream(ctx, request, func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil || result.Answer != "answer after summary failure" || summaries.Load() != 1 || answers.Load() != 1 {
		t.Fatalf("oversized summary did not fall back to native answer: summaries=%d answers=%d returned_messages=%d err=%v", summaries.Load(), answers.Load(), len(result.Messages), err)
	}
	select {
	case <-summaryClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("oversized summary response was not closed before its body ended")
	}
	if len(events) != 2 || events[0].Type != "message.start" || events[1].Type != "message.delta" || events[1].Delta != result.Answer {
		t.Fatalf("oversized summary entered visible generation events: %+v", events)
	}
	if len(result.Messages) != len(request.History)+2 {
		t.Fatalf("oversized summary changed returned history length: %d", len(result.Messages))
	}
	for i, previous := range request.History {
		if result.Messages[i] != previous {
			t.Fatalf("oversized summary replaced returned history at %d", i)
		}
	}
	encoded, err := json.Marshal(result)
	if err != nil || strings.Contains(string(encoded), "PRIVATE_OVERSIZED_SUMMARY") {
		t.Fatalf("oversized summary was retained in the result: %v", err)
	}
}

func TestStreamNativeDiscardPrecedesNextVisibleAttempt(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			sendStream(w, providerChunk("   ", "stop")+"data: [DONE]\n\n")
			return
		}
		sendStream(w, providerChunk("second attempt", "length")+"data: [DONE]\n\n")
	}))
	defer server.Close()
	var events []Event
	result, err := mustClient(t, liveStreamConfig(t, server.URL)).Stream(context.Background(), requestForTest(), func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil || calls.Load() != 2 || result.Answer != "second attempt" || len(events) != 5 || events[2].Type != "message.discard" || events[2].Round != 1 || events[2].Delta != "" || events[3].Type != "message.start" || events[3].Round != 2 || events[4].Type != "message.delta" || events[4].Round != 2 {
		t.Fatalf("new attempt did not reset: events=%+v result=%+v err=%v", events, result, err)
	}
}

func TestStreamNativeEventMappingBindsResponseAndHidesPrivateEvents(t *testing.T) {
	type nativeEvent struct {
		Type      string         `json:"type"`
		RunID     string         `json:"run_id"`
		SessionID string         `json:"session_id"`
		Data      map[string]any `json:"data"`
	}
	event := func(kind string, data map[string]any) nativeEvent {
		return nativeEvent{Type: kind, RunID: "run-1", SessionID: "session-1", Data: data}
	}
	response := func(kind, id string, round int) nativeEvent {
		return event(kind, map[string]any{"response_id": id, "round": round})
	}
	started := event("run_started", map[string]any{"query": "PRIVATE_NATIVE_QUERY"})
	round := event("round_started", map[string]any{"round": 0})
	attempt := response("model_response_started", "PRIVATE_RESPONSE_ID", 0)
	delta := response("text_delta", "PRIVATE_RESPONSE_ID", 0)
	delta.Data["content"] = "public answer"
	complete := response("model_response_completed", "PRIVATE_RESPONSE_ID", 0)
	complete.Data["kind"], complete.Data["finish_reason"] = "answer", "stop"
	discard := response("model_response_discarded", "PRIVATE_RESPONSE_ID", 0)
	discard.Data["reason"] = "empty"
	final := event("final_answer", map[string]any{"content": "public answer"})
	terminal := event("run_completed", map[string]any{})
	wrongResponse := response("text_delta", "OTHER_RESPONSE_ID", 0)
	wrongResponse.Data["content"] = "must not forward"
	wrongRound := response("text_delta", "PRIVATE_RESPONSE_ID", 1)
	wrongRound.Data["content"] = "must not forward"
	foreignPrivate := event("context_compacted", map[string]any{"summary": "PRIVATE_NATIVE_SUMMARY"})
	foreignPrivate.SessionID = "foreign-session"
	attemptAt := func(n int) nativeEvent { return response("model_response_started", fmt.Sprintf("attempt-%d", n), n) }
	deltaAt := func(n int) nativeEvent {
		e := response("text_delta", fmt.Sprintf("attempt-%d", n), n)
		e.Data["content"] = "public answer"
		return e
	}
	completeAt := func(n int) nativeEvent {
		e := response("model_response_completed", fmt.Sprintf("attempt-%d", n), n)
		e.Data["kind"], e.Data["finish_reason"] = "answer", "stop"
		return e
	}
	cases := map[string]struct {
		input     []nativeEvent
		wantError bool
		wantCount int
		resume    bool
	}{
		"zero tool limit synthesis":     {input: []nativeEvent{started, attempt, delta, complete, final, terminal}, wantCount: 2},
		"implicit synthesis round":      {input: []nativeEvent{started, round, attempt, delta, complete, attemptAt(1), deltaAt(1), completeAt(1), final, terminal}, wantCount: 4},
		"recovery at tool limit":        {input: []nativeEvent{event("run_resumed", map[string]any{}), attemptAt(12), deltaAt(12), completeAt(12), final, terminal}, resume: true, wantCount: 2},
		"implicit skipped round":        {input: []nativeEvent{started, round, attempt, delta, complete, attemptAt(2)}, wantError: true, wantCount: 2},
		"implicit backward round":       {input: []nativeEvent{started, attemptAt(0), completeAt(0), attemptAt(1), completeAt(1), response("model_response_started", "different", 0)}, wantError: true, wantCount: 2},
		"invalid first synthesis round": {input: []nativeEvent{started, attemptAt(12)}, wantError: true},
		"negative synthesis round":      {input: []nativeEvent{started, attemptAt(-1)}, wantError: true},
		"overlapping synthesis":         {input: []nativeEvent{started, attempt, attemptAt(1)}, wantError: true, wantCount: 1},
		"private native metadata": {
			input: []nativeEvent{
				started, event("context_compacted", map[string]any{"summary": "PRIVATE_NATIVE_SUMMARY"}), round, attempt,
				event("thought", map[string]any{"content": "PRIVATE_NATIVE_THOUGHT"}),
				event("tool_call", map[string]any{"arguments": "PRIVATE_NATIVE_TOOL_ARGUMENTS"}),
				event("tool_result", map[string]any{"result": "PRIVATE_NATIVE_TOOL_RESULT"}),
				delta, complete, final, terminal,
			},
			wantCount: 2,
		},
		"wrong response ID":              {input: []nativeEvent{started, round, attempt, wrongResponse, complete, final, terminal}, wantError: true, wantCount: 1},
		"wrong response round":           {input: []nativeEvent{started, round, attempt, wrongRound, complete, final, terminal}, wantError: true, wantCount: 1},
		"missing response ID":            {input: []nativeEvent{started, round, response("model_response_started", "", 0), delta, complete, final, terminal}, wantError: true},
		"reused response ID":             {input: []nativeEvent{started, round, attempt, delta, complete, round, attempt, final, terminal}, wantError: true, wantCount: 2},
		"delta after discard":            {input: []nativeEvent{started, round, attempt, discard, delta, complete, final, terminal}, wantError: true, wantCount: 2},
		"foreign private event":          {input: []nativeEvent{started, foreignPrivate, round, attempt, delta, complete, final, terminal}, wantError: true},
		"completion before final answer": {input: []nativeEvent{started, round, attempt, delta, complete, terminal}, wantError: true, wantCount: 2},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			config := harnessConfig(t)
			bridgeDir, err := filepath.Abs(filepath.Dir(config.Script))
			if err != nil {
				t.Fatal(err)
			}
			input, err := json.Marshal(test.input)
			if err != nil {
				t.Fatal(err)
			}
			script := fmt.Sprintf(`import json, sys
sys.path.insert(0, %q)
from streaming import StreamOutput
from magent_harness.events import Event, EventType
from magent_harness.sse import encode_sse_event
output = StreamOutput(dict(json.load(sys.stdin), recover=json.loads(%q)))
for item in json.loads(%q):
    output.accept(encode_sse_event(Event(EventType(item["type"]), item["run_id"], item["session_id"], item["data"])))
output.finish({"run_id":"run-1","session_id":"session-1","status":"completed","mode":"demo","answer":"public answer","messages":[{"role":"user","content":"query"},{"role":"assistant","content":"public answer"}]})
`, bridgeDir, fmt.Sprint(test.resume), string(input))
			config.Script = filepath.Join(t.TempDir(), "native-events.py")
			if err := os.WriteFile(config.Script, []byte(script), 0600); err != nil {
				t.Fatal(err)
			}
			var events []Event
			result, err := mustClient(t, config).Stream(context.Background(), requestForTest(), func(event Event) error {
				events = append(events, event)
				return nil
			})
			if len(events) != test.wantCount {
				t.Fatalf("native mapper forwarded an invalid or private event: %+v err=%v", events, err)
			}
			if test.wantError {
				if !errors.Is(err, ErrFailed) || result.RunID != "" {
					t.Fatalf("invalid native event returned a result: %+v %v", result, err)
				}
			} else if err != nil || result.Answer != "public answer" {
				t.Fatalf("valid native stream failed: %+v %v", result, err)
			}
			encoded, err := json.Marshal(events)
			if err != nil || strings.Contains(string(encoded), "PRIVATE_") || strings.Contains(string(encoded), "must not forward") {
				t.Fatalf("native-only data entered the public protocol: %s %v", encoded, err)
			}
		})
	}
}

func TestStreamRejectsMalformedProcessProtocol(t *testing.T) {
	start := `{"event":{"type":"message.start","run_id":"run-1","session_id":"session-1","round":1}}` + "\n"
	delta := `{"event":{"type":"message.delta","run_id":"run-1","session_id":"session-1","round":1,"delta":"answer"}}` + "\n"
	discard := `{"event":{"type":"message.discard","run_id":"run-1","session_id":"session-1","round":1}}` + "\n"
	terminal := `{"result":{"run_id":"run-1","session_id":"session-1","status":"completed","mode":"demo","answer":"answer","messages":[{"role":"user","content":"query"},{"role":"assistant","content":"answer"}]}}` + "\n"
	cases := map[string]string{
		"delta before start":    delta + terminal,
		"wrong run":             strings.Replace(start, "run-1", "run-other", 1) + terminal,
		"wrong session":         strings.Replace(start, "session-1", "session-other", 1) + terminal,
		"skipped round":         strings.Replace(start, `"round":1`, `"round":2`, 1) + terminal,
		"delta wrong round":     start + strings.Replace(delta, `"round":1`, `"round":2`, 1) + terminal,
		"empty delta":           start + strings.Replace(delta, `"answer"`, `""`, 1) + terminal,
		"start with delta":      strings.Replace(delta, "message.delta", "message.start", 1) + terminal,
		"discard before start":  discard + terminal,
		"discard wrong round":   start + strings.Replace(discard, `"round":1`, `"round":2`, 1) + terminal,
		"discard wrong run":     start + strings.Replace(discard, "run-1", "run-other", 1) + terminal,
		"discard wrong session": start + strings.Replace(discard, "session-1", "session-other", 1) + terminal,
		"discard with delta":    start + strings.Replace(delta, "message.delta", "message.discard", 1) + terminal,
		"duplicate discard":     start + discard + discard + terminal,
		"delta after discard":   start + discard + delta + terminal,
		"private event":         strings.Replace(start, "message.start", "thought", 1) + terminal,
		"native response ID":    strings.Replace(start, `"round":1`, `"round":1,"response_id":"private-native-response"`, 1) + terminal,
		"early completed":       strings.Replace(start, "message.start", "run.completed", 1) + terminal,
		"result first":          terminal,
		"duplicate terminal":    start + delta + terminal + terminal,
		"terminal then event":   start + delta + terminal + delta,
		"trailing bytes":        start + delta + terminal + "x",
		"missing terminal":      start + delta,
		"partial terminal":      start + delta + strings.TrimSuffix(terminal, "\n"),
		"malformed result":      start + `{"result":{}}` + "\n",
		"both record kinds":     `{"event":{"type":"message.start"},"result":{}}` + "\n",
	}
	for name, output := range cases {
		t.Run(name, func(t *testing.T) {
			config := fakeScriptConfig(t, fmt.Sprintf("import sys\nsys.stdout.write(%q)\nsys.stdout.flush()\n", output))
			_, err := mustClient(t, config).Stream(context.Background(), requestForTest(), func(Event) error { return nil })
			if !errors.Is(err, ErrFailed) {
				t.Fatalf("malformed process stream was accepted: %v", err)
			}
		})
	}
	for _, script := range []string{
		fmt.Sprintf("print('x' * %d)", maxOutputBytes+1),
		"import json\nfor round in range(1, 5):\n print(json.dumps({'event':{'type':'message.start','run_id':'run-1','session_id':'session-1','round':round}}))\n for i in range(25000):\n  print(json.dumps({'event':{'type':'message.delta','run_id':'run-1','session_id':'session-1','round':round,'delta':'x'}}))\n",
	} {
		_, err := mustClient(t, fakeScriptConfig(t, script)).Stream(context.Background(), requestForTest(), func(Event) error { return nil })
		if !errors.Is(err, ErrFailed) || !strings.Contains(err.Error(), "limit") {
			t.Fatalf("stream output limits not enforced: %v", err)
		}
	}
}

func TestStreamCallbackFailureReapsProcess(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "should-not-appear")
	script := fmt.Sprintf("import sys,time\nprint('{\"event\":{\"type\":\"message.start\",\"run_id\":\"run-1\",\"session_id\":\"session-1\",\"round\":1}}', flush=True)\ntime.sleep(0.8)\nopen(%q, 'w').write('leaked')\n", marker)
	sentinel := errors.New("callback stopped")
	_, err := mustClient(t, fakeScriptConfig(t, script)).Stream(context.Background(), requestForTest(), func(Event) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("callback failure not returned: %v", err)
	}
	time.Sleep(850 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("callback error left Python executing")
	}
}

func TestStreamCallbackPanicIsContained(t *testing.T) {
	script := "import time\nprint('{\"event\":{\"type\":\"message.start\",\"run_id\":\"run-1\",\"session_id\":\"session-1\",\"round\":1}}', flush=True)\ntime.sleep(10)\n"
	start := time.Now()
	_, err := mustClient(t, fakeScriptConfig(t, script)).Stream(context.Background(), requestForTest(), func(Event) error {
		panic("sensitive-panic-detail")
	})
	if !errors.Is(err, ErrFailed) || strings.Contains(err.Error(), "sensitive-panic-detail") || time.Since(start) > 2*time.Second {
		t.Fatalf("callback panic was not sanitized and reaped: %v", err)
	}
}
