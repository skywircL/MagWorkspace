package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestContextConfigurationNormalizesBudgetsAndRejectsImpossibleWindows(t *testing.T) {
	config, err := normalizeContextConfig(Config{})
	if err != nil || config.MaxContextTokens != 128000 || config.ReserveTokens != 16384 || config.MaxCompletionTokens != 4096 || config.SummaryTimeout != time.Minute {
		t.Fatalf("unexpected defaults: %+v %v", config, err)
	}
	config, err = normalizeContextConfig(Config{MaxCompletionTokens: 20000})
	if err != nil || config.ReserveTokens != 20000 {
		t.Fatal("completion budget was not reserved")
	}
	for _, invalid := range []Config{
		{MaxContextTokens: -1}, {MaxContextTokens: 1024}, {ReserveTokens: -1},
		{MaxCompletionTokens: 128000}, {MaxSummaryTokens: 128000},
		{SummaryTimeout: -time.Second}, {SummaryTimeout: 11 * time.Minute},
	} {
		if _, err := normalizeContextConfig(invalid); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("invalid budget accepted: %+v %v", invalid, err)
		}
	}
}

func contextProviderConfig(t *testing.T, endpoint string) Config {
	config := harnessConfig(t)
	config.Mode, config.BaseURL, config.APIKey, config.Model = "live", endpoint, "local-test-key", "test-model"
	config.MaxContextTokens, config.ReserveTokens = 4096, 1024
	config.KeepRecentTokens, config.MaxSummaryTokens, config.MaxCompletionTokens = 256, 256, 512
	config.SummaryTimeout = time.Second
	return config
}

func TestHarnessContextBudgetsPrivateCompactionAndHistoryNotice(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint("stream=", streaming), func(t *testing.T) {
			var summaries, answers atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Messages        []Message `json:"messages"`
					Stream          bool      `json:"stream"`
					MaxTokens       int       `json:"max_completion_tokens"`
					NativeMaxTokens int       `json:"max_tokens"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				if len(body.Messages) > 0 && strings.Contains(body.Messages[0].Content, "structured checkpoint") {
					summaries.Add(1)
					tokens := max(body.MaxTokens, body.NativeMaxTokens)
					if body.Stream || tokens <= 0 || tokens > 256 {
						t.Error("private summary streamed or ignored its token budget")
					}
					fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"PRIVATE_CONTEXT_SUMMARY"},"finish_reason":"stop"}]}`)
					return
				}
				answers.Add(1)
				if max(body.MaxTokens, body.NativeMaxTokens) != 512 || body.Stream != streaming {
					t.Error("main model token allowance or mode changed")
				}
				encoded, _ := json.Marshal(body.Messages)
				if !strings.Contains(string(encoded), "PRIVATE_CONTEXT_SUMMARY") || body.Messages[len(body.Messages)-1].Content != "CURRENT_QUERY_EXACT" {
					t.Error("bounded checkpoint or exact current query missing")
				}
				if streaming {
					sendStream(w, providerChunk("public answer", "stop")+"data: [DONE]\n\n")
				} else {
					fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"public answer"},"finish_reason":"stop"}]}`)
				}
			}))
			defer provider.Close()
			request := requestForTest()
			request.Query, request.HistoryWindowed = "CURRENT_QUERY_EXACT", true
			for i := range 24 {
				role := "user"
				if i%2 == 1 {
					role = "assistant"
				}
				request.History = append(request.History, Message{Role: role, Content: strings.Repeat("界", 300)})
			}
			client := mustClient(t, contextProviderConfig(t, provider.URL))
			var result Result
			var err error
			var events []Event
			if streaming {
				result, err = client.Stream(context.Background(), request, func(event Event) error { events = append(events, event); return nil })
			} else {
				result, err = client.Run(context.Background(), request)
			}
			if err != nil || summaries.Load() != 1 || answers.Load() != 1 || result.Answer != "public answer" {
				t.Fatalf("context flow failed: summaries=%d answers=%d err=%v", summaries.Load(), answers.Load(), err)
			}
			encoded, _ := json.Marshal(struct {
				Result Result
				Events []Event
			}{result, events})
			if strings.Contains(string(encoded), "PRIVATE_CONTEXT_SUMMARY") || strings.Contains(string(encoded), "bounded selection") || len(result.Messages) != len(request.History)+2 {
				t.Fatal("private checkpoint or runtime history notice entered ordinary transcript/events")
			}
		})
	}
}

func TestHarnessContextHistoryNoticeIsTrustedAndNotUserTranscript(t *testing.T) {
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if len(body.Messages) != 3 || body.Messages[1].Role != "user" || !strings.Contains(body.Messages[1].Content, "bounded selection") || !strings.Contains(body.Messages[1].Content, "search_conversations") {
			t.Error("bounded history was passed without an explicit source/omission notice")
		}
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}`)
	}))
	defer provider.Close()
	request := requestForTest()
	request.HistoryWindowed = true
	result, err := mustClient(t, contextProviderConfig(t, provider.URL)).Run(context.Background(), request)
	if err != nil || len(result.Messages) != 2 {
		t.Fatalf("runtime notice leaked into transcript: %v", err)
	}
}

func TestHarnessContextRejectsImmutableOversizeBeforeProvider(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint(streaming), func(t *testing.T) {
			var calls atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Error(w, "must not execute", 500) }))
			defer provider.Close()
			config := contextProviderConfig(t, provider.URL)
			request := requestForTest()
			request.Query = strings.Repeat("界", 4000)
			client := mustClient(t, config)
			var err error
			if streaming {
				_, err = client.Stream(context.Background(), request, func(event Event) error {
					if event.Type == "message.delta" {
						t.Error("oversize query exposed model text")
					}
					return nil
				})
			} else {
				_, err = client.Run(context.Background(), request)
			}
			if !errors.Is(err, ErrFailed) || calls.Load() != 0 {
				t.Fatalf("oversize query reached provider: calls=%d err=%v", calls.Load(), err)
			}
		})
	}
}

func TestHarnessContextSummaryTimeoutDegradesWithoutLeakingPartialSummary(t *testing.T) {
	var summaries, answers atomic.Int32
	closed := make(chan struct{}, 1)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages        []Message `json:"messages"`
			MaxTokens       int       `json:"max_completion_tokens"`
			NativeMaxTokens int       `json:"max_tokens"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if len(body.Messages) > 0 && strings.Contains(body.Messages[0].Content, "structured checkpoint") {
			summaries.Add(1)
			fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"PARTIAL_PRIVATE_SUMMARY`)
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
				closed <- struct{}{}
			case <-time.After(3 * time.Second):
			}
			return
		}
		answers.Add(1)
		encoded, _ := json.Marshal(body.Messages)
		if strings.Contains(string(encoded), "PARTIAL_PRIVATE_SUMMARY") || !strings.Contains(string(encoded), "omitted") || body.Messages[len(body.Messages)-1].Content != "CURRENT_QUERY_EXACT" {
			t.Error("summary timeout lost its omission notice/current query or retained partial output")
		}
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"answer after timeout"},"finish_reason":"stop"}]}`)
	}))
	defer provider.Close()
	config := contextProviderConfig(t, provider.URL)
	config.SummaryTimeout = 50 * time.Millisecond
	request := requestForTest()
	request.Query = "CURRENT_QUERY_EXACT"
	for i := range 24 {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		request.History = append(request.History, Message{Role: role, Content: strings.Repeat("界", 300)})
	}
	result, err := mustClient(t, config).Run(context.Background(), request)
	if err != nil || result.Answer != "answer after timeout" || summaries.Load() != 1 || answers.Load() != 1 {
		t.Fatalf("summary timeout did not recover safely: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("timed-out private summary response stayed open")
	}
}

func TestHarnessContextPlainJSONRecoversExplicitProviderOverflow(t *testing.T) {
	for _, status := range []int{400, 422} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var summaries, attempts atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Messages        []Message `json:"messages"`
					MaxTokens       int       `json:"max_completion_tokens"`
					NativeMaxTokens int       `json:"max_tokens"`
					Stream          bool      `json:"stream"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				if len(body.Messages) > 0 && strings.Contains(body.Messages[0].Content, "structured checkpoint") {
					summaries.Add(1)
					fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"PRIVATE_OVERFLOW_CHECKPOINT"},"finish_reason":"stop"}]}`)
					return
				}
				if body.Stream || body.MaxTokens != 512 || body.NativeMaxTokens != 0 {
					t.Error("plain JSON protocol was unexpectedly changed")
				}
				if attempts.Add(1) == 1 {
					w.WriteHeader(status)
					fmt.Fprint(w, `{"error":{"code":"context_length_exceeded","message":"PRIVATE_ERROR_BODY"}}`)
					return
				}
				encoded, _ := json.Marshal(body.Messages)
				if !strings.Contains(string(encoded), "PRIVATE_OVERFLOW_CHECKPOINT") || strings.Contains(string(encoded), "PRIVATE_ERROR_BODY") || body.Messages[len(body.Messages)-1].Content != "CURRENT_QUERY_EXACT" {
					t.Error("forced fit lost current request or failed to use a safe checkpoint")
				}
				fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"answer after overflow"},"finish_reason":"stop"}]}`)
			}))
			defer provider.Close()
			config := contextProviderConfig(t, provider.URL)
			config.MaxContextTokens = 65536
			request := requestForTest()
			request.Query = "CURRENT_QUERY_EXACT"
			for i := range 24 {
				role := "user"
				if i%2 == 1 {
					role = "assistant"
				}
				request.History = append(request.History, Message{Role: role, Content: strings.Repeat("界", 300)})
			}
			result, err := mustClient(t, config).Run(context.Background(), request)
			if err != nil || result.Answer != "answer after overflow" || attempts.Load() != 2 || summaries.Load() != 1 {
				t.Fatalf("explicit overflow did not force one bounded recovery: attempts=%d summaries=%d err=%v", attempts.Load(), summaries.Load(), err)
			}
			encoded, _ := json.Marshal(result)
			if strings.Contains(string(encoded), "PRIVATE_ERROR_BODY") || strings.Contains(string(encoded), "PRIVATE_OVERFLOW_CHECKPOINT") {
				t.Fatal("provider error or private checkpoint entered returned history")
			}
		})
	}
}

func TestHarnessContextPlainJSONDoesNotRetryOrdinaryOrOversizedErrors(t *testing.T) {
	for name, body := range map[string]string{
		"ordinary":  `{"error":{"code":"invalid_request","message":"PRIVATE_ERROR_BODY"}}`,
		"malformed": `{"error":{"code":"context_length_exceeded"}`,
		"oversized": `{"error":{"code":"context_length_exceeded","message":"` + strings.Repeat("PRIVATE_ERROR_BODY", 1000) + `"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(400); fmt.Fprint(w, body) }))
			defer provider.Close()
			_, err := mustClient(t, contextProviderConfig(t, provider.URL)).Run(context.Background(), requestForTest())
			if !errors.Is(err, ErrFailed) || calls.Load() != 1 || strings.Contains(err.Error(), "PRIVATE_ERROR_BODY") {
				t.Fatalf("ordinary/unsafe provider error retried or leaked: calls=%d err=%v", calls.Load(), err)
			}
		})
	}
}
