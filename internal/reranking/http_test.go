package reranking

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

func TestHTTPClientTEIContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rerank" || r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("wrong configured endpoint or authorization")
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil ||
			body["query"] != "退款期限" || body["truncate"] != true ||
			body["raw_scores"] != false || body["return_text"] != false || len(body) != 5 {
			t.Errorf("wrong TEI payload: %#v", body)
		}
		if len(body["texts"].([]any)) != 2 {
			t.Error("wrong candidate count")
		}
		fmt.Fprint(w, `[{"index":1,"score":0.9},{"index":0,"score":0.2}]`)
	}))
	defer server.Close()
	client, err := NewHTTPClient(server.URL+"/rerank", "test-key", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	scores, err := client.Score(context.Background(), "退款期限", []string{"配送说明", "七天"})
	if err != nil || len(scores) != 2 || scores[0].Index != 1 || scores[0].RelevanceScore != 0.9 {
		t.Fatalf("wrong normalized response: %+v, %v", scores, err)
	}
}

func TestHTTPClientRejectsMalformedOrPartialResponses(t *testing.T) {
	bodies := []string{
		`[]`,
		`[{"index":0,"score":0.8}]`,
		`[{"index":0,"score":0.8},{"index":0,"score":0.7}]`,
		`[{"index":0,"score":0.8},{"index":2,"score":0.7}]`,
		`[{"score":0.8},{"index":1,"score":0.7}]`,
		`[{"index":0},{"index":1,"score":0.7}]`,
		`[{"index":null,"score":0.8},{"index":1,"score":0.7}]`,
		`[{"index":0,"score":null},{"index":1,"score":0.7}]`,
		`[{"index":0,"score":-2},{"index":1,"score":0.7}]`,
		`[{"index":0,"score":1.1},{"index":1,"score":0.7}]`,
		`[{"index":0,"score":NaN},{"index":1,"score":0.7}]`,
		`{"results":[{"index":0,"score":0.8},{"index":1,"score":0.7}]}`,
		`[{"index":0,"score":0.8},{"index":1,"score":0.7}] {}`,
		strings.Repeat("x", maxResponseBytes+1),
	}
	for i, body := range bodies {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
			defer server.Close()
			client, _ := NewHTTPClient(server.URL, "", time.Second)
			_, err := client.Score(context.Background(), "query", []string{"first", "second"})
			if !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("malformed response accepted: %v", err)
			}
		})
	}
}

func TestHTTPClientNoRedirectProxyOrProviderErrorLeak(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { redirected.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprint(w, "secret credentials and submitted document")
	}))
	defer server.Close()
	t.Setenv("HTTP_PROXY", target.URL)
	t.Setenv("HTTPS_PROXY", target.URL)
	for _, path := range []string{"/redirect", "/error"} {
		client, _ := NewHTTPClient(server.URL+path, "key", time.Second)
		transport := client.client.Transport.(*http.Transport)
		if transport.Proxy != nil {
			t.Fatal("environment proxy is enabled")
		}
		_, err := client.Score(context.Background(), "query", []string{"body"})
		if !errors.Is(err, ErrModelUnavailable) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unsafe provider error: %v", err)
		}
	}
	if redirected.Load() != 0 {
		t.Fatal("redirect followed or credentials sent to proxy")
	}
}

func TestHTTPClientCancellationAndBodyDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	client, _ := NewHTTPClient(server.URL, "", time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Score(ctx, "query", []string{"body"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancel ignored: %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := client.Score(ctx, "query", []string{"body"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller deadline not propagated: %v", err)
	}
	started := time.Now()
	if _, err := client.Score(context.Background(), "query", []string{"body"}); !errors.Is(err, ErrTimeout) {
		t.Fatalf("body timeout unbounded: %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("body timeout exceeded configured bound")
	}
}

func TestHTTPClientConfigurationAndInputValidation(t *testing.T) {
	for _, endpoint := range []string{
		"file:///tmp/socket", "ftp://host/rerank", "http:///rerank", "http://u:p@host/rerank",
		"http://host/rerank?key=secret", "http://host/rerank?", "http://host/rerank#fragment", " http://host/rerank",
	} {
		if _, err := NewHTTPClient(endpoint, "", time.Second); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("invalid endpoint accepted: %s", endpoint)
		}
	}
	if _, err := NewHTTPClient("http://host/rerank", "key\r\ninjected", time.Second); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("header injection accepted")
	}
	if _, err := NewHTTPClient("http://host/rerank", "", 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("unbounded timeout accepted")
	}
	client, _ := NewHTTPClient("http://127.0.0.1:1/rerank", "", time.Second)
	for _, passages := range [][]string{nil, {""}, {strings.Repeat("文", maxPassageRunes+1)}, {"\xff"}} {
		if _, err := client.Score(context.Background(), "query", passages); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("invalid passage dispatched: %v", err)
		}
	}
	oversized := make([]string, DefaultMaxCandidates)
	for i := range oversized {
		oversized[i] = strings.Repeat("文", maxPassageRunes)
	}
	if _, err := client.Score(context.Background(), "query", oversized); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("oversized JSON payload was dispatched: %v", err)
	}
}
