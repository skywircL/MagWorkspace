package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxJournalRecordBytes   = 2 << 20
	maxJournalRequestBytes  = 3 << 20
	maxJournalResponseBytes = 17 << 20 // 16 MiB journal plus the JSON response wrapper.
)

// PersistenceHandler is bound to an authenticated run and execution generation.
// Neither its capability nor its operations are offered to the model as tools.
// The backend must enforce a live lease and generation on every operation.
type PersistenceHandler func(context.Context, string, json.RawMessage) (any, error)

func startJournalServer(ctx context.Context, handler PersistenceHandler) (*retrievalEndpoint, func(), error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, func() {}, fmt.Errorf("%w: journal transport unavailable", ErrUnavailable)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		_ = listener.Close()
		return nil, func() {}, fmt.Errorf("%w: journal transport unavailable", ErrUnavailable)
	}
	token := hex.EncodeToString(secret)
	server := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      toolRequestTimeout + time.Second,
		IdleTimeout:       5 * time.Second,
		MaxHeaderBytes:    4 << 10,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		Handler:           journalHTTPHandler(token, handler),
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	stopCancel := context.AfterFunc(ctx, func() { _ = server.Close() })
	cleanup := func() { stopCancel(); _ = server.Close(); <-done }
	return &retrievalEndpoint{URL: "http://" + listener.Addr().String() + "/journal", Token: token}, cleanup, nil
}

func journalHTTPHandler(token string, handler PersistenceHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		auth := r.Header.Get("Authorization")
		provided := strings.TrimPrefix(auth, "Bearer ")
		if len(r.Header.Values("Authorization")) != 1 || !strings.HasPrefix(auth, "Bearer ") || len(provided) != len(token) || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/journal" || r.URL.RawQuery != "" || r.Header.Get("Content-Type") != "application/json" || len(r.Header.Values("Content-Type")) != 1 {
			http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxJournalRequestBytes))
		if err != nil || !utf8.Valid(raw) || !uniqueJSON(raw) {
			http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
			return
		}
		var request struct {
			Operation string          `json:"operation"`
			Arguments json.RawMessage `json:"arguments"`
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&request) != nil || decoder.Decode(new(any)) != io.EOF || !validJournalArguments(request.Operation, request.Arguments) {
			http.Error(w, `{"error":"invalid journal request"}`, http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), toolRequestTimeout)
		defer cancel()
		result, err := callToolHandler(ctx, ToolHandler(handler), request.Operation, request.Arguments)
		if err != nil || ctx.Err() != nil {
			http.Error(w, `{"error":"journal unavailable"}`, http.StatusForbidden)
			return
		}
		encoded, err := encodeJournalResult(result)
		if err != nil || len(encoded) > maxJournalResponseBytes {
			http.Error(w, `{"error":"invalid journal result"}`, http.StatusBadGateway)
			return
		}
		_, _ = w.Write(encoded)
	})
}

func validJournalArguments(operation string, raw json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &fields) != nil || fields == nil {
		return false
	}
	switch operation {
	case "load", "checkpoint_read":
		return len(fields) == 0
	case "append":
		record, ok := fields["record"]
		if len(fields) != 1 || !ok || len(record) > maxJournalRecordBytes {
			return false
		}
		var envelope map[string]json.RawMessage
		if json.Unmarshal(record, &envelope) != nil || len(envelope) != 6 {
			return false
		}
		for _, key := range []string{"version", "seq", "kind", "data", "prev_hash", "digest"} {
			if _, ok := envelope[key]; !ok {
				return false
			}
		}
		var version, seq uint64
		var kind, previous, digest string
		var data map[string]json.RawMessage
		if json.Unmarshal(envelope["version"], &version) != nil || version != 1 || json.Unmarshal(envelope["seq"], &seq) != nil || seq == 0 || json.Unmarshal(envelope["kind"], &kind) != nil || strings.TrimSpace(kind) == "" || len(kind) > 128 || json.Unmarshal(envelope["data"], &data) != nil || data == nil || json.Unmarshal(envelope["prev_hash"], &previous) != nil || json.Unmarshal(envelope["digest"], &digest) != nil {
			return false
		}
		return validJournalHash(digest) && (previous == "" || validJournalHash(previous))
	case "checkpoint_write":
		var data map[string]json.RawMessage
		return len(fields) == 1 && len(fields["data"]) <= maxJournalRecordBytes && json.Unmarshal(fields["data"], &data) == nil && data != nil
	default:
		return false
	}
}

func validJournalHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

// Preserve stored envelope spelling. encoding/json rewrites RawMessage strings
// containing HTML characters and Unicode line separators even without HTML
// escaping, which can inflate an allowed journal beyond the response budget.
func encodeJournalResult(result any) ([]byte, error) {
	if raw, ok := result.(json.RawMessage); ok {
		if !utf8.Valid(raw) || !json.Valid(raw) {
			return nil, fmt.Errorf("invalid journal JSON")
		}
		return append([]byte{}, raw...), nil
	}
	if object, ok := result.(map[string]any); ok && len(object) == 1 {
		if records, ok := object["records"].([]json.RawMessage); ok {
			encoded := []byte(`{"records":[`)
			for index, raw := range records {
				if !utf8.Valid(raw) || !json.Valid(raw) {
					return nil, fmt.Errorf("invalid journal JSON")
				}
				if index != 0 {
					encoded = append(encoded, ',')
				}
				encoded = append(encoded, raw...)
			}
			return append(encoded, ']', '}'), nil
		}
	}
	return json.Marshal(result)
}
