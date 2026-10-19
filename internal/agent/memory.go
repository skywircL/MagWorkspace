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
	"math"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxMemoryRequestBytes  = 64 << 10
	maxMemoryResponseBytes = 16 << 20
)

func startMemoryServer(ctx context.Context, handler PersistenceHandler) (*retrievalEndpoint, func(), error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, func() {}, fmt.Errorf("%w: memory transport unavailable", ErrUnavailable)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		_ = listener.Close()
		return nil, func() {}, fmt.Errorf("%w: memory transport unavailable", ErrUnavailable)
	}
	token := hex.EncodeToString(secret)
	server := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      toolRequestTimeout + time.Second,
		IdleTimeout:       5 * time.Second,
		MaxHeaderBytes:    4 << 10,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		Handler:           memoryHTTPHandler(token, handler),
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	stopCancel := context.AfterFunc(ctx, func() { _ = server.Close() })
	cleanup := func() { stopCancel(); _ = server.Close(); <-done }
	return &retrievalEndpoint{URL: "http://" + listener.Addr().String() + "/memory", Token: token}, cleanup, nil
}

func memoryHTTPHandler(token string, handler PersistenceHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		auth := r.Header.Get("Authorization")
		provided := strings.TrimPrefix(auth, "Bearer ")
		if len(r.Header.Values("Authorization")) != 1 || !strings.HasPrefix(auth, "Bearer ") || len(provided) != len(token) || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/memory" || r.URL.RawQuery != "" || r.Header.Get("Content-Type") != "application/json" || len(r.Header.Values("Content-Type")) != 1 {
			http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxMemoryRequestBytes))
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
		if decoder.Decode(&request) != nil || decoder.Decode(new(any)) != io.EOF || !validMemoryArguments(request.Operation, request.Arguments) {
			http.Error(w, `{"error":"invalid memory request"}`, http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), toolRequestTimeout)
		defer cancel()
		result, err := callToolHandler(ctx, ToolHandler(handler), request.Operation, request.Arguments)
		if err != nil || ctx.Err() != nil {
			http.Error(w, `{"error":"memory unavailable"}`, http.StatusForbidden)
			return
		}
		encoded, err := json.Marshal(result)
		if err != nil || len(encoded) > maxMemoryResponseBytes {
			http.Error(w, `{"error":"invalid memory result"}`, http.StatusBadGateway)
			return
		}
		_, _ = w.Write(encoded)
	})
}

func validMemoryArguments(operation string, raw json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return false
	}
	switch operation {
	case "list":
		if len(fields) > 1 {
			return false
		}
		if len(fields) == 0 {
			return true
		}
		var status string
		return fields["status"] != nil && json.Unmarshal(fields["status"], &status) == nil && (status == "active" || status == "superseded" || status == "archived")
	case "get_items", "touch":
		if (operation == "get_items" && len(fields) != 1) || (operation == "touch" && len(fields) != 2) {
			return false
		}
		var ids []string
		if json.Unmarshal(fields["item_ids"], &ids) != nil || ids == nil || len(ids) > 1000 {
			return false
		}
		for _, id := range ids {
			if !promptIDPattern.MatchString(id) {
				return false
			}
		}
		if operation == "touch" {
			var now float64
			if string(fields["now"]) == "null" || json.Unmarshal(fields["now"], &now) != nil || math.IsNaN(now) || math.IsInf(now, 0) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
