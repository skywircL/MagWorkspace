package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"magworkspace/internal/domain"
)

const (
	maxToolRequestBytes  = 256 << 10
	maxToolResponseBytes = 1 << 20
	toolRequestTimeout   = 90 * time.Second
)

// ToolHandler is bound by the authenticated backend to one caller and run.
// Tool arguments never choose the caller, workspace, session or resource scope.
type ToolHandler func(context.Context, string, json.RawMessage) (any, error)

type toolCallKey struct{}

func ToolCallID(ctx context.Context) string { id, _ := ctx.Value(toolCallKey{}).(string); return id }

type retrievalEndpoint struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

// bridgePayload creates a per-run private transport. Its random capability is
// passed only over the child process stdin, never to model context or public API.
func bridgePayload(ctx context.Context, request Request) ([]byte, func(), error) {
	var endpoint *retrievalEndpoint
	var persistence *retrievalEndpoint
	var memory *retrievalEndpoint
	closeCallback := func() {}
	if request.ToolHandler != nil {
		var err error
		endpoint, closeCallback, err = startToolServer(ctx, request.ToolHandler)
		if err != nil {
			return nil, closeCallback, err
		}
	}
	if request.JournalHandler != nil {
		var stopJournal func()
		var err error
		persistence, stopJournal, err = startJournalServer(ctx, request.JournalHandler)
		if err != nil {
			closeCallback()
			return nil, func() {}, err
		}
		stopTools := closeCallback
		closeCallback = func() { stopJournal(); stopTools() }
	}
	if request.MemoryHandler != nil {
		var err error
		var stopMemory func()
		memory, stopMemory, err = startMemoryServer(ctx, request.MemoryHandler)
		if err != nil {
			closeCallback()
			return nil, func() {}, err
		}
		stopPrevious := closeCallback
		closeCallback = func() { stopMemory(); stopPrevious() }
	}
	payload, err := marshalBridgePayload(request, endpoint, persistence, memory)
	if err != nil {
		closeCallback()
		return nil, func() {}, err
	}
	return payload, closeCallback, nil
}

// Both transports serialize the same authenticated request and scope. Only the
// callback endpoint lifecycle differs between a local process and an HTTP run.
func marshalBridgePayload(request Request, endpoint, persistence, memory *retrievalEndpoint) ([]byte, error) {
	// A non-nil scope pointer always restricts the run, including a pointer
	// to a nil Go slice. JSON null must be reserved for unrestricted scope.
	scope := request.DocumentIDs
	if scope != nil {
		ids := append([]string{}, (*scope)...)
		scope = &ids
	}
	wire := struct {
		Request
		Retrieval   *retrievalEndpoint `json:"retrieval,omitempty"`
		Persistence *retrievalEndpoint `json:"persistence,omitempty"`
		Memory      *retrievalEndpoint `json:"memory,omitempty"`
		DocumentIDs *[]string          `json:"document_ids"`
	}{Request: request, Retrieval: endpoint, Persistence: persistence, Memory: memory, DocumentIDs: scope}
	payload, err := json.Marshal(wire)
	if err != nil || len(payload) > maxRequestBytes {
		return nil, fmt.Errorf("%w: request exceeds the bridge limit", ErrFailed)
	}
	return payload, nil
}

func startToolServer(ctx context.Context, handler ToolHandler) (*retrievalEndpoint, func(), error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, func() {}, fmt.Errorf("%w: retrieval transport unavailable", ErrUnavailable)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		_ = listener.Close()
		return nil, func() {}, fmt.Errorf("%w: retrieval transport unavailable", ErrUnavailable)
	}
	token := hex.EncodeToString(secret)
	server := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      toolRequestTimeout + time.Second,
		IdleTimeout:       5 * time.Second,
		MaxHeaderBytes:    4 << 10,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		Handler:           toolHTTPHandler(token, handler),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	stopCancel := context.AfterFunc(ctx, func() { _ = server.Close() })
	cleanup := func() {
		stopCancel()
		_ = server.Close()
		<-done
	}
	return &retrievalEndpoint{URL: "http://" + listener.Addr().String() + "/tools", Token: token}, cleanup, nil
}

func toolHTTPHandler(token string, handler ToolHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		// Validate before reading body, and never include supplied credentials or
		// handler errors in a reply: they may contain private document content.
		auth := r.Header.Get("Authorization")
		provided := strings.TrimPrefix(auth, "Bearer ")
		if !strings.HasPrefix(auth, "Bearer ") || len(provided) != len(token) || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/tools" || r.URL.RawQuery != "" || r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxToolRequestBytes))
		if err != nil || !utf8.Valid(raw) || !uniqueJSON(raw) {
			http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
			return
		}
		var request struct {
			ToolCallID string          `json:"tool_call_id,omitempty"`
			Tool       string          `json:"tool"`
			Arguments  json.RawMessage `json:"arguments"`
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF || !validToolArguments(request.Tool, request.Arguments) {
			http.Error(w, `{"error":"invalid tool request"}`, http.StatusBadRequest)
			return
		}
		write := request.Tool == "create_document" || request.Tool == "update_document"
		if (write && (strings.TrimSpace(request.ToolCallID) == "" || len(request.ToolCallID) > 256)) || (!write && request.ToolCallID != "") {
			http.Error(w, `{"error":"invalid tool call"}`, 400)
			return
		}
		ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), toolCallKey{}, request.ToolCallID), toolRequestTimeout)
		defer cancel()
		result, err := callToolHandler(ctx, handler, request.Tool, request.Arguments)
		if err != nil || ctx.Err() != nil {
			code := "tool_unavailable"
			var failure *ToolError
			if errors.As(err, &failure) && safeToolError(failure.Code) {
				code = failure.Code
			}
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code}})
			return
		}
		encoded, err := json.Marshal(result)
		if err != nil || len(encoded) > maxToolResponseBytes {
			http.Error(w, `{"error":"invalid retrieval result"}`, http.StatusBadGateway)
			return
		}
		_, _ = w.Write(encoded)
	})
}

// The transport enforces the closed model-visible schema as defense in depth.
// The authenticated store handler remains the final authority for resource IDs.
func validToolArguments(tool string, raw json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return false
	}
	switch tool {
	case "create_document", "update_document":
		_, _, err := domain.ParseDocumentMutation(tool, raw)
		return err == nil
	case "search_documents", "search_knowledge":
		if len(fields) < 1 || len(fields) > 3 {
			return false
		}
		var query string
		if json.Unmarshal(fields["query"], &query) != nil || utf8.RuneCountInString(strings.TrimSpace(query)) < 2 || len(query) > 512 || strings.IndexFunc(query, unicode.IsControl) >= 0 {
			return false
		}
		for key, value := range fields {
			switch key {
			case "query":
			case "mode":
				var mode string
				if json.Unmarshal(value, &mode) != nil || ((tool == "search_documents" && mode != "hybrid" && mode != "lexical" && mode != "vector") || (tool == "search_knowledge" && mode != "hybrid" && mode != "keyword" && mode != "semantic")) {
					return false
				}
			case "limit":
				var limit int
				if json.Unmarshal(value, &limit) != nil || limit < 1 || limit > 10 {
					return false
				}
			default:
				return false
			}
		}
		return true
	case "resolve_citation_aliases":
		var ids []string
		if len(fields) != 1 || json.Unmarshal(fields["evidence_ids"], &ids) != nil || len(ids) == 0 || len(ids) > 64 {
			return false
		}
		seen := map[string]bool{}
		for _, id := range ids {
			if (len(id) != 32 || strings.Trim(id, "0123456789abcdef") != "") || seen[id] {
				return false
			}
			seen[id] = true
		}
		return true
	case "restore_evidence":
		return len(fields) == 0
	case "list_documents", "search_conversations":
		if tool == "search_conversations" && !validToolText(fields["query"], 512, true) {
			return false
		}
		for key, value := range fields {
			switch key {
			case "query":
				if tool != "search_conversations" || !validToolText(value, 512, true) {
					return false
				}
			case "keyword":
				if tool != "list_documents" || !validToolText(value, 256, false) {
					return false
				}
			case "limit":
				max := 20
				if tool == "search_conversations" {
					max = 8
				}
				if !validToolInt(value, 1, max) {
					return false
				}
			case "offset":
				max := 10000
				if tool == "search_conversations" {
					max = 200
				}
				if !validToolInt(value, 0, max) {
					return false
				}
			default:
				return false
			}
		}
		return true
	case "read_document":
		if value, exists := fields["source"]; exists {
			if string(value) != "true" || !validToolText(fields["document_id"], 32, true) {
				return false
			}
			for key, value := range fields {
				switch key {
				case "source", "document_id":
				case "version_id":
					if !validToolText(value, 32, true) {
						return false
					}
				case "source_offset":
					if !validToolInt(value, 0, domain.MaxAgentDocumentBytes) {
						return false
					}
					var n int
					_ = json.Unmarshal(value, &n)
					if n > 0 && fields["version_id"] == nil {
						return false
					}
				default:
					return false
				}
			}
			return true
		}
		_, doc := fields["document_id"]
		_, evidence := fields["evidence_id"]
		if doc == evidence {
			return false
		}
		_, version := fields["version_id"]
		_, index := fields["index_id"]
		if version != index {
			return false
		}
		for key, value := range fields {
			switch key {
			case "document_id", "evidence_id", "version_id", "index_id":
				if !validToolText(value, 256, true) {
					return false
				}
			case "query":
				if !validToolText(value, 256, true) {
					return false
				}
			case "limit":
				if !validToolInt(value, 1, 10) {
					return false
				}
			case "offset":
				if !validToolInt(value, 0, 2048) {
					return false
				}
				var n int
				_ = json.Unmarshal(value, &n)
				if n > 0 && !version {
					return false
				}
			case "context":
				if !validToolInt(value, 0, 3) {
					return false
				}
			default:
				return false
			}
			if evidence && (key == "version_id" || key == "index_id" || key == "offset" || key == "query") {
				return false
			}
		}
		return true
	case "read_evidence":
		var id string
		return len(fields) == 1 && json.Unmarshal(fields["evidence_id"], &id) == nil && strings.TrimSpace(id) != "" && len(id) <= 256
	default:
		return false
	}
}

func callToolHandler(ctx context.Context, handler ToolHandler, tool string, raw json.RawMessage) (result any, err error) {
	defer func() {
		if recover() != nil {
			result, err = nil, errors.New("retrieval handler failed")
		}
	}()
	return handler(ctx, tool, raw)
}

// uniqueJSON rejects duplicate fields at every level. Standard JSON decoding
// accepts last-key-wins input, which would make signed/scoped payloads ambiguous.
func uniqueJSON(raw []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value func(int) bool
	value = func(depth int) bool {
		if depth > 16 {
			return false
		}
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return true
		}
		switch delim {
		case '{':
			keys := map[string]bool{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				key, ok := keyToken.(string)
				if err != nil || !ok || keys[key] {
					return false
				}
				keys[key] = true
				if !value(depth + 1) {
					return false
				}
			}
			end, err := decoder.Token()
			return err == nil && end == json.Delim('}')
		case '[':
			for decoder.More() {
				if !value(depth + 1) {
					return false
				}
			}
			end, err := decoder.Token()
			return err == nil && end == json.Delim(']')
		default:
			return false
		}
	}
	return value(0) && decoder.Decode(new(any)) == io.EOF
}

// ToolError exposes only a fixed public code, never an underlying private error.
type ToolError struct{ Code string }

func (e *ToolError) Error() string { return e.Code }
func safeToolError(code string) bool {
	switch code {
	case "indexing_unavailable", "index_not_ready", "index_failed", "evidence_limit", "not_found", "forbidden", "invalid_arguments", "tool_unavailable", "write_conflict", "approval_required":
		return true
	}
	return false
}
func validToolText(raw json.RawMessage, limit int, required bool) bool {
	var value string
	return len(raw) > 0 && string(raw) != "null" && json.Unmarshal(raw, &value) == nil && utf8.ValidString(value) && len(value) <= limit && (!required || strings.TrimSpace(value) != "") && strings.IndexFunc(value, unicode.IsControl) < 0
}
func validToolInt(raw json.RawMessage, min, max int) bool {
	var value int
	return len(raw) > 0 && string(raw) != "null" && json.Unmarshal(raw, &value) == nil && value >= min && value <= max
}
