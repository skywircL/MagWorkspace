package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type callbackRegistration struct {
	path    string
	ctx     context.Context
	cancel  context.CancelFunc
	handler http.Handler
}

// The private listener is distinct from the public API. Capabilities select
// already authenticated closures; no caller or resource scope comes from HTTP.
type callbackGateway struct {
	baseURL string
	server  *http.Server
	done    chan struct{}
	mu      sync.Mutex
	entries map[string]callbackRegistration
	closed  bool
}

func newCallbackGateway(address, baseURL string) (*callbackGateway, error) {
	if address == "" {
		address = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("%w: callback listener unavailable", ErrUnavailable)
	}
	if baseURL == "" {
		address := listener.Addr().(*net.TCPAddr)
		if !address.IP.IsLoopback() {
			_ = listener.Close()
			return nil, fmt.Errorf("%w: non-loopback callbacks require a configured origin", ErrUnavailable)
		}
		baseURL = "http://" + listener.Addr().String()
	}
	parsed, err := parseServiceURL(baseURL)
	if err != nil || parsed.Path != "" || parsed.Hostname() == "" || parsed.Port() == "" || strings.IndexFunc(parsed.Hostname(), func(char rune) bool {
		return !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '.' || char == '-')
	}) >= 0 {
		_ = listener.Close()
		return nil, fmt.Errorf("%w: invalid callback origin", ErrUnavailable)
	}
	port, _ := strconv.Atoi(parsed.Port())
	if baseURL != parsed.Scheme+"://"+parsed.Hostname()+":"+strconv.Itoa(port) {
		_ = listener.Close()
		return nil, fmt.Errorf("%w: invalid callback origin", ErrUnavailable)
	}
	g := &callbackGateway{baseURL: baseURL, done: make(chan struct{}), entries: make(map[string]callbackRegistration)}
	g.server = &http.Server{
		Handler: g, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: toolRequestTimeout + time.Second, IdleTimeout: 5 * time.Second,
		MaxHeaderBytes: 4 << 10, ErrorLog: log.New(io.Discard, "", 0),
	}
	go func() { defer close(g.done); _ = g.server.Serve(listener) }()
	return g, nil
}

func (g *callbackGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path != "/tools" && r.URL.Path != "/journal" && r.URL.Path != "/memory" {
		http.NotFound(w, r)
		return
	}
	auth := r.Header.Get("Authorization")
	if len(r.Header.Values("Authorization")) != 1 || !strings.HasPrefix(auth, "Bearer ") || len(auth) != len("Bearer ")+64 {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	g.mu.Lock()
	entry, ok := g.entries[strings.TrimPrefix(auth, "Bearer ")]
	g.mu.Unlock()
	if !ok || entry.path != r.URL.Path || entry.ctx.Err() != nil {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	// Propagate both run revocation and a disconnected callback caller, while
	// retaining only the backend-bound context's identity and trace values.
	ctx, cancel := context.WithCancel(entry.ctx)
	defer cancel()
	stop := context.AfterFunc(r.Context(), cancel)
	defer stop()
	entry.handler.ServeHTTP(w, r.WithContext(ctx))
}

func (g *callbackGateway) payload(ctx context.Context, request Request) ([]byte, func(), error) {
	runCtx, cancel := context.WithCancel(ctx)
	var endpoints [3]*retrievalEndpoint
	var tokens []string
	paths := []string{"/tools", "/journal", "/memory"}
	handlers := []func(string) http.Handler{nil, nil, nil}
	if request.ToolHandler != nil {
		handlers[0] = func(token string) http.Handler { return toolHTTPHandler(token, request.ToolHandler) }
	}
	if request.JournalHandler != nil {
		handlers[1] = func(token string) http.Handler { return journalHTTPHandler(token, request.JournalHandler) }
	}
	if request.MemoryHandler != nil {
		handlers[2] = func(token string) http.Handler { return memoryHTTPHandler(token, request.MemoryHandler) }
	}
	var once sync.Once
	revoke := func() {
		once.Do(func() {
			cancel()
			g.mu.Lock()
			defer g.mu.Unlock()
			for _, token := range tokens {
				delete(g.entries, token)
			}
		})
	}
	g.mu.Lock()
	if g.closed || ctx.Err() != nil {
		g.mu.Unlock()
		revoke()
		return nil, func() {}, fmt.Errorf("%w: callback gateway is closed", ErrUnavailable)
	}
	for index, factory := range handlers {
		if factory == nil {
			continue
		}
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			g.mu.Unlock()
			revoke()
			return nil, func() {}, fmt.Errorf("%w: callback capability unavailable", ErrUnavailable)
		}
		token := hex.EncodeToString(secret)
		tokens = append(tokens, token)
		g.entries[token] = callbackRegistration{path: paths[index], ctx: runCtx, cancel: cancel, handler: factory(token)}
		endpoints[index] = &retrievalEndpoint{URL: g.baseURL + paths[index], Token: token}
	}
	g.mu.Unlock()
	stop := context.AfterFunc(runCtx, revoke)
	cleanup := func() { stop(); revoke() }
	data, err := marshalBridgePayload(request, endpoints[0], endpoints[1], endpoints[2])
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	return data, cleanup, nil
}

func (g *callbackGateway) Close() error {
	g.mu.Lock()
	g.closed = true
	for token, entry := range g.entries {
		entry.cancel()
		delete(g.entries, token)
	}
	g.mu.Unlock()
	err := g.server.Close()
	<-g.done
	return err
}
