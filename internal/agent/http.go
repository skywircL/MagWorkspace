package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// HTTPConfig contains service credentials only. Model/provider credentials are
// owned by the standalone Harness deployment and never enter this client.
type HTTPConfig struct {
	URL                string
	APIKey             string
	Mode               string
	Timeout            time.Duration
	CallbackListenAddr string
	CallbackBaseURL    string
}

type HTTPClient struct {
	config  HTTPConfig
	client  *http.Client
	gateway *callbackGateway
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewHTTPClient(config HTTPConfig) (*HTTPClient, error) {
	if config.Mode != "demo" && config.Mode != "live" {
		return nil, fmt.Errorf("%w: HTTP mode must be demo or live", ErrUnavailable)
	}
	if _, err := parseServiceURL(config.URL); err != nil {
		return nil, fmt.Errorf("%w: invalid Harness service URL", ErrUnavailable)
	}
	if len(config.APIKey) < 32 || strings.IndexFunc(config.APIKey, func(char rune) bool { return char < 33 || char > 126 }) >= 0 {
		return nil, fmt.Errorf("%w: Harness service key must contain at least 32 printable ASCII characters without spaces", ErrUnavailable)
	}
	if config.Timeout <= 0 {
		config.Timeout = 120 * time.Second
	}
	gateway, err := newCallbackGateway(config.CallbackListenAddr, config.CallbackBaseURL)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy:             nil,
		DialContext:       (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, MaxIdleConns: 32, MaxIdleConnsPerHost: 16,
		IdleConnTimeout: 60 * time.Second, TLSHandshakeTimeout: 10 * time.Second,
		MaxResponseHeaderBytes: 16 << 10, DisableCompression: true,
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &HTTPClient{config: config, gateway: gateway, ctx: ctx, cancel: cancel, client: &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

// HTTP origins are restricted to deployment-local networks. Hostnames are
// configured by operators, never supplied by a user, callback, or model output.
func parseServiceURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || strings.ContainsAny(raw, "\\\r\n\x00") {
		return nil, fmt.Errorf("invalid service URL")
	}
	if u.Port() != "" {
		port, err := strconv.Atoi(u.Port())
		if err != nil || port < 1 || port > 65535 {
			return nil, fmt.Errorf("invalid service port")
		}
	}
	if u.Scheme == "https" {
		return u, nil
	}
	host := strings.ToLower(u.Hostname())
	ip := net.ParseIP(host)
	internal := ip != nil && (ip.IsLoopback() || ip.IsPrivate())
	if ip == nil {
		internal = host == "localhost" || (!strings.Contains(host, ".") && host != "") || strings.HasSuffix(host, ".internal") || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".svc") || strings.HasSuffix(host, ".cluster.local")
	}
	if u.Scheme != "http" || !internal {
		return nil, fmt.Errorf("unencrypted service must use an internal origin")
	}
	return u, nil
}

func (c *HTTPClient) Close() error {
	c.cancel()
	c.client.CloseIdleConnections()
	return c.gateway.Close()
}

func (c *HTTPClient) Run(ctx context.Context, request Request) (Result, error) {
	return c.execute(ctx, request, nil)
}

func (c *HTTPClient) Stream(ctx context.Context, request Request, onEvent func(Event) error) (Result, error) {
	if onEvent == nil {
		return Result{}, fmt.Errorf("%w: stream callback is required", ErrFailed)
	}
	return c.execute(ctx, request, onEvent)
}

func (c *HTTPClient) execute(ctx context.Context, request Request, onEvent func(Event) error) (Result, error) {
	if err := validateRequest(request); err != nil {
		return Result{}, err
	}
	if c.ctx.Err() != nil {
		return Result{}, fmt.Errorf("%w: HTTP client is closed", ErrUnavailable)
	}
	runCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	if err := runCtx.Err(); err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrFailed, err)
	}
	payload, cleanup, err := c.gateway.payload(runCtx, request)
	if err != nil {
		return Result{}, err
	}
	defer cleanup()
	req, err := http.NewRequestWithContext(runCtx, http.MethodPost, strings.TrimRight(c.config.URL, "/")+"/v1/runs", bytes.NewReader(payload))
	if err != nil {
		return Result{}, fmt.Errorf("%w: invalid service request", ErrFailed)
	}
	// A run may already have executed after a lost response. Never replay POST,
	// forward credentials to redirects, or use the host's environment proxy.
	req.GetBody = nil
	req.Header.Set("Authorization", "Bearer "+c.config.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Magent-Protocol", "1")
	deadline, _ := runCtx.Deadline()
	milliseconds := (time.Until(deadline) + time.Millisecond - 1) / time.Millisecond
	if milliseconds <= 0 {
		return Result{}, fmt.Errorf("%w: %w", ErrFailed, context.DeadlineExceeded)
	}
	req.Header.Set("X-Magent-Timeout-Ms", strconv.FormatInt(int64(milliseconds), 10))
	accept := "application/json"
	if onEvent != nil {
		accept = "text/event-stream"
	}
	req.Header.Set("Accept", accept)
	response, err := c.client.Do(req)
	if err != nil {
		return Result{}, c.requestError(runCtx, "service request unavailable", ErrUnavailable)
	}
	defer func() {
		cancel()
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusOK {
		kind := ErrFailed
		switch response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			kind = ErrUnavailable
		}
		return Result{}, fmt.Errorf("%w: service rejected request", kind)
	}
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if len(response.Header.Values("X-Magent-Protocol")) != 1 || response.Header.Get("X-Magent-Protocol") != "1" || len(response.Header.Values("Content-Type")) != 1 || err != nil || contentType != accept || response.Header.Get("Content-Encoding") != "" {
		return Result{}, fmt.Errorf("%w: invalid service response protocol", ErrFailed)
	}
	if onEvent != nil {
		result, err := readHTTPStream(response.Body, request, c.config.Mode, onEvent, cancel)
		if err != nil && runCtx.Err() != nil {
			// Preserve callbackError and its errors.Is chain; it deliberately
			// cancels the context before returning to this layer.
			if _, ok := err.(*callbackError); !ok {
				return Result{}, c.requestError(runCtx, "service stream failed", ErrFailed)
			}
		}
		return result, err
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxOutputBytes+1))
	if err != nil {
		return Result{}, c.requestError(runCtx, "service response interrupted", ErrFailed)
	}
	if len(data) > maxOutputBytes {
		return Result{}, fmt.Errorf("%w: service response exceeds the limit", ErrFailed)
	}
	return decodeResult(data, request, c.config.Mode)
}

func (c *HTTPClient) requestError(ctx context.Context, message string, kind error) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrFailed, err)
	}
	return fmt.Errorf("%w: %s", kind, message)
}

func readHTTPStream(reader io.Reader, request Request, mode string, callback func(Event) error, cancel context.CancelFunc) (Result, error) {
	limited := &io.LimitedReader{R: reader, N: maxStreamBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 4096), maxOutputBytes+128)
	output := &streamOutput{request: request, mode: mode, callback: callback, cancel: cancel}
	var event string
	var data []byte
	haveData := false
	fail := func(reason string) (Result, error) {
		return Result{}, fmt.Errorf("%w: %s", ErrFailed, reason)
	}
	for scanner.Scan() {
		line := scanner.Bytes()
		if limited.N == 0 {
			return fail("service stream exceeds the limit")
		}
		if output.result != nil {
			return fail("trailing service stream record")
		}
		if !utf8.Valid(line) {
			return fail("invalid service stream encoding")
		}
		if len(line) == 0 {
			if event == "" && !haveData {
				continue
			}
			if event == "" || !haveData || !uniqueJSON(data) {
				return fail("invalid service stream record")
			}
			switch event {
			case "message.start", "message.delta", "message.discard":
				var decoded Event
				if json.Unmarshal(data, &decoded) != nil || decoded.Type != event {
					return fail("inconsistent service stream event")
				}
				wrapped := append([]byte(`{"event":`), data...)
				if err := output.record(append(wrapped, '}')); err != nil {
					cancel()
					return Result{}, err
				}
			case "result":
				wrapped := append([]byte(`{"result":`), data...)
				if err := output.record(append(wrapped, '}')); err != nil {
					return Result{}, err
				}
			case "error":
				var decoded struct {
					Code string `json:"code"`
				}
				decoder := json.NewDecoder(bytes.NewReader(data))
				decoder.DisallowUnknownFields()
				if decoder.Decode(&decoded) != nil || decoder.Decode(new(any)) != io.EOF {
					return fail("invalid service error")
				}
				if decoded.Code == "agent_unavailable" {
					return Result{}, fmt.Errorf("%w: service execution unavailable", ErrUnavailable)
				}
				if decoded.Code != "agent_failed" {
					return fail("invalid service error")
				}
				return fail("service execution failed")
			default:
				return fail("unsupported service stream event")
			}
			event, data, haveData = "", nil, false
			continue
		}
		if line[0] == ':' {
			continue
		}
		field, value, found := strings.Cut(string(line), ":")
		if !found {
			return fail("invalid service stream field")
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			if event != "" || len(value) > 32 {
				return fail("invalid service stream event name")
			}
			event = value
		case "data":
			if haveData {
				data = append(data, '\n')
			}
			if len(data)+len(value) > maxOutputBytes {
				return fail("service stream record exceeds the limit")
			}
			data = append(data, value...)
			haveData = true
		default:
			return fail("unsupported service stream field")
		}
	}
	if scanner.Err() != nil || limited.N == 0 || event != "" || haveData || output.result == nil {
		return fail("incomplete service stream")
	}
	return *output.result, nil
}
