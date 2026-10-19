// Package agent connects the workspace backend to the separate Magent Harness.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrUnavailable = errors.New("agent unavailable")
	ErrFailed      = errors.New("agent run failed")
)

const (
	maxRequestBytes = 2 << 20
	maxOutputBytes  = 2 << 20
	maxStderrBytes  = 16 << 10
	maxMessageBytes = 64 << 10
	maxHistory      = 512
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Request identity, role and history must come from the authenticated backend.
// These fields must never be copied from an untrusted HTTP request body.
type Request struct {
	RunID           string             `json:"run_id"`
	SessionID       string             `json:"session_id"`
	WorkspaceID     string             `json:"workspace_id"`
	UserID          string             `json:"user_id"`
	Role            string             `json:"role"`
	History         []Message          `json:"history"`
	HistoryWindowed bool               `json:"history_windowed,omitempty"`
	Query           string             `json:"query"`
	DocumentIDs     *[]string          `json:"document_ids,omitempty"`
	ToolHandler     ToolHandler        `json:"-"`
	Resume          *ResumeRequest     `json:"resume,omitempty"`
	Recover         bool               `json:"recover,omitempty"`
	JournalHandler  PersistenceHandler `json:"-"`
	MemoryHandler   PersistenceHandler `json:"-"`
	Generation      uint64             `json:"-"`
}

// Resume is assembled only from a claimed, authenticated persisted prompt.
type ResumeRequest struct {
	Prompt json.RawMessage `json:"prompt"`
}

// UserPrompt is an internal result. Snapshot must never enter public responses.
type UserPrompt struct {
	Kind       string          `json:"kind,omitempty"`
	Request    json.RawMessage `json:"request,omitempty"`
	ID         string          `json:"id"`
	ToolCallID string          `json:"tool_call_id"`
	Question   string          `json:"question"`
	Options    []string        `json:"options"`
	Snapshot   string          `json:"snapshot"`
	ExpiresAt  float64         `json:"expires_at"`
}

type Result struct {
	RunID    string      `json:"run_id"`
	Answer   string      `json:"answer"`
	Mode     string      `json:"mode"`
	Messages []Message   `json:"messages"`
	Status   string      `json:"status"`
	Prompt   *UserPrompt `json:"prompt,omitempty"`
}

type Client interface {
	Run(context.Context, Request) (Result, error)
}

type Config struct {
	Python              string
	Script              string
	HarnessPath         string
	Mode                string
	BaseURL             string
	APIKey              string
	Model               string
	Timeout             time.Duration
	MaxContextTokens    int
	ReserveTokens       int
	KeepRecentTokens    int
	MaxSummaryTokens    int
	MaxCompletionTokens int
	SummaryTimeout      time.Duration
}

type ProcessClient struct {
	config Config
	env    []string
}

// NewProcessClient validates deployment configuration, without invoking a model.
// No mode is enabled implicitly; demo must be deliberately selected.
func NewProcessClient(config Config) (*ProcessClient, error) {
	if config.Mode != "demo" && config.Mode != "live" {
		return nil, fmt.Errorf("%w: set MAGENT_MODE to demo or live", ErrUnavailable)
	}
	if config.Python == "" || config.Script == "" || config.HarnessPath == "" {
		return nil, fmt.Errorf("%w: Python, Script and HarnessPath are required", ErrUnavailable)
	}
	python, err := exec.LookPath(config.Python)
	if err != nil {
		return nil, fmt.Errorf("%w: Python executable was not found", ErrUnavailable)
	}
	config.Python, err = filepath.Abs(python)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Python path", ErrUnavailable)
	}
	config.Script, err = filepath.Abs(config.Script)
	if err != nil || !regularFile(config.Script) {
		return nil, fmt.Errorf("%w: bridge script was not found", ErrUnavailable)
	}
	config.HarnessPath, err = filepath.Abs(config.HarnessPath)
	if err != nil || !regularFile(filepath.Join(config.HarnessPath, "src", "magent_harness", "engine.py")) {
		return nil, fmt.Errorf("%w: Harness source was not found", ErrUnavailable)
	}
	if strings.ContainsRune(config.HarnessPath, os.PathListSeparator) {
		return nil, fmt.Errorf("%w: Harness path contains a path-list separator", ErrUnavailable)
	}
	if config.Timeout <= 0 {
		config.Timeout = 120 * time.Second
	}
	if config.Mode == "live" {
		if !validBaseURL(config.BaseURL) || strings.TrimSpace(config.APIKey) == "" || strings.TrimSpace(config.Model) == "" || strings.ContainsAny(config.APIKey, "\r\n\x00") {
			return nil, fmt.Errorf("%w: live mode needs a valid HTTPS BaseURL (or loopback HTTP), APIKey and Model", ErrUnavailable)
		}
	}
	config, err = normalizeContextConfig(config)
	if err != nil {
		return nil, err
	}
	// Inherit no unrelated application credentials, PYTHONPATH, startup hooks or
	// proxy configuration. The bridge explicitly loads its pinned dependencies;
	// -S keeps site-packages and executable .pth/startup hooks disabled.
	env := []string{
		"PYTHONPATH=" + filepath.Join(config.HarnessPath, "src"),
		"PYTHONIOENCODING=utf-8",
		"MAGENT_MODE=" + config.Mode,
		"MAGENT_BASE_URL=" + config.BaseURL,
		"MAGENT_API_KEY=" + config.APIKey,
		"MAGENT_MODEL=" + config.Model,
		fmt.Sprintf("MAGENT_HTTP_TIMEOUT=%g", config.Timeout.Seconds()),
		fmt.Sprintf("MAGENT_MAX_CONTEXT_TOKENS=%d", config.MaxContextTokens),
		fmt.Sprintf("MAGENT_RESERVE_TOKENS=%d", config.ReserveTokens),
		fmt.Sprintf("MAGENT_KEEP_RECENT_TOKENS=%d", config.KeepRecentTokens),
		fmt.Sprintf("MAGENT_MAX_SUMMARY_TOKENS=%d", config.MaxSummaryTokens),
		fmt.Sprintf("MAGENT_MAX_COMPLETION_TOKENS=%d", config.MaxCompletionTokens),
		fmt.Sprintf("MAGENT_SUMMARY_TIMEOUT_S=%g", config.SummaryTimeout.Seconds()),
	}
	for _, key := range []string{"PATH", "SYSTEMROOT", "WINDIR", "LANG", "LC_ALL"} {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	return &ProcessClient{config: config, env: env}, nil
}

func regularFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

func validBaseURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	if u.Scheme == "https" {
		return true
	}
	if u.Scheme != "http" {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	return strings.EqualFold(u.Hostname(), "localhost") || (ip != nil && ip.IsLoopback())
}

func (c *ProcessClient) Run(ctx context.Context, request Request) (Result, error) {
	if err := validateRequest(request); err != nil {
		return Result{}, err
	}
	runCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()
	payload, closeCallback, err := bridgePayload(runCtx, request)
	if err != nil {
		return Result{}, err
	}
	defer closeCallback()
	cmd := exec.CommandContext(runCtx, c.config.Python, "-S", "-B", c.config.Script)
	cmd.Env = c.env
	cmd.Stdin = bytes.NewReader(payload)
	stdout := &boundedOutput{limit: maxOutputBytes, cancel: cancel}
	stderr := &boundedOutput{limit: maxStderrBytes, cancel: cancel}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	// Bound waiting for inherited pipe descriptors even if an unexpected bridge
	// implementation spawns a child. The supplied bridge never spawns processes.
	cmd.WaitDelay = time.Second
	err = cmd.Run()
	if stdout.overflow || stderr.overflow {
		return Result{}, fmt.Errorf("%w: bridge output exceeds the limit", ErrFailed)
	}
	if runCtx.Err() != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrFailed, runCtx.Err())
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 78 {
			return Result{}, fmt.Errorf("%w: bridge runtime is not configured", ErrUnavailable)
		}
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			return Result{}, fmt.Errorf("%w: bridge could not start", ErrUnavailable)
		}
		// Never expose stdout/stderr: provider errors can contain credentials,
		// private document content or internal deployment details.
		return Result{}, fmt.Errorf("%w: bridge execution failed", ErrFailed)
	}
	return decodeResult(stdout.Bytes(), request, c.config.Mode)
}

func decodeResult(data []byte, request Request, mode string) (Result, error) {
	if !utf8.Valid(data) || !uniqueJSON(data) {
		return Result{}, fmt.Errorf("%w: invalid bridge response encoding", ErrFailed)
	}
	var wire struct {
		Result
		SessionID string `json:"session_id"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return Result{}, fmt.Errorf("%w: invalid bridge response", ErrFailed)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Result{}, fmt.Errorf("%w: trailing bridge response", ErrFailed)
	}
	if wire.RunID != request.RunID || wire.SessionID != request.SessionID || wire.Mode != mode {
		return Result{}, fmt.Errorf("%w: inconsistent bridge result", ErrFailed)
	}
	if wire.Status == "suspended" {
		if wire.Answer != "" || len(wire.Messages) != 0 || !validUserPrompt(wire.Prompt, request) {
			return Result{}, fmt.Errorf("%w: invalid suspended prompt", ErrFailed)
		}
		return wire.Result, nil
	}
	if wire.Status != "completed" || wire.Prompt != nil || strings.TrimSpace(wire.Answer) == "" || len(wire.Answer) > maxMessageBytes {
		return Result{}, fmt.Errorf("%w: inconsistent completed result", ErrFailed)
	}
	if len(wire.Messages) < 2 || validateMessages(wire.Messages) != nil {
		return Result{}, fmt.Errorf("%w: invalid bridge transcript", ErrFailed)
	}
	last := wire.Messages[len(wire.Messages)-1]
	if last.Role != "assistant" || last.Content != wire.Answer {
		return Result{}, fmt.Errorf("%w: inconsistent final answer", ErrFailed)
	}
	return wire.Result, nil
}

func validateRequest(request Request) error {
	if request.Recover && (request.JournalHandler == nil || request.ToolHandler == nil) {
		return fmt.Errorf("%w: recovery requires authenticated persistence and tools", ErrFailed)
	}
	for _, id := range []string{request.RunID, request.SessionID, request.WorkspaceID, request.UserID} {
		if strings.TrimSpace(id) == "" || len(id) > 256 {
			return fmt.Errorf("%w: run identity is required", ErrFailed)
		}
	}
	switch request.Role {
	case "owner", "admin", "contributor", "viewer":
	default:
		return fmt.Errorf("%w: invalid workspace role", ErrFailed)
	}
	if strings.TrimSpace(request.Query) == "" || len(request.Query) > maxMessageBytes {
		return fmt.Errorf("%w: query is empty or too long", ErrFailed)
	}
	// Reserve room for this round's user message and final assistant answer.
	if len(request.History) > maxHistory-2 {
		return fmt.Errorf("%w: transcript is too long for another turn", ErrFailed)
	}
	if request.DocumentIDs != nil {
		if len(*request.DocumentIDs) > 1000 {
			return fmt.Errorf("%w: resource scope is too large", ErrFailed)
		}
		for _, id := range *request.DocumentIDs {
			if strings.TrimSpace(id) == "" || len(id) > 256 {
				return fmt.Errorf("%w: invalid resource scope", ErrFailed)
			}
		}
	}
	if request.Resume != nil && (len(request.Resume.Prompt) == 0 || len(request.Resume.Prompt) > maxOutputBytes || !json.Valid(request.Resume.Prompt) || !uniqueJSON(request.Resume.Prompt)) {
		return fmt.Errorf("%w: invalid resume prompt", ErrFailed)
	}
	return validateMessages(request.History)
}

func validateMessages(messages []Message) error {
	if len(messages) > maxHistory {
		return fmt.Errorf("%w: transcript is too long", ErrFailed)
	}
	for _, message := range messages {
		if (message.Role != "user" && message.Role != "assistant") || len(message.Content) > maxMessageBytes {
			return fmt.Errorf("%w: invalid transcript message", ErrFailed)
		}
	}
	return nil
}

type boundedOutput struct {
	bytes.Buffer
	limit    int
	overflow bool
	cancel   context.CancelFunc
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		b.overflow = true
		b.cancel()
		return 0, errors.New("bridge output limit exceeded")
	}
	return b.Buffer.Write(p)
}
