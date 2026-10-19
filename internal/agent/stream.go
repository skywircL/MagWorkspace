package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

const maxStreamBytes = 8 << 20

// Event describes provisional answer text. A new start replaces the previous
// attempt's display; only the final Result is authoritative and persistable.
type Event struct {
	Type      string `json:"type"`
	RunID     string `json:"run_id"`
	SessionID string `json:"session_id"`
	Round     int    `json:"round"`
	Delta     string `json:"delta,omitempty"`
}

type StreamingClient interface {
	Stream(context.Context, Request, func(Event) error) (Result, error)
}

// Stream invokes onEvent serially while Python receives upstream text. The
// callback must return promptly (HTTP writers should have a write deadline).
// A callback error cancels and reaps Python before Stream returns.
func (c *ProcessClient) Stream(ctx context.Context, request Request, onEvent func(Event) error) (Result, error) {
	if onEvent == nil {
		return Result{}, fmt.Errorf("%w: stream callback is required", ErrFailed)
	}
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
	cmd := exec.CommandContext(runCtx, c.config.Python, "-S", "-B", c.config.Script, "--stream")
	cmd.Env, cmd.Stdin = c.env, bytes.NewReader(payload)
	output := &streamOutput{request: request, mode: c.config.Mode, callback: onEvent, cancel: cancel}
	stderr := &boundedOutput{limit: maxStderrBytes, cancel: cancel}
	cmd.Stdout, cmd.Stderr = output, stderr
	cmd.WaitDelay = time.Second
	err = cmd.Run()
	if output.err != nil {
		return Result{}, output.err
	}
	if stderr.overflow {
		return Result{}, fmt.Errorf("%w: bridge output exceeds the limit", ErrFailed)
	}
	if runCtx.Err() != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrFailed, runCtx.Err())
	}
	if err != nil {
		var exitErr *exec.ExitError
		var pathErr *os.PathError
		if (errors.As(err, &exitErr) && exitErr.ExitCode() == 78) || errors.As(err, &pathErr) {
			return Result{}, fmt.Errorf("%w: bridge runtime is not configured", ErrUnavailable)
		}
		return Result{}, fmt.Errorf("%w: bridge execution failed", ErrFailed)
	}
	if output.result == nil || len(output.pending) != 0 {
		return Result{}, fmt.Errorf("%w: incomplete bridge stream", ErrFailed)
	}
	return *output.result, nil
}

// streamOutput is written by one exec copy goroutine, then inspected after Wait.
// Keeping parsing in the writer gives cancellation the same pipe lifecycle as Run.
type streamOutput struct {
	request    Request
	mode       string
	callback   func(Event) error
	cancel     context.CancelFunc
	pending    []byte
	total      int
	round      int
	roundBytes int
	discarded  bool
	result     *Result
	err        error
}

func (s *streamOutput) Write(data []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	n := len(data)
	s.total += n
	if s.total > maxStreamBytes {
		return s.fail(fmt.Errorf("%w: bridge stream exceeds the limit", ErrFailed))
	}
	for len(data) > 0 {
		if s.result != nil {
			return s.fail(fmt.Errorf("%w: trailing bridge stream record", ErrFailed))
		}
		end := bytes.IndexByte(data, '\n')
		length := len(data)
		if end >= 0 {
			length = end
		}
		if len(s.pending)+length > maxOutputBytes {
			return s.fail(fmt.Errorf("%w: bridge stream record exceeds the limit", ErrFailed))
		}
		s.pending = append(s.pending, data[:length]...)
		if end < 0 {
			break
		}
		if err := s.record(s.pending); err != nil {
			return s.fail(err)
		}
		s.pending = s.pending[:0]
		data = data[end+1:]
	}
	return n, nil
}

func (s *streamOutput) fail(err error) (int, error) {
	s.err = err
	s.cancel()
	return 0, err
}

func (s *streamOutput) record(raw []byte) error {
	var record struct {
		Event  *Event          `json:"event"`
		Result json.RawMessage `json:"result"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return fmt.Errorf("%w: invalid bridge stream record", ErrFailed)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("%w: multiple values in bridge stream record", ErrFailed)
	}
	if (record.Event == nil) == (record.Result == nil) {
		return fmt.Errorf("%w: invalid bridge stream record kind", ErrFailed)
	}
	if record.Result != nil {
		result, err := decodeResult(record.Result, s.request, s.mode)
		if err != nil {
			return err
		}
		if s.round == 0 && result.Status != "suspended" && !s.request.Recover {
			return fmt.Errorf("%w: bridge result preceded all message attempts", ErrFailed)
		}
		s.result = &result
		return nil
	}
	event := *record.Event
	if event.RunID != s.request.RunID || event.SessionID != s.request.SessionID {
		return fmt.Errorf("%w: bridge event identity mismatch", ErrFailed)
	}
	switch event.Type {
	case "message.start":
		if event.Round != s.round+1 || event.Round > 32 || event.Delta != "" {
			return fmt.Errorf("%w: invalid message attempt", ErrFailed)
		}
		s.round, s.roundBytes, s.discarded = event.Round, 0, false
	case "message.delta":
		if s.round == 0 || s.discarded || event.Round != s.round || event.Delta == "" || len(event.Delta) > maxMessageBytes-s.roundBytes {
			return fmt.Errorf("%w: invalid message delta", ErrFailed)
		}
		s.roundBytes += len(event.Delta)
	case "message.discard":
		if s.round == 0 || s.discarded || event.Round != s.round || event.Delta != "" {
			return fmt.Errorf("%w: invalid message discard", ErrFailed)
		}
		s.discarded = true
	default:
		return fmt.Errorf("%w: unsupported bridge event", ErrFailed)
	}
	if err := callStreamCallback(s.callback, event); err != nil {
		return &callbackError{cause: err}
	}
	return nil
}

func callStreamCallback(callback func(Event) error, event Event) (err error) {
	// exec copies stdout on its own goroutine, outside an HTTP worker's recover.
	// Convert a callback panic into cancellation instead of crashing the server.
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("%w: stream callback panicked", ErrFailed)
		}
	}()
	return callback(event)
}

// Preserve errors.Is for cancellation callers without exposing callback details.
type callbackError struct{ cause error }

func (e *callbackError) Error() string   { return "agent run failed: stream callback failed" }
func (e *callbackError) Unwrap() []error { return []error{ErrFailed, e.cause} }
