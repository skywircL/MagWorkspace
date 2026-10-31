// Package stream manages transient output, stop signals and execution markers.
// Callers must authorize against the database; cached state never grants access.
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var (
	ErrHistory     = errors.New("stream history unavailable")
	ErrUnavailable = errors.New("stream storage unavailable")
	ErrConflict    = errors.New("stream execution conflict")
	ErrInvalid     = errors.New("invalid stream operation")
)

const (
	DefaultTTL     = time.Hour
	EventLimit     = 4096
	BatchSize      = 128
	maxEventBytes  = 256 << 10
	maxStreamBytes = 8 << 20
)

// Key uses the durable Run rather than an assistant message that is not yet saved.
type Key struct {
	SessionID, RunID string
	Generation       uint64
}
type Event struct {
	Sequence  uint64          `json:"sequence"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}
type LiveRun struct {
	RunID      string `json:"run_id"`
	Generation uint64 `json:"generation"`
	RequestID  string `json:"request_id"`
}

func (k Key) valid() bool     { return validID(k.SessionID) && validID(k.RunID) && k.Generation > 0 }
func validID(s string) bool   { return s != "" && len(s) <= 128 && !strings.ContainsAny(s, ":{}\r\n") }
func (l LiveRun) valid() bool { return validID(l.RunID) && l.Generation > 0 && validID(l.RequestID) }
func encodeEvent(e Event) ([]byte, error) {
	if e.Sequence == 0 || e.Type == "" || len(e.Type) > 80 || strings.ContainsAny(e.Type, "\r\n") || !json.Valid(e.Data) {
		return nil, ErrInvalid
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	raw, err := json.Marshal(e)
	if err != nil || len(raw) > maxEventBytes {
		return nil, ErrInvalid
	}
	return raw, nil
}

// Event offsets are absolute counts, never destructive reads.
type Manager interface {
	AppendEvent(context.Context, Key, Event) error
	GetEvents(context.Context, Key, uint64) ([]Event, uint64, error)
	ResetEvents(context.Context, Key) error
	// RequestStop is idempotent and applies only to this execution generation.
	RequestStop(context.Context, Key) error
	// StopRequested also renews the matching live marker while the producer polls.
	StopRequested(context.Context, Key) (bool, error)
	SetLiveRun(context.Context, string, LiveRun) error
	// Claim is a compare-and-swap handoff; stale workers cannot replace a newer run.
	ClaimLiveRun(context.Context, string, LiveRun, LiveRun) error
	GetLiveRun(context.Context, string) (LiveRun, error)
	ClearLiveRun(context.Context, string, LiveRun) error
}
