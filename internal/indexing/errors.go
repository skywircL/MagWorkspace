package indexing

import (
	"context"
	"errors"
)

// Failure exposes a bounded diagnostic code, never parser/provider response text.
type Failure struct{ Code string }

func (e *Failure) Error() string { return e.Code }

func ErrorCode(err error) string {
	if errors.Is(err, context.Canceled) {
		return "processing_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "processing_timeout"
	}
	var failure *Failure
	if errors.As(err, &failure) {
		return failure.Code
	}
	return "processing_failed"
}
