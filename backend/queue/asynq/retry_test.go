package asynq

import (
	"errors"
	"testing"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/types"
)

func TestHandlerErrorMarksPermanentErrorsSkipRetry(t *testing.T) {
	cause := errors.New("bad config")
	err := handlerError(errors.Join(types.ErrPermanent, cause), false)

	if !errors.Is(err, asynqlib.SkipRetry) {
		t.Fatalf("err = %v, want asynq SkipRetry", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("err = %v, want original cause", err)
	}
}

func TestHandlerErrorLeavesTransientErrorsRetryable(t *testing.T) {
	cause := errors.New("runner unavailable")
	err := handlerError(cause, false)

	if !errors.Is(err, cause) {
		t.Fatalf("err = %v, want original cause", err)
	}
	if errors.Is(err, asynqlib.SkipRetry) {
		t.Fatalf("err = %v, should remain retryable", err)
	}
}

// In transient mode every non-nil handler error must skip retry —
// fire-and-forget tasks are dropped on failure rather than re-enqueued.
func TestHandlerErrorTransientModeAlwaysSkipsRetry(t *testing.T) {
	cause := errors.New("runner unavailable")
	err := handlerError(cause, true)

	if !errors.Is(err, asynqlib.SkipRetry) {
		t.Fatalf("err = %v, want asynq SkipRetry in transient mode", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("err = %v, want original cause preserved", err)
	}

	if err := handlerError(nil, true); err != nil {
		t.Fatalf("nil error in transient mode = %v, want nil", err)
	}
}
