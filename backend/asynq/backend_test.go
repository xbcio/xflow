package asynq

import (
	"errors"
	"testing"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/types"
)

func TestAsynqHandlerErrorMarksPermanentErrorsSkipRetry(t *testing.T) {
	cause := errors.New("bad config")
	err := asynqHandlerError(errors.Join(types.ErrPermanent, cause))

	if !errors.Is(err, asynqlib.SkipRetry) {
		t.Fatalf("err = %v, want asynq SkipRetry", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("err = %v, want original cause", err)
	}
}

func TestAsynqHandlerErrorLeavesTransientErrorsRetryable(t *testing.T) {
	cause := errors.New("runner unavailable")
	err := asynqHandlerError(cause)

	if !errors.Is(err, cause) {
		t.Fatalf("err = %v, want original cause", err)
	}
	if errors.Is(err, asynqlib.SkipRetry) {
		t.Fatalf("err = %v, should remain retryable", err)
	}
}
