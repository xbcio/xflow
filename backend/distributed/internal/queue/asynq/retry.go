package asynq

import (
	"errors"
	"fmt"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/types"
)

// handlerError translates a handler error into the Asynq retry policy.
//
// In transient (fire-and-forget) mode every non-nil error is wrapped with
// asynq.SkipRetry so failed tasks are dropped immediately instead of being
// re-enqueued. In default/durable mode only errors wrapping types.ErrPermanent
// (explicitly marked non-retryable) skip retry; other errors remain retryable.
func handlerError(err error, transient bool) error {
	if err == nil {
		return nil
	}
	if transient || errors.Is(err, types.ErrPermanent) {
		return fmt.Errorf("%w: %w", asynqlib.SkipRetry, err)
	}
	return err
}
