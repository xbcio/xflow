package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestStatusWriteCancelingFence pins the canceling fence of updateExecutionStatusLua:
// once an execution is canceling, the only status that may replace it is
// canceled. Without the fence a completion racing a Cancel (success or failed
// from completeExecution, running or paused from a resume, timeout from the
// deadline sweep) overwrites the in-flight cancellation and the execution ends
// up in a status Cancel never asked for.
//
// The terminal fence is covered by TestUpdateExecutionStatusCancelingRejectedByTerminal
// and the absent-execution guard by TestStatusWriteOnAnAbsentExecutionWritesNothing;
// this is the third branch of the script, and neither of those reaches it: they
// never hold an execution in canceling while a different status is written.
//
// UpdateExecutionStatus reports no applied flag, so a refused write is told
// from an applied one by what is read back: the status key still says canceling
// and the error message the refused write carried was not stored beside it.
func TestStatusWriteCancelingFence(t *testing.T) {
	ctx := context.Background()
	srv, _, s := newMiniredisStore(t)
	statusKey := func(id types.ExecutionID) string { return execKey(namespace.Default, id, "status") }
	errorKey := func(id types.ExecutionID) string { return execKey(namespace.Default, id, "error") }

	canceling := func(t *testing.T, id types.ExecutionID) {
		t.Helper()
		createWithDeadline(t, s, ctx, id, time.Now().Add(replicaBudget))
		if err := s.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(canceling): %v", err)
		}
		if got, _ := srv.Get(statusKey(id)); got != string(types.ExecutionStatusCanceling) {
			t.Fatalf("status after the canceling write = %q, want %q", got, types.ExecutionStatusCanceling)
		}
	}

	// Every status other than canceled is tried, not only the terminal ones: a
	// fence that names one status leaves the others writing.
	for _, status := range []types.ExecutionStatus{
		types.ExecutionStatusPending, types.ExecutionStatusQueued, types.ExecutionStatusRunning,
		types.ExecutionStatusPaused, types.ExecutionStatusSuccess, types.ExecutionStatusFailed,
		types.ExecutionStatusTimeout,
	} {
		t.Run("rejects "+string(status), func(t *testing.T) {
			id := types.ExecutionID("canceling-fence-" + string(status))
			canceling(t, id)

			if err := s.UpdateExecutionStatus(ctx, id, status, "late completion"); err != nil {
				t.Fatalf("UpdateExecutionStatus(%s) on a canceling execution: %v", status, err)
			}
			if got, _ := srv.Get(statusKey(id)); got != string(types.ExecutionStatusCanceling) {
				t.Fatalf("status after the %s write = %q, want %q (fenced)", status, got, types.ExecutionStatusCanceling)
			}
			if srv.Exists(errorKey(id)) {
				got, _ := srv.Get(errorKey(id))
				t.Fatalf("the refused %s write stored an error message (%q) beside the canceling status", status, got)
			}
		})
	}

	// The control: the fence lets the one write it exists to let through.
	t.Run("lets canceled through", func(t *testing.T) {
		id := types.ExecutionID("canceling-fence-canceled")
		canceling(t, id)

		if err := s.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, "canceled by request"); err != nil {
			t.Fatalf("UpdateExecutionStatus(canceled) on a canceling execution: %v", err)
		}
		if got, _ := srv.Get(statusKey(id)); got != string(types.ExecutionStatusCanceled) {
			t.Fatalf("status after the canceled write = %q, want %q", got, types.ExecutionStatusCanceled)
		}
		if got, _ := srv.Get(errorKey(id)); got != "canceled by request" {
			t.Fatalf("error message after the canceled write = %q, want it stored", got)
		}
	})
}
