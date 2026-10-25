package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestCheckCompletionTerminalFence pins the early return checkCompletionLua takes
// for an execution that is already terminal or canceling. Every node is done, so
// the only thing standing between the script and a SET of success over the
// status is that guard. Without it a completion racing a Cancel or the deadline
// sweep overwrites the cancellation (or the timeout) in progress and reports the
// execution finished.
//
// The answer is (false, false, nil) in every case: a true from this backend means
// the script wrote the terminal status itself, and here it must write nothing.
// success and failed are included although they are terminal for the same
// reason: for success the overwrite would be invisible in the status alone, so
// the done flag is what tells the guard is there.
//
// The control, an existing running execution whose nodes are all done being
// finalized, is TestCheckCompletionStillFinalizesAnExistingExecution; the absent
// execution guard above it in the script is TestCheckCompletionRefusesAnAbsentExecution.
func TestCheckCompletionTerminalFence(t *testing.T) {
	ctx := context.Background()
	srv, _, s := newMiniredisStore(t)
	g := twoNodeGraph(t)

	for _, status := range []types.ExecutionStatus{
		types.ExecutionStatusCanceling, types.ExecutionStatusCanceled, types.ExecutionStatusTimeout,
		types.ExecutionStatusSuccess, types.ExecutionStatusFailed,
	} {
		t.Run(string(status), func(t *testing.T) {
			id := types.ExecutionID("check-completion-fence-" + string(status))
			statusKey := execKey(namespace.Default, id, "status")
			createWithDeadline(t, s, ctx, id, time.Now().Add(replicaBudget))
			if err := s.UpdateExecutionStatus(ctx, id, status, ""); err != nil {
				t.Fatalf("UpdateExecutionStatus(%s): %v", status, err)
			}
			if got, _ := srv.Get(statusKey); got != string(status) {
				t.Fatalf("status before CheckCompletion = %q, want %q", got, status)
			}
			// After the status write: a terminal write drops the in-process graph.
			s.graphs.Put(id, g, 0)
			plantSuccessfulNodes(t, srv, id, "a", "b")

			done, failed, err := s.CheckCompletion(ctx, id, 2)
			if err != nil || done || failed {
				t.Fatalf("CheckCompletion on a %s execution = (%v, %v, %v), want (false, false, nil)", status, done, failed, err)
			}
			if got, _ := srv.Get(statusKey); got != string(status) {
				t.Fatalf("status after CheckCompletion = %q, want %q (not rewritten)", got, status)
			}
		})
	}
}
