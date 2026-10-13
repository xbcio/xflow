package trigger

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// The capability below is discovered by type assertion on the runtime a trigger
// was activated with (types.ExecutionStatusReader). A rename or a changed
// signature here would not fail to compile anywhere — the trigger would simply
// stop finding the capability and degrade every cluster deployment to the
// behaviour that needs no status. This assertion is what makes that a build
// failure instead.
var _ types.ExecutionStatusReader = (*Primitives)(nil)

// TestPrimitivesForwardTheExecutionStatusQuestion: the primitives answer from
// the execution store they were built with, not from Redis state of their own.
// The question travels with the caller's context, because the status key the
// store reads is namespaced by it.
func TestPrimitivesForwardTheExecutionStatusQuestion(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("ns-a"))
	const id = types.ExecutionID("exec-status-1")
	store := &stubExecutionStatus{status: types.ExecutionStatusRunning, found: true}
	primitives := New(nil, store)

	status, found, err := primitives.GetExecutionStatus(ctx, id)
	if err != nil || !found || status != types.ExecutionStatusRunning {
		t.Fatalf("GetExecutionStatus() = %q, %v, %v; want the store's answer %q",
			status, found, err, types.ExecutionStatusRunning)
	}
	if store.asked != id {
		t.Fatalf("the store was asked about %q, want %q", store.asked, id)
	}
	if got := namespace.FromContext(store.ctx); got != "ns-a" {
		t.Fatalf("the store saw namespace %q, want the caller's ns-a: it reads the status key "+
			"under this namespace, so dropping it would answer from another tenant's record", got)
	}
}

// TestPrimitivesWithoutAStoreSaySo: a primitives value built without an
// execution store — only a test arrangement, since every provider has one — must
// report that it cannot answer rather than let the caller read a zero status as
// a finished run.
func TestPrimitivesWithoutAStoreSaySo(t *testing.T) {
	status, found, err := New(nil, nil).GetExecutionStatus(context.Background(), "exec-status-1")
	if !errors.Is(err, types.ErrExecutionStatusUnavailable) {
		t.Fatalf("GetExecutionStatus() = %q, %v, %v; want types.ErrExecutionStatusUnavailable", status, found, err)
	}
}

type stubExecutionStatus struct {
	status types.ExecutionStatus
	found  bool
	asked  types.ExecutionID
	ctx    context.Context
}

func (s *stubExecutionStatus) GetExecutionStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	s.asked = id
	s.ctx = ctx
	return s.status, s.found, nil
}
