package local

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestSubExecutionCompletionWaitsForANonTerminalChild pins what "all done"
// means for a parent waiting on its children.
//
// The parent's answer is what decides whether it consumes the batch or keeps
// waiting, so a child that is counted as done but has no result puts a hole in
// that batch — silently, because an empty result is a legitimate result. Only a
// terminal child is finished; "not running" is a wider set that also contains
// every status a child could be parked in.
func TestSubExecutionCompletionWaitsForANonTerminalChild(t *testing.T) {
	state := newMemoryState()
	ctx := context.Background()
	parent := types.ExecutionID("parent-expansion")

	// child-1 is the one being reported: it lands terminal in this call. child-0
	// is parked in a status that neither runs nor finished, which is the shape
	// the completion check has to keep waiting on.
	state.subExecs[string(parent)+"/foreach"] = []*engine.SubExecution{
		{ParentExecID: parent, ParentNode: "foreach", ChildExecID: "child-0", Status: types.ExecutionStatusQueued},
		{ParentExecID: parent, ParentNode: "foreach", ChildExecID: "child-1", Status: types.ExecutionStatusRunning},
	}

	allDone, err := state.CompleteSubExecution(ctx, parent, "foreach", "child-1", types.ExecutionStatusSuccess, map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("CompleteSubExecution() error = %v", err)
	}
	if allDone {
		t.Fatal("allDone = true with a child still in queue: the parent would consume a batch that is missing that child's result")
	}

	// The same question answered by the other completion path, which is the one
	// the engine's expansion barrier calls.
	state.subExecs[string(parent)+"/foreach"][0].Status = types.ExecutionStatusSuccess
	state.subExecs[string(parent)+"/foreach"][0].Result = map[string]any{"ok": true}
	allDone, err = state.CompleteSubExecution(ctx, parent, "foreach", "child-1", types.ExecutionStatusSuccess, map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("CompleteSubExecution() error = %v", err)
	}
	if !allDone {
		t.Fatal("allDone = false with every child terminal, want true")
	}
}
