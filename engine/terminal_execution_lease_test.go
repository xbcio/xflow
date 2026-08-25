package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// statusReaderState is fakeState plus ExecutionStatusReader, so the suite can
// drive both halves of executionActive against the same behaviour.
//
// It counts calls because an arm that intended to exercise the narrow read but
// silently fell back to GetExecution would still satisfy every assertion below
// — the preference is a type assertion, so nothing at the call site announces
// which path ran.
type statusReaderState struct {
	*fakeState
	statusReads int
}

func (s *statusReaderState) GetExecutionStatus(ctx context.Context, id types.ExecutionID) (types.ExecutionStatus, bool, error) {
	s.statusReads++
	snap, err := s.fakeState.GetExecution(ctx, id)
	if err != nil || snap == nil {
		return "", false, err
	}
	return snap.Status, true, nil
}

// TestBuildTaskLeaseRefusesTerminalExecution pins that loadActiveGraph will not
// hand a graph to the lease path once the execution has reached a terminal
// status, on both the cache-hit and the cache-miss branch and through both
// implementations of the activeness check.
//
// This is the arm the suite was missing. Replacing executionActive's body with
// `return true, nil` used to leave every engine test green: the running case is
// covered many times over by every other BuildTaskLease test, so a check that
// always says "active" costs nothing there. Only a terminal execution can tell
// the guard from its absence, and nothing drove one into BuildTaskLease.
//
// The failure it guards against is a lease issued against a canceled or
// finished execution — the runner would then execute a node and commit a result
// for work the control plane already accounted for as over.
func TestBuildTaskLeaseRefusesTerminalExecution(t *testing.T) {
	if _, ok := any(newFakeState()).(ExecutionStatusReader); ok {
		t.Fatal("fakeState now implements ExecutionStatusReader: the fallback arm " +
			"below would stop covering the GetExecution path and silently duplicate " +
			"the reader arm. Give it a state that does not implement it.")
	}

	states := []struct {
		name       string
		newState   func() StateStore
		narrowRead bool
	}{
		{
			name:     "GetExecution fallback",
			newState: func() StateStore { return newFakeState() },
		},
		{
			name:       "ExecutionStatusReader",
			newState:   func() StateStore { return &statusReaderState{fakeState: newFakeState()} },
			narrowRead: true,
		},
	}
	branches := []struct {
		name   string
		cached bool
	}{
		{name: "graph cached", cached: true},
		{name: "graph evicted", cached: false},
	}
	// Cancel and completion take different write paths in the real backends, so
	// agreement under one does not imply agreement under the other.
	terminals := []types.ExecutionStatus{
		types.ExecutionStatusCanceled,
		types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusTimeout,
	}

	for _, st := range states {
		for _, br := range branches {
			for _, terminal := range terminals {
				t.Run(st.name+"/"+br.name+"/"+string(terminal), func(t *testing.T) {
					g, err := graph.Compile(&types.WorkflowDef{
						Name:  "terminal-lease",
						Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
					})
					if err != nil {
						t.Fatalf("Compile() error = %v", err)
					}
					state := st.newState()
					queue := &fakeQueue{}
					reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.echo": &echoHandler{}}}
					eng := newTestEngine(t, state, queue, reg)
					ctx := context.Background()

					id, err := eng.Submit(ctx, g, nil)
					if err != nil {
						t.Fatalf("Submit() error = %v", err)
					}
					tasks := queue.Drain()
					if len(tasks) != 1 {
						t.Fatalf("task count = %d, want 1", len(tasks))
					}

					// Driven through the state, not eng.Cancel: Cancel evicts the
					// graph cache, which would collapse the cached branch into the
					// evicted one and leave the cache-hit guard untested.
					if err := state.UpdateExecutionStatus(ctx, id, terminal, "terminated by test"); err != nil {
						t.Fatalf("UpdateExecutionStatus(%q) error = %v", terminal, err)
					}
					if !br.cached {
						eng.EvictExecution(id)
					}

					if reader, ok := state.(*statusReaderState); ok {
						reader.statusReads = 0
					}

					lease, err := eng.BuildTaskLease(ctx, tasks[0])
					if !errors.Is(err, ErrExecutionInactive) {
						t.Fatalf("BuildTaskLease() on a %q execution = (%v, %v), want ErrExecutionInactive. "+
							"A lease here lets a runner execute and commit a node for an execution "+
							"the control plane already finished.", terminal, lease, err)
					}

					if st.narrowRead {
						reader := state.(*statusReaderState)
						if reader.statusReads == 0 {
							t.Errorf("GetExecutionStatus was never called: this arm fell back to " +
								"GetExecution and therefore tests the same path as the arm above")
						}
					}

					// This, not the error above, is what discriminates. Input
					// assembly re-reads the execution and returns a wrapped
					// ErrExecutionInactive of its own, so the errors.Is check is
					// satisfied even with loadActiveGraph's guard deleted — it
					// just arrives several round trips later. Measured: with the
					// guard removed on either branch, the error assertion still
					// passes and only this one fails.
					//
					// The cache is where the difference shows. Refusing means
					// the terminal execution's graph is not in e.graphs when we
					// return: evicted on the cache-hit branch, never inserted on
					// the cache-miss branch. Leaving it there is not just waste —
					// it is the state loadActiveGraph's cache-hit branch exists
					// to distrust.
					eng.mu.RLock()
					_, stillCached := eng.graphs[id]
					eng.mu.RUnlock()
					if stillCached {
						t.Errorf("graph for %q is in e.graphs after a refused lease on a %q execution "+
							"(entered this call %s)", id, terminal,
							map[bool]string{true: "cached", false: "uncached"}[br.cached])
					}
				})
			}
		}
	}
}
