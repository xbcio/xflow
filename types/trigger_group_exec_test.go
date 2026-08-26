package types

import (
	"context"
	"testing"
)

// fakeGroupExecRuntime exists only for a compile-time check that
// GroupExecRuntime's method set is satisfiable with the intended shape.
//
// It deliberately has no result fields and the test below makes no runtime
// assertion. It used to carry a GroupExecResult that ExecuteGroup returned
// verbatim, and the test asserted Outcome/Exits against the same literal it
// had written two lines earlier — a comparison of a value with itself, which
// no change to any production file could make fail. That is worse than an
// empty test: it read like coverage of group execution while covering nothing.
//
// This package has no code that derives a GroupExecResult from real inputs.
// The real adapter is service/runner.groupExecTriggerRuntime, covered by
// TestGroupExecTriggerRuntime_ExecuteGroupRunsRealMembers and
// TestGroupExecTriggerRuntime_DeterministicFromMemberClassification in
// service/runner/group_exec_trigger_runtime_test.go. types cannot borrow those
// — service/runner imports types, so referencing it here is an import cycle.
type fakeGroupExecRuntime struct{}

func (f *fakeGroupExecRuntime) ExecuteGroup(_ context.Context, _ map[string]any) (GroupExecResult, error) {
	return GroupExecResult{}, nil
}

func TestGroupExecRuntime_Satisfiable(t *testing.T) {
	var _ GroupExecRuntime = (*fakeGroupExecRuntime)(nil)
}
