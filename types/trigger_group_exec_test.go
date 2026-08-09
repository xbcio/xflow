package types

import (
	"context"
	"testing"
)

// fakeGroupExecRuntime is a minimal types.GroupExecRuntime implementation used
// only to prove the interface is satisfiable with the intended method shape.
type fakeGroupExecRuntime struct {
	result GroupExecResult
	err    error
}

func (f *fakeGroupExecRuntime) ExecuteGroup(_ context.Context, input map[string]any) (GroupExecResult, error) {
	return f.result, f.err
}

func TestGroupExecRuntime_Satisfiable(t *testing.T) {
	var rt GroupExecRuntime = &fakeGroupExecRuntime{
		result: GroupExecResult{
			Outcome: "success",
			Exits:   []BoundaryExit{{NodeName: "member", Port: "main", Data: map[string]any{"x": 1}}},
		},
	}
	res, err := rt.ExecuteGroup(context.Background(), map[string]any{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != "success" || len(res.Exits) != 1 || res.Exits[0].NodeName != "member" {
		t.Fatalf("result = %+v", res)
	}
}
