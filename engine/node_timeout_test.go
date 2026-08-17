package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestResolveNodeTimeout(t *testing.T) {
	e := &Engine{defaultNodeTimeout: 30 * time.Minute}
	cases := []struct {
		name string
		raw  time.Duration
		want time.Duration
	}{
		{"unset inherits the default", 0, 30 * time.Minute},
		{"negative means no limit", -1, 0},
		{"positive passes through", 45 * time.Second, 45 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := e.resolveNodeTimeout(tc.raw); got != tc.want {
				t.Fatalf("resolveNodeTimeout(%v) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

// TestWithDefaultNodeTimeoutZeroDisables pins the option's inverted zero: the
// FIELD's zero means "not configured", but the OPTION's zero means "turn the
// default off". Without this an unconfigured node would silently stay bounded
// and the documented escape hatch would be a no-op.
func TestWithDefaultNodeTimeoutZeroDisables(t *testing.T) {
	e := &Engine{defaultNodeTimeout: DefaultNodeTimeout}
	WithDefaultNodeTimeout(0)(e)
	if got := e.resolveNodeTimeout(0); got != 0 {
		t.Fatalf("with the default disabled, an unset node resolved to %v, want 0 (unbounded)", got)
	}
}

func TestWithDefaultNodeTimeoutRaises(t *testing.T) {
	e := &Engine{defaultNodeTimeout: DefaultNodeTimeout}
	WithDefaultNodeTimeout(2 * time.Hour)(e)
	if got := e.resolveNodeTimeout(0); got != 2*time.Hour {
		t.Fatalf("resolveNodeTimeout(0) = %v, want 2h", got)
	}
}

// leaseTimeoutDef builds a single-node workflow whose node carries the given raw
// Timeout. The helper mirrors the shape of the existing BuildTaskLease tests in
// this package (see runner_commit_test.go): Submit, drain the root task, then
// BuildTaskLease against it.
func leaseTimeoutDef(t *testing.T, raw time.Duration) (*Engine, *fakeState, *fakeQueue, *Task) {
	t.Helper()
	def := &types.WorkflowDef{
		Name: "node-timeout",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo", Timeout: raw},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()
	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("root tasks = %d, want 1", len(tasks))
	}
	return eng, state, queue, tasks[0]
}

// TestBuildTaskLeaseStampsTimeoutFromConfiguredValue covers the positive case:
// a node that declares its own Timeout drives both Input.Timeout (the resolved
// budget the runner prints in the timeout error) and ExecutionDeadline (the
// absolute instant the server backstop reads).
func TestBuildTaskLeaseStampsTimeoutFromConfiguredValue(t *testing.T) {
	eng, _, _, task := leaseTimeoutDef(t, 45*time.Second)
	ctx := context.Background()
	before := time.Now().UTC()
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	if got := lease.Input.Timeout; got != 45*time.Second {
		t.Fatalf("Input.Timeout = %v, want 45s", got)
	}
	if lease.ExecutionDeadline.IsZero() {
		t.Fatalf("ExecutionDeadline is zero, want ~now+45s")
	}
	lo, hi := before.Add(44*time.Second), before.Add(46*time.Second)
	if lease.ExecutionDeadline.Before(lo) || lease.ExecutionDeadline.After(hi) {
		t.Fatalf("ExecutionDeadline = %v, want within [%v, %v] (now+45s, dispatch slack tolerant)",
			lease.ExecutionDeadline, lo, hi)
	}
}

// TestBuildTaskLeaseNegativeTimeoutIsUnbounded covers the opt-out: a node that
// declares Timeout<0 resolves to Input.Timeout==0 and leaves ExecutionDeadline
// zero so neither runner nor server can time it out.
func TestBuildTaskLeaseNegativeTimeoutIsUnbounded(t *testing.T) {
	eng, _, _, task := leaseTimeoutDef(t, -1)
	ctx := context.Background()
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	if got := lease.Input.Timeout; got != 0 {
		t.Fatalf("Input.Timeout = %v, want 0 (negative opt-out must resolve to unbounded)", got)
	}
	if !lease.ExecutionDeadline.IsZero() {
		t.Fatalf("ExecutionDeadline = %v, want zero (unbounded nodes carry no deadline)", lease.ExecutionDeadline)
	}
}

// TestBuildTaskLeaseUnsetTimeoutInheritsDefault covers the unset case: a node
// that leaves Timeout blank resolves to the engine default (DefaultNodeTimeout
// = 30m). ExecutionDeadline is therefore now+30m.
func TestBuildTaskLeaseUnsetTimeoutInheritsDefault(t *testing.T) {
	eng, _, _, task := leaseTimeoutDef(t, 0)
	ctx := context.Background()
	before := time.Now().UTC()
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	if got := lease.Input.Timeout; got != DefaultNodeTimeout {
		t.Fatalf("Input.Timeout = %v, want %v (engine default)", got, DefaultNodeTimeout)
	}
	if lease.ExecutionDeadline.IsZero() {
		t.Fatalf("ExecutionDeadline is zero, want ~now+%v", DefaultNodeTimeout)
	}
	lo, hi := before.Add(DefaultNodeTimeout-1*time.Second), before.Add(DefaultNodeTimeout+1*time.Second)
	if lease.ExecutionDeadline.Before(lo) || lease.ExecutionDeadline.After(hi) {
		t.Fatalf("ExecutionDeadline = %v, want within [%v, %v]",
			lease.ExecutionDeadline, lo, hi)
	}
}
