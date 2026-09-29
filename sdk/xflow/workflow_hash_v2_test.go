package xflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// v1RuntimeHash is the pre-v2 algorithm: the v2 hash of def with every node
// Timeout and Output cleared, which is exactly what v1 hashed. It stands in
// for a hash stored before Timeout/Output joined the runtime identity.
func v1RuntimeHash(t *testing.T, def *types.WorkflowDef) string {
	t.Helper()
	stripped := *def
	stripped.Nodes = append([]types.NodeDef(nil), def.Nodes...)
	for i := range stripped.Nodes {
		stripped.Nodes[i].Timeout = 0
		stripped.Nodes[i].Output = nil
	}
	h := mustRuntimeHash(t, &stripped)
	if !strings.HasPrefix(h, runtimeHashPrefix) {
		t.Fatalf("stripped hash %q lacks the v1 prefix", h)
	}
	return h
}

func TestRuntimeHashV2CoversNodeTimeoutAndOutput(t *testing.T) {
	base := mustRuntimeHash(t, baseRuntimeDef())
	if !strings.HasPrefix(base, runtimeHashPrefix) {
		t.Fatalf("definition without Timeout/Output = %q, want the v1 prefix", base)
	}

	withTimeout := func(d time.Duration) string {
		def := baseRuntimeDef()
		def.Nodes[0].Timeout = d
		return mustRuntimeHash(t, def)
	}
	oneMinute, twoMinutes, unlimited := withTimeout(time.Minute), withTimeout(2*time.Minute), withTimeout(-1)
	for _, h := range []string{oneMinute, twoMinutes, unlimited} {
		if !strings.HasPrefix(h, runtimeHashPrefixV2) {
			t.Fatalf("definition with a node Timeout = %q, want the v2 prefix", h)
		}
	}
	if oneMinute == twoMinutes || oneMinute == unlimited {
		t.Fatal("different node timeouts must hash differently")
	}

	private := baseRuntimeDef()
	private.Nodes[0].Output = &types.NodeOutputPolicy{Private: true}
	if h := mustRuntimeHash(t, private); !strings.HasPrefix(h, runtimeHashPrefixV2) || h == base {
		t.Fatalf("private output hash = %q, want a v2 hash distinct from %q", h, base)
	}

	// An explicit empty policy runs like no policy and must hash like one.
	empty := baseRuntimeDef()
	empty.Nodes[0].Output = &types.NodeOutputPolicy{}
	if h := mustRuntimeHash(t, empty); h != base {
		t.Fatalf("empty output policy hash = %q, want %q", h, base)
	}
}

func TestReconcileDefinitionHashV1AndV2(t *testing.T) {
	def := baseRuntimeDef()
	def.Nodes[0].Timeout = time.Minute
	current := mustRuntimeHash(t, def)
	stale := v1RuntimeHash(t, def)

	cases := []struct {
		name        string
		stored      string
		storedDef   *types.WorkflowDef
		wantHash    string
		wantUpgrade bool
	}{
		{"v2 is current", current, def, current, false},
		{"v2 with nil def", current, nil, current, false},
		{"stale v1 is recomputed", stale, def, current, true},
		{"v1 with nil def is kept", stale, nil, stale, false},
		{"current v1 needs no upgrade", mustRuntimeHash(t, baseRuntimeDef()), baseRuntimeDef(), mustRuntimeHash(t, baseRuntimeDef()), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, upgrade, err := reconcileDefinitionHash(tc.stored, tc.storedDef)
			if err != nil {
				t.Fatalf("reconcileDefinitionHash: %v", err)
			}
			if got != tc.wantHash || upgrade != tc.wantUpgrade {
				t.Fatalf("reconcile = (%q, %v), want (%q, %v)", got, upgrade, tc.wantHash, tc.wantUpgrade)
			}
		})
	}
}

func TestAddWorkflowConflictsOnTimeoutOrOutputChange(t *testing.T) {
	cases := []struct {
		name   string
		first  func(*WorkflowBuilder)
		second func(*WorkflowBuilder)
	}{
		{"timeout changed",
			func(wf *WorkflowBuilder) { wf.Node("start", node.Start()).Timeout(time.Minute) },
			func(wf *WorkflowBuilder) { wf.Node("start", node.Start()).Timeout(2 * time.Minute) }},
		{"timeout added",
			func(wf *WorkflowBuilder) { wf.Node("start", node.Start()) },
			func(wf *WorkflowBuilder) { wf.Node("start", node.Start()).Timeout(time.Minute) }},
		{"output made private",
			func(wf *WorkflowBuilder) { wf.Node("start", node.Start()) },
			func(wf *WorkflowBuilder) { wf.Node("start", node.Start()).PrivateOutput() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			eng, err := NewLocal()
			if err != nil {
				t.Fatalf("NewLocal: %v", err)
			}
			defer eng.Stop()

			first := Workflow("hash-v2-conflict")
			tc.first(first)
			if _, err := eng.AddWorkflow(ctx, first); err != nil {
				t.Fatalf("AddWorkflow(first): %v", err)
			}
			second := Workflow("hash-v2-conflict")
			tc.second(second)
			if _, err := eng.AddWorkflow(ctx, second); !errors.Is(err, backend.ErrWorkflowConflict) {
				t.Fatalf("AddWorkflow(second) error = %v, want ErrWorkflowConflict", err)
			}
			// The unchanged definition still registers idempotently.
			again := Workflow("hash-v2-conflict")
			tc.first(again)
			if _, err := eng.AddWorkflow(ctx, again); err != nil {
				t.Fatalf("AddWorkflow(first again): %v", err)
			}
		})
	}
}

// A record written before v2 stores the v1 hash of a definition whose node
// Timeout the v1 algorithm ignored.
func TestAddWorkflowUpgradesStaleV1RecordWithTimeout(t *testing.T) {
	ctx := context.Background()
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	build := func(timeout time.Duration) (*WorkflowBuilder, *types.WorkflowDef) {
		wf := Workflow("hash-v2-legacy")
		ref := wf.Node("start", node.Start())
		if timeout != 0 {
			ref.Timeout(timeout)
		}
		def, err := wf.build()
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		return wf, def
	}
	wf, def := build(time.Minute)
	stale := v1RuntimeHash(t, def)
	id, _ := seedLegacyRecord(t, eng, def, stale)

	// Same definition: idempotent, and the stored hash is upgraded to v2.
	gotID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow(same definition): %v", err)
	}
	if gotID != id {
		t.Fatalf("AddWorkflow id = %q, want %q", gotID, id)
	}
	rec, err := eng.workflowRegistry.GetWorkflow(ctx, id)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if want := mustRuntimeHash(t, def); rec.DefinitionHash != want {
		t.Fatalf("stored hash = %q, want upgraded %q", rec.DefinitionHash, want)
	}
}

// Without the post-match recompute, the registry's plain hash comparison
// would accept a definition that drops the timeout: its v1 hash equals the
// stale stored one.
func TestAddWorkflowRejectsTimeoutRemovalAgainstStaleV1Record(t *testing.T) {
	ctx := context.Background()
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	withTimeout := Workflow("hash-v2-removal")
	withTimeout.Node("start", node.Start()).Timeout(time.Minute)
	def, err := withTimeout.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	id, _ := seedLegacyRecord(t, eng, def, v1RuntimeHash(t, def))

	without := Workflow("hash-v2-removal")
	without.Node("start", node.Start())
	if _, err := eng.AddWorkflow(ctx, without); !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("AddWorkflow(timeout removed) error = %v, want ErrWorkflowConflict", err)
	}

	// The stale hash was corrected, so the stored definition re-registers
	// directly.
	rec, err := eng.workflowRegistry.GetWorkflow(ctx, id)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if want := mustRuntimeHash(t, def); rec.DefinitionHash != want {
		t.Fatalf("stored hash = %q, want corrected %q", rec.DefinitionHash, want)
	}
	if _, err := eng.AddWorkflow(ctx, withTimeout); err != nil {
		t.Fatalf("AddWorkflow(stored definition): %v", err)
	}
}
