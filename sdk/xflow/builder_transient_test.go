package xflow

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
)

// TestWorkflowBuilderTransientEmitsOptions pins the builder-side entry point for
// per-workflow transient mode.
//
// The engine reads Transient/TransientTTL/TransientCompletionTTL off
// types.WorkflowOptions (engine/graph/compile.go), and that flag is the only
// thing that keeps an execution's node payloads out of the SQL audit
// projection. Without a builder method there is no way to set it from the CDK
// path -- a workflow whose messages carry credentials would persist them.
func TestWorkflowBuilderTransientEmitsOptions(t *testing.T) {
	wf := Workflow("transient-wf").Transient(2*time.Minute, 30*time.Second)
	wf.Node("start", node.Start())

	def, err := wf.build()
	if err != nil {
		t.Fatal(err)
	}
	if def.Options == nil || !def.Options.Transient {
		t.Fatalf("Options = %+v, want transient", def.Options)
	}
	if def.Options.TransientTTL != 2*time.Minute {
		t.Errorf("TransientTTL = %v, want 2m", def.Options.TransientTTL)
	}
	if def.Options.TransientCompletionTTL != 30*time.Second {
		t.Errorf("TransientCompletionTTL = %v, want 30s", def.Options.TransientCompletionTTL)
	}
}

// TestWorkflowBuilderTransientZeroTTLsLeaveEngineDefaults checks that passing
// zero durations does not fabricate a TTL. Zero means "use the engine-wide
// transient TTL" per the field docs, so the builder must pass it through
// untouched rather than substituting a default of its own.
func TestWorkflowBuilderTransientZeroTTLsLeaveEngineDefaults(t *testing.T) {
	wf := Workflow("transient-default-ttl").Transient(0, 0)
	wf.Node("start", node.Start())

	def, err := wf.build()
	if err != nil {
		t.Fatal(err)
	}
	if def.Options == nil || !def.Options.Transient {
		t.Fatalf("Options = %+v, want transient", def.Options)
	}
	if def.Options.TransientTTL != 0 || def.Options.TransientCompletionTTL != 0 {
		t.Errorf("TTLs = (%v, %v), want both zero so the engine defaults apply",
			def.Options.TransientTTL, def.Options.TransientCompletionTTL)
	}
}

// TestWorkflowBuilderTransientAndAllowCyclesCompose is the regression that
// motivates routing every option writer through a shared accessor.
//
// AllowCycles used to assign w.options wholesale. A second option setter
// written the same way would silently drop whichever call came first --
// and for Transient that means the SQL projection quietly comes back on for a
// workflow that declared itself ephemeral. Both orders must survive.
func TestWorkflowBuilderTransientAndAllowCyclesCompose(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func() *WorkflowBuilder
	}{
		{"transient-then-cycles", func() *WorkflowBuilder {
			return Workflow("compose-a").Transient(time.Minute, time.Second).AllowCycles(9)
		}},
		{"cycles-then-transient", func() *WorkflowBuilder {
			return Workflow("compose-b").AllowCycles(9).Transient(time.Minute, time.Second)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := tc.build()
			wf.Node("start", node.Start())

			def, err := wf.build()
			if err != nil {
				t.Fatal(err)
			}
			if def.Options == nil {
				t.Fatal("Options = nil")
			}
			if !def.Options.Transient {
				t.Error("Transient = false: the later setter overwrote the earlier one")
			}
			if def.Options.TransientTTL != time.Minute {
				t.Errorf("TransientTTL = %v, want 1m", def.Options.TransientTTL)
			}
			if !def.Options.AllowCycles || def.Options.MaxAutoDepth != 9 {
				t.Errorf("AllowCycles = %v, MaxAutoDepth = %d, want true/9",
					def.Options.AllowCycles, def.Options.MaxAutoDepth)
			}
		})
	}
}
