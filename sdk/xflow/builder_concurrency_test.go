package xflow

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// TestWorkflowBuilderConcurrencyReachesSettings guards the transient-field
// lesson: a setting the compiler reads but no builder can set is a switch
// nobody can press.
func TestWorkflowBuilderConcurrencyReachesSettings(t *testing.T) {
	wf := Workflow("conc-builder").
		OnError(types.OnErrorContinue).
		Concurrency(types.ConcurrencySettings{
			Limit:        8,
			Mode:         types.ConcurrencyModeQueue,
			MaxQueued:    16,
			QueueTimeout: 30 * time.Second,
		})
	wf.Node("start", node.Start())

	def, err := wf.build()
	if err != nil {
		t.Fatal(err)
	}
	c := def.Settings.Concurrency
	if c == nil || c.Limit != 8 || c.Mode != types.ConcurrencyModeQueue || c.MaxQueued != 16 || c.QueueTimeout != 30*time.Second {
		t.Fatalf("Settings.Concurrency = %+v, want limit 8 / queue / 16 / 30s", c)
	}
	if def.Settings.OnError != string(types.OnErrorContinue) {
		t.Fatalf("Settings.OnError = %q, want it kept alongside concurrency", def.Settings.OnError)
	}
}

func TestWorkflowBuilderConcurrencyNonPositiveLimitIsUnlimited(t *testing.T) {
	for _, limit := range []int{0, -1} {
		wf := Workflow("conc-unlimited").
			Concurrency(types.ConcurrencySettings{Limit: 4, Mode: types.ConcurrencyModeReject}).
			Concurrency(types.ConcurrencySettings{Limit: limit, Mode: types.ConcurrencyModeReject})
		wf.Node("start", node.Start())

		def, err := wf.build()
		if err != nil {
			t.Fatal(err)
		}
		if def.Settings != nil {
			t.Fatalf("limit %d: Settings = %+v, want nil so the definition hash stays unchanged", limit, def.Settings)
		}
	}
}

func TestWorkflowBuilderConcurrencyBuiltDefinitionDoesNotAlias(t *testing.T) {
	wf := Workflow("conc-alias").Concurrency(types.ConcurrencySettings{Limit: 5, Mode: types.ConcurrencyModeReject})
	wf.Node("start", node.Start())

	def, err := wf.build()
	if err != nil {
		t.Fatal(err)
	}
	wf.concurrency.Limit = 99
	if got := def.Settings.Concurrency.Limit; got != 5 {
		t.Fatalf("built definition limit = %d after builder mutation, want 5", got)
	}
}

// TestWorkflowBuilderConcurrencyCompilesForTransientAndRejectsFAF pins the two
// compile-time facts the collection workflow depends on: a transient workflow
// may carry a limit, a FAF workflow may not.
func TestWorkflowBuilderConcurrencyCompilesForTransientAndRejectsFAF(t *testing.T) {
	transient := Workflow("conc-transient").
		Transient(time.Minute, 10*time.Second).
		Concurrency(types.ConcurrencySettings{Limit: 3, Mode: types.ConcurrencyModeReject})
	transient.Node("start", node.Start())
	def, err := transient.build()
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("transient workflow with a limit must compile: %v", err)
	}
	if c := g.Concurrency(); c == nil || c.Limit != 3 || c.EffectiveMode() != types.ConcurrencyModeReject {
		t.Fatalf("compiled Concurrency = %+v, want limit 3 / reject", c)
	}

	faf := Workflow("conc-faf").
		FAF().
		Concurrency(types.ConcurrencySettings{Limit: 3, Mode: types.ConcurrencyModeReject})
	faf.Node("start", node.Start())
	fafDef, err := faf.build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Compile(fafDef); err == nil {
		t.Fatal("faf workflow with a non-zero limit must fail to compile")
	}
}
