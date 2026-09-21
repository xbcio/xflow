package xflow

import (
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestWorkflowBuilderFAFEmitsCopySafeOptions(t *testing.T) {
	wf := Workflow("faf-builder").FAF().AllowCycles(7)
	wf.Node("start", node.Start())

	def, err := wf.build()
	if err != nil {
		t.Fatal(err)
	}
	if def.Options == nil || !def.Options.FAF {
		t.Fatalf("Options = %+v, want FAF enabled", def.Options)
	}
	if !def.Options.AllowCycles || def.Options.MaxAutoDepth != 7 {
		t.Fatalf("Options = %+v, want FAF and AllowCycles/7 to compose", def.Options)
	}

	// A built definition may already have been hashed or registered. Mutating the
	// builder's private options after build must not rewrite that definition.
	wf.options.FAF = false
	if !def.Options.FAF {
		t.Fatal("built definition aliases the builder's FAF option")
	}

	got := wf.Options()
	got.FAF = true
	if wf.Options().FAF {
		t.Fatal("Options() returned an alias of the builder's FAF option")
	}
}
