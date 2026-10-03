package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestGroupOnErrorOutputRequiresErrorOutputs pins the compile-time contract
// introduced for group-level on_error=error_output: the policy and its
// targets must agree. Declaring the policy with no error_outputs targets
// routes nowhere, so it is rejected rather than silently compiling into a
// policy that can never fire anywhere.
func TestGroupOnErrorOutputRequiresErrorOutputs(t *testing.T) {
	def := mkGroupDef([]string{"ingest", "analyze"})
	def.Groups[0].OnError = string(types.OnErrorOutput)
	_, err := Compile(def)
	if err == nil {
		t.Fatal("on_error=error_output with no error_outputs targets compiled clean")
	}
	if !strings.Contains(err.Error(), "error_outputs") {
		t.Errorf("error %q does not mention error_outputs", err.Error())
	}
}

// TestGroupOnErrorOutputWithTargetsCompiles is the positive half: a group
// declaring on_error=error_output together with a valid error_outputs target
// now compiles — the mechanism NODE-GROUP-COLOCATION.md §12.2 scoped as
// unbuilt is implemented.
func TestGroupOnErrorOutputWithTargetsCompiles(t *testing.T) {
	def := mkGroupDef([]string{"ingest", "analyze"})
	def.Groups[0].OnError = string(types.OnErrorOutput)
	def.Groups[0].ErrorOutputs = []types.Connection{{Node: "store"}}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("on_error=error_output with a valid target must compile: %v", err)
	}
	gm := g.Groups()[0]
	if len(gm.ErrorOutputs) != 1 {
		t.Fatalf("want 1 resolved error output, got %d", len(gm.ErrorOutputs))
	}
	storeIdx, _ := g.NodeIndex("store")
	if gm.ErrorOutputs[0].NodeIdx != storeIdx || gm.ErrorOutputs[0].Port != types.DefaultInputPort {
		t.Fatalf("resolved error output = %+v, want {NodeIdx: %d, Port: %q}",
			gm.ErrorOutputs[0], storeIdx, types.DefaultInputPort)
	}
}

// TestGroupErrorOutputsWithoutOnErrorOutputIsRejected is the mirror case: a
// declared error_outputs target with any other on_error policy is a route
// that is never taken — equally a silent no-op, equally rejected.
func TestGroupErrorOutputsWithoutOnErrorOutputIsRejected(t *testing.T) {
	for _, policy := range []string{"", string(types.OnErrorStop), string(types.OnErrorContinue)} {
		def := mkGroupDef([]string{"ingest", "analyze"})
		def.Groups[0].OnError = policy
		def.Groups[0].ErrorOutputs = []types.Connection{{Node: "store"}}
		_, err := Compile(def)
		if err == nil {
			t.Fatalf("on_error=%q with error_outputs set compiled clean", policy)
		}
	}
}

// TestGroupErrorOutputsRejectsBadTargets covers the target-resolution half:
// an unknown node, a target that is a member of the same group (there is no
// running engine left to deliver to once the group fails), and a target port
// the destination does not declare are all rejected the same way an ordinary
// connection would be.
func TestGroupErrorOutputsRejectsBadTargets(t *testing.T) {
	unknown := mkGroupDef([]string{"ingest", "analyze"})
	unknown.Groups[0].OnError = string(types.OnErrorOutput)
	unknown.Groups[0].ErrorOutputs = []types.Connection{{Node: "ghost"}}

	selfMember := mkGroupDef([]string{"ingest", "analyze"})
	selfMember.Groups[0].OnError = string(types.OnErrorOutput)
	selfMember.Groups[0].ErrorOutputs = []types.Connection{{Node: "analyze"}}

	undeclaredPort := mkGroupDef([]string{"ingest", "analyze"})
	undeclaredPort.Nodes[2].Inputs = []types.PortDecl{{Name: "specific", Required: true}}
	undeclaredPort.Groups[0].OnError = string(types.OnErrorOutput)
	undeclaredPort.Groups[0].ErrorOutputs = []types.Connection{{Node: "store"}} // targets implicit "main", not declared

	duplicate := mkGroupDef([]string{"ingest", "analyze"})
	duplicate.Groups[0].OnError = string(types.OnErrorOutput)
	duplicate.Groups[0].ErrorOutputs = []types.Connection{{Node: "store"}, {Node: "store"}}

	cases := map[string]*types.WorkflowDef{
		"unknown target":   unknown,
		"target is member": selfMember,
		"undeclared port":  undeclaredPort,
		"duplicate target": duplicate,
	}
	for name, def := range cases {
		if _, err := Compile(def); err == nil {
			t.Errorf("%s: compiled clean, want rejection", name)
		}
	}
}

// TestGroupOnErrorMainOutputIsRejectedAtCompile pins the remaining half of
// the compile-time contract: main_output stays rejected. Unlike a node, a
// group that failed before committing has no single member output to stand
// in for "the group's main result" — there is no error_outputs-shaped escape
// hatch for it the way there is for error_output.
func TestGroupOnErrorMainOutputIsRejectedAtCompile(t *testing.T) {
	def := mkGroupDef([]string{"ingest", "analyze"})
	def.Groups[0].OnError = string(types.OnErrorMainOutput)
	_, err := Compile(def)
	if err == nil {
		t.Fatal("on_error=main_output on a group compiled clean")
	}
	msg := err.Error()
	for _, want := range []string{string(types.OnErrorMainOutput), "on_error"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not mention %q", msg, want)
		}
	}
}

// TestGroupOnErrorStopAndContinueStillCompile is the other half: adding
// error_output must not narrow the two pre-existing policies.
// groupOnErrorFatal maps continue and error_output => non-fatal and
// everything else => fatal.
func TestGroupOnErrorStopAndContinueStillCompile(t *testing.T) {
	for _, policy := range []string{"", string(types.OnErrorStop), string(types.OnErrorContinue)} {
		def := mkGroupDef([]string{"ingest", "analyze"})
		def.Groups[0].OnError = policy
		if _, err := Compile(def); err != nil {
			t.Fatalf("on_error=%q on a group must compile: %v", policy, err)
		}
	}
}

// TestGroupOnErrorUnknownValueIsRejected closes the hole found while pinning
// this down: OnError was stored as a plain string with no validation at all,
// so a typo (`error-output`, `Stop`, `fail`) also compiled clean and ran as
// fatal. `fail` is worth calling out — types/group.go's own doc comment says
// "不存在 OnErrorFail", which is exactly the value an author would guess.
func TestGroupOnErrorUnknownValueIsRejected(t *testing.T) {
	for _, policy := range []string{"fail", "error-output", "Stop", "retry"} {
		def := mkGroupDef([]string{"ingest", "analyze"})
		def.Groups[0].OnError = policy
		if _, err := Compile(def); err == nil {
			t.Fatalf("on_error=%q is not a known policy but compiled clean", policy)
		}
	}
}
