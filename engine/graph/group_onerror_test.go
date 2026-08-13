package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestGroupOnErrorOutputIsRejectedAtCompile pins the compile-time contract for
// group-level on_error.
//
// error_output and main_output are node-level policies that select which
// already-compiled output port's edges to activate. A group has no such port:
// GroupMeta.BoundaryOutputs is derived purely from member edges that cross the
// boundary, compileOneGroup never synthesizes one from OnError, and
// CommitGroupResult rejects any exit whose (nodeIdx, port) is absent from
// BoundaryOutputs. See NODE-GROUP-COLOCATION.md §12.2.
//
// Accepting the value and silently running it as `stop` is the failure mode
// this rejects: the author asked for the failure to be routed to a downstream
// branch, and got the whole execution failed instead, with no diagnostic. That
// is worse than a compile error because it only shows up as a production
// incident on the error path — the path least likely to be exercised in
// testing.
func TestGroupOnErrorOutputIsRejectedAtCompile(t *testing.T) {
	for _, policy := range []types.OnError{types.OnErrorOutput, types.OnErrorMainOutput} {
		def := mkGroupDef([]string{"ingest", "analyze"})
		def.Groups[0].OnError = string(policy)
		_, err := Compile(def)
		if err == nil {
			t.Fatalf("on_error=%q on a group compiled clean; it would then run "+
				"as `stop` and fail the whole execution instead of routing", policy)
		}
		// The message must name the group and the offending value, and must not
		// leak anything else: compile diagnostics carry node/group names and
		// parameter names only.
		msg := err.Error()
		for _, want := range []string{"edge", string(policy), "on_error"} {
			if !strings.Contains(msg, want) {
				t.Errorf("on_error=%q: error %q does not mention %q", policy, msg, want)
			}
		}
	}
}

// TestGroupOnErrorStopAndContinueStillCompile is the other half: rejecting the
// two output policies must not narrow the two that ARE implemented.
// groupOnErrorFatal maps continue => non-fatal and everything else => fatal, so
// both of these have real, distinct runtime behavior.
func TestGroupOnErrorStopAndContinueStillCompile(t *testing.T) {
	for _, policy := range []string{"", string(types.OnErrorStop), string(types.OnErrorContinue)} {
		def := mkGroupDef([]string{"ingest", "analyze"})
		def.Groups[0].OnError = policy
		if _, err := Compile(def); err != nil {
			t.Fatalf("on_error=%q on a group must compile: %v", policy, err)
		}
	}
}

// TestGroupOnErrorUnknownValueIsRejected closes the third hole found while
// pinning this down: OnError was stored as a plain string with no validation at
// all, so a typo (`error-output`, `Stop`, `fail`) also compiled clean and ran
// as fatal. `fail` is worth calling out — types/group.go's own doc comment says
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
