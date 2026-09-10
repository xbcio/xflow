package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestGroupRetryIsRejectedAtCompile pins the compile-time contract for
// group-level retry.
//
// GroupDef.Retry's doc comment promises "组级 retry = 从入口整组重跑", but no
// runtime code enforces it: compileOneGroup only copies the value into
// GroupMeta, and engine/group_exec.go's own comment on executeGroup says
// enforcement of GroupMeta.Retry.MaxAttempts "belongs to a future milestone".
// The group's Attempt is only ever used as a lease fencing token
// (engine/group_lease.go) and never compared against a limit.
//
// Accepting the field and silently ignoring it is the failure mode this
// rejects: an operator configures `retry: {max_attempts: 3}` on a group,
// compiles clean, and gets zero behavior change — discovered only when a
// group fails in production and nothing reruns it.
func TestGroupRetryIsRejectedAtCompile(t *testing.T) {
	def := mkGroupDef([]string{"ingest", "analyze"})
	def.Groups[0].Retry = &types.RetrySettings{Enabled: true, MaxAttempts: 3}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("group-level retry compiled clean; it has no runtime enforcement " +
			"and would silently never retry")
	}
	msg := err.Error()
	for _, want := range []string{"edge", "retry"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not mention %q", msg, want)
		}
	}
}

// TestGroupRetryNilStillCompiles is the other half: rejecting a configured
// Retry must not break the (overwhelmingly common) case where a group leaves
// it unset.
func TestGroupRetryNilStillCompiles(t *testing.T) {
	def := mkGroupDef([]string{"ingest", "analyze"})
	def.Groups[0].Retry = nil
	if _, err := Compile(def); err != nil {
		t.Fatalf("group with Retry=nil must compile: %v", err)
	}
}

// TestMemberNodeRetryUnaffectedByGroupRetryRejection guards against the
// obvious way to get this wrong: rejecting GroupDef.Retry must not reach into
// member NodeDef.Retry. Node-level retry is fully implemented (engine/retry.go,
// engine/atomic_commit.go) and must keep compiling whether or not the node
// happens to be a group member.
func TestMemberNodeRetryUnaffectedByGroupRetryRejection(t *testing.T) {
	def := mkGroupDef([]string{"ingest", "analyze"})
	def.Nodes[1].Retry = &types.RetrySettings{Enabled: true, MaxAttempts: 5}
	if _, err := Compile(def); err != nil {
		t.Fatalf("member node-level retry must compile even inside a group: %v", err)
	}
}
