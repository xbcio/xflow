package xflow

import (
	"testing"
	"time"
)

// TestNodeRefTimeoutReachesNodeDef guards the transient-field lesson: a field
// that the compiler reads but no builder can set is a switch nobody can press.
func TestNodeRefTimeoutReachesNodeDef(t *testing.T) {
	wf := Workflow("timeout-builder")
	n := wf.LocalNode("worker", nil)
	n.Timeout(90 * time.Second)

	def, err := wf.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	var found bool
	for _, nd := range def.Nodes {
		if nd.Name == "worker" {
			found = true
			if nd.Timeout != 90*time.Second {
				t.Fatalf("NodeDef.Timeout = %v, want 90s", nd.Timeout)
			}
		}
	}
	if !found {
		t.Fatal("node \"worker\" not in built def")
	}
}

// TestNodeRefTimeoutNegative confirms the escape hatch survives the builder:
// a negative value must reach NodeDef verbatim, not be normalized to zero.
func TestNodeRefTimeoutNegative(t *testing.T) {
	wf := Workflow("timeout-builder-neg")
	wf.LocalNode("unbounded", nil).Timeout(-1)

	def, err := wf.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, nd := range def.Nodes {
		if nd.Name == "unbounded" && nd.Timeout != -1 {
			t.Fatalf("NodeDef.Timeout = %v, want -1", nd.Timeout)
		}
	}
}
