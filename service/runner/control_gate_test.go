package runner

import (
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

func TestRunnerControlGateFailsClosedAndOrdersGenerations(t *testing.T) {
	gate := newRunnerControlGate()
	if gate.recoveryOnly() {
		t.Fatal("new gate unexpectedly recovery-only")
	}
	if generation, draining := gate.drainingGeneration(); generation != 0 || draining {
		t.Fatalf("new gate draining generation = (%d, %t), want (0, false)", generation, draining)
	}
	gate.apply(&protocol.RunnerControlDirective{DesiredState: "draining", Generation: 3, RecoveryOnly: true})
	if !gate.recoveryOnly() {
		t.Fatal("draining directive did not close local ordinary polling")
	}
	if generation, draining := gate.drainingGeneration(); generation != 3 || !draining {
		t.Fatalf("draining generation = (%d, %t), want (3, true)", generation, draining)
	}
	gate.apply(&protocol.RunnerControlDirective{DesiredState: "active", Generation: 2})
	if !gate.recoveryOnly() {
		t.Fatal("stale active directive reopened drain")
	}
	gate.apply(&protocol.RunnerControlDirective{DesiredState: "active", Generation: 3})
	if !gate.recoveryOnly() {
		t.Fatal("equal-generation conflicting active directive reopened drain")
	}
	gate.apply(&protocol.RunnerControlDirective{DesiredState: "active", Generation: 4})
	if gate.recoveryOnly() {
		t.Fatal("newer active directive did not reopen normal polling")
	}
	if generation, draining := gate.drainingGeneration(); generation != 0 || draining {
		t.Fatalf("active gate draining generation = (%d, %t), want (0, false)", generation, draining)
	}
	gate.apply(&protocol.RunnerControlDirective{DesiredState: "unexpected", Generation: 5})
	if !gate.recoveryOnly() {
		t.Fatal("unknown desired state did not fail closed")
	}
}
