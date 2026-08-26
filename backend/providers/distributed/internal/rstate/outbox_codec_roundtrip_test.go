package rstate

import (
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// roundTripOutboxTask sends a task through the outbox wire format and back.
func roundTripOutboxTask(t *testing.T, task engine.Task) engine.Task {
	t.Helper()
	raw, err := marshalRedisOutboxEntry("e1", task, time.Time{})
	if err != nil {
		t.Fatalf("marshalRedisOutboxEntry: %v", err)
	}
	entry, err := unmarshalRedisOutboxEntry(raw)
	if err != nil {
		t.Fatalf("unmarshalRedisOutboxEntry: %v", err)
	}
	return entry.Task
}

// TestOutboxEntryCarriesTheFieldsHiddenFromRunnerJSON pins the whole class of
// engine.Task fields tagged json:"-".
//
// Those tags keep scheduler internals out of the public runner contract, so the
// envelope has to republish each one by hand under its own key and copy it back
// on decode. That is three edits per field in two functions, and nothing about
// adding a fourth field makes the compiler ask for them: a field that gets the
// struct entry but not the marshal line simply arrives zero on the far side of
// a durable queue, with no error anywhere. This test is the thing that asks.
//
// It covers every such field rather than only the newest one. A test named
// after one field leaves exactly the trap it was written to close.
func TestOutboxEntryCarriesTheFieldsHiddenFromRunnerJSON(t *testing.T) {
	port := "review_failed"
	got := roundTripOutboxTask(t, engine.Task{
		ExecutionID:  types.ExecutionID("exec-1"),
		NodeName:     "review",
		NodeIdx:      3,
		Type:         engine.TaskTypeNodeAdvance,
		AutoDepth:    7,
		ActivationID: 5,
		UnitIdx:      2,
		Port:         &port,
	})

	if got.AutoDepth != 7 {
		t.Errorf("AutoDepth = %d, want 7", got.AutoDepth)
	}
	if got.ActivationID != 5 {
		t.Errorf("ActivationID = %d, want 5", got.ActivationID)
	}
	if got.UnitIdx != 2 {
		t.Errorf("UnitIdx = %d, want 2", got.UnitIdx)
	}
	if got.Port == nil {
		t.Fatalf("Port = nil, want %q carried across the wire", port)
	}
	if *got.Port != port {
		t.Errorf("Port = %q, want %q", *got.Port, port)
	}
}

// TestOutboxEntryKeepsAbsentAndEmptyPortApart is the assertion the round trip
// above cannot make.
//
// Both UnitIdx and Port have a legitimate value that a plain omitempty field
// would encode identically to "absent" -- unit index 0 and the empty port. The
// empty port is not a curiosity: a skipped node commits with one, and it is
// what tells the advance branch to propagate the skip instead of activating a
// branch. Absent means something different and incompatible: the entry predates
// the field, and the advance has to fall back to reading the node.
//
// Collapse the two and a rolling deploy routes live executions as though every
// pre-upgrade advance came from a skipped node. Both directions are asserted,
// because a codec that always reports absent and one that always reports empty
// each satisfy half of this.
func TestOutboxEntryKeepsAbsentAndEmptyPortApart(t *testing.T) {
	empty := ""
	carried := roundTripOutboxTask(t, engine.Task{
		ExecutionID: types.ExecutionID("exec-1"),
		NodeName:    "review",
		Type:        engine.TaskTypeNodeAdvance,
		Port:        &empty,
	})
	if carried.Port == nil {
		t.Error("Port = nil for a task carrying the empty port, want a non-nil " +
			"pointer to \"\" (a skipped node's advance is this shape)")
	} else if *carried.Port != "" {
		t.Errorf("Port = %q, want the empty port preserved", *carried.Port)
	}

	absent := roundTripOutboxTask(t, engine.Task{
		ExecutionID: types.ExecutionID("exec-1"),
		NodeName:    "review",
		Type:        engine.TaskTypeNodeAdvance,
	})
	if absent.Port != nil {
		t.Errorf("Port = %q for a task carrying no port, want nil so the "+
			"advance takes the read-the-node fallback", *absent.Port)
	}
}

// TestOutboxEntryKeepsAbsentAndZeroUnitIdxApart is the same distinction for the
// field that established the pattern. See engine.UnitIdxUnknown.
func TestOutboxEntryKeepsAbsentAndZeroUnitIdxApart(t *testing.T) {
	zero := roundTripOutboxTask(t, engine.Task{
		ExecutionID: types.ExecutionID("exec-1"),
		NodeName:    "review",
		Type:        engine.TaskTypeNodeExec,
		UnitIdx:     0,
	})
	if zero.UnitIdx != 0 {
		t.Errorf("UnitIdx = %d, want the real unit index 0 preserved", zero.UnitIdx)
	}

	unknown := roundTripOutboxTask(t, engine.Task{
		ExecutionID: types.ExecutionID("exec-1"),
		NodeName:    "review",
		Type:        engine.TaskTypeNodeExec,
		UnitIdx:     engine.UnitIdxUnknown,
	})
	if unknown.UnitIdx != engine.UnitIdxUnknown {
		t.Errorf("UnitIdx = %d, want engine.UnitIdxUnknown (%d) so a caller can "+
			"tell it apart from a real index of 0",
			unknown.UnitIdx, engine.UnitIdxUnknown)
	}
}
