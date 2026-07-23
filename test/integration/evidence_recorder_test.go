//go:build integration

package integration

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/test/integration/internal/evidence"
	"github.com/xbcio/xflow/types"
)

// evidenceRecorder accumulates one scenario's raw observations into a fragment
// envelope bound to a shared run ID, then flushes it atomically at the end of
// the scenario. It is the bridge between a scenario's drained runtime evidence
// buffer and the independent verifier: it only transports already-produced raw
// records (events, counters, protocol observations, state snapshots) and never
// fabricates, pre-aggregates, or writes derived_observations / pass:true.
//
// Design (per task-15a brief): each test writes its own fragment envelope;
// the evidence-verify CLI merges all fragments. No TestMain, no package-level
// shared accumulator, no mutex — fragments have unique filenames so there is no
// write contention. The recorder is env-gated: if either of
// XFLOW_G0_EVIDENCE_RUN_ID / XFLOW_G0_EVIDENCE_RAW_DIR is unset, every method
// is a no-op (nil receiver) so normal test runs and the existing
// a0_fault_matrix_report.json artifact are unaffected.
type evidenceRecorder struct {
	runID  string
	rawDir string
	name   string // fragment filename stem (unique per scenario)
	env    *evidence.Envelope
}

// newEvidenceRecorder returns a recorder bound to the run ID and raw dir
// declared via environment. Returns nil (no-op) if either env var is unset, so
// normal test runs are unaffected. The name is the fragment filename stem and
// MUST be unique within one run (use the manifest scenario string).
func newEvidenceRecorder(t *testing.T, name string) *evidenceRecorder {
	t.Helper()
	runID := os.Getenv("XFLOW_G0_EVIDENCE_RUN_ID")
	rawDir := os.Getenv("XFLOW_G0_EVIDENCE_RAW_DIR")
	if runID == "" || rawDir == "" {
		return nil
	}
	return &evidenceRecorder{
		runID:  runID,
		rawDir: rawDir,
		name:   name,
		env: &evidence.Envelope{
			SchemaVersion: evidence.SchemaVersion,
			RunID:         runID,
			StartedAt:     time.Now().UTC(),
		},
	}
}

// recordRuntimeEvents appends drained buffer events. The events already carry
// ExecutionID/NodeName/Attempt/Applied/CommitOutcome/OutboxIDs/ErrorSource/
// Classified/Kind/Retryable/etc. — the recorder only transports them verbatim;
// it must not fabricate or pre-aggregate.
func (r *evidenceRecorder) recordRuntimeEvents(evs []engine.RuntimeEvidenceEvent) {
	if r == nil {
		return
	}
	r.env.Raw.RuntimeEvents = append(r.env.Raw.RuntimeEvents, evs...)
}

// recordCounter appends a counter snapshot. The counting wrappers from
// Tasks 8/10/11 (buildInstrumentedHandler / startCounter) supply the value.
func (r *evidenceRecorder) recordCounter(topology string, execID types.ExecutionID, node, counterID string, value int) {
	if r == nil {
		return
	}
	r.env.Raw.CounterSnapshots = append(r.env.Raw.CounterSnapshots, evidence.CounterSnapshot{
		RunID:        r.runID,
		Topology:     topology,
		ExecutionID:  execID,
		NodeName:     node,
		CounterID:    counterID,
		HandlerName:  counterID,
		Value:        value,
		ObservedAt:   time.Now().UTC(),
	})
}

// recordA0ScenarioMarker appends a scenario_marker protocol observation.
//
// CRITICAL field semantics (confirmed against verifier.go:362,399):
// computeDerivedObservations reads po.Topology AS THE SCENARIO NAME (not a
// topology) for scenario_marker, and findDuplicateMarkers (verifier.go:353)
// keys duplicates by po.Topology. So this helper MUST set
// ProtocolObservation.Topology = scenario and ExecutionID = execID. Detail is
// optional and left nil here.
func (r *evidenceRecorder) recordA0ScenarioMarker(execID types.ExecutionID, scenario string) {
	if r == nil {
		return
	}
	r.env.Raw.ProtocolObservations = append(r.env.Raw.ProtocolObservations, evidence.ProtocolObservation{
		RunID:        r.runID,
		Topology:     scenario, // verifier reads this as the scenario name
		ExecutionID:  execID,
		Type:         "scenario_marker",
		ObservedAt:   time.Now().UTC(),
	})
}

// recordA3RowMarker appends an a3_row_marker protocol observation.
//
// CRITICAL field semantics (confirmed against verifier.go:406-409):
// computeDerivedObservations reads po.Detail["fixture"] and po.Detail["topology"]
// for a3_row_marker. So this helper MUST set Detail = {"fixture": fixture,
// "topology": topology} and ExecutionID = execID. (Task 15b wires A3 rows; this
// helper exists now so the recorder API is complete.)
func (r *evidenceRecorder) recordA3RowMarker(execID types.ExecutionID, fixture, topology string) {
	if r == nil {
		return
	}
	r.env.Raw.ProtocolObservations = append(r.env.Raw.ProtocolObservations, evidence.ProtocolObservation{
		RunID:       r.runID,
		Topology:    topology,
		ExecutionID: execID,
		Type:        "a3_row_marker",
		ObservedAt:  time.Now().UTC(),
		Detail: map[string]any{
			"fixture":   fixture,
			"topology":   topology,
		},
	})
}

// recordProtocol appends a generic protocol observation (loss_injection,
// authority_rejected, lease_reclaim, system_task_delivery, ...). For these,
// Topology is the real topology (not a scenario name). The verifier's
// checkA0 inspects authority_rejected / lease_reclaim by ExecutionID.
func (r *evidenceRecorder) recordProtocol(topology string, execID types.ExecutionID, typ string, detail map[string]any) {
	if r == nil {
		return
	}
	r.env.Raw.ProtocolObservations = append(r.env.Raw.ProtocolObservations, evidence.ProtocolObservation{
		RunID:       r.runID,
		Topology:    topology,
		ExecutionID: execID,
		Type:        typ,
		ObservedAt:  time.Now().UTC(),
		Detail:      detail,
	})
}

// recordState appends a state snapshot.
func (r *evidenceRecorder) recordState(topology string, execID types.ExecutionID, typ string, state map[string]any) {
	if r == nil {
		return
	}
	r.env.Raw.StateSnapshots = append(r.env.Raw.StateSnapshots, evidence.StateSnapshot{
		RunID:       r.runID,
		Topology:    topology,
		ExecutionID: execID,
		Type:        typ,
		ObservedAt:  time.Now().UTC(),
		State:       state,
	})
}

// flush atomically writes the fragment envelope to $RAW_DIR/{name}.json via a
// temp file + rename so a partial write is never observed by the CLI merge.
func (r *evidenceRecorder) flush(t *testing.T) {
	t.Helper()
	if r == nil {
		return
	}
	if err := os.MkdirAll(r.rawDir, 0o755); err != nil {
		t.Fatalf("evidence recorder: mkdir %q: %v", r.rawDir, err)
	}
	data, err := json.MarshalIndent(r.env, "", "  ")
	if err != nil {
		t.Fatalf("evidence recorder: marshal fragment: %v", err)
	}
	finalPath := filepath.Join(r.rawDir, r.name+".json")
	tmpPath := finalPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		t.Fatalf("evidence recorder: write temp %q: %v", tmpPath, err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		t.Fatalf("evidence recorder: rename %q: %v", finalPath, err)
	}
	t.Logf("evidence fragment written: %s (run_id=%s runtime_events=%d counters=%d protocol=%d state=%d)",
		finalPath, r.runID, len(r.env.Raw.RuntimeEvents), len(r.env.Raw.CounterSnapshots),
		len(r.env.Raw.ProtocolObservations), len(r.env.Raw.StateSnapshots))
}
