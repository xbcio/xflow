package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// fakeProvenance returns controlled values so tests never touch real git.
type fakeProvenance struct {
	commitSHA          string
	relevantTreeClean  bool
	relevantDiffSHA256 string
	testBinarySHA256   string
	goVersion          string
}

func (f fakeProvenance) CommitSHA() (string, error)          { return f.commitSHA, nil }
func (f fakeProvenance) RelevantTreeClean([]string) (bool, string, error) {
	return f.relevantTreeClean, "", nil
}
func (f fakeProvenance) RelevantDiffDigest([]string) (string, error) {
	return f.relevantDiffSHA256, nil
}
func (f fakeProvenance) TestBinaryDigest(string) (string, error) { return f.testBinarySHA256, nil }
func (f fakeProvenance) GoVersion() string                       { return f.goVersion }

func defaultFakeProvenance() fakeProvenance {
	return fakeProvenance{
		commitSHA:          "abcdef1234567890abcdef1234567890abcdef12",
		relevantTreeClean:  true,
		relevantDiffSHA256: sha256String("clean"),
		testBinarySHA256:   sha256String("binary"),
		goVersion:          runtime.Version(),
	}
}

func sha256String(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func validEnvelope() *Envelope {
	env := NewEnvelope()
	env.Source = SourceProvenance{
		CommitSHA:          "abcdef1234567890abcdef1234567890abcdef12",
		RelevantTreeClean:  true,
		RelevantDiffSHA256: sha256String("clean"),
		TestBinarySHA256:   sha256String("binary"),
		GoVersion:          runtime.Version(),
	}
	env.Environment = Environment{RedisVersion: "7.2", MySQLVersion: "8.0"}
	env.Suite = SuiteSummary{ExitCode: 0, SkipCount: 0, DroppedRuntimeEvents: 0}
	return env
}

func markA0Scenario(env *Envelope, scenario A0Scenario, execID types.ExecutionID) {
	env.Raw.ProtocolObservations = append(env.Raw.ProtocolObservations, ProtocolObservation{
		RunID: env.RunID, Topology: string(scenario), ExecutionID: execID,
		Type: "scenario_marker", ObservedAt: time.Now().UTC(),
	})
	if scenario == A0ReportRequestLoss {
		env.Raw.ProtocolObservations = append(env.Raw.ProtocolObservations, ProtocolObservation{
			RunID: env.RunID, Topology: string(scenario), ExecutionID: execID,
			Type: "authority_rejected", ObservedAt: time.Now().UTC(),
		})
	}
	env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents,
		engine.RuntimeEvidenceEvent{
			EventID: "commit-" + string(scenario), Type: engine.RuntimeEvidenceCommit,
			ExecutionID: execID, NodeName: "node", CommitOutcome: engine.CommitOutcomeAccepted,
		},
		engine.RuntimeEvidenceEvent{
			EventID: "advance-" + string(scenario), Type: engine.RuntimeEvidenceAdvance,
			ExecutionID: execID, NodeName: "node", Applied: true,
		},
	)
}

func markA3Row(env *Envelope, fixture A3Fixture, topology A3Topology, execID types.ExecutionID, classified bool) {
	env.Raw.ProtocolObservations = append(env.Raw.ProtocolObservations, ProtocolObservation{
		RunID: env.RunID, Topology: string(topology), ExecutionID: execID,
		Type: "a3_row_marker", ObservedAt: time.Now().UTC(),
		Detail: map[string]any{"fixture": string(fixture), "topology": string(topology)},
	})
	env.Raw.CounterSnapshots = append(env.Raw.CounterSnapshots, CounterSnapshot{
		RunID: env.RunID, Topology: string(topology), ExecutionID: execID,
		NodeName: "node", CounterID: "counter-" + string(fixture) + "-" + string(topology),
		HandlerName: "handler", Value: 1, ObservedAt: time.Now().UTC(),
	})
	commitEvent := engine.RuntimeEvidenceEvent{
		EventID:     "commit-" + string(fixture) + "-" + string(topology),
		Type:        engine.RuntimeEvidenceCommit,
		ExecutionID: execID,
		NodeName:    "node",
		CommitOutcome: engine.CommitOutcomeAccepted,
		Applied:     true,
		Classified:  classified,
		ErrorSource: engine.ErrorSourceSystem,
		ErrorKind:   types.ErrorKindTransient,
	}
	if classified {
		retryable := true
		commitEvent.Retryable = &retryable
	}
	env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, commitEvent)
	env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, engine.RuntimeEvidenceEvent{
		EventID: "advance-" + string(fixture) + "-" + string(topology),
		Type:    engine.RuntimeEvidenceAdvance, ExecutionID: execID, NodeName: "node", Applied: true,
	})
}

func markAllRequired(env *Envelope) {
	for i, s := range A0RequiredScenarios() {
		markA0Scenario(env, s, types.ExecutionID("exec-a0-"+strconv.Itoa(i)))
	}
	for _, row := range A3RequiredRows() {
		classified := row.Fixture == A3TransientRetryExhausted || row.Fixture == A3PermanentNoRetry || row.Fixture == A3ErrorPortRetryExhausted
		markA3Row(env, row.Fixture, row.Topology, types.ExecutionID("exec-a3-"+string(row.Fixture)+"-"+string(row.Topology)), classified)
	}
}

func passEvents() []GoTestEvent {
	return []GoTestEvent{
		{Action: "run", Package: "github.com/xbcio/xflow/test/integration", Test: "TestA0FaultMatrix"},
		{Action: "pass", Package: "github.com/xbcio/xflow/test/integration", Test: "TestA0FaultMatrix"},
		{Action: "pass", Package: "github.com/xbcio/xflow/test/integration"},
	}
}

func requireNotPassed(t *testing.T, v Verification, reason string) {
	t.Helper()
	if v.Passed {
		t.Fatalf("expected verification to fail: %s", reason)
	}
}

func requirePassed(t *testing.T, v Verification) {
	t.Helper()
	if !v.Passed {
		t.Fatalf("expected verification to pass, got errors: %v", v.Errors)
	}
}

func TestVerifyPassesForValidEnvelope(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requirePassed(t, res)
}

func TestVerifyRejectsMissingA0Scenario(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// Remove CommitThenFlushBeforeDelivery events.
	var filtered []engine.RuntimeEvidenceEvent
	for _, ev := range env.Raw.RuntimeEvents {
		if ev.EventID != "commit-"+string(A0CommitThenFlushBeforeDelivery) &&
			ev.EventID != "advance-"+string(A0CommitThenFlushBeforeDelivery) {
			filtered = append(filtered, ev)
		}
	}
	env.Raw.RuntimeEvents = filtered
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "missing A0 scenario")
}

func TestVerifyRejectsDuplicateA0Scenario(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// Add a second commit for the same scenario by duplicating with a new ID.
	for _, ev := range env.Raw.RuntimeEvents {
		if ev.EventID == "commit-"+string(A0CommitThenFlushBeforeDelivery) {
			dup := ev
			dup.EventID = "commit-dup"
			env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, dup)
			break
		}
	}
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "duplicate A0 scenario")
}

func TestVerifyRejectsCrossRunReference(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	env.Raw.CounterSnapshots[0].RunID = "other-run"
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "cross-run reference")
}

func TestVerifyRejectsUnknownCommitSHA(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	env.Source.CommitSHA = "unknown"
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "unknown commit SHA")
}

func TestVerifyRejectsRelevantDirty(t *testing.T) {
	prov := defaultFakeProvenance()
	prov.relevantTreeClean = false
	prov.relevantDiffSHA256 = sha256String("dirty")
	env := validEnvelope()
	markAllRequired(env)
	v := NewVerifier(prov)
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "relevant tree dirty")
}

func TestVerifyRejectsBinaryDigestMismatch(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	env.Source.TestBinarySHA256 = sha256String("different")
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "binary digest mismatch")
}

func TestVerifyRejectsSkip(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	events := []GoTestEvent{
		{Action: "run", Package: "p", Test: "TestFoo"},
		{Action: "skip", Package: "p", Test: "TestFoo"},
		{Action: "pass", Package: "p"},
	}
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, events)
	requireNotPassed(t, res, "skipped test")
}

func TestVerifyRejectsNonZeroExit(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	events := []GoTestEvent{
		{Action: "run", Package: "p", Test: "TestFoo"},
		{Action: "fail", Package: "p", Test: "TestFoo"},
		{Action: "fail", Package: "p"},
	}
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, events)
	requireNotPassed(t, res, "non-zero suite exit")
}

func TestVerifyRejectsDroppedRuntimeEvents(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	env.Suite.DroppedRuntimeEvents = 3
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "dropped runtime events")
}

func TestVerifyRejectsFixtureStampEvidenceSource(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// Inject a protocol observation that carries a pre-aggregated/fixed count
	// as if it were a derived observation. The verifier must reject any
	// evidence_source:"fixture" or "stamp" records in the raw ledger.
	env.Raw.ProtocolObservations = append(env.Raw.ProtocolObservations, ProtocolObservation{
		RunID: env.RunID, Topology: string(A3Local), ExecutionID: "exec-stamp",
		Type: "fixture", ObservedAt: time.Now().UTC(),
		Detail: map[string]any{"handler_invocations": 42, "evidence_source": "fixture"},
	})
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "fixed/stamp evidence source")
}

func TestVerifyRejectsMissingA3Row(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// Remove all rows for local topology.
	var filtered []engine.RuntimeEvidenceEvent
	for _, ev := range env.Raw.RuntimeEvents {
		if ev.ExecutionID != types.ExecutionID("exec-a3-"+string(A3TransientThenSuccess)+"-"+string(A3Local)) {
			filtered = append(filtered, ev)
		}
	}
	env.Raw.RuntimeEvents = filtered
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "missing A3 row")
}

func TestVerifyRejectsDuplicateA3Row(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// Duplicate events for one fixture/topology with new IDs.
	var dup []engine.RuntimeEvidenceEvent
	for _, ev := range env.Raw.RuntimeEvents {
		if ev.ExecutionID == types.ExecutionID("exec-a3-"+string(A3TransientThenSuccess)+"-"+string(A3Local)) {
			d := ev
			d.EventID = ev.EventID + "-dup"
			dup = append(dup, d)
		}
	}
	env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, dup...)
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "duplicate A3 row")
}

func TestVerifyRejectsWrongDatabaseTopologyClaim(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// Claim a server-runner row executed in cluster-durable by relabeling the
	// a3_row_marker topology. The verifier must detect that the marker does not
	// match the required manifest row.
	for i := range env.Raw.ProtocolObservations {
		po := &env.Raw.ProtocolObservations[i]
		if po.Type != "a3_row_marker" {
			continue
		}
		if po.Detail["fixture"] == string(A3TransientThenSuccess) &&
			po.Detail["topology"] == string(A3ServerRunner) {
			po.Detail["topology"] = string(A3ClusterDurable)
		}
	}
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "wrong Database topology claim")
}

func TestVerifyRejectsPartialFileMissingRunID(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	env.RunID = ""
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "partial file missing run_id")
}

func TestVerifyRejectsDuplicateEventID(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	if len(env.Raw.RuntimeEvents) > 0 {
		env.Raw.RuntimeEvents[1].EventID = env.Raw.RuntimeEvents[0].EventID
	}
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "duplicate event ID")
}

func TestVerifyRejectsPreaggregatedDerivedObservations(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	env.DerivedObservations = []DerivedObservation{{
		Kind: "a0_scenario", Scenario: string(A0OSKillSIGKILL),
		EvidenceSource: "fixture", AcceptedCommit: true, AppliedAdvance: true,
	}}
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "pre-aggregated derived observations")
}

func TestAtomicFinalizeOnlyOnPass(t *testing.T) {
	env := validEnvelope()
	env.Verification = Verification{Passed: false}
	dir := t.TempDir()
	_, _, err := AtomicFinalize(env, dir)
	if err == nil {
		t.Fatal("expected finalize to fail when verification did not pass")
	}
}

func TestAtomicFinalizeWritesArtifactAndDigest(t *testing.T) {
	env := validEnvelope()
	env.Verification = Verification{Passed: true}
	dir := t.TempDir()
	art, dig, err := AtomicFinalize(env, dir)
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if _, err := os.Stat(art); err != nil {
		t.Fatalf("artifact not written: %v", err)
	}
	if _, err := os.Stat(dig); err != nil {
		t.Fatalf("digest not written: %v", err)
	}
	if filepath.Ext(art) != ".json" {
		t.Fatalf("expected .json artifact, got %s", art)
	}
	if filepath.Ext(dig) != ".sha256" {
		t.Fatalf("expected .sha256 digest, got %s", dig)
	}
}

func TestMarshalCanonicalStable(t *testing.T) {
	env := validEnvelope()
	a, err := MarshalCanonical(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	b, err := MarshalCanonical(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(a) != string(b) {
		t.Fatal("canonical marshal is not stable")
	}
}

func TestDefaultManifestCoverage(t *testing.T) {
	m := DefaultManifest()
	if len(m.A0Scenarios) != 5 {
		t.Fatalf("expected 5 A0 scenarios, got %d", len(m.A0Scenarios))
	}
	if len(m.A3Rows) != 15 {
		t.Fatalf("expected 15 A3 rows, got %d", len(m.A3Rows))
	}
}
