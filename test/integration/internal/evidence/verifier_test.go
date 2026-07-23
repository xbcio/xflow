package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
			EventID: "commit-" + string(scenario) + "-" + string(execID), Type: engine.RuntimeEvidenceCommit,
			ExecutionID: execID, NodeName: "node", CommitOutcome: engine.CommitOutcomeAccepted,
		},
		engine.RuntimeEvidenceEvent{
			EventID: "advance-" + string(scenario) + "-" + string(execID), Type: engine.RuntimeEvidenceAdvance,
			ExecutionID: execID, NodeName: "node", Applied: true,
		},
	)
}

func markA3Row(env *Envelope, fixture A3Fixture, topology A3Topology, execID types.ExecutionID) {
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
		EventID:       "commit-" + string(fixture) + "-" + string(topology),
		Type:          engine.RuntimeEvidenceCommit,
		ExecutionID:   execID,
		NodeName:      "node",
		CommitOutcome: engine.CommitOutcomeAccepted,
		Applied:       true,
	}

	switch fixture {
	case A3TransientThenSuccess:
		// Non-fatal success after a retry; classification empty.
		commitEvent.ErrorSource = engine.ErrorSourceUnclassified
		commitEvent.Classified = false
		commitEvent.Attempt = 2
	case A3TransientRetryExhausted:
		commitEvent.ErrorSource = engine.ErrorSourceSystem
		commitEvent.Classified = true
		commitEvent.ErrorKind = types.ErrorKindTransient
		commitEvent.Attempt = 2
		r := true
		commitEvent.Retryable = &r
	case A3PermanentNoRetry:
		commitEvent.ErrorSource = engine.ErrorSourceSystem
		commitEvent.Classified = true
		commitEvent.ErrorKind = types.ErrorKindPermanent
		commitEvent.Attempt = 1
		p := true
		f := false
		commitEvent.Permanent = &p
		commitEvent.Retryable = &f
	case A3BusinessErrorNoRetry:
		commitEvent.ErrorSource = engine.ErrorSourceBusiness
		commitEvent.Classified = false
		commitEvent.ErrorKind = types.ErrorKindBusiness
		commitEvent.Attempt = 1
	case A3ErrorPortRetryExhausted:
		// Terminal unclassified failure after exhausting error-port retries.
		commitEvent.ErrorSource = engine.ErrorSourceUnclassified
		commitEvent.Classified = false
		commitEvent.Attempt = 3
	}

	env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, commitEvent)

	// Only non-fatal success produces an applied advance task.
	if fixture == A3TransientThenSuccess {
		env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, engine.RuntimeEvidenceEvent{
			EventID: "advance-" + string(fixture) + "-" + string(topology),
			Type:    engine.RuntimeEvidenceAdvance, ExecutionID: execID, NodeName: "node", Applied: true,
		})
	}

	// Add retry receipts for fixtures that retry before the terminal commit.
	switch fixture {
	case A3TransientThenSuccess:
		env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, engine.RuntimeEvidenceEvent{
			EventID: "retry-" + string(fixture) + "-" + string(topology),
			Type:    engine.RuntimeEvidenceRetry, ExecutionID: execID, NodeName: "node", Attempt: 1,
		})
	case A3TransientRetryExhausted:
		env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, engine.RuntimeEvidenceEvent{
			EventID: "retry-" + string(fixture) + "-" + string(topology),
			Type:    engine.RuntimeEvidenceRetry, ExecutionID: execID, NodeName: "node", Attempt: 1,
		})
	case A3ErrorPortRetryExhausted:
		env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents,
			engine.RuntimeEvidenceEvent{
				EventID: "retry1-" + string(fixture) + "-" + string(topology),
				Type:    engine.RuntimeEvidenceRetry, ExecutionID: execID, NodeName: "node", Attempt: 1,
			},
			engine.RuntimeEvidenceEvent{
				EventID: "retry2-" + string(fixture) + "-" + string(topology),
				Type:    engine.RuntimeEvidenceRetry, ExecutionID: execID, NodeName: "node", Attempt: 2,
			},
		)
	}
}

func markAllRequired(env *Envelope) {
	for i, s := range A0RequiredScenarios() {
		markA0Scenario(env, s, types.ExecutionID("exec-a0-"+strconv.Itoa(i)))
	}
	for _, row := range A3RequiredRows() {
		markA3Row(env, row.Fixture, row.Topology, types.ExecutionID("exec-a3-"+string(row.Fixture)+"-"+string(row.Topology)))
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
	// Remove CommitThenFlushBeforeDelivery events (exec-a0-0).
	var filtered []engine.RuntimeEvidenceEvent
	for _, ev := range env.Raw.RuntimeEvents {
		if ev.ExecutionID != types.ExecutionID("exec-a0-0") {
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
	// Add a second distinct execution claiming the same scenario. Each has a
	// single accepted commit, so the rejection must come from duplicate-marker
	// detection, not from commit-uniqueness.
	markA0Scenario(env, A0CommitThenFlushBeforeDelivery, types.ExecutionID("exec-a0-dup"))
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "duplicate A0 scenario")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "duplicate scenario row CommitThenFlushBeforeDelivery") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected duplicate scenario error, got %v", res.Errors)
	}
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
	// Add a second distinct execution claiming the same fixture/topology. Each
	// has a single accepted commit, so the rejection must come from
	// duplicate-marker detection, not from commit-uniqueness.
	markA3Row(env, A3TransientThenSuccess, A3Local, types.ExecutionID("exec-a3-dup"))
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "duplicate A3 row")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "duplicate matrix row (transient_then_success, local)") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected duplicate matrix row error, got %v", res.Errors)
	}
}

func TestVerifyRejectsMissingRetryEventReference(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// Remove the retry receipt for one transient_then_success row.
	var filtered []engine.RuntimeEvidenceEvent
	for _, ev := range env.Raw.RuntimeEvents {
		if ev.EventID == "retry-"+string(A3TransientThenSuccess)+"-"+string(A3Local) {
			continue
		}
		filtered = append(filtered, ev)
	}
	env.Raw.RuntimeEvents = filtered
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "missing retry event reference")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "retried fixture missing retry event reference") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected missing retry event error, got %v", res.Errors)
	}
}

func TestVerifyRejectsMissingAppliedAdvance(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// Remove the applied advance for the transient_then_success local row.
	var filtered []engine.RuntimeEvidenceEvent
	for _, ev := range env.Raw.RuntimeEvents {
		if ev.EventID == "advance-"+string(A3TransientThenSuccess)+"-"+string(A3Local) {
			continue
		}
		filtered = append(filtered, ev)
	}
	env.Raw.RuntimeEvents = filtered
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "missing applied advance")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "success fixture missing applied advance") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected missing applied advance error, got %v", res.Errors)
	}
}

func TestVerifyRejectsAppliedAdvanceOnTerminalFailure(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// Inject an applied advance for a terminal-failure fixture that must not
	// produce one (permanent_no_retry local).
	env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, engine.RuntimeEvidenceEvent{
		EventID:     "advance-bad-" + string(A3PermanentNoRetry) + "-" + string(A3Local),
		Type:        engine.RuntimeEvidenceAdvance,
		ExecutionID: types.ExecutionID("exec-a3-" + string(A3PermanentNoRetry) + "-" + string(A3Local)),
		NodeName:    "node",
		Applied:     true,
	})
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "applied advance on terminal failure")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "terminal failure fixture has applied advance") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected terminal failure applied-advance error, got %v", res.Errors)
	}
}

func TestVerifyRejectsBusinessErrorMissingClassification(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// Replace the business_error_no_retry local commit with an unclassified one.
	for i := range env.Raw.RuntimeEvents {
		ev := &env.Raw.RuntimeEvents[i]
		if ev.EventID == "commit-"+string(A3BusinessErrorNoRetry)+"-"+string(A3Local) {
			ev.ErrorSource = engine.ErrorSourceUnclassified
			ev.Classified = false
			ev.ErrorKind = ""
		}
	}
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "business error missing classification")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "terminal classified fixture (business_error_no_retry, local) missing classification") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected business classification error, got %v", res.Errors)
	}
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
