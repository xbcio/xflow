package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
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

func (f fakeProvenance) CommitSHA() (string, error) { return f.commitSHA, nil }
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

func TestRelevantSourcePathsCoverEntireWorktreeFromPackageDirectory(t *testing.T) {
	paths := RelevantSourcePaths()
	if len(paths) != 1 || paths[0] != ":(top)" {
		t.Fatalf("RelevantSourcePaths() = %q, want one root-anchored full-worktree pathspec", paths)
	}

	repo := t.TempDir()
	runGit := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}

	_ = runGit("init", "-q")
	for _, dir := range []string{"backend", "test/integration/testdata/evidence", "web"} {
		if err := os.MkdirAll(filepath.Join(repo, dir), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	ignorePath := filepath.Join(repo, ".gitignore")
	if err := os.WriteFile(ignorePath, []byte("*.test\ntest/integration/testdata/evidence/\n"), 0o644); err != nil {
		t.Fatalf("write ignore fixture: %v", err)
	}
	trackedPath := filepath.Join(repo, "backend", "candidate.go")
	if err := os.WriteFile(trackedPath, []byte("package backend\n"), 0o644); err != nil {
		t.Fatalf("write tracked fixture: %v", err)
	}
	_ = runGit("add", ".gitignore", "backend/candidate.go")
	_ = runGit("-c", "user.name=xflow-test", "-c", "user.email=xflow-test@example.invalid", "commit", "-q", "-m", "initial")

	t.Chdir(filepath.Join(repo, "test", "integration"))

	prov := RealProvenance{}
	clean, details, err := prov.RelevantTreeClean(paths)
	if err != nil {
		t.Fatalf("check initial worktree: %v", err)
	}
	if !clean || details != "" {
		t.Fatalf("initial worktree clean=%v details=%q, want clean", clean, details)
	}
	ignoredArtifact := filepath.Join(repo, "test", "integration", "testdata", "evidence", "fragment.json")
	if err := os.WriteFile(ignoredArtifact, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write ignored artifact fixture: %v", err)
	}
	clean, details, err = prov.RelevantTreeClean(paths)
	if err != nil {
		t.Fatalf("check generated ignored artifact: %v", err)
	}
	if !clean || details != "" {
		t.Fatalf("ignored evidence artifact clean=%v details=%q, want clean", clean, details)
	}

	untrackedPath := filepath.Join(repo, "web", "candidate.ts")
	if err := os.WriteFile(untrackedPath, []byte("export {};\n"), 0o644); err != nil {
		t.Fatalf("write untracked fixture: %v", err)
	}
	clean, details, err = prov.RelevantTreeClean(paths)
	if err != nil {
		t.Fatalf("check untracked root-level change: %v", err)
	}
	if clean || !strings.Contains(details, "web/") {
		t.Fatalf("untracked out-of-package change clean=%v details=%q, want detected", clean, details)
	}
	if err := os.Remove(untrackedPath); err != nil {
		t.Fatalf("remove untracked fixture: %v", err)
	}

	if err := os.WriteFile(trackedPath, []byte("package backend\n// changed\n"), 0o644); err != nil {
		t.Fatalf("modify tracked fixture: %v", err)
	}
	clean, details, err = prov.RelevantTreeClean(paths)
	if err != nil {
		t.Fatalf("check tracked root-level change: %v", err)
	}
	if clean || !strings.Contains(details, "backend/candidate.go") {
		t.Fatalf("tracked out-of-package change clean=%v details=%q, want detected", clean, details)
	}

	gotDigest, err := prov.RelevantDiffDigest(paths)
	if err != nil {
		t.Fatalf("digest full-worktree diff: %v", err)
	}
	wantDiff := runGit("diff", "HEAD", "--", ":(top)")
	wantDigest := sha256.Sum256([]byte(wantDiff))
	if gotDigest != hex.EncodeToString(wantDigest[:]) {
		t.Fatalf("full-worktree diff digest = %s, want %s", gotDigest, hex.EncodeToString(wantDigest[:]))
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
	env.Suite = SuiteSummary{
		ExitCode:             0,
		SkipCount:            0,
		DroppedRuntimeEvents: 0,
		RequiredRows:         20,
		ObservedRows:         20,
	}
	env.Raw.SuiteRecords = []SuiteRecord{{
		RunID: env.RunID, TestName: "TestA0FaultMatrix",
		Package: "github.com/xbcio/xflow/test/integration", Action: "pass",
	}}
	env.Raw.RunIdentities = []RunIdentity{{
		RunID:            env.RunID,
		TestBinaryDigest: sha256String("binary"),
		ManifestDigest:   sha256String("manifest"),
		ProducerID:       "test-producer",
	}}
	env.Raw.EnvironmentObservations = []EnvironmentObservation{
		{RunID: env.RunID, Component: "redis", Query: "INFO server", Result: "7.2"},
		{RunID: env.RunID, Component: "mysql", Query: "SELECT VERSION()", Result: "8.0"},
	}
	return env
}

// wrapEvent builds a CollectedRuntimeEvidenceEvent with valid Meta for tests.
func wrapEvent(env *Envelope, ev engine.RuntimeEvidenceEvent) CollectedRuntimeEvidenceEvent {
	return CollectedRuntimeEvidenceEvent{
		Meta: EvidenceRecordMeta{
			RunID:       env.RunID,
			ProducerID:  "test-producer",
			ExecutionID: ev.ExecutionID,
			ObservedAt:  time.Now().UTC(),
		},
		Event: ev,
	}
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
	// OSKillSIGKILL phase B is direct-drive: business handler_invocations is
	// legitimately 0, so no counter snapshot is recorded. The scenario is
	// required instead to carry a system_task_delivery protocol observation
	// (spec §3.5). Every other scenario records a counter snapshot so
	// checkA0 can enforce handler_invocations > 0 with a counter reference.
	if scenario == A0OSKillSIGKILL {
		env.Raw.ProtocolObservations = append(env.Raw.ProtocolObservations, ProtocolObservation{
			RunID: env.RunID, Topology: "cluster-durable", ExecutionID: execID,
			Type: "system_task_delivery", ObservedAt: time.Now().UTC(),
			Detail: map[string]any{
				"system_task_deliveries":       1,
				"deliveries":                   1,
				"business_handler_invocations": 0,
			},
		})
	} else {
		env.Raw.CounterSnapshots = append(env.Raw.CounterSnapshots, CounterSnapshot{
			RunID: env.RunID, Topology: string(scenario), ExecutionID: execID,
			NodeName: "node", CounterID: "counter-a0-" + string(scenario),
			HandlerName: A0ScenarioHandlerType(scenario), Value: 1, ObservedAt: time.Now().UTC(),
		})
	}
	env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents,
		wrapEvent(env, engine.RuntimeEvidenceEvent{
			EventID: "commit-" + string(scenario) + "-" + string(execID), Type: engine.RuntimeEvidenceCommit,
			ExecutionID: execID, NodeName: "node", CommitOutcome: engine.CommitOutcomeAccepted,
		}),
		wrapEvent(env, engine.RuntimeEvidenceEvent{
			EventID: "advance-" + string(scenario) + "-" + string(execID), Type: engine.RuntimeEvidenceAdvance,
			ExecutionID: execID, NodeName: "node", Applied: true,
		}),
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

	env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, wrapEvent(env, commitEvent))

	// Only non-fatal success produces an applied advance task.
	if fixture == A3TransientThenSuccess {
		env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, wrapEvent(env, engine.RuntimeEvidenceEvent{
			EventID: "advance-" + string(fixture) + "-" + string(topology),
			Type:    engine.RuntimeEvidenceAdvance, ExecutionID: execID, NodeName: "node", Applied: true,
		}))
	}

	// Add retry receipts for fixtures that retry before the terminal commit.
	switch fixture {
	case A3TransientThenSuccess:
		env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, wrapEvent(env, engine.RuntimeEvidenceEvent{
			EventID: "retry-" + string(fixture) + "-" + string(topology),
			Type:    engine.RuntimeEvidenceRetry, ExecutionID: execID, NodeName: "node", Attempt: 1,
		}))
	case A3TransientRetryExhausted:
		env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, wrapEvent(env, engine.RuntimeEvidenceEvent{
			EventID: "retry-" + string(fixture) + "-" + string(topology),
			Type:    engine.RuntimeEvidenceRetry, ExecutionID: execID, NodeName: "node", Attempt: 1,
		}))
	case A3ErrorPortRetryExhausted:
		env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents,
			wrapEvent(env, engine.RuntimeEvidenceEvent{
				EventID: "retry1-" + string(fixture) + "-" + string(topology),
				Type:    engine.RuntimeEvidenceRetry, ExecutionID: execID, NodeName: "node", Attempt: 1,
			}),
			wrapEvent(env, engine.RuntimeEvidenceEvent{
				EventID: "retry2-" + string(fixture) + "-" + string(topology),
				Type:    engine.RuntimeEvidenceRetry, ExecutionID: execID, NodeName: "node", Attempt: 2,
			}),
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
	// Simulate the false-positive recorder shape: the suite summary fields and
	// suite_records are left empty. recomputeSuite + derived must repopulate
	// RequiredRows=20, ObservedRows=20, and a non-empty suite_records so the
	// gate's suite-rows checks pass for a valid raw ledger.
	env.Suite.RequiredRows = 0
	env.Suite.ObservedRows = 0
	env.Raw.SuiteRecords = nil
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requirePassed(t, res)
	if env.Suite.RequiredRows != 20 {
		t.Fatalf("expected recomputed RequiredRows=20, got %d", env.Suite.RequiredRows)
	}
	if env.Suite.ObservedRows != 20 {
		t.Fatalf("expected recomputed ObservedRows=20, got %d", env.Suite.ObservedRows)
	}
	if len(env.Raw.SuiteRecords) == 0 {
		t.Fatal("expected recomputed suite_records to be non-empty")
	}
	if env.Environment.RedisVersion != "7.2" || env.Environment.MySQLVersion != "8.0" {
		t.Fatalf("expected recomputed environment redis=7.2 mysql=8.0, got redis=%q mysql=%q", env.Environment.RedisVersion, env.Environment.MySQLVersion)
	}
}

// TestVerifyRejectsFalsePositiveEnvelope constructs the minimal envelope that
// passes ALL the OLD verifier checks but carries the exact false-positive shape
// from the 2026-07-23 review (empty environment, required_rows==0,
// observed_rows==0, null suite_records, all five A0 handler_invocations==0, and
// a dirty relevant tree that honestly reports false). The new verifier MUST fail
// each surviving false-positive dimension with an error naming it.
//
// After the R8b recompute fix, the suite-rows dimensions (RequiredRows,
// ObservedRows, SuiteRecords) are RECOMPUTED from the manifest/events/derived, so
// a recorder that left them at 0 is healed and no longer a false-positive
// surface. The remaining dimensions — dirty tree, missing run_identity, missing
// environment observations (so the Environment block cannot be derived), and A0
// handler_invocations==0 — still fail with field-specific errors. The suite-rows
// assertions are therefore dropped; the positive-path test
// (TestVerifyPassesForValidEnvelope) guards that recompute repopulates them.
func TestVerifyRejectsFalsePositiveEnvelope(t *testing.T) {
	prov := defaultFakeProvenance()
	// Dirty relevant tree, honestly reported as false by the envelope. The OLD
	// mismatch-only check saw false==false and passed; the new check requires
	// clean==true. The diff digest is set to match the recomputed (dirty) value
	// so the OLD diff-digest check still passes — isolating the failure to the
	// new enforcements.
	prov.relevantTreeClean = false
	prov.relevantDiffSHA256 = sha256String("dirty")

	env := validEnvelope()
	markAllRequired(env)

	// Revert every field the new enforcements cover to the false-positive state.
	env.Source.RelevantTreeClean = false // honest: tree is dirty
	env.Source.RelevantDiffSHA256 = sha256String("dirty")
	// Corrupt the RAW ledger (not the recomputed summary fields) so recompute
	// cannot heal the false-positive. Deleting EnvironmentObservations and
	// RunIdentities leaves the Environment block un-derivable and the run
	// un-identified; RequiredRows/ObservedRows/SuiteRecords are left to
	// recompute (which repopulates them) — those dimensions are no longer
	// false-positive surfaces after R8b.
	env.Raw.RunIdentities = nil
	env.Raw.EnvironmentObservations = nil

	// Strip A0 counter snapshots and the OSKill system_task_delivery
	// observation so all five A0 scenarios carry handler_invocations==0 (the
	// OLD verifier never inspected A0 counters, so it still passes).
	var keptCounters []CounterSnapshot
	for _, cs := range env.Raw.CounterSnapshots {
		if strings.HasPrefix(cs.CounterID, "counter-a0-") {
			continue
		}
		keptCounters = append(keptCounters, cs)
	}
	env.Raw.CounterSnapshots = keptCounters
	var keptPO []ProtocolObservation
	for _, po := range env.Raw.ProtocolObservations {
		if po.Type == "system_task_delivery" {
			continue
		}
		keptPO = append(keptPO, po)
	}
	env.Raw.ProtocolObservations = keptPO

	v := NewVerifier(prov)
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "false-positive envelope")

	requireErrorContains := func(needle string) {
		t.Helper()
		for _, e := range res.Errors {
			if strings.Contains(e, needle) {
				return
			}
		}
		t.Fatalf("expected an error containing %q, got: %v", needle, res.Errors)
	}

	// dirty relevant tree (honest false) must now fail.
	requireErrorContains("relevant_tree_clean must be true")
	// empty environment + no typed observations (recompute cannot derive the
	// block from absent observations).
	requireErrorContains("environment_observation record required")
	requireErrorContains("redis_version must be non-empty")
	requireErrorContains("mysql_version must be non-empty")
	// run_identity missing.
	requireErrorContains("run_identity: at least one run_identity record required")
	// A0 handler_invocations: the four non-OSKill scenarios must each fail
	// handler_invocations > 0, and OSKillSIGKILL must fail system_task_delivery.
	requireErrorContains("a0: scenario CommitThenFlushBeforeDelivery handler_invocations must be > 0")
	requireErrorContains("a0: scenario ReportAckLoss handler_invocations must be > 0")
	requireErrorContains("a0: scenario ReportRequestLoss handler_invocations must be > 0")
	requireErrorContains("a0: scenario QueueHandoff handler_invocations must be > 0")
	requireErrorContains("a0: scenario OSKillSIGKILL requires system_task_delivery >= 1")
}

// TestVerifyRejectsInvalidArtifacts is the adversarial negative-case table from
// spec §11.5. Each row starts from the shared minimal-valid envelope, mutates a
// single field, and asserts that verification fails with an error naming the
// specific violation.
func TestVerifyRejectsInvalidArtifacts(t *testing.T) {
	setRunID := func(env *Envelope, runID string) {
		env.RunID = runID
		for i := range env.Raw.SuiteRecords {
			env.Raw.SuiteRecords[i].RunID = runID
		}
		for i := range env.Raw.RunIdentities {
			env.Raw.RunIdentities[i].RunID = runID
		}
		for i := range env.Raw.EnvironmentObservations {
			env.Raw.EnvironmentObservations[i].RunID = runID
		}
		for i := range env.Raw.CounterSnapshots {
			env.Raw.CounterSnapshots[i].RunID = runID
		}
		for i := range env.Raw.ProtocolObservations {
			env.Raw.ProtocolObservations[i].RunID = runID
		}
		for i := range env.Raw.StateSnapshots {
			env.Raw.StateSnapshots[i].RunID = runID
		}
		for i := range env.Raw.RuntimeEvents {
			env.Raw.RuntimeEvents[i].Meta.RunID = runID
		}
	}

	cases := []struct {
		name          string
		mutate        func(*Envelope)
		wantErrSubstr string
	}{
		{
			// Corrupting the RAW environment observations (not the recomputed
			// Environment block) so recomputeEnvironment cannot derive the
			// block; checkEnvironmentIntegrity then fails on redis/mysql.
			name: "environment observations removed",
			mutate: func(env *Envelope) {
				env.Raw.EnvironmentObservations = nil
			},
			wantErrSubstr: "redis_version must be non-empty",
		},
		// The "required_rows zero", "observed_rows zero", and
		// "suite_records empty" cases are intentionally absent after R8b:
		// recomputeSuite/recomputeDerived repopulate these from the manifest
		// and events, so mutating the recomputed summary fields no longer
		// produces a verifiable failure. The positive-path test
		// (TestVerifyPassesForValidEnvelope) guards that they are repopulated
		// to 20/20/non-empty for a valid ledger.
		{
			name: "a0 scenario missing counter snapshot and handler_invocations zero",
			mutate: func(env *Envelope) {
				var kept []CounterSnapshot
				for _, cs := range env.Raw.CounterSnapshots {
					if cs.CounterID == "counter-a0-"+string(A0CommitThenFlushBeforeDelivery) {
						continue
					}
					kept = append(kept, cs)
				}
				env.Raw.CounterSnapshots = kept
			},
			wantErrSubstr: "handler_invocations must be > 0",
		},
		{
			// A counter with the right CounterID+Value but a HandlerName that is
			// not the scenario's production delegate must be rejected: this is
			// the checkA0 HandlerName introspection that closes the pre-existing
			// trust boundary where any counter satisfied handler_invocations>0.
			name: "a0 counter handler_name not the production delegate",
			mutate: func(env *Envelope) {
				for i, cs := range env.Raw.CounterSnapshots {
					if cs.CounterID == "counter-a0-"+string(A0CommitThenFlushBeforeDelivery) {
						env.Raw.CounterSnapshots[i].HandlerName = "not-the-real-handler"
					}
				}
			},
			wantErrSubstr: "counter handler_name",
		},
		{
			name: "dirty relevant tree with envelope honestly false",
			mutate: func(env *Envelope) {
				env.Source.RelevantTreeClean = false
				env.Source.RelevantDiffSHA256 = sha256String("dirty")
			},
			wantErrSubstr: "relevant_tree_clean must be true",
		},
		{
			name: "run_id is a 40-hex commit SHA",
			mutate: func(env *Envelope) {
				setRunID(env, "abcdef1234567890abcdef1234567890abcdef12")
			},
			wantErrSubstr: "looks like a commit SHA",
		},
		{
			name: "bare runtime evidence event without meta wrapper",
			mutate: func(env *Envelope) {
				env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, CollectedRuntimeEvidenceEvent{
					Event: engine.RuntimeEvidenceEvent{
						EventID:       "bare-event-no-meta",
						Type:          engine.RuntimeEvidenceCommit,
						ExecutionID:   types.ExecutionID("exec-bare"),
						NodeName:      "node",
						CommitOutcome: engine.CommitOutcomeAccepted,
					},
				})
			},
			wantErrSubstr: "meta.execution_id",
		},
		{
			name: "duplicate event ID across producers",
			mutate: func(env *Envelope) {
				if len(env.Raw.RuntimeEvents) == 0 {
					t.Skip("no runtime events to duplicate")
				}
				dup := env.Raw.RuntimeEvents[0]
				dup.Meta.ProducerID = "other-producer"
				env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, dup)
			},
			wantErrSubstr: "duplicate runtime event_id",
		},
		{
			name: "pre-aggregated derived observations",
			mutate: func(env *Envelope) {
				env.DerivedObservations = []DerivedObservation{{
					Kind: "a0_scenario", Scenario: string(A0OSKillSIGKILL),
					EvidenceSource: "fixture", AcceptedCommit: true,
				}}
			},
			wantErrSubstr: "derived_observations must be empty on input",
		},
		{
			name: "environment observation cross-run reference",
			mutate: func(env *Envelope) {
				if len(env.Raw.EnvironmentObservations) > 0 {
					env.Raw.EnvironmentObservations[0].RunID = "other-run"
				}
			},
			wantErrSubstr: "environment observation 0 cross-run reference",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnvelope()
			markAllRequired(env)
			tc.mutate(env)

			prov := defaultFakeProvenance()
			if tc.name == "dirty relevant tree with envelope honestly false" {
				prov.relevantTreeClean = false
				prov.relevantDiffSHA256 = sha256String("dirty")
			}

			res := NewVerifier(prov).Verify(env, passEvents())
			if res.Passed {
				t.Fatalf("expected verification to fail for %q, but it passed", tc.name)
			}
			for _, e := range res.Errors {
				if strings.Contains(e, tc.wantErrSubstr) {
					return
				}
			}
			t.Fatalf("expected an error containing %q for %q, got: %v", tc.wantErrSubstr, tc.name, res.Errors)
		})
	}
}

func TestVerifyRejectsMissingA0Scenario(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// Remove CommitThenFlushBeforeDelivery events (exec-a0-0).
	var filtered []CollectedRuntimeEvidenceEvent
	for _, ev := range env.Raw.RuntimeEvents {
		if ev.Event.ExecutionID != types.ExecutionID("exec-a0-0") {
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
	var filtered []CollectedRuntimeEvidenceEvent
	for _, ev := range env.Raw.RuntimeEvents {
		if ev.Event.ExecutionID != types.ExecutionID("exec-a3-"+string(A3TransientThenSuccess)+"-"+string(A3Local)) {
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
	var filtered []CollectedRuntimeEvidenceEvent
	for _, ev := range env.Raw.RuntimeEvents {
		if ev.Event.EventID == "retry-"+string(A3TransientThenSuccess)+"-"+string(A3Local) {
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
	var filtered []CollectedRuntimeEvidenceEvent
	for _, ev := range env.Raw.RuntimeEvents {
		if ev.Event.EventID == "advance-"+string(A3TransientThenSuccess)+"-"+string(A3Local) {
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
	env.Raw.RuntimeEvents = append(env.Raw.RuntimeEvents, wrapEvent(env, engine.RuntimeEvidenceEvent{
		EventID:     "advance-bad-" + string(A3PermanentNoRetry) + "-" + string(A3Local),
		Type:        engine.RuntimeEvidenceAdvance,
		ExecutionID: types.ExecutionID("exec-a3-" + string(A3PermanentNoRetry) + "-" + string(A3Local)),
		NodeName:    "node",
		Applied:     true,
	}))
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
		ev := &env.Raw.RuntimeEvents[i].Event
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
		env.Raw.RuntimeEvents[1].Event.EventID = env.Raw.RuntimeEvents[0].Event.EventID
	}
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "duplicate event ID")
}

func TestVerifyRejectsMismatchedMetaExecutionID(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	if len(env.Raw.RuntimeEvents) > 0 {
		env.Raw.RuntimeEvents[0].Meta.ExecutionID = types.ExecutionID("mismatched")
	}
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "mismatched meta execution_id")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "meta.execution_id") && strings.Contains(e, "event.execution_id") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected meta/event execution_id mismatch error, got %v", res.Errors)
	}
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

// The six tests below close gaps where checkA0/checkA3 branches were
// evaluated by every other test (they are on the hot path of Verify) but
// never actually driven to fire: no fixture in this file ever attached a
// lease_reclaim/synthetic_os_kill marker, gave the request-loss first attempt
// an accepted outcome, stripped the authority_rejected marker, misaligned a
// Database real-pair row, or classified a non-terminal fixture. Before this
// change, deleting any one of those checkA0/checkA3 branches entirely left
// every existing test green.

func TestVerifyRejectsAckLossLeaseReclaim(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// A0RequiredScenarios()[1] is ReportAckLoss; markAllRequired names its
	// execution "exec-a0-1".
	ackExecID := types.ExecutionID("exec-a0-1")
	env.Raw.ProtocolObservations = append(env.Raw.ProtocolObservations, ProtocolObservation{
		RunID: env.RunID, Topology: string(A0ReportAckLoss), ExecutionID: ackExecID,
		Type: "lease_reclaim", ObservedAt: time.Now().UTC(),
	})
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "ack-loss scenario carries a lease_reclaim observation")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "ACK-loss scenario contains lease reclaim") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected ACK-loss lease reclaim error, got %v", res.Errors)
	}
}

func TestVerifyRejectsSyntheticOSKillObservation(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// A0RequiredScenarios()[4] is OSKillSIGKILL; markAllRequired names its
	// execution "exec-a0-4".
	env.Raw.ProtocolObservations = append(env.Raw.ProtocolObservations, ProtocolObservation{
		RunID: env.RunID, Topology: string(A0OSKillSIGKILL), ExecutionID: types.ExecutionID("exec-a0-4"),
		Type: "synthetic_os_kill", ObservedAt: time.Now().UTC(),
	})
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "envelope carries a synthetic_os_kill observation")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "synthetic os-kill observation is not allowed") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected synthetic os-kill rejection error, got %v", res.Errors)
	}
}

func TestVerifyRejectsMissingAuthorityRejectedObservation(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// markA0Scenario attaches "authority_rejected" only for ReportRequestLoss;
	// strip it so the request-loss row is missing its required marker.
	var kept []ProtocolObservation
	for _, po := range env.Raw.ProtocolObservations {
		if po.Type == "authority_rejected" {
			continue
		}
		kept = append(kept, po)
	}
	env.Raw.ProtocolObservations = kept
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "request-loss missing authority_rejected marker")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "request-loss missing authority rejected observation") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected missing authority-rejected error, got %v", res.Errors)
	}
}

func TestVerifyRejectsRequestLossFirstReportAccepted(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// A0RequiredScenarios()[2] is ReportRequestLoss; markAllRequired names its
	// execution "exec-a0-2". Its commit event's CommitOutcome is already
	// Accepted (set unconditionally by markA0Scenario); only Attempt needs to
	// become 1 to simulate a first-report accepted commit, which the request-
	// loss contract forbids (the first report must be rejected/lost).
	reqExecID := types.ExecutionID("exec-a0-2")
	for i := range env.Raw.RuntimeEvents {
		ev := &env.Raw.RuntimeEvents[i].Event
		if ev.ExecutionID == reqExecID && ev.Type == engine.RuntimeEvidenceCommit {
			ev.Attempt = 1
		}
	}
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "request-loss first report accepted")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "request-loss first report produced accepted commit") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected request-loss first-report error, got %v", res.Errors)
	}
}

func TestVerifyRejectsDatabaseRealPairMismatch(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// transient_then_success is a Database real-pair fixture (isDatabaseFixture
	// treats all five A3 fixtures as such). Flip only the server-runner row's
	// advance to unapplied while leaving its commit intact, so AcceptedCommit
	// still matches across the pair but AppliedAdvance disagrees between
	// server-runner and cluster-durable.
	srAdvanceID := "advance-" + string(A3TransientThenSuccess) + "-" + string(A3ServerRunner)
	for i := range env.Raw.RuntimeEvents {
		ev := &env.Raw.RuntimeEvents[i].Event
		if ev.EventID == srAdvanceID {
			ev.Applied = false
		}
	}
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "database real-pair rows disagree")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "Database real-pair mismatch") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected Database real-pair mismatch error, got %v", res.Errors)
	}
}

func TestVerifyRejectsSuccessFixtureWithClassification(t *testing.T) {
	env := validEnvelope()
	markAllRequired(env)
	// transient_then_success is not in checkA3's classifiedTerminalFixtures
	// set, so its derived observation must never carry a classification.
	// Setting ErrorSource to a recognized category on its commit event makes
	// EffectiveClassificationFromEvent return non-nil even though Classified
	// stays false, which must be rejected.
	commitID := "commit-" + string(A3TransientThenSuccess) + "-" + string(A3Local)
	for i := range env.Raw.RuntimeEvents {
		ev := &env.Raw.RuntimeEvents[i].Event
		if ev.EventID == commitID {
			ev.ErrorSource = engine.ErrorSourceBusiness
		}
	}
	v := NewVerifier(defaultFakeProvenance())
	res := v.Verify(env, passEvents())
	requireNotPassed(t, res, "success fixture unexpectedly classified")
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "must not have classification") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected unexpected-classification error, got %v", res.Errors)
	}
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

	// Independently recompute SHA-256 over the artifact bytes actually
	// persisted to disk, using the standard library directly rather than any
	// production helper. Without this, AtomicFinalize's digest algorithm
	// could be swapped out entirely (e.g. for a truncated SHA-512) and the
	// test would stay green, while every consumer that verifies the
	// sidecar .sha256 file against a real SHA-256 of the artifact (the whole
	// point of the chain-of-custody digest) would start rejecting every run.
	artifactBytes, err := os.ReadFile(art)
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	wantSum := sha256.Sum256(artifactBytes)
	wantDigest := hex.EncodeToString(wantSum[:]) + "\n"
	gotDigest, err := os.ReadFile(dig)
	if err != nil {
		t.Fatalf("read digest: %v", err)
	}
	if string(gotDigest) != wantDigest {
		t.Fatalf("digest file = %q, want SHA-256(artifact) = %q", gotDigest, wantDigest)
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

	// MarshalCanonical must disable json.Encoder's default HTML-escaping
	// (SetEscapeHTML(false)) so the characters "<", ">", and "&" are written
	// as literal bytes. If this setting regresses to the encoding/json
	// default (true), every persisted evidence artifact containing these
	// characters gets silently rewritten to its six-character unicode escape
	// on the next marshal, which would change the artifact's bytes -- and
	// therefore its digest -- for a value that never changed. The expected
	// strings below are hard-coded literals, not something computed by
	// calling MarshalCanonical itself.
	probe, err := MarshalCanonical(map[string]string{"probe": "<a>&b"})
	if err != nil {
		t.Fatalf("marshal probe: %v", err)
	}
	if !strings.Contains(string(probe), `"<a>&b"`) {
		t.Fatalf("expected literal unescaped HTML characters in output, got: %s", probe)
	}
	// The probe value "<a>&b" itself contains no backslash. If HTML-escaping
	// were re-enabled, encoding/json would rewrite each of "<", ">", "&" to
	// its six-character unicode escape, which is the only way a backslash
	// could appear here.
	if strings.Contains(string(probe), `\`) {
		t.Fatalf("expected SetEscapeHTML(false), found an HTML-escape backslash in output: %s", probe)
	}
}

// TestDefaultManifestCoverage pins WHICH scenarios and matrix cells the
// manifest requires, not how many there are.
//
// Counting cannot detect the failure this manifest exists to prevent. Replace
// A3ErrorPortRetryExhausted in A3RequiredRows' fixture list with a second copy
// of A3TransientThenSuccess and the length is still 5x3=15, so the old
// assertions hold -- while error-port retry exhaustion drops out of the
// required set entirely and is never again enforced at the merge gate. Nothing
// downstream catches it either: markAllRequired (the "all required evidence is
// present" baseline every other test builds on) and NewVerifier (the checker)
// both call A3RequiredRows, so they rot in lockstep and confirm each other. A3
// execution IDs are derived from fixture+topology, so a duplicated row reuses
// one ID and slips past the duplicate detector as well.
//
// The expected sets below are written out here on purpose: a second copy of
// the list, in a different file, is what makes a one-line edit to the first
// one visible.
func TestDefaultManifestCoverage(t *testing.T) {
	m := DefaultManifest()

	wantA0 := []A0Scenario{
		A0CommitThenFlushBeforeDelivery,
		A0ReportAckLoss,
		A0ReportRequestLoss,
		A0QueueHandoff,
		A0OSKillSIGKILL,
	}
	if len(m.A0Scenarios) != len(wantA0) {
		t.Fatalf("A0 scenarios = %v, want %v", m.A0Scenarios, wantA0)
	}
	for i, want := range wantA0 {
		if m.A0Scenarios[i] != want {
			t.Errorf("A0Scenarios[%d] = %q, want %q (canonical order is part of "+
				"the contract: the verifier requires exactly one derived "+
				"observation per scenario)", i, m.A0Scenarios[i], want)
		}
	}

	wantFixtures := []A3Fixture{
		A3TransientThenSuccess,
		A3TransientRetryExhausted,
		A3PermanentNoRetry,
		A3BusinessErrorNoRetry,
		A3ErrorPortRetryExhausted,
	}
	wantTopologies := []A3Topology{A3Local, A3ServerRunner, A3ClusterDurable}

	type cell struct {
		f A3Fixture
		t A3Topology
	}
	got := make(map[cell]A3MatrixRow, len(m.A3Rows))
	for _, row := range m.A3Rows {
		key := cell{row.Fixture, row.Topology}
		if _, dup := got[key]; dup {
			t.Errorf("A3 row %s/%s appears more than once: a duplicated cell "+
				"means some other cell went missing without changing the count",
				row.Fixture, row.Topology)
		}
		got[key] = row
	}
	if len(got) != len(wantFixtures)*len(wantTopologies) {
		t.Errorf("A3 matrix has %d distinct cells, want %d",
			len(got), len(wantFixtures)*len(wantTopologies))
	}

	for _, f := range wantFixtures {
		for _, topo := range wantTopologies {
			row, ok := got[cell{f, topo}]
			if !ok {
				t.Errorf("A3 matrix is missing %s/%s: that combination is no "+
					"longer required, so no run has to produce evidence for it",
					f, topo)
				continue
			}
			// The two flags decide whether the verifier enforces real-MySQL
			// parity for this cell (spec 8.6). Silently flipping either one
			// relaxes the contract without changing any count.
			wantRealPair := topo == A3ServerRunner || topo == A3ClusterDurable
			if row.DatabaseRealPair != wantRealPair {
				t.Errorf("A3 row %s/%s DatabaseRealPair = %v, want %v",
					f, topo, row.DatabaseRealPair, wantRealPair)
			}
			if wantLocalFake := topo == A3Local; row.IsDatabaseLocalFake != wantLocalFake {
				t.Errorf("A3 row %s/%s IsDatabaseLocalFake = %v, want %v",
					f, topo, row.IsDatabaseLocalFake, wantLocalFake)
			}
		}
	}
}
