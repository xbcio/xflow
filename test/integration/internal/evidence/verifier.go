package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// a3RowKey identifies one cell of the A3 fixture × topology matrix.
type a3RowKey struct{ fixture, topology string }

// ProvenanceProvider abstracts git/binary provenance so unit tests can inject
// controlled values without touching the real repo or filesystem.
type ProvenanceProvider interface {
	CommitSHA() (string, error)
	RelevantTreeClean(paths []string) (bool, string, error)
	RelevantDiffDigest(paths []string) (string, error)
	TestBinaryDigest(path string) (string, error)
	GoVersion() string
}

// RealProvenance uses os/exec git, crypto/sha256, and runtime.Version().
// TestBinaryPath is the path to the test binary whose digest is recomputed.
type RealProvenance struct {
	TestBinaryPath string
}

// CommitSHA returns the full SHA of HEAD.
func (RealProvenance) CommitSHA() (string, error) {
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("git rev-parse HEAD: %w", err)
	}
	sha := strings.TrimSpace(string(out))
	if sha == "" {
		return "", fmt.Errorf("git returned empty SHA")
	}
	return sha, nil
}

// RelevantTreeClean reports whether the supplied paths have uncommitted changes.
func (RealProvenance) RelevantTreeClean(paths []string) (bool, string, error) {
	args := append([]string{"status", "--porcelain"}, paths...)
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return false, "", fmt.Errorf("git status: %w", err)
	}
	clean := len(strings.TrimSpace(string(out))) == 0
	return clean, string(out), nil
}

// RelevantDiffDigest returns the SHA-256 of `git diff HEAD` over the supplied paths.
func (RealProvenance) RelevantDiffDigest(paths []string) (string, error) {
	if len(paths) == 0 {
		return "", nil
	}
	args := append([]string{"diff", "HEAD"}, paths...)
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return "", fmt.Errorf("git diff: %w", err)
	}
	sum := sha256.Sum256(out)
	return hex.EncodeToString(sum[:]), nil
}

// TestBinaryDigest returns the SHA-256 of the configured test binary. The path
// argument is kept for interface compatibility; RealProvenance uses its own
// TestBinaryPath field so the verifier does not accidentally trust a path from
// the envelope.
func (p RealProvenance) TestBinaryDigest(path string) (string, error) {
	bp := p.TestBinaryPath
	if bp == "" {
		bp = path
	}
	if bp == "" {
		return "", fmt.Errorf("no test binary path configured")
	}
	data, err := os.ReadFile(bp)
	if err != nil {
		return "", fmt.Errorf("read test binary: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// GoVersion returns the Go runtime version string.
func (RealProvenance) GoVersion() string { return runtime.Version() }

// RelevantSourcePaths lists the production and test paths that affect A0/A3
// evidence semantics. UI-only or unrelated files are excluded from the relevant
// diff digest.
func RelevantSourcePaths() []string {
	return []string{
		"engine/",
		"service/control/",
		"service/runner/",
		"test/integration/a0_fault_matrix_test.go",
		"test/integration/cyclic_reliability_process_test.go",
		"test/integration/action_parity_test.go",
		"test/integration/action_parity_http_test.go",
		"test/integration/action_parity_grpc_test.go",
		"test/integration/action_parity_script_test.go",
		"test/integration/action_parity_onerror_test.go",
		"test/integration/action_parity_database_test.go",
		"test/integration/action_parity_database_server_test.go",
		"test/integration/internal/evidence/",
	}
}

// GoTestEvent mirrors one line of `go test -json` output.
type GoTestEvent struct {
	Time    time.Time `json:"Time"`
	Action  string    `json:"Action"`
	Package string    `json:"Package"`
	Test    string    `json:"Test"`
	Elapsed float64   `json:"Elapsed"`
	Output  string    `json:"Output"`
}

// Verifier performs the independent verification described in spec §8.
type Verifier struct {
	Provenance ProvenanceProvider
	Manifest   *Manifest
}

// NewVerifier creates a verifier with the given provenance provider and the
// default required manifest.
func NewVerifier(prov ProvenanceProvider) *Verifier {
	if prov == nil {
		prov = RealProvenance{}
	}
	return &Verifier{
		Provenance: prov,
		Manifest:   DefaultManifest(),
	}
}

func (v *Verifier) binaryPath() string {
	if rp, ok := v.Provenance.(RealProvenance); ok {
		return rp.TestBinaryPath
	}
	return ""
}

// Verify independently recomputes source provenance, suite outcome, and all
// derived observations. It returns a Verification value; the envelope is
// mutated in place with recomputed derived_observations and verification.
func (v *Verifier) Verify(env *Envelope, suiteEvents []GoTestEvent) Verification {
	var errors []string

	// 1. Source provenance: recompute and compare; never trust self-reported values.
	commitSHA, err := v.Provenance.CommitSHA()
	if err != nil {
		errors = append(errors, fmt.Sprintf("source: cannot recompute commit SHA: %v", err))
	} else if commitSHA == "" || commitSHA == "unknown" {
		errors = append(errors, "source: commit SHA is empty or unknown")
	} else if env.Source.CommitSHA != commitSHA {
		errors = append(errors, fmt.Sprintf("source: commit SHA mismatch: envelope=%s recomputed=%s", env.Source.CommitSHA, commitSHA))
	}

	relevantPaths := RelevantSourcePaths()
	clean, dirtyDetails, err := v.Provenance.RelevantTreeClean(relevantPaths)
	if err != nil {
		errors = append(errors, fmt.Sprintf("source: cannot check relevant tree cleanliness: %v", err))
	} else if !clean {
		// Spec §8.2: recomputed_clean must be true. A dirty relevant tree fails
		// verification EVEN IF the envelope honestly reports RelevantTreeClean
		// == false. The previous mismatch-only check let an honest-false dirty
		// tree through; this is the false-positive root cause.
		errors = append(errors, "source: relevant_tree_clean must be true (dirty relevant tree)")
		if dirtyDetails != "" {
			errors = append(errors, fmt.Sprintf("source: dirty details: %s", strings.TrimSpace(dirtyDetails)))
		}
	} else if env.Source.RelevantTreeClean != clean {
		errors = append(errors, fmt.Sprintf("source: relevant_tree_clean mismatch: envelope=%v recomputed=%v", env.Source.RelevantTreeClean, clean))
	}

	diffDigest, err := v.Provenance.RelevantDiffDigest(relevantPaths)
	if err != nil {
		errors = append(errors, fmt.Sprintf("source: cannot recompute relevant diff digest: %v", err))
	} else if env.Source.RelevantDiffSHA256 != diffDigest {
		errors = append(errors, fmt.Sprintf("source: relevant_diff_sha256 mismatch: envelope=%s recomputed=%s", env.Source.RelevantDiffSHA256, diffDigest))
	}

	binaryDigest, err := v.Provenance.TestBinaryDigest(v.binaryPath())
	if err != nil {
		errors = append(errors, fmt.Sprintf("source: cannot recompute test binary digest: %v", err))
	} else if env.Source.TestBinarySHA256 != binaryDigest {
		errors = append(errors, fmt.Sprintf("source: test_binary_sha256 mismatch: envelope=%s recomputed=%s", env.Source.TestBinarySHA256, binaryDigest))
	}

	goVer := v.Provenance.GoVersion()
	if env.Source.GoVersion != goVer {
		errors = append(errors, fmt.Sprintf("source: go_version mismatch: envelope=%s recomputed=%s", env.Source.GoVersion, goVer))
	}

	sourceRecomputed := true
	for _, e := range errors {
		if strings.Contains(e, "cannot recompute") {
			sourceRecomputed = false
			break
		}
	}

	// 2. Suite outcome: recompute from go test -json events.
	recomputedSuite := recomputeSuite(suiteEvents)
	if env.Suite.ExitCode != recomputedSuite.ExitCode {
		errors = append(errors, fmt.Sprintf("suite: exit_code mismatch: envelope=%d recomputed=%d", env.Suite.ExitCode, recomputedSuite.ExitCode))
	}
	if env.Suite.SkipCount != recomputedSuite.SkipCount {
		errors = append(errors, fmt.Sprintf("suite: skip_count mismatch: envelope=%d recomputed=%d", env.Suite.SkipCount, recomputedSuite.SkipCount))
	}
	if recomputedSuite.ExitCode != 0 {
		errors = append(errors, fmt.Sprintf("suite: non-zero exit code: %d", recomputedSuite.ExitCode))
	}
	if recomputedSuite.SkipCount != 0 {
		errors = append(errors, fmt.Sprintf("suite: skip count must be zero: %d", recomputedSuite.SkipCount))
	}
	if env.Suite.DroppedRuntimeEvents != 0 {
		errors = append(errors, fmt.Sprintf("suite: dropped_runtime_events must be zero: %d", env.Suite.DroppedRuntimeEvents))
	}

	// Spec §8.5: required_rows and observed_rows must both be non-zero and
	// must be exactly equal. The OLD verifier never checked these, so an
	// artifact with both at 0 (and no observed matrix) passed. The raw
	// suite_records slice must also be non-empty (spec §8.3 step 12 / §8.4).
	if env.Suite.RequiredRows <= 0 {
		errors = append(errors, fmt.Sprintf("suite: required_rows must be > 0 (got %d)", env.Suite.RequiredRows))
	}
	if env.Suite.ObservedRows <= 0 {
		errors = append(errors, fmt.Sprintf("suite: observed_rows must be > 0 (got %d)", env.Suite.ObservedRows))
	}
	if env.Suite.RequiredRows != env.Suite.ObservedRows {
		errors = append(errors, fmt.Sprintf("suite: required_rows != observed_rows: required=%d observed=%d", env.Suite.RequiredRows, env.Suite.ObservedRows))
	}
	if len(env.Raw.SuiteRecords) == 0 {
		errors = append(errors, "suite: suite_records must be non-empty")
	}

	suiteRecomputed := true

	// 3. Raw ledger integrity.
	errors = append(errors, checkRawLedgerIntegrity(env)...)

	// 3b. Reject pre-aggregated or self-reported derived fields in the raw ledger.
	errors = append(errors, checkNoPreaggregatedFields(env)...)

	// 3c. run_identity integrity (spec §8.3 step 3): the raw ledger MUST carry
	// at least one RunIdentity record, and all records MUST share the same
	// RunID, TestBinaryDigest, and ManifestDigest. Missing, duplicate, or
	// inconsistent run_identity fails verification.
	errors = append(errors, checkRunIdentityIntegrity(env)...)

	// 3d. environment integrity (spec §8.1/§8.5): redis_version and mysql_version
	// MUST be non-empty AND sourced from typed EnvironmentObservation records,
	// not hardcoded image tags. The typed records are authoritative; the typed
	// Environment block is cross-checked against them and rejected on disagreement.
	errors = append(errors, checkEnvironmentIntegrity(env)...)

	// 4. Compute derived observations from raw ledger; do not trust input derived_observations.
	derived := v.computeDerivedObservations(env)
	env.DerivedObservations = derived

	// 4b. Detect duplicate scenario/row markers from distinct executions.
	dupA0, dupA3 := findDuplicateMarkers(env.Raw.ProtocolObservations)

	// 5. A0 checks.
	errors = append(errors, v.checkA0(env, derived, dupA0)...)

	// 6. A3 checks.
	errors = append(errors, v.checkA3(env, derived, dupA3)...)

	passed := len(errors) == 0
	env.Verification = Verification{
		Passed:           passed,
		Errors:           errors,
		SourceRecomputed: sourceRecomputed,
		SuiteRecomputed:  suiteRecomputed,
	}
	return env.Verification
}

func recomputeSuite(events []GoTestEvent) SuiteSummary {
	summary := SuiteSummary{}
	if len(events) == 0 {
		summary.ExitCode = 1 // no output means failure
		return summary
	}

	// Collect per-test outcomes and package-level pass/fail.
	testFailed := make(map[string]bool)
	testSkipped := make(map[string]bool)
	var packageFail bool
	for _, ev := range events {
		if ev.Test != "" {
			switch ev.Action {
			case "fail":
				testFailed[ev.Test] = true
			case "skip":
				testSkipped[ev.Test] = true
			}
		} else if ev.Action == "fail" {
			packageFail = true
		}
	}

	if packageFail || len(testFailed) > 0 {
		summary.ExitCode = 1
	}
	summary.SkipCount = len(testSkipped)
	return summary
}

func checkRawLedgerIntegrity(env *Envelope) []string {
	var errs []string
	if env.RunID == "" {
		errs = append(errs, "ledger: empty run_id")
	} else if looksLikeCommitSHA(env.RunID) {
		// Spec §8.3 step 1: run_id must be a random UUIDv4, never a commit SHA.
		errs = append(errs, fmt.Sprintf("ledger: run_id %q looks like a commit SHA; run_id must be a UUIDv4", env.RunID))
	}

	eventIDs := make(map[string]int)
	for i, ev := range env.Raw.RuntimeEvents {
		if ev.Meta.ExecutionID != ev.Event.ExecutionID {
			errs = append(errs, fmt.Sprintf("ledger: runtime event %d meta.execution_id %q != event.execution_id %q", i, ev.Meta.ExecutionID, ev.Event.ExecutionID))
		}
		if ev.Event.EventID == "" {
			errs = append(errs, fmt.Sprintf("ledger: runtime event %d with empty event_id", i))
			continue
		}
		eventIDs[ev.Event.EventID]++
	}
	for id, n := range eventIDs {
		if n > 1 {
			errs = append(errs, fmt.Sprintf("ledger: duplicate runtime event_id %s", id))
		}
	}

	for i, cs := range env.Raw.CounterSnapshots {
		if cs.RunID != "" && cs.RunID != env.RunID {
			errs = append(errs, fmt.Sprintf("ledger: counter snapshot %d cross-run reference", i))
		}
		if cs.CounterID == "" {
			errs = append(errs, fmt.Sprintf("ledger: counter snapshot %d missing counter_id", i))
		}
	}
	for i, po := range env.Raw.ProtocolObservations {
		if po.RunID != "" && po.RunID != env.RunID {
			errs = append(errs, fmt.Sprintf("ledger: protocol observation %d cross-run reference", i))
		}
	}
	for i, ss := range env.Raw.StateSnapshots {
		if ss.RunID != "" && ss.RunID != env.RunID {
			errs = append(errs, fmt.Sprintf("ledger: state snapshot %d cross-run reference", i))
		}
	}
	for i, eo := range env.Raw.EnvironmentObservations {
		if eo.RunID != "" && eo.RunID != env.RunID {
			errs = append(errs, fmt.Sprintf("ledger: environment observation %d cross-run reference", i))
		}
	}

	return errs
}

// looksLikeCommitSHA reports whether s is a 40-character hexadecimal string,
// i.e. a full git commit SHA. UUIDv4 run IDs must not collide with this shape.
func looksLikeCommitSHA(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}

func checkNoPreaggregatedFields(env *Envelope) []string {
	var errs []string
	if len(env.DerivedObservations) > 0 {
		errs = append(errs, "ledger: derived_observations must be empty on input; verifier recomputes them")
	}
	for i, po := range env.Raw.ProtocolObservations {
		switch po.Type {
		case "fixture", "stamp", "preaggregated", "gate_summary":
			errs = append(errs, fmt.Sprintf("ledger: protocol observation %d has pre-aggregated type %q", i, po.Type))
		}
		if src, ok := po.Detail["evidence_source"].(string); ok {
			if src == "fixture" || src == "stamp" {
				errs = append(errs, fmt.Sprintf("ledger: protocol observation %d has fixed evidence_source %q", i, src))
			}
		}
	}
	return errs
}

// checkRunIdentityIntegrity enforces spec §8.3 step 3: the raw ledger MUST
// carry at least one RunIdentity, and all records MUST share the same RunID,
// TestBinaryDigest, and ManifestDigest. The RunID MUST also match the envelope
// run_id (the same cross-run-reference rule applied to every other raw record
// type). Missing, duplicate, or inconsistent run_identity fails.
func checkRunIdentityIntegrity(env *Envelope) []string {
	var errs []string
	if len(env.Raw.RunIdentities) == 0 {
		errs = append(errs, "run_identity: at least one run_identity record required")
		return errs
	}
	first := env.Raw.RunIdentities[0]
	if first.RunID == "" {
		errs = append(errs, "run_identity: record 0 has empty run_id")
	} else if first.RunID != env.RunID {
		errs = append(errs, fmt.Sprintf("run_identity: record 0 run_id %q != envelope run_id %q", first.RunID, env.RunID))
	}
	if first.TestBinaryDigest == "" {
		errs = append(errs, "run_identity: record 0 has empty test_binary_digest")
	}
	if first.ManifestDigest == "" {
		errs = append(errs, "run_identity: record 0 has empty manifest_digest")
	}
	for i, ri := range env.Raw.RunIdentities {
		if i == 0 {
			continue
		}
		if ri.RunID != first.RunID {
			errs = append(errs, fmt.Sprintf("run_identity: record %d run_id %q != %q (cross-identity)", i, ri.RunID, first.RunID))
		}
		if ri.TestBinaryDigest != first.TestBinaryDigest {
			errs = append(errs, fmt.Sprintf("run_identity: record %d test_binary_digest %q != %q", i, ri.TestBinaryDigest, first.TestBinaryDigest))
		}
		if ri.ManifestDigest != first.ManifestDigest {
			errs = append(errs, fmt.Sprintf("run_identity: record %d manifest_digest %q != %q", i, ri.ManifestDigest, first.ManifestDigest))
		}
	}
	return errs
}

// checkEnvironmentIntegrity enforces spec §8.1/§8.5: redis_version and
// mysql_version MUST be non-empty AND sourced from typed EnvironmentObservation
// records. The typed records are authoritative; the self-reported Environment
// block is cross-checked against them and rejected if they disagree or are
// empty. At least one redis and one mysql observation with non-empty Result is
// required; all redis Results must agree; all mysql Results must agree
// (deduplication across fragments is fine, but inconsistent versions fail).
func checkEnvironmentIntegrity(env *Envelope) []string {
	var errs []string

	if len(env.Raw.EnvironmentObservations) == 0 {
		errs = append(errs, "environment: at least one environment_observation record required")
		if env.Environment.RedisVersion == "" {
			errs = append(errs, "environment: redis_version must be non-empty")
		}
		if env.Environment.MySQLVersion == "" {
			errs = append(errs, "environment: mysql_version must be non-empty")
		}
		return errs
	}

	var redisResults, mysqlResults []string
	for i, eo := range env.Raw.EnvironmentObservations {
		switch eo.Component {
		case "redis":
			if eo.Result == "" {
				errs = append(errs, fmt.Sprintf("environment: environment_observation %d (redis) has empty result", i))
				continue
			}
			redisResults = append(redisResults, eo.Result)
		case "mysql":
			if eo.Result == "" {
				errs = append(errs, fmt.Sprintf("environment: environment_observation %d (mysql) has empty result", i))
				continue
			}
			mysqlResults = append(mysqlResults, eo.Result)
		}
	}
	if len(redisResults) == 0 {
		errs = append(errs, "environment: at least one redis observation with non-empty result required")
	}
	if len(mysqlResults) == 0 {
		errs = append(errs, "environment: at least one mysql observation with non-empty result required")
	}
	redisAgreed := valuesAgree(redisResults)
	mysqlAgreed := valuesAgree(mysqlResults)
	if !redisAgreed {
		errs = append(errs, fmt.Sprintf("environment: redis observation results disagree: %v", redisResults))
	}
	if !mysqlAgreed {
		errs = append(errs, fmt.Sprintf("environment: mysql observation results disagree: %v", mysqlResults))
	}
	if env.Environment.RedisVersion == "" {
		errs = append(errs, "environment: redis_version must be non-empty")
	} else if redisAgreed && len(redisResults) > 0 && env.Environment.RedisVersion != redisResults[0] {
		errs = append(errs, fmt.Sprintf("environment: redis_version %q != observed result %q", env.Environment.RedisVersion, redisResults[0]))
	}
	if env.Environment.MySQLVersion == "" {
		errs = append(errs, "environment: mysql_version must be non-empty")
	} else if mysqlAgreed && len(mysqlResults) > 0 && env.Environment.MySQLVersion != mysqlResults[0] {
		errs = append(errs, fmt.Sprintf("environment: mysql_version %q != observed result %q", env.Environment.MySQLVersion, mysqlResults[0]))
	}
	return errs
}

// valuesAgree reports whether all non-empty strings in vals are identical.
func valuesAgree(vals []string) bool {
	if len(vals) == 0 {
		return true
	}
	first := vals[0]
	for _, v := range vals[1:] {
		if v != first {
			return false
		}
	}
	return true
}

// hasSystemTaskDelivery reports whether a "system_task_delivery" protocol
// observation bound to execID carries system_task_deliveries (or deliveries) >= 1.
// This is the OSKillSIGKILL phase-B exception in checkA0 (spec §3.5).
func hasSystemTaskDelivery(env *Envelope, execID types.ExecutionID) bool {
	for _, po := range env.Raw.ProtocolObservations {
		if po.Type != "system_task_delivery" {
			continue
		}
		if execID != "" && po.ExecutionID != execID {
			continue
		}
		if n, ok := intFromDetail(po.Detail, "system_task_deliveries"); ok && n >= 1 {
			return true
		}
		if n, ok := intFromDetail(po.Detail, "deliveries"); ok && n >= 1 {
			return true
		}
	}
	return false
}

// intFromDetail extracts an int from a protocol-observation Detail map value,
// tolerating the int / int64 / float64 representations produced by Go construction
// and JSON unmarshalling.
func intFromDetail(m map[string]any, key string) (int, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	}
	return 0, false
}
// claiming the same A0 scenario or A3 row. The verifier uses these maps to
// reject duplicate rows before deriving observations, because the derivation
// loop only produces one observation per manifest entry.
func findDuplicateMarkers(pos []ProtocolObservation) (map[string]struct{}, map[a3RowKey]struct{}) {
	dupA0 := make(map[string]struct{})
	dupA3 := make(map[a3RowKey]struct{})
	a0Seen := make(map[string]types.ExecutionID)
	a3Seen := make(map[a3RowKey]types.ExecutionID)

	for _, po := range pos {
		switch po.Type {
		case "scenario_marker":
			scenario := po.Topology
			if prev, ok := a0Seen[scenario]; ok && prev != po.ExecutionID {
				dupA0[scenario] = struct{}{}
			} else {
				a0Seen[scenario] = po.ExecutionID
			}
		case "a3_row_marker":
			fixture, _ := po.Detail["fixture"].(string)
			topology, _ := po.Detail["topology"].(string)
			key := a3RowKey{fixture: fixture, topology: topology}
			if prev, ok := a3Seen[key]; ok && prev != po.ExecutionID {
				dupA3[key] = struct{}{}
			} else {
				a3Seen[key] = po.ExecutionID
			}
		}
	}
	return dupA0, dupA3
}

func (v *Verifier) computeDerivedObservations(env *Envelope) []DerivedObservation {
	// Index runtime events by execution ID.
	eventsByExec := make(map[types.ExecutionID][]engine.RuntimeEvidenceEvent)
	for _, ev := range env.Raw.RuntimeEvents {
		eventsByExec[ev.Event.ExecutionID] = append(eventsByExec[ev.Event.ExecutionID], ev.Event)
	}

	// Index counter snapshots by execution ID.
	countersByExec := make(map[types.ExecutionID][]CounterSnapshot)
	for _, cs := range env.Raw.CounterSnapshots {
		countersByExec[cs.ExecutionID] = append(countersByExec[cs.ExecutionID], cs)
	}

	// Build execution -> A0 scenario mapping from markers.
	a0ExecToScenario := make(map[types.ExecutionID]A0Scenario)
	for _, po := range env.Raw.ProtocolObservations {
		if po.Type == "scenario_marker" {
			a0ExecToScenario[po.ExecutionID] = A0Scenario(po.Topology)
		}
	}

	// Build execution -> A3 row mapping from markers.
	a3ExecToRow := make(map[types.ExecutionID]a3RowKey)
	for _, po := range env.Raw.ProtocolObservations {
		if po.Type == "a3_row_marker" {
			fixture, _ := po.Detail["fixture"].(string)
			topology, _ := po.Detail["topology"].(string)
			a3ExecToRow[po.ExecutionID] = a3RowKey{fixture: fixture, topology: topology}
		}
	}

	// Helper to collect events for an execution.
	collect := func(execID types.ExecutionID) (commits, advances, retries []engine.RuntimeEvidenceEvent) {
		for _, ev := range eventsByExec[execID] {
			switch ev.Type {
			case engine.RuntimeEvidenceCommit:
				if ev.CommitOutcome == engine.CommitOutcomeAccepted {
					commits = append(commits, ev)
				}
			case engine.RuntimeEvidenceAdvance:
				if ev.Applied {
					advances = append(advances, ev)
				}
			case engine.RuntimeEvidenceRetry:
				retries = append(retries, ev)
			}
		}
		return
	}

	var derived []DerivedObservation

	// A0 observations: one per required scenario.
	for _, scenario := range v.Manifest.A0Scenarios {
		obs := DerivedObservation{
			Kind:           "a0_scenario",
			Scenario:       string(scenario),
			EvidenceSource: "runtime_receipt",
		}
		// Find the execution mapped to this scenario.
		var execID types.ExecutionID
		for e, s := range a0ExecToScenario {
			if s == scenario {
				execID = e
				break
			}
		}
		if execID == "" {
			obs.Reason = "no execution marker observed"
			derived = append(derived, obs)
			continue
		}
		obs.ExecutionID = execID
		commits, advances, _ := collect(execID)
		for _, c := range commits {
			obs.SourceEventIDs = append(obs.SourceEventIDs, c.EventID)
		}
		for _, a := range advances {
			obs.SourceEventIDs = append(obs.SourceEventIDs, a.EventID)
		}
		if len(commits) == 1 {
			obs.AcceptedCommit = true
			obs.CommitEventID = commits[0].EventID
		}
		if len(advances) == 1 {
			obs.AppliedAdvance = true
			obs.AdvanceEventID = advances[0].EventID
		}
		// Aggregate handler counters for the A0 execution so checkA0 can enforce
		// handler_invocations > 0 (except OSKillSIGKILL phase B, which is
		// direct-drive and uses system_task_delivery instead). Each value MUST
		// reference a counter snapshot ID — a bare number with no counter reference
		// is rejected by checkA0.
		for _, cs := range countersByExec[execID] {
			obs.HandlerInvocations += cs.Value
			obs.CounterSnapshotID = cs.CounterID
		}
		derived = append(derived, obs)
	}

	// A3 observations: one per required fixture × topology row.
	for _, row := range v.Manifest.A3Rows {
		obs := DerivedObservation{
			Kind:           "a3_matrix_row",
			Fixture:        string(row.Fixture),
			Topology:       string(row.Topology),
			EvidenceSource: "runtime_receipt",
		}
		var execID types.ExecutionID
		for e, k := range a3ExecToRow {
			if k.fixture == string(row.Fixture) && k.topology == string(row.Topology) {
				execID = e
				break
			}
		}
		if execID == "" {
			obs.Reason = "no execution marker observed"
			derived = append(derived, obs)
			continue
		}
		obs.ExecutionID = execID
		commits, advances, retries := collect(execID)
		for _, c := range commits {
			obs.SourceEventIDs = append(obs.SourceEventIDs, c.EventID)
		}
		for _, a := range advances {
			obs.SourceEventIDs = append(obs.SourceEventIDs, a.EventID)
		}
		for _, r := range retries {
			obs.SourceEventIDs = append(obs.SourceEventIDs, r.EventID)
		}
		if len(commits) == 1 {
			obs.AcceptedCommit = true
			obs.CommitEventID = commits[0].EventID
			obs.Classification = EffectiveClassificationFromEvent(commits[0])
		}
		if len(advances) == 1 {
			obs.AppliedAdvance = true
			obs.AdvanceEventID = advances[0].EventID
		}
		if len(retries) > 0 {
			// Reference the latest retry whose failed attempt precedes the
			// terminal accepted commit. This satisfies the spec requirement that
			// success-after-retry fixtures reference a corresponding retry event.
			chosen := retries[len(retries)-1]
			if len(commits) == 1 {
				commitAttempt := commits[0].Attempt
				for _, r := range retries {
					if r.Attempt < commitAttempt && r.Attempt > chosen.Attempt {
						chosen = r
					}
				}
			}
			obs.RetryEventID = chosen.EventID
		}
		for _, cs := range countersByExec[execID] {
			obs.HandlerInvocations += cs.Value
			obs.CounterSnapshotID = cs.CounterID
		}
		derived = append(derived, obs)
	}

	sort.Slice(derived, func(i, j int) bool {
		if derived[i].Scenario != derived[j].Scenario {
			return derived[i].Scenario < derived[j].Scenario
		}
		if derived[i].Fixture != derived[j].Fixture {
			return derived[i].Fixture < derived[j].Fixture
		}
		return derived[i].Topology < derived[j].Topology
	})

	return derived
}

func (v *Verifier) checkA0(env *Envelope, derived []DerivedObservation, duplicateScenarios map[string]struct{}) []string {
	var errs []string

	for scenario := range duplicateScenarios {
		errs = append(errs, fmt.Sprintf("a0: duplicate scenario row %s", scenario))
	}

	byScenario := make(map[string]DerivedObservation)
	for _, obs := range derived {
		if obs.Kind == "a0_scenario" {
			byScenario[obs.Scenario] = obs
		}
	}

	for _, scenario := range v.Manifest.A0Scenarios {
		obs, ok := byScenario[string(scenario)]
		if !ok {
			errs = append(errs, fmt.Sprintf("a0: missing scenario row %s", scenario))
			continue
		}
		if !obs.AcceptedCommit {
			errs = append(errs, fmt.Sprintf("a0: scenario %s has no accepted commit", scenario))
		}
		if scenario != A0ReportAckLoss && scenario != A0ReportRequestLoss && !obs.AppliedAdvance {
			errs = append(errs, fmt.Sprintf("a0: scenario %s has no applied advance", scenario))
		}

		// Every numeric field must have evidence_source or a null reason.
		if obs.EvidenceSource == "" && obs.Reason == "" {
			errs = append(errs, fmt.Sprintf("a0: scenario %s missing evidence_source or reason", scenario))
		}

		// A0 handler_invocations (spec §8.5): each scenario MUST show positive
		// handler activity backed by a counter snapshot reference. The only
		// exception is OSKillSIGKILL phase B, which is direct-drive: business
		// handler_invocations is legitimately 0, so it is required instead to
		// show system_task_delivery >= 1 (spec §3.5). A bare handler_invocations
		// number with no counter snapshot reference is rejected.
		if scenario == A0OSKillSIGKILL {
			if !hasSystemTaskDelivery(env, obs.ExecutionID) {
				errs = append(errs, fmt.Sprintf("a0: scenario %s requires system_task_delivery >= 1 (direct-drive phase B)", scenario))
			}
		} else {
			if obs.HandlerInvocations <= 0 {
				errs = append(errs, fmt.Sprintf("a0: scenario %s handler_invocations must be > 0", scenario))
			}
			if obs.CounterSnapshotID == "" {
				errs = append(errs, fmt.Sprintf("a0: scenario %s handler_invocations missing counter snapshot reference", scenario))
			}
		}
	}

	// ACK-loss must not show lease reclaim.
	if ack, ok := byScenario[string(A0ReportAckLoss)]; ok {
		for _, po := range env.Raw.ProtocolObservations {
			if po.Type != "lease_reclaim" {
				continue
			}
			if ack.ExecutionID != "" && po.ExecutionID == ack.ExecutionID {
				errs = append(errs, "a0: ACK-loss scenario contains lease reclaim")
			} else if po.Topology == ack.Scenario {
				errs = append(errs, "a0: ACK-loss scenario contains lease reclaim")
			}
		}
	}

	// Request-loss: first report no accepted commit, stale authority rejected,
	// final unique accepted commit/advance.
	if req, ok := byScenario[string(A0ReportRequestLoss)]; ok {
		var firstAccepted, authorityRejected bool
		for _, ev := range env.Raw.RuntimeEvents {
			if ev.Event.Type != engine.RuntimeEvidenceCommit || ev.Event.ExecutionID != req.ExecutionID {
				continue
			}
			if ev.Event.Attempt == 1 && ev.Event.CommitOutcome == engine.CommitOutcomeAccepted {
				firstAccepted = true
			}
		}
		for _, po := range env.Raw.ProtocolObservations {
			if po.ExecutionID == req.ExecutionID && po.Type == "authority_rejected" {
				authorityRejected = true
			}
		}
		if firstAccepted {
			errs = append(errs, "a0: request-loss first report produced accepted commit")
		}
		if !authorityRejected {
			errs = append(errs, "a0: request-loss missing authority rejected observation")
		}
	}

	// Reject synthetic os-kill observations.
	for _, po := range env.Raw.ProtocolObservations {
		if po.Type == "synthetic_os_kill" {
			errs = append(errs, "a0: synthetic os-kill observation is not allowed")
		}
	}

	return errs
}

func (v *Verifier) checkA3(env *Envelope, derived []DerivedObservation, duplicateRows map[a3RowKey]struct{}) []string {
	var errs []string

	for key := range duplicateRows {
		errs = append(errs, fmt.Sprintf("a3: duplicate matrix row (%s, %s)", key.fixture, key.topology))
	}

	byRow := make(map[a3RowKey]DerivedObservation)
	for _, obs := range derived {
		if obs.Kind == "a3_matrix_row" {
			byRow[a3RowKey{fixture: obs.Fixture, topology: obs.Topology}] = obs
		}
	}

	classifiedTerminalFixtures := map[A3Fixture]bool{
		A3TransientRetryExhausted: true,
		A3PermanentNoRetry:        true,
		A3BusinessErrorNoRetry:    true,
	}
	retriedFixtures := map[A3Fixture]bool{
		A3TransientThenSuccess:    true,
		A3TransientRetryExhausted: true,
		A3ErrorPortRetryExhausted: true,
	}

	for _, row := range v.Manifest.A3Rows {
		key := a3RowKey{fixture: string(row.Fixture), topology: string(row.Topology)}
		obs, ok := byRow[key]
		if !ok {
			errs = append(errs, fmt.Sprintf("a3: missing matrix row (%s, %s)", row.Fixture, row.Topology))
			continue
		}
		if obs.CommitEventID == "" {
			errs = append(errs, fmt.Sprintf("a3: row (%s, %s) missing commit_event_id", row.Fixture, row.Topology))
			continue
		}
		if !obs.AcceptedCommit {
			errs = append(errs, fmt.Sprintf("a3: row (%s, %s) commit is not accepted", row.Fixture, row.Topology))
		}

		if classifiedTerminalFixtures[row.Fixture] && obs.Classification == nil {
			errs = append(errs, fmt.Sprintf("a3: terminal classified fixture (%s, %s) missing classification", row.Fixture, row.Topology))
		}
		if !classifiedTerminalFixtures[row.Fixture] && obs.Classification != nil {
			errs = append(errs, fmt.Sprintf("a3: success/unclassified fixture (%s, %s) must not have classification", row.Fixture, row.Topology))
		}

		if row.Fixture == A3TransientThenSuccess && !obs.AppliedAdvance {
			errs = append(errs, fmt.Sprintf("a3: row (%s, %s) success fixture missing applied advance", row.Fixture, row.Topology))
		}
		if row.Fixture != A3TransientThenSuccess && obs.AppliedAdvance {
			errs = append(errs, fmt.Sprintf("a3: row (%s, %s) terminal failure fixture has applied advance", row.Fixture, row.Topology))
		}

		if retriedFixtures[row.Fixture] {
			if obs.RetryEventID == "" {
				errs = append(errs, fmt.Sprintf("a3: row (%s, %s) retried fixture missing retry event reference", row.Fixture, row.Topology))
			} else {
				var re *engine.RuntimeEvidenceEvent
				for i := range env.Raw.RuntimeEvents {
					if env.Raw.RuntimeEvents[i].Event.EventID == obs.RetryEventID {
						re = &env.Raw.RuntimeEvents[i].Event
						break
					}
				}
				if re == nil || re.Type != engine.RuntimeEvidenceRetry || re.ExecutionID != obs.ExecutionID {
					errs = append(errs, fmt.Sprintf("a3: row (%s, %s) retry event reference invalid", row.Fixture, row.Topology))
				} else {
					var commitAttempt int
					for _, ev := range env.Raw.RuntimeEvents {
						if ev.Event.EventID == obs.CommitEventID {
							commitAttempt = ev.Event.Attempt
							break
						}
					}
					if commitAttempt > 0 && re.Attempt >= commitAttempt {
						errs = append(errs, fmt.Sprintf("a3: row (%s, %s) retry event attempt >= commit attempt", row.Fixture, row.Topology))
					}
				}
			}
		}

		if !row.IsDatabaseLocalFake && obs.HandlerInvocations <= 0 {
			errs = append(errs, fmt.Sprintf("a3: row (%s, %s) built-in handler_invocations must be > 0", row.Fixture, row.Topology))
		}
	}

	// Database real-pair parity: server-runner and cluster-durable rows must agree.
	realPairRows := make(map[A3Fixture]map[A3Topology]DerivedObservation)
	for _, row := range v.Manifest.A3Rows {
		if !row.DatabaseRealPair {
			continue
		}
		if realPairRows[row.Fixture] == nil {
			realPairRows[row.Fixture] = make(map[A3Topology]DerivedObservation)
		}
		if obs, ok := byRow[a3RowKey{fixture: string(row.Fixture), topology: string(row.Topology)}]; ok {
			realPairRows[row.Fixture][row.Topology] = obs
		}
	}
	for fixture, pairs := range realPairRows {
		sr, okSR := pairs[A3ServerRunner]
		cd, okCD := pairs[A3ClusterDurable]
		if !okSR || !okCD {
			continue
		}
		if sr.AcceptedCommit != cd.AcceptedCommit || sr.AppliedAdvance != cd.AppliedAdvance {
			errs = append(errs, fmt.Sprintf("a3: Database real-pair mismatch for %s (server-runner vs cluster-durable)", fixture))
		}
		if sr.Classification != nil && cd.Classification != nil {
			if sr.Classification.Kind != cd.Classification.Kind {
				errs = append(errs, fmt.Sprintf("a3: Database real-pair classification mismatch for %s", fixture))
			}
		} else if (sr.Classification == nil) != (cd.Classification == nil) {
			errs = append(errs, fmt.Sprintf("a3: Database real-pair classification presence mismatch for %s", fixture))
		}
	}

	// Reject any derived observation whose evidence source is "fixture" or empty
	// without reason.
	for _, obs := range derived {
		if obs.Kind != "a3_matrix_row" {
			continue
		}
		if obs.EvidenceSource == "fixture" || obs.EvidenceSource == "stamp" {
			errs = append(errs, fmt.Sprintf("a3: row (%s, %s) uses fixed/stamp evidence source", obs.Fixture, obs.Topology))
		}
		if obs.EvidenceSource == "" && obs.Reason == "" {
			errs = append(errs, fmt.Sprintf("a3: row (%s, %s) missing evidence_source or reason", obs.Fixture, obs.Topology))
		}
	}

	return errs
}
