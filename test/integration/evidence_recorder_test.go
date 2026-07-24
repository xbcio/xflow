//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/test/integration/internal/evidence"
	"github.com/xbcio/xflow/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	_ "github.com/go-sql-driver/mysql"
)

// cachedSourceProvenance holds the source provenance observed at test time,
// computed once per process via cachedSourceOnce. All fragments in one run
// stamp identical Source (HEAD, the test binary, and the Go runtime do not
// change during a single test invocation), so recomputing per flush would be
// wasteful — and notably would re-read the (large, race-instrumented) test
// binary on every flush, causing GC/scheduling pressure that destabilizes
// timing-sensitive drains in later tests. Computing once keeps the recorder
// cheap and keeps non-recorder tests (which pass a nil recorder) unaffected.
var (
	cachedSourceOnce sync.Once
	cachedSource     evidence.SourceProvenance
	cachedEnvOnce    sync.Once
	cachedEnv        evidence.Environment
)

// queryRedisVersion parses redis_version from Redis INFO server output.
func queryRedisVersion(addr string) (string, error) {
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	info, err := c.Info(ctx, "server").Result()
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(info, "\r\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "redis_version:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "redis_version:")), nil
		}
	}
	return "", fmt.Errorf("redis_version not found in INFO server")
}

// queryMySQLVersion returns the result of SELECT VERSION().
func queryMySQLVersion(dsn string) (string, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var ver string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&ver); err != nil {
		return "", err
	}
	return ver, nil
}

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
	runID      string
	rawDir     string
	name       string // fragment filename stem (unique per scenario)
	producerID string // per-collector crypto/rand UUID; distinct per OS process
	env        *evidence.Envelope
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
		runID:      runID,
		rawDir:     rawDir,
		name:       name,
		producerID: uuid.New().String(), // crypto/rand UUID, distinct per collector instance / OS process
		env: &evidence.Envelope{
			SchemaVersion: evidence.SchemaVersion,
			RunID:         runID,
			StartedAt:     time.Now().UTC(),
		},
	}
}

// recordRuntimeEvents appends drained buffer events. The events already carry
// ExecutionID/NodeName/Attempt/Applied/CommitOutcome/OutboxIDs/ErrorSource/
// Classified/Kind/Retryable/etc. — the recorder wraps each one with
// EvidenceRecordMeta and must not fabricate or pre-aggregate.
func (r *evidenceRecorder) recordRuntimeEvents(evs []engine.RuntimeEvidenceEvent) {
	if r == nil {
		return
	}
	for _, ev := range evs {
		r.env.Raw.RuntimeEvents = append(r.env.Raw.RuntimeEvents, r.wrapRuntimeEvent(ev))
	}
}

// wrapRuntimeEvent wraps a raw engine event with collector-side metadata.
// ProducerID is generated once per evidenceRecorder instance, so distinct OS
// processes (e.g. the SIGKILL scenario) produce distinct producer IDs.
func (r *evidenceRecorder) wrapRuntimeEvent(ev engine.RuntimeEvidenceEvent) evidence.CollectedRuntimeEvidenceEvent {
	return evidence.CollectedRuntimeEvidenceEvent{
		Meta: evidence.EvidenceRecordMeta{
			RunID:      r.runID,
			ProducerID: r.producerID,
			// Topology is the scenario/topology context carried by the recorder name.
			Topology: r.name,
			// ExecutionID must match Event.ExecutionID; see verifier integrity check.
			ExecutionID: ev.ExecutionID,
			ObservedAt:  time.Now().UTC(),
			// SourceDigest = relevant-diff digest; TestBinaryDigest = fixed test binary SHA-256.
			SourceDigest:     cachedSource.RelevantDiffSHA256,
			TestBinaryDigest: cachedSource.TestBinarySHA256,
		},
		Event: ev,
	}
}

// recordCounter appends a counter snapshot. The counting wrappers from
// Tasks 8/10/11 (buildInstrumentedHandler / startCounter) supply the value.
// counterID identifies the counter instance (distinct per SIGKILL process /
// scenario); handlerName is the production handler type the counter is bound
// to (e.g. "test.fault", "test.a0.start"), which the verifier introspects in
// checkA0 to reject a bare counter with an arbitrary name.
func (r *evidenceRecorder) recordCounter(topology string, execID types.ExecutionID, node, counterID, handlerName string, value int) {
	if r == nil {
		return
	}
	r.env.Raw.CounterSnapshots = append(r.env.Raw.CounterSnapshots, evidence.CounterSnapshot{
		RunID:        r.runID,
		Topology:     topology,
		ExecutionID:  execID,
		NodeName:     node,
		CounterID:    counterID,
		HandlerName:  handlerName,
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

// stampProvenance records the real source provenance observed at test time
// (git HEAD SHA, relevant-tree cleanliness, relevant-diff digest, test-binary
// digest, Go version) into the fragment's env.Source. It uses RealProvenance
// with TestBinaryPath from XFLOW_G0_TEST_BIN (set by the make target) falling
// back to os.Args[0], so the binary digest it records is the digest of the
// exact fixed test binary this scenario ran as — which the verifier recomputes
// independently from the same binary path and compares.
//
// This is an honest observation, not a fabrication or a constant: the values
// reflect the repo and binary state at the moment the test ran. The verifier
// recomputes them at verify time; if the tree or binary changed between test
// and verify, the comparison catches it. All fragments in one run stamp
// identical Source (HEAD, binary, and Go version do not change during a single
// test invocation), and MergeRawEnvelopes keeps the first fragment's Source.
//
// The provenance is computed once per process (see cachedSourceOnce) and
// reused for every fragment, so the (large) test binary is read and hashed
// exactly once per run rather than once per flush.
//
// nil-safe: a no-op when the recorder is nil or env was never initialized.
func (r *evidenceRecorder) stampProvenance(t *testing.T) {
	t.Helper()
	if r == nil || r.env == nil {
		return
	}
	cachedSourceOnce.Do(func() {
		binaryPath := os.Getenv("XFLOW_G0_TEST_BIN")
		if binaryPath == "" {
			binaryPath = os.Args[0]
		}
		prov := evidence.RealProvenance{TestBinaryPath: binaryPath}
		if sha, err := prov.CommitSHA(); err == nil {
			cachedSource.CommitSHA = sha
		} else {
			t.Logf("evidence recorder: commit SHA unavailable: %v", err)
		}
		paths := evidence.RelevantSourcePaths()
		if clean, dirty, err := prov.RelevantTreeClean(paths); err == nil {
			cachedSource.RelevantTreeClean = clean
			if dirty != "" {
				t.Logf("evidence recorder: relevant tree dirty:\n%s", strings.TrimSpace(dirty))
			}
		} else {
			t.Logf("evidence recorder: relevant tree clean unavailable: %v", err)
		}
		if dig, err := prov.RelevantDiffDigest(paths); err == nil {
			cachedSource.RelevantDiffSHA256 = dig
		} else {
			t.Logf("evidence recorder: relevant diff digest unavailable: %v", err)
		}
		if dig, err := prov.TestBinaryDigest(""); err == nil {
			cachedSource.TestBinarySHA256 = dig
		} else {
			t.Logf("evidence recorder: test binary digest unavailable: %v", err)
		}
		cachedSource.GoVersion = prov.GoVersion()
	})
	r.env.Source = cachedSource
}

// stampEnvironment queries Redis and MySQL at runtime and records their versions
// in both the typed environment block and typed environment_observations. The
// query is performed once per process; each fragment copies the cached result.
// In the required gate (XFLOW_REQUIRE_*_INTEGRATION=1) an unreachable service
// fails the test; otherwise the capture is skipped gracefully so ordinary
// `go test` runs without services are not broken.
func (r *evidenceRecorder) stampEnvironment(t *testing.T) {
	t.Helper()
	if r == nil || r.env == nil {
		return
	}
	cachedEnvOnce.Do(func() {
		requiredRedis := os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1"
		requiredMySQL := os.Getenv("XFLOW_REQUIRE_MYSQL_INTEGRATION") == "1"

		addr := redisAddr(t)
		if ver, err := queryRedisVersion(addr); err == nil && ver != "" {
			cachedEnv.RedisVersion = ver
		} else {
			msg := fmt.Sprintf("evidence recorder: redis version unavailable at %s: %v", addr, err)
			if requiredRedis {
				t.Fatalf("%s", msg)
			}
			t.Logf("%s", msg)
		}

		dsn := mysqlDSN(t)
		if ver, err := queryMySQLVersion(dsn); err == nil && ver != "" {
			cachedEnv.MySQLVersion = ver
		} else {
			// Do not log the DSN: it embeds MYSQL_ROOT_PASSWORD.
			port := envOr("MYSQL_PORT", "3306")
			msg := fmt.Sprintf("evidence recorder: mysql version unavailable at localhost:%s: %v", port, err)
			if requiredMySQL {
				t.Fatalf("%s", msg)
			}
			t.Logf("%s", msg)
		}
	})
	r.env.Environment = cachedEnv
	if cachedEnv.RedisVersion != "" {
		r.env.Raw.EnvironmentObservations = append(r.env.Raw.EnvironmentObservations, evidence.EnvironmentObservation{
			RunID:      r.runID,
			Topology:   r.name,
			Component:  "redis",
			Query:      "INFO server",
			Result:     cachedEnv.RedisVersion,
			ObservedAt: time.Now().UTC(),
		})
	}
	if cachedEnv.MySQLVersion != "" {
		r.env.Raw.EnvironmentObservations = append(r.env.Raw.EnvironmentObservations, evidence.EnvironmentObservation{
			RunID:      r.runID,
			Topology:   r.name,
			Component:  "mysql",
			Query:      "SELECT VERSION()",
			Result:     cachedEnv.MySQLVersion,
			ObservedAt: time.Now().UTC(),
		})
	}
}

// stampRunIdentity emits a typed run_identity record carrying the random run_id,
// the fixed test binary digest, and the required manifest digest. It reuses the
// source-provenance test binary digest so the large binary is hashed once.
func (r *evidenceRecorder) stampRunIdentity(t *testing.T) {
	t.Helper()
	if r == nil || r.env == nil {
		return
	}
	var manifestDig string
	if dig, err := evidence.ManifestDigest(evidence.DefaultManifest()); err == nil {
		manifestDig = dig
	} else {
		t.Logf("evidence recorder: manifest digest unavailable: %v", err)
	}
	r.env.Raw.RunIdentities = append(r.env.Raw.RunIdentities, evidence.RunIdentity{
		RunID:            r.runID,
		TestBinaryDigest: cachedSource.TestBinarySHA256,
		ManifestDigest:   manifestDig,
		ProducerID:       r.producerID,
		ObservedAt:       time.Now().UTC(),
	})
}

// flush atomically writes the fragment envelope to $RAW_DIR/{name}.json via a
// temp file + rename so a partial write is never observed by the CLI merge.
func (r *evidenceRecorder) flush(t *testing.T) {
	t.Helper()
	if r == nil {
		return
	}
	r.stampProvenance(t)
	r.stampEnvironment(t)
	r.stampRunIdentity(t)
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
