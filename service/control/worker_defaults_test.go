package control

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
)

// sleepRecorder stands in for sleepWithContext and records every duration the
// loop asks to wait for, stopping the loop after the first tick so the test is
// bounded no matter what the period turns out to be.
type sleepRecorder struct {
	mu   sync.Mutex
	got  []time.Duration
	stop error
}

func (r *sleepRecorder) sleep(_ context.Context, d time.Duration) error {
	r.mu.Lock()
	r.got = append(r.got, d)
	r.mu.Unlock()
	return r.stop
}

func (r *sleepRecorder) first(t *testing.T) time.Duration {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.got) == 0 {
		t.Fatal("the loop never asked to sleep")
	}
	return r.got[0]
}

// recordingReconciler captures the arguments the worker scans with.
type recordingReconciler struct {
	mu     sync.Mutex
	before []time.Time
	limits []int
}

func (r *recordingReconciler) CountUnreconciledAdmissions(context.Context, time.Time) (int, time.Time, error) {
	return 0, time.Time{}, nil
}

func (r *recordingReconciler) ListUnreconciledAdmissions(_ context.Context, before time.Time, _ uint64, limit int) ([]*store.AuditRecord, error) {
	r.mu.Lock()
	r.before = append(r.before, before)
	r.limits = append(r.limits, limit)
	r.mu.Unlock()
	return nil, nil
}

func (r *recordingReconciler) AppendOutcomeIfAbsent(context.Context, *store.AuditRecord) (bool, error) {
	return false, nil
}

// TestAuditReconcileWorkerDefaultsAreLoadBearing pins the three fallbacks in
// NewAuditReconcileWorker (audit_reconcile_worker.go:174-182).
//
// They are not belt-and-braces. The only production construction —
// sdk/xflow/server.go:353 — builds AuditReconcileConfig{Logger, Elector} and
// sets none of Period, BacklogAge or Batch, so every value the worker runs on
// comes from these three lines. Widening any guard from `<= 0` to `< 0`, or
// deleting it, leaves all ten packages that import service/control green:
// every AuditReconcileConfig literal in the package's own tests passes an
// explicit Period, and nothing anywhere constructs the zero-value config the
// server actually uses.
//
// What each one costs when it stops firing:
//
//   - Period. Run's tick is sleepFunc(ctx, w.period), which is
//     sleepWithContext, and that returns nil immediately for d <= 0 — without
//     consulting ctx. So Period=0 is not merely a busy loop hammering
//     CountUnreconciledAdmissions and ListUnreconciledAdmissions at full CPU;
//     it is an *uncancellable* one. Run's only exit is a non-nil sleep error,
//     so cancelling the context no longer stops it. Unlike the lease sweeper,
//     whose identical spin at least blocks ControlPlane.Stop and surfaces as a
//     hang, nothing waits on this goroutine — the ten packages stay green while
//     the worker spins, which is why it needs an assertion rather than a
//     symptom.
//   - BacklogAge. `before := now.Add(-w.backlog)` is what makes a row old
//     enough to be presumed abandoned. At 0, before == now, so admissions
//     written microseconds ago — requests still in flight, whose handler has
//     simply not appended its outcome yet — are treated as crashed and settled
//     from observed state underneath the running request.
//   - Batch. It is the LIMIT on the scan. memstore returns one row for limit=0
//     and sqlstore generates a literal `LIMIT 0` and returns none, so the
//     worker either crawls one row per sweep or silently reconciles nothing
//     forever, with no error anywhere. See
//     [[list-options-zero-limit-returns-nothing]].
//
// The assertions go through Run and ReconcileOnce rather than reading the
// struct fields, so copying a constant into the field is not enough — the
// value has to actually reach the loop and the scan.
func TestAuditReconcileWorkerDefaultsAreLoadBearing(t *testing.T) {
	rec := &recordingReconciler{}
	sleeper := &sleepRecorder{stop: context.Canceled}

	// Exactly the shape sdk/xflow/server.go builds: nothing but the logger and
	// the leader gate.
	w := NewAuditReconcileWorker(rec, nil, AuditReconcileConfig{})
	w.sleepFunc = sleeper.sleep

	before := time.Now().UTC()
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	after := time.Now().UTC()

	if got := sleeper.first(t); got != DefaultReconcilePeriod {
		t.Fatalf("the loop ticks every %v, want %v: with a non-positive period "+
			"sleepWithContext returns immediately and never looks at ctx, so Run "+
			"becomes an uncancellable full-CPU scan loop over the audit table",
			got, DefaultReconcilePeriod)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.limits) == 0 {
		t.Fatal("the worker never scanned")
	}
	if rec.limits[0] != DefaultReconcileBatch {
		t.Fatalf("scan limit = %d, want %d: a non-positive limit reaches the store "+
			"verbatim, where memstore yields one row and sqlstore emits LIMIT 0 and "+
			"yields none — the worker then reconciles almost nothing, silently",
			rec.limits[0], DefaultReconcileBatch)
	}
	// The cutoff must sit a full backlog behind the worker's own clock. That
	// clock is read somewhere inside [before, after], so the exact instant is
	// unknowable from here and the value is squeezed from both sides instead:
	// at least one backlog behind `after`, at most one behind `before`. Any
	// other backlog — zero or otherwise — falls outside that window.
	if age := after.Sub(rec.before[0]); age < DefaultReconcileBacklog {
		t.Fatalf("the scan cutoff is only %v in the past, want at least %v: at zero "+
			"backlog the worker settles admissions whose request is still in flight, "+
			"overwriting the outcome the live handler is about to append", age, DefaultReconcileBacklog)
	}
	if age := before.Sub(rec.before[0]); age > DefaultReconcileBacklog {
		t.Fatalf("the scan cutoff is %v in the past, want at most %v: too long a "+
			"backlog is the quieter failure — crashed admissions stay Indeterminate "+
			"for that whole window before anyone settles them", age, DefaultReconcileBacklog)
	}
}

// stubLeaseState is a LeaseLister that also implements LeaseIndexRepairer, so
// RepairOnce reaches the rate limiter and the batch argument.
type stubLeaseState struct {
	mu     sync.Mutex
	limits []int
}

func (s *stubLeaseState) ListExpiredLeases(context.Context, time.Time) ([]engine.ExpiredLease, error) {
	return nil, nil
}

func (s *stubLeaseState) RepairLeaseIndex(_ context.Context, limit int) (int, error) {
	s.mu.Lock()
	s.limits = append(s.limits, limit)
	s.mu.Unlock()
	return 0, nil
}

// TestLeaseSweeperDefaultsAreLoadBearing is the same shape for
// NewLeaseSweeper's three fallbacks (lease_sweeper.go:110-118). Its only
// production construction, controlplane.go:346, builds
// LeaseSweeperConfig{Elector, Logger, RunnerDirectory} and sets none of them.
//
// Two of the three are uncovered today; Period is not, and the difference is
// worth stating precisely.
//
// Period feeds the same sleepWithContext as the reconcile worker. Widening its
// guard does not go unnoticed — but what it produces is a hang, not a failure.
// Run's only exit is a non-nil sleep error, sleepWithContext returns nil for
// d <= 0 without consulting ctx, and controlplane.go:628-634 registers the
// sweeper on cp.wg while Stop waits on cp.wg.Wait(). So the control plane can
// no longer shut down: measured over the ten packages that import
// service/control, that mutation red-lines sdk/xflow's
// TestServerRunRunsTheReconciler with "Run did not return after ctx cancel" and
// then takes sdk/xflow, service/apiserver and service/control down entirely on
// the 10-minute test-timeout panic, with the panic naming whichever unrelated
// test happened to be running. This assertion is therefore not new coverage.
// What it adds is attribution: the same defect in 0.00s, naming
// NewLeaseSweeper, instead of three dead package binaries.
//
// LeaseRepairPeriod and LeaseRepairBatch are genuinely uncovered — both survive
// all ten packages green. LeaseRepairPeriod is the rate limit on RepairOnce and
// the reason RepairOnce is safe to call from the top of every SweepOnce; at
// zero, `now.Sub(s.lastRepair) < s.repairPeriod` can never hold, so a full
// lease-index reconciliation scan rides along with every sweep instead of once
// a minute. LeaseRepairBatch is passed straight through as RepairLeaseIndex's
// limit, the same unbounded/empty-scan hazard as the reconcile worker's Batch.
//
// The package's existing sweeper tests all call SweepOnce or RepairOnce
// directly on a hand-built struct, or pass an explicit Period; none of them
// constructs a zero-value config and lets the loop read what it got.
func TestLeaseSweeperDefaultsAreLoadBearing(t *testing.T) {
	state := &stubLeaseState{}
	sleeper := &sleepRecorder{stop: context.Canceled}

	s := NewLeaseSweeper(state, nil, LeaseSweeperConfig{})
	s.sleepFunc = sleeper.sleep

	s.Run(context.Background())

	if got := sleeper.first(t); got != DefaultSweepPeriod {
		t.Fatalf("the sweep loop ticks every %v, want %v: a non-positive period makes "+
			"sleepWithContext return without consulting ctx, so Run never exits and "+
			"ControlPlane.Stop blocks on cp.wg.Wait() forever", got, DefaultSweepPeriod)
	}

	state.mu.Lock()
	limits := append([]int(nil), state.limits...)
	state.mu.Unlock()
	if len(limits) != 1 {
		t.Fatalf("RepairLeaseIndex ran %d times during one Run, want 1: Run repairs "+
			"once at startup and the repair period must suppress the rest", len(limits))
	}
	if limits[0] != defaultLeaseRepairBatch {
		t.Fatalf("repair batch = %d, want %d", limits[0], defaultLeaseRepairBatch)
	}

	// The rate limiter itself: a second RepairOnce right after the startup one
	// must be suppressed. With a zero repair period the `<` comparison can
	// never hold and every sweep drags a full index reconciliation with it.
	s.RepairOnce(context.Background())
	state.mu.Lock()
	after := len(state.limits)
	state.mu.Unlock()
	if after != 1 {
		t.Fatalf("RepairLeaseIndex ran %d times, want 1: a repair immediately after "+
			"the previous one must be suppressed by LeaseRepairPeriod, which is what "+
			"makes calling RepairOnce from the top of every SweepOnce affordable", after)
	}
}

// defaultLeaseRepairBatch mirrors the literal 256 in NewLeaseSweeper. It has no
// exported constant, unlike the other five defaults in this file — worth
// noting rather than worth changing here.
const defaultLeaseRepairBatch = 256
