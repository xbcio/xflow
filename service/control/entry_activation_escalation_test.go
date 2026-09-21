package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// recordedLogEntry is one engine.Logger call, level-tagged and with the
// key/value argument pairs preserved.
type recordedLogEntry struct {
	level string
	msg   string
	args  []any
}

// field returns the value the entry carries for key ("" when absent). Loggers
// take args as alternating key/value, which is exactly how the reconciler
// builds them.
func (e recordedLogEntry) field(key string) any {
	for i := 0; i+1 < len(e.args); i += 2 {
		if k, ok := e.args[i].(string); ok && k == key {
			return e.args[i+1]
		}
	}
	return nil
}

// recordingLogger records every call with its argument pairs.
//
// It exists because the package's two existing fakes each capture one level
// only: warnCapturingLogger (controlplane_test.go) records that a Warn happened
// but drops its arguments, and logCapturingLogger (issued_identity_test.go)
// records Error only. The signal under test is a Warn whose FIELDS are the
// deliverable — an operator triages from workflow_id/runner_id/generation/
// consecutive_failures/error, not from the fact that something warned — so
// neither existing fake can assert it.
type recordingLogger struct {
	entries []recordedLogEntry
}

func (l *recordingLogger) record(level, msg string, args ...any) {
	l.entries = append(l.entries, recordedLogEntry{level: level, msg: msg, args: append([]any(nil), args...)})
}

func (l *recordingLogger) withMsg(msg string) []recordedLogEntry {
	var out []recordedLogEntry
	for _, e := range l.entries {
		if e.msg == msg {
			out = append(out, e)
		}
	}
	return out
}

func (l *recordingLogger) Debug(msg string, args ...any) { l.record("debug", msg, args...) }
func (l *recordingLogger) Debugf(string, ...any)         {}
func (l *recordingLogger) Info(msg string, args ...any)  { l.record("info", msg, args...) }
func (l *recordingLogger) Infof(string, ...any)          {}
func (l *recordingLogger) Warn(msg string, args ...any)  { l.record("warn", msg, args...) }
func (l *recordingLogger) Warnf(string, ...any)          {}
func (l *recordingLogger) Error(msg string, args ...any) { l.record("error", msg, args...) }
func (l *recordingLogger) Errorf(string, ...any)         {}
func (l *recordingLogger) Panic(msg string, args ...any) { l.record("panic", msg, args...) }
func (l *recordingLogger) Panicf(string, ...any)         {}

// activationEscalationMsg is the escalation record's message. Kept as a
// constant so a rename cannot silently make this test assert on an empty set.
const activationEscalationMsg = "entry activation repeatedly failing"

// escalationCounts returns the consecutive_failures field of every escalation
// record emitted so far, in order.
func escalationCounts(t *testing.T, l *recordingLogger) []int {
	t.Helper()
	var out []int
	for _, e := range l.withMsg(activationEscalationMsg) {
		n, ok := e.field("consecutive_failures").(int)
		if !ok {
			t.Fatalf("escalation record %q carries consecutive_failures=%v (%T), want int",
				e.msg, e.field("consecutive_failures"), e.field("consecutive_failures"))
		}
		if e.level != "warn" {
			t.Fatalf("escalation must be emitted at WARN, got %q", e.level)
		}
		out = append(out, n)
	}
	return out
}

// TestEntryActivationReconciler_EscalatesRepeatedActivationFailures is the
// regression test for the observability gap: the reconciler's failure reasons
// were reachable only through a single per-event record on the runner-decline
// path, so an activation that kept failing to stay hosted produced nothing an
// operator could read and left no recoverable reason in-process.
//
// It drives the two ways an activation repeatedly fails — a runner declining
// the activation, and the reconciler fencing its own assignment — and requires:
//
//   - the escalating signal to appear at the ladder rungs only (3, 6, 12, 24),
//     so the record count is logarithmic in the failure count rather than one
//     per reconcile tick;
//   - it NOT to appear below the first rung (a single fence or decline is
//     ordinary and must stay silent);
//   - it NOT to appear on healthy reconcile ticks, which is the case that used
//     to reset the retry bookkeeping and would otherwise keep the escalation
//     unreachable in production;
//   - the retained per-activation state to carry the reason and the count, so
//     the reason survives the log record.
func TestEntryActivationReconciler_EscalatesRepeatedActivationFailures(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	// newReconciler wires the store/lister/logger the two subtests share, using
	// the package's existing memory store and runner-lister fakes.
	newReconciler := func(t *testing.T) (*EntryActivationReconciler, *MemoryEntryActivationStore, *mockRunnerLister, RunnerSnapshot, *recordingLogger) {
		t.Helper()
		store := NewMemoryEntryActivationStore()
		act := testEntryActivation()
		if err := store.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		runner := RunnerSnapshot{
			RunnerID: "runner-a",
			Capacity: 4,
			Labels:   map[string]string{"zone": "a"},
		}
		lister := &mockRunnerLister{runners: []RunnerSnapshot{runner}}
		logger := &recordingLogger{}

		r := newTestReconciler(t)
		r.cfg.Store = store
		r.cfg.Lister = lister
		r.cfg.Logger = logger
		return r, store, lister, runner, logger
	}

	t.Run("runner declines", func(t *testing.T) {
		const failures = 24
		// The reason string a runner puts in ack.Error — the one datum that was
		// unrecoverable after the fact.
		const declineReason = `supply "sas-traffic-collection-clean-rules" content unavailable`

		r, store, lister, runner, logger := newReconciler(t)
		key := keyOfActivation(testEntryActivation())
		// Real clock: MarkActivationFailed stamps failures with time.Now(), so a
		// fake clock here would make "failing for" nonsense.
		now := time.Now()

		runner.LastHeartbeat = now
		lister.runners = []RunnerSnapshot{runner}
		if err := r.Reconcile(ctx, now); err != nil {
			t.Fatalf("initial Reconcile: %v", err)
		}

		for i := 1; i <= failures; i++ {
			act, ok, err := store.Get(ctx, key)
			if err != nil || !ok {
				t.Fatalf("failure %d: Get: ok=%v err=%v", i, ok, err)
			}
			if act.RunnerID != "runner-a" {
				t.Fatalf("failure %d: expected the activation assigned to runner-a, got %q", i, act.RunnerID)
			}

			// The real runner-decline path: core.go routes a status=failed ack
			// here, and it carries the only reason the runner ever gives.
			ack := protocol.ActivationAck{
				RunnerID:        "runner-a",
				SessionID:       act.SessionID,
				WorkflowID:      string(act.WorkflowID),
				WorkflowVersion: act.WorkflowVersion,
				GroupID:         act.EntryUnitID,
				ReplicaIndex:    act.ReplicaIndex,
				Generation:      act.Generation,
				Status:          protocol.ActivationStatusFailed,
				Error:           declineReason,
			}
			if err := r.MarkActivationFailed(ctx, "runner-a", ack); err != nil {
				t.Fatalf("failure %d: MarkActivationFailed: %v", i, err)
			}

			// Past the maximum jittered backoff, then let the reconciler
			// reassign, then take one more pass with a live owner — the healthy
			// tick that calls clearRetryBackoff.
			now = now.Add(2 * DefaultActivationRetryBackoffMax)
			runner.LastHeartbeat = now
			lister.runners = []RunnerSnapshot{runner}
			if err := r.Reconcile(ctx, now); err != nil {
				t.Fatalf("failure %d: Reconcile (reassign): %v", i, err)
			}
			if err := r.Reconcile(ctx, now); err != nil {
				t.Fatalf("failure %d: Reconcile (healthy tick): %v", i, err)
			}
		}

		got := escalationCounts(t, logger)
		want := []int{3, 6, 12, 24}
		if len(got) != len(want) {
			t.Fatalf("escalation records after %d consecutive failures = %v, want exactly %v (one per rung, not one per event)", failures, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("escalation records = %v, want %v", got, want)
			}
		}
		if got[0] <= ActivationFailureEscalationStart-1 {
			t.Fatalf("first escalation at %d failures: nothing may be reported below the first rung (%d)", got[0], ActivationFailureEscalationStart)
		}

		// The retained state is the other half of the fix: the reason and the
		// count must outlive the log record.
		r.mu.Lock()
		state, ok := r.retryBackoff[key]
		r.mu.Unlock()
		if !ok {
			t.Fatal("no retained failure state for the failing activation")
		}
		if state.reason != declineReason {
			t.Fatalf("retained reason = %q, want %q", state.reason, declineReason)
		}
		if state.consecutiveFailures != failures {
			t.Fatalf("retained consecutive failures = %d, want %d (a healthy tick must not reset the count)", state.consecutiveFailures, failures)
		}
		if state.firstFailureAt.IsZero() {
			t.Fatal("retained first-failure time is zero; time-since-first-failure would be unreportable")
		}
		if state.runnerID != "runner-a" {
			t.Fatalf("retained runner = %q, want runner-a", state.runnerID)
		}
		if state.generation == 0 {
			t.Fatal("retained generation is zero; the escalation cannot name the generation that failed")
		}

		// Every record must carry the fields an operator triages from, and every
		// decline must still reach the per-event diagnostic (proof the logger is
		// actually wired to this reconciler rather than silently nil).
		first := logger.withMsg(activationEscalationMsg)[0]
		for _, want := range []struct {
			key  string
			want any
		}{
			{"workflow_id", types.WorkflowID("wf-1")},
			{"entry_unit_id", "tg"},
			{"replica_index", uint32(0)},
			{"runner_id", "runner-a"},
			{"consecutive_failures", ActivationFailureEscalationStart},
			{"error", declineReason},
		} {
			if got := first.field(want.key); got != want.want {
				t.Fatalf("escalation record field %q = %v (%T), want %v (%T)", want.key, got, got, want.want, want.want)
			}
		}
		if got := first.field("generation"); got == nil || got == uint64(0) {
			t.Fatalf("escalation record generation = %v, want the failing generation", got)
		}
		failingFor, ok := first.field("failing_for").(time.Duration)
		if !ok || failingFor < 0 {
			t.Fatalf("escalation record failing_for = %v (%T), want a non-negative duration", first.field("failing_for"), first.field("failing_for"))
		}

		declines := logger.withMsg("activation fenced after runner decline")
		if len(declines) != failures {
			t.Fatalf("per-decline diagnostics = %d, want %d (the reconciler's logger must be the configured one, not nil)", len(declines), failures)
		}

		// Healthy ticks must stay silent: five passes with a live, matching
		// owner and no failure may not add a record.
		before := len(logger.withMsg(activationEscalationMsg))
		for i := 0; i < 5; i++ {
			now = now.Add(time.Second)
			runner.LastHeartbeat = now
			lister.runners = []RunnerSnapshot{runner}
			if err := r.Reconcile(ctx, now); err != nil {
				t.Fatalf("healthy Reconcile %d: %v", i, err)
			}
		}
		if after := len(logger.withMsg(activationEscalationMsg)); after != before {
			t.Fatalf("healthy reconcile ticks added %d escalation record(s); the signal must not track the reconcile period", after-before)
		}
	})

	t.Run("reconciler fences the assignment", func(t *testing.T) {
		// This is the path production actually executes when a runner is fenced
		// and re-assigned every few minutes: reconcileExisting revokes an owner
		// whose lease expired, fenceAndDeactivate fences it, and the runner's
		// resulting Deactivate receipt is acknowledged as "deactivated" (never
		// routed to MarkActivationFailed). Before this change that loop recorded
		// nothing at all, on either side.
		const fences = 12

		r, store, lister, runner, logger := newReconciler(t)
		key := keyOfActivation(testEntryActivation())
		now := time.Now()

		runner.LastHeartbeat = now
		lister.runners = []RunnerSnapshot{runner}
		if err := r.Reconcile(ctx, now); err != nil {
			t.Fatalf("initial Reconcile: %v", err)
		}

		var generations []uint64
		for i := 1; i <= fences; i++ {
			act, ok, err := store.Get(ctx, key)
			if err != nil || !ok {
				t.Fatalf("fence %d: Get: ok=%v err=%v", i, ok, err)
			}
			generations = append(generations, act.Generation)

			// The runner stays live; only its lease lapses — past the revive
			// grace, so the pass still fences (inside the grace a live owner is
			// renewed instead, which is not the path this test is about).
			now = now.Add(r.cfg.LeaseTTL + r.leaseGrace() + time.Second)
			runner.LastHeartbeat = now
			lister.runners = []RunnerSnapshot{runner}
			if err := r.Reconcile(ctx, now); err != nil {
				t.Fatalf("fence %d: Reconcile: %v", i, err)
			}
		}

		got := escalationCounts(t, logger)
		want := []int{3, 6, 12}
		if len(got) != len(want) {
			t.Fatalf("escalation records after %d fences = %v, want exactly %v", fences, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("escalation records after %d fences = %v, want %v", fences, got, want)
			}
		}

		// The reason must name the fence decision, not a runner decline: that
		// distinction is the whole point of recording the fence path.
		r.mu.Lock()
		state, ok := r.retryBackoff[key]
		r.mu.Unlock()
		if !ok {
			t.Fatal("no retained failure state after repeated fences")
		}
		if state.reason != fenceReasonLeaseExpired {
			t.Fatalf("retained reason after repeated fences = %q, want %q", state.reason, fenceReasonLeaseExpired)
		}
		if state.consecutiveFailures != fences {
			t.Fatalf("retained consecutive fences = %d, want %d", state.consecutiveFailures, fences)
		}
		if state.generation == 0 {
			t.Fatal("retained generation is zero after a fence")
		}
		if last := logger.withMsg(activationEscalationMsg); last[len(last)-1].field("error") != fenceReasonLeaseExpired {
			t.Fatalf("escalation error field = %v, want %q", last[len(last)-1].field("error"), fenceReasonLeaseExpired)
		}

		// Each fence was followed by a reassignment (the loop the reason
		// explains), and the generation must have advanced with each one.
		final, _, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get after fences: %v", err)
		}
		if final.Generation <= generations[len(generations)-1] {
			t.Fatalf("generation %d did not advance past %d after reassignment", final.Generation, generations[len(generations)-1])
		}
	})
}

// TestActivationFailureEscalates pins the escalation ladder itself: exactly the
// rungs (ActivationFailureEscalationStart * 2^k) are reported, and nothing
// below the first one. The rungs are derived from the definition rather than
// written out, so this test cannot inherit a mistake in the listing it checks.
func TestActivationFailureEscalates(t *testing.T) {
	rungs := map[int]bool{}
	for n := ActivationFailureEscalationStart; n <= 100000; n *= 2 {
		rungs[n] = true
	}
	for n := 0; n <= 1000; n++ {
		if got, want := activationFailureEscalates(n), rungs[n]; got != want {
			t.Fatalf("activationFailureEscalates(%d) = %v, want %v", n, got, want)
		}
	}
	// The two properties that bound the output, stated directly: below the first
	// rung nothing is reported, and the record count for n failures grows
	// logarithmically rather than linearly.
	for n := 0; n < ActivationFailureEscalationStart; n++ {
		if activationFailureEscalates(n) {
			t.Fatalf("activationFailureEscalates(%d) must be false: nothing is reported below the first rung (%d)", n, ActivationFailureEscalationStart)
		}
	}
	records := 0
	for n := 1; n <= 100000; n++ {
		if activationFailureEscalates(n) {
			records++
		}
	}
	if records > 20 {
		t.Fatalf("%d records for 100000 consecutive failures: the ladder must stay logarithmic", records)
	}
}
