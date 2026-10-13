package control

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// recordingSelectorRejectObserver records claim-path selector rejections by
// reason. It is goroutine-safe because a claim walk may run under the
// directory's lock from any test goroutine.
type recordingSelectorRejectObserver struct {
	mu      sync.Mutex
	reasons map[string]int
}

func newRecordingSelectorRejectObserver() *recordingSelectorRejectObserver {
	return &recordingSelectorRejectObserver{reasons: make(map[string]int)}
}

func (o *recordingSelectorRejectObserver) OnRunnerSelectorRejected(_ context.Context, reason string) {
	o.mu.Lock()
	o.reasons[reason]++
	o.mu.Unlock()
}

// The other two RunnerClaimObserver methods exist so the same value satisfies
// the base interface the directories require.
func (o *recordingSelectorRejectObserver) OnRunnerClaimReclaimed(context.Context, int) {}
func (o *recordingSelectorRejectObserver) OnRunnerLeaseReplayed(context.Context)       {}

func (o *recordingSelectorRejectObserver) count(reason string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.reasons[reason]
}

// graceTestClock is a settable clock the memory directory's claim path reads
// through clockNow, so a 30-second window is one assignment instead of a sleep.
type graceTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newGraceTestClock(now time.Time) *graceTestClock { return &graceTestClock{now: now} }

func (c *graceTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *graceTestClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// graceTestRunner registers a runner with the given labels and reports its
// session ID. Capability and policy always cover http.request, so a test that
// varies only the selector is varying only the label decision.
func graceTestRunner(t *testing.T, dir *MemoryRunnerDirectory, id string, labels map[string]string) string {
	t.Helper()
	session, err := dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     id,
		Capacity:     4,
		Labels:       labels,
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"http.request"}},
		Now:          dir.clockNow(),
	})
	if err != nil {
		t.Fatalf("Register(%s): %v", id, err)
	}
	return session.SessionID
}

// graceTestAssignment builds a node-exec assignment with the given selector.
func graceTestAssignment(id AssignmentID, sel *types.RunnerSelector) Assignment {
	return Assignment{
		AssignmentID: id,
		Task: engine.Task{
			ExecutionID:  "exec-grace",
			NodeName:     "node-" + string(id),
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
		Routing: engine.TaskRouting{
			NodeType:       "http.request",
			RunnerSelector: sel,
		},
	}
}

func claimFor(t *testing.T, dir *MemoryRunnerDirectory, sessionID, runnerID string) (Claim, bool) {
	t.Helper()
	claim, ok, err := dir.ClaimForRunner(context.Background(), ClaimRequest{
		RunnerID:  runnerID,
		SessionID: sessionID,
		Now:       dir.clockNow(),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner(%s): %v", runnerID, err)
	}
	return claim, ok
}

// TestClaimGrace_DefaultModeFallsBackAfterWindow is the core #21 contract: a
// default-mode selector whose labels no live runner satisfies is a PREFERENCE.
// It is enforced while the window runs and relaxed once the window elapses —
// the same rule entry activation already applies — while capability, policy
// and namespace stay enforced throughout.
func TestClaimGrace_DefaultModeFallsBackAfterWindow(t *testing.T) {
	ctx := context.Background()
	clock := newGraceTestClock(time.Unix(1000, 0))
	observer := newRecordingSelectorRejectObserver()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryClock(clock.Now),
		WithMemoryRunnerDirectoryObserver(observer),
		// A short window keeps the test about the rule, not about the default.
		WithMemoryRunnerDirectorySelector(RunnerSelector{LiveTTL: time.Minute, FallbackGrace: 5 * time.Second}),
	)
	session := graceTestRunner(t, dir, "runner-aws", map[string]string{"cloud": "aws"})

	// Default mode (Mode left empty, which the compiler treats as default).
	task := graceTestAssignment("a1", &types.RunnerSelector{MatchLabels: map[string]string{"cloud": "tencent"}})
	if _, err := dir.EnqueueAssignment(ctx, task); err != nil {
		t.Fatalf("EnqueueAssignment: %v", err)
	}

	// Inside the window: not claimable, and the rejection is classified as
	// pending (the soft-mode wait), not as a required-mode hard filter.
	if claim, ok := claimFor(t, dir, session, "runner-aws"); ok {
		t.Fatalf("claimed %q inside the grace window; default-mode labels are enforced until the window elapses", claim.Assignment.AssignmentID)
	}
	if got := observer.count(SelectorRejectLabelPending); got != 1 {
		t.Fatalf("label_pending rejections = %d, want 1", got)
	}
	if got := observer.count(SelectorRejectLabelRequired); got != 0 {
		t.Fatalf("label_required rejections = %d, want 0 for a default-mode selector", got)
	}

	// Advance to just past the window.
	clock.Advance(5*time.Second + time.Millisecond)

	claim, ok := claimFor(t, dir, session, "runner-aws")
	if !ok {
		t.Fatal("default-mode selector did not fall back after its grace window elapsed")
	}
	if claim.Assignment.AssignmentID != task.AssignmentID {
		t.Fatalf("claimed %q, want %q", claim.Assignment.AssignmentID, task.AssignmentID)
	}
}

// TestClaimGrace_RequiredModeNeverFallsBack pins the other half: a
// "required"-mode selector is a hard filter. No amount of waiting relaxes it,
// and the rejection is classified as such.
func TestClaimGrace_RequiredModeNeverFallsBack(t *testing.T) {
	ctx := context.Background()
	clock := newGraceTestClock(time.Unix(1000, 0))
	observer := newRecordingSelectorRejectObserver()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryClock(clock.Now),
		WithMemoryRunnerDirectoryObserver(observer),
		WithMemoryRunnerDirectorySelector(RunnerSelector{LiveTTL: time.Minute, FallbackGrace: 5 * time.Second}),
	)
	session := graceTestRunner(t, dir, "runner-aws", map[string]string{"cloud": "aws"})

	task := graceTestAssignment("a1", &types.RunnerSelector{
		Mode:        types.RunnerSelectorModeRequired,
		MatchLabels: map[string]string{"cloud": "tencent"},
	})
	if _, err := dir.EnqueueAssignment(ctx, task); err != nil {
		t.Fatalf("EnqueueAssignment: %v", err)
	}

	if claim, ok := claimFor(t, dir, session, "runner-aws"); ok {
		t.Fatalf("claimed %q; a required selector must never fall back", claim.Assignment.AssignmentID)
	}
	// Far past any window, in case the mode were being misread.
	clock.Advance(time.Hour)
	if claim, ok := claimFor(t, dir, session, "runner-aws"); ok {
		t.Fatalf("claimed %q after an hour; a required selector must never fall back", claim.Assignment.AssignmentID)
	}
	if got := observer.count(SelectorRejectLabelRequired); got == 0 {
		t.Fatal("required-mode rejection was not reported")
	}
	if got := observer.count(SelectorRejectLabelPending); got != 0 {
		t.Fatalf("label_pending rejections = %d, want 0 for a required-mode selector", got)
	}
}

// TestClaimGrace_DoesNotRelaxCapabilityOrNamespace is the authorization guard:
// the grace fallback relaxes LABELS ONLY. A runner that cannot host the node
// type, or is outside the assignment's namespace, stays ineligible after the
// window elapses exactly as it did before.
func TestClaimGrace_DoesNotRelaxCapabilityOrNamespace(t *testing.T) {
	ctx := context.Background()
	clock := newGraceTestClock(time.Unix(1000, 0))
	observer := newRecordingSelectorRejectObserver()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryClock(clock.Now),
		WithMemoryRunnerDirectoryObserver(observer),
		WithMemoryRunnerDirectorySelector(RunnerSelector{LiveTTL: time.Minute, FallbackGrace: 5 * time.Second}),
	)

	// A runner whose policy allows the type but which declares a DIFFERENT
	// node type: capability mismatch, never relaxed.
	capsession, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-nocap",
		Capacity:     4,
		Labels:       map[string]string{"cloud": "aws"},
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"http.request"}},
		Now:          clock.Now(),
	})
	if err != nil {
		t.Fatalf("Register(runner-nocap): %v", err)
	}

	task := graceTestAssignment("a1", &types.RunnerSelector{MatchLabels: map[string]string{"cloud": "tencent"}})
	if _, err := dir.EnqueueAssignment(ctx, task); err != nil {
		t.Fatalf("EnqueueAssignment: %v", err)
	}
	clock.Advance(time.Hour)
	if claim, ok := claimFor(t, dir, capsession.SessionID, "runner-nocap"); ok {
		t.Fatalf("claimed %q on a capability-mismatched runner after the grace window", claim.Assignment.AssignmentID)
	}
	if got := observer.count(SelectorRejectCapability); got == 0 {
		t.Fatal("capability rejection was not reported")
	}
	if got := observer.count(SelectorRejectLabelPending); got != 0 {
		t.Fatalf("label_pending = %d, want 0: capability is checked before labels and is never a grace wait", got)
	}

	// A namespace-mismatched runner: same shape, authorization boundary.
	nssession, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-other-ns",
		Capacity:     4,
		Labels:       map[string]string{"cloud": "aws"},
		Capabilities: []protocol.Capability{{NodeType: "http.request"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"http.request"}},
		Namespaces:   []namespace.Namespace{"namespace-other"},
		Now:          clock.Now(),
	})
	if err != nil {
		t.Fatalf("Register(runner-other-ns): %v", err)
	}
	if claim, ok := claimFor(t, dir, nssession.SessionID, "runner-other-ns"); ok {
		t.Fatalf("claimed %q on a namespace-mismatched runner after the grace window", claim.Assignment.AssignmentID)
	}
	if got := observer.count(SelectorRejectNamespace); got == 0 {
		t.Fatal("namespace rejection was not reported")
	}
}

// TestClaimGrace_UnstampedAssignmentWaits pins the missing-stamp rule: an
// entry an older control plane queued carries no queued-at stamp, and it must
// NOT be treated as having already waited out the window.
func TestClaimGrace_UnstampedAssignmentWaits(t *testing.T) {
	ctx := context.Background()
	clock := newGraceTestClock(time.Unix(1000, 0))
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryClock(clock.Now),
		WithMemoryRunnerDirectorySelector(RunnerSelector{LiveTTL: time.Minute, FallbackGrace: 5 * time.Second}),
	)
	session := graceTestRunner(t, dir, "runner-aws", map[string]string{"cloud": "aws"})

	task := graceTestAssignment("a1", &types.RunnerSelector{MatchLabels: map[string]string{"cloud": "tencent"}})
	if _, err := dir.EnqueueAssignment(ctx, task); err != nil {
		t.Fatalf("EnqueueAssignment: %v", err)
	}
	// Simulate the pre-stamp writer.
	dir.mu.Lock()
	delete(dir.queuedAt, task.AssignmentID)
	dir.mu.Unlock()

	clock.Advance(time.Hour)
	if claim, ok := claimFor(t, dir, session, "runner-aws"); ok {
		t.Fatalf("claimed %q with no queued-at stamp; an unstamped entry must not skip the window", claim.Assignment.AssignmentID)
	}
}

// TestClaimGrace_MatchingRunnerStillWinsInsideWindow guards the ordinary path
// from the fallback machinery: a label-matching runner claims immediately,
// with no grace wait and no rejection counted.
func TestClaimGrace_MatchingRunnerStillWinsInsideWindow(t *testing.T) {
	ctx := context.Background()
	clock := newGraceTestClock(time.Unix(1000, 0))
	observer := newRecordingSelectorRejectObserver()
	dir := NewMemoryRunnerDirectory(
		WithMemoryRunnerDirectoryClock(clock.Now),
		WithMemoryRunnerDirectoryObserver(observer),
		WithMemoryRunnerDirectorySelector(RunnerSelector{LiveTTL: time.Minute, FallbackGrace: time.Hour}),
	)
	session := graceTestRunner(t, dir, "runner-tencent", map[string]string{"cloud": "tencent"})

	task := graceTestAssignment("a1", &types.RunnerSelector{MatchLabels: map[string]string{"cloud": "tencent"}})
	if _, err := dir.EnqueueAssignment(ctx, task); err != nil {
		t.Fatalf("EnqueueAssignment: %v", err)
	}
	claim, ok := claimFor(t, dir, session, "runner-tencent")
	if !ok {
		t.Fatal("label-matching runner did not claim")
	}
	if claim.Assignment.AssignmentID != task.AssignmentID {
		t.Fatalf("claimed %q, want %q", claim.Assignment.AssignmentID, task.AssignmentID)
	}
	if total := observer.count(SelectorRejectLabelPending) + observer.count(SelectorRejectLabelRequired); total != 0 {
		t.Fatalf("label rejections = %d, want 0 when a matching runner claimed first", total)
	}
}

// TestSelectorGraceElapsed exercises the shared predicate directly, including
// the nil-selector case (no labels to relax: never a grace decision).
func TestSelectorGraceElapsed(t *testing.T) {
	sel := RunnerSelector{FallbackGrace: 30 * time.Second}
	queued := time.Unix(1000, 0)
	defaultSel := &types.RunnerSelector{MatchLabels: map[string]string{"a": "b"}}
	emptyMode := &types.RunnerSelector{} // Mode "" == default per the compiler
	requiredSel := &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"a": "b"}}

	tests := []struct {
		name     string
		selector *types.RunnerSelector
		queuedAt time.Time
		now      time.Time
		want     bool
	}{
		{"nil selector never elapses", nil, queued, queued.Add(time.Hour), false},
		{"default inside window", defaultSel, queued, queued.Add(29 * time.Second), false},
		{"default at the boundary", defaultSel, queued, queued.Add(30 * time.Second), true},
		{"default past window", defaultSel, queued, queued.Add(31 * time.Second), true},
		{"empty mode is default", emptyMode, queued, queued.Add(31 * time.Second), true},
		{"required never elapses", requiredSel, queued, queued.Add(24 * time.Hour), false},
		{"missing stamp never elapses", defaultSel, time.Time{}, queued.Add(24 * time.Hour), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sel.selectorGraceElapsed(tt.selector, tt.queuedAt, tt.now); got != tt.want {
				t.Fatalf("selectorGraceElapsed = %v, want %v", got, tt.want)
			}
		})
	}
}

var _ RunnerClaimObserver = (*recordingSelectorRejectObserver)(nil)
var _ RunnerSelectorRejectObserver = (*recordingSelectorRejectObserver)(nil)
var _ engine.Logger = (*noopEngineLogger)(nil)

// noopEngineLogger satisfies engine.Logger for tests that only care that a
// logger was accepted.
type noopEngineLogger struct{}

func (noopEngineLogger) Debug(string, ...any)  {}
func (noopEngineLogger) Debugf(string, ...any) {}
func (noopEngineLogger) Info(string, ...any)   {}
func (noopEngineLogger) Infof(string, ...any)  {}
func (noopEngineLogger) Warn(string, ...any)   {}
func (noopEngineLogger) Warnf(string, ...any)  {}
func (noopEngineLogger) Error(string, ...any)  {}
func (noopEngineLogger) Errorf(string, ...any) {}
func (noopEngineLogger) Panic(string, ...any)  {}
func (noopEngineLogger) Panicf(string, ...any) {}
