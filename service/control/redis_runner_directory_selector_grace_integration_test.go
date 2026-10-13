package control

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// realRedisGraceHarness bundles a real-Redis runner directory (uuid key prefix,
// torn down by newRealRedisRunnerDirectory) with the doubles the grace
// assertions read. It deliberately uses the harness's literal directory: the
// zero-value selector is the shipped DefaultRunnerSelector, so these tests
// exercise the production 30s window instead of a shortened test-only one.
type realRedisGraceHarness struct {
	dir      *RedisRunnerDirectory
	rdb      *redis.Client
	observer *recordingSelectorRejectObserver
	logger   *recordingWarnLogger
}

func newRealRedisGraceHarness(t *testing.T) *realRedisGraceHarness {
	t.Helper()
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_REDIS_INTEGRATION=1: XFLOW_TEST_REDIS_ADDR not set (use 127.0.0.1:6380)")
		}
		t.Skip("XFLOW_TEST_REDIS_ADDR not set; skipping the real-Redis selector-grace test")
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		t.Fatalf("ping real Redis at %s: %v", addr, err)
	}
	h := &realRedisGraceHarness{
		rdb:      rdb,
		observer: newRecordingSelectorRejectObserver(),
		logger:   newRecordingWarnLogger(),
	}
	h.dir = newRealRedisRunnerDirectory(t, rdb) // owns cleanup and closes rdb
	h.dir.observer = h.observer
	h.dir.log = h.logger
	return h
}

// register brings up a runner with the given labels and capabilities. The
// policy always allows the assignment's node type, so a test that varies only
// the capability exercises the capability check rather than the policy one.
func (h *realRedisGraceHarness) register(t *testing.T, id string, labels map[string]string, caps []protocol.Capability) RunnerSession {
	t.Helper()
	session, err := h.dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     id,
		Capacity:     4,
		Labels:       labels,
		Capabilities: caps,
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Now:          time.Now(),
	})
	if err != nil {
		t.Fatalf("Register(%s): %v", id, err)
	}
	return session
}

func (h *realRedisGraceHarness) claim(t *testing.T, session RunnerSession) (Claim, bool) {
	t.Helper()
	claim, ok, err := h.dir.ClaimForRunner(context.Background(), ClaimRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Now:       time.Now(),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner(%s): %v", session.RunnerID, err)
	}
	return claim, ok
}

// ageQueuedAt rewrites the assignment's queued-at stamp d into the past,
// measured on the Redis server clock the claim path compares against. It is
// what d of real waiting does to the stamp, without the wait.
func (h *realRedisGraceHarness) ageQueuedAt(t *testing.T, id AssignmentID, d time.Duration) {
	t.Helper()
	ctx := context.Background()
	serverNow, err := h.rdb.Time(ctx).Result()
	if err != nil {
		t.Fatalf("read Redis server time: %v", err)
	}
	aged := strconv.FormatInt(serverNow.Add(-d).UnixMilli(), 10)
	if err := h.rdb.HSet(ctx, h.dir.keys.assignmentQueuedAt, string(id), aged).Err(); err != nil {
		t.Fatalf("rewrite the queued-at stamp of %s: %v", id, err)
	}
}

// realRedisGraceAssignment is a node-exec assignment (xflow.function, default
// namespace, in the scratch execution the redis test helper uses) whose
// selector is the only decision under test.
func realRedisGraceAssignment(id AssignmentID, sel *types.RunnerSelector) Assignment {
	a := redisDirectoryTestAssignment(id)
	a.Routing.RunnerSelector = sel
	return a
}

// recordingWarnLogger captures the claim path's Warn calls. The default-mode
// grace fallback is the only Warn this path emits, so "reported exactly once"
// is assertable without matching unrelated messages.
type recordingWarnLogger struct {
	mu    sync.Mutex
	warns []recordedWarn
}

type recordedWarn struct {
	msg string
	kv  map[string]any
}

func newRecordingWarnLogger() *recordingWarnLogger { return &recordingWarnLogger{} }

func (l *recordingWarnLogger) Warn(msg string, kv ...any) {
	record := recordedWarn{msg: msg, kv: make(map[string]any, len(kv)/2)}
	for i := 0; i+1 < len(kv); i += 2 {
		key, ok := kv[i].(string)
		if !ok {
			continue
		}
		record.kv[key] = kv[i+1]
	}
	l.mu.Lock()
	l.warns = append(l.warns, record)
	l.mu.Unlock()
}

// fallbacks returns the captured grace-fallback lines, identified by the
// message the shared fallback logger writes.
func (l *recordingWarnLogger) fallbacks() []recordedWarn {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []recordedWarn
	for _, w := range l.warns {
		if strings.Contains(w.msg, "selector grace elapsed") {
			out = append(out, w)
		}
	}
	return out
}

func (l *recordingWarnLogger) Debug(string, ...any)  {}
func (l *recordingWarnLogger) Debugf(string, ...any) {}
func (l *recordingWarnLogger) Info(string, ...any)   {}
func (l *recordingWarnLogger) Infof(string, ...any)  {}
func (l *recordingWarnLogger) Warnf(string, ...any)  {}
func (l *recordingWarnLogger) Error(string, ...any)  {}
func (l *recordingWarnLogger) Errorf(string, ...any) {}
func (l *recordingWarnLogger) Panic(string, ...any)  {}
func (l *recordingWarnLogger) Panicf(string, ...any) {}

var _ engine.Logger = (*recordingWarnLogger)(nil)

// TestRedisClaimGraceDefaultModeFallsBackAfterWindow is the #21 contract on the
// real Redis claim path (the memory directory has its own copy; this one pins
// the Lua-backed walk): a default-mode selector whose labels no runner
// satisfies is a preference, enforced while the window runs and relaxed once
// it elapses, with the relaxation classed as pending — not as a required-mode
// hard filter — while it waits, and reported in the log when it lands.
func TestRedisClaimGraceDefaultModeFallsBackAfterWindow(t *testing.T) {
	h := newRealRedisGraceHarness(t)
	session := h.register(t, "grace-aws", map[string]string{"cloud": "aws"}, []protocol.Capability{{NodeType: "xflow.function"}})

	// Default mode (Mode left empty, which the compiler treats as default).
	assignment := realRedisGraceAssignment("exec-grace/default-mode/activation-1",
		&types.RunnerSelector{MatchLabels: map[string]string{"cloud": "tencent"}})
	mustEnqueueRedisDirectoryAssignment(t, context.Background(), h.dir, assignment)

	if claim, ok := h.claim(t, session); ok {
		t.Fatalf("claimed %q inside the grace window; default-mode labels are enforced until the window elapses",
			claim.Assignment.AssignmentID)
	}
	if got := h.observer.count(SelectorRejectLabelPending); got != 1 {
		t.Fatalf("label_pending rejections = %d, want 1", got)
	}
	if got := h.observer.count(SelectorRejectLabelRequired); got != 0 {
		t.Fatalf("label_required rejections = %d, want 0 for a default-mode selector", got)
	}

	h.ageQueuedAt(t, assignment.AssignmentID, DefaultSelectorFallback+time.Second)

	claim, ok := h.claim(t, session)
	if !ok {
		t.Fatal("default-mode selector did not fall back after its grace window elapsed; " +
			"the claim path re-reads the pending page and the matured entry must be claimable")
	}
	if claim.Assignment.AssignmentID != assignment.AssignmentID {
		t.Fatalf("claimed %q, want %q", claim.Assignment.AssignmentID, assignment.AssignmentID)
	}
	fallbacks := h.logger.fallbacks()
	if len(fallbacks) != 1 {
		t.Fatalf("grace-fallback log lines = %d, want exactly 1 (the successful grace claim)", len(fallbacks))
	}
	if got := fallbacks[0].kv["assignment_id"]; got != string(assignment.AssignmentID) {
		t.Fatalf("fallback log names assignment %v, want %q", got, assignment.AssignmentID)
	}
}

// TestRedisClaimGraceRequiredModeNeverFallsBack pins the other half on Redis: a
// required-mode selector is a hard filter. No amount of waiting relaxes it, it
// is never classed as a grace wait, and no fallback line is written.
func TestRedisClaimGraceRequiredModeNeverFallsBack(t *testing.T) {
	h := newRealRedisGraceHarness(t)
	session := h.register(t, "grace-aws", map[string]string{"cloud": "aws"}, []protocol.Capability{{NodeType: "xflow.function"}})

	assignment := realRedisGraceAssignment("exec-grace/required-mode/activation-1",
		&types.RunnerSelector{
			Mode:        types.RunnerSelectorModeRequired,
			MatchLabels: map[string]string{"cloud": "tencent"},
		})
	mustEnqueueRedisDirectoryAssignment(t, context.Background(), h.dir, assignment)

	if claim, ok := h.claim(t, session); ok {
		t.Fatalf("claimed %q; a required selector must never fall back", claim.Assignment.AssignmentID)
	}
	if got := h.observer.count(SelectorRejectLabelRequired); got != 1 {
		t.Fatalf("label_required rejections = %d, want 1", got)
	}

	// Far past any window, in case the mode were being misread.
	h.ageQueuedAt(t, assignment.AssignmentID, time.Hour)

	if claim, ok := h.claim(t, session); ok {
		t.Fatalf("claimed %q after an hour; a required selector must never fall back", claim.Assignment.AssignmentID)
	}
	if got := h.observer.count(SelectorRejectLabelRequired); got != 2 {
		t.Fatalf("label_required rejections = %d, want 2 (one per claim attempt)", got)
	}
	if got := h.observer.count(SelectorRejectLabelPending); got != 0 {
		t.Fatalf("label_pending rejections = %d, want 0 for a required-mode selector", got)
	}
	if got := len(h.logger.fallbacks()); got != 0 {
		t.Fatalf("grace-fallback log lines = %d, want 0 for a required-mode selector", got)
	}
}

// TestRedisClaimGraceDoesNotRelaxCapability is the Redis authorization guard:
// the grace fallback relaxes LABELS ONLY, and a capability mismatch is not a
// label wait at all. A runner that cannot host the node type stays ineligible
// after the window elapses exactly as it did before, and the rejection is
// classed as capability, never as pending.
func TestRedisClaimGraceDoesNotRelaxCapability(t *testing.T) {
	h := newRealRedisGraceHarness(t)
	// Declares a different node type: capability mismatch against the
	// assignment's xflow.function, one branch ahead of policy.
	session := h.register(t, "grace-nocap", map[string]string{"cloud": "aws"}, []protocol.Capability{{NodeType: "xflow.map"}})

	assignment := realRedisGraceAssignment("exec-grace/capability/activation-1",
		&types.RunnerSelector{MatchLabels: map[string]string{"cloud": "tencent"}})
	mustEnqueueRedisDirectoryAssignment(t, context.Background(), h.dir, assignment)

	if claim, ok := h.claim(t, session); ok {
		t.Fatalf("claimed %q on a capability-mismatched runner", claim.Assignment.AssignmentID)
	}
	if got := h.observer.count(SelectorRejectCapability); got == 0 {
		t.Fatal("capability rejection was not reported")
	}

	h.ageQueuedAt(t, assignment.AssignmentID, DefaultSelectorFallback+time.Second)

	if claim, ok := h.claim(t, session); ok {
		t.Fatalf("claimed %q after the grace window; capability is never a grace wait", claim.Assignment.AssignmentID)
	}
	if got := h.observer.count(SelectorRejectLabelPending); got != 0 {
		t.Fatalf("label_pending rejections = %d, want 0: capability is checked before labels", got)
	}
	if got := len(h.logger.fallbacks()); got != 0 {
		t.Fatalf("grace-fallback log lines = %d, want 0 for a capability mismatch", got)
	}
}

// TestRedisClaimGraceUnstampedAssignmentWaits pins the missing-stamp rule on
// Redis: an entry an older control plane queued carries no queued-at stamp,
// and it must NOT be treated as having already waited out the window.
func TestRedisClaimGraceUnstampedAssignmentWaits(t *testing.T) {
	h := newRealRedisGraceHarness(t)
	session := h.register(t, "grace-aws", map[string]string{"cloud": "aws"}, []protocol.Capability{{NodeType: "xflow.function"}})

	assignment := realRedisGraceAssignment("exec-grace/unstamped/activation-1",
		&types.RunnerSelector{MatchLabels: map[string]string{"cloud": "tencent"}})
	mustEnqueueRedisDirectoryAssignment(t, context.Background(), h.dir, assignment)

	// Simulate the pre-stamp writer.
	if err := h.rdb.HDel(context.Background(), h.dir.keys.assignmentQueuedAt, string(assignment.AssignmentID)).Err(); err != nil {
		t.Fatalf("remove the queued-at stamp: %v", err)
	}

	if claim, ok := h.claim(t, session); ok {
		t.Fatalf("claimed %q with no queued-at stamp; an unstamped entry must not skip the window",
			claim.Assignment.AssignmentID)
	}
	if got := h.observer.count(SelectorRejectLabelPending); got != 1 {
		t.Fatalf("label_pending rejections = %d, want 1", got)
	}
	if got := len(h.logger.fallbacks()); got != 0 {
		t.Fatalf("grace-fallback log lines = %d, want 0 for an unstamped entry", got)
	}
}
