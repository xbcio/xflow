package control

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// The grace verdict is taken after a page's candidates are collected and before
// the walk returns, and it must not change the walk's cursor or budget
// semantics. These tests pin that: an immature page must not pin the poll to
// itself (the head-of-line bug), a partially matured page still reports the
// siblings that are waiting, and the stamp read degrades instead of failing the
// walk.

// registerGraceOrderRunner registers the labelled runner these tests claim
// with. The policy and capability always allow the assignment's node type, so
// the selector is the only decision under test.
func registerGraceOrderRunner(t *testing.T, directory *RedisRunnerDirectory, id string, labels map[string]string) RunnerSession {
	t.Helper()
	session, err := directory.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     id,
		Capacity:     4,
		Labels:       labels,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register(%s): %v", id, err)
	}
	return session
}

// immatureLabelMissAssignment is a default-mode assignment whose labels this
// runner does not satisfy, queued fresh so its grace window has just started.
func immatureLabelMissAssignment(id AssignmentID) Assignment {
	a := redisDirectoryTestAssignment(id)
	a.Routing.RunnerSelector = &types.RunnerSelector{MatchLabels: map[string]string{"cloud": "tencent"}}
	return a
}

// enqueueImmaturePage enqueues one full queue page of immature default-mode
// label misses, so whatever the caller enqueues next lands on the second page.
func enqueueImmaturePage(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory) []Assignment {
	t.Helper()
	page := make([]Assignment, 0, redisClaimQueuePage)
	for i := 0; i < redisClaimQueuePage; i++ {
		a := immatureLabelMissAssignment(AssignmentID(fmt.Sprintf("exec-grace-order/immature-%02d/activation-1", i)))
		mustEnqueueRedisDirectoryAssignment(t, ctx, directory, a)
		page = append(page, a)
	}
	return page
}

func claimGraceOrderAssignment(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, session RunnerSession) (Claim, bool) {
	t.Helper()
	claim, ok, err := directory.ClaimForRunner(ctx, ClaimRequest{
		RunnerID: session.RunnerID, SessionID: session.SessionID, Now: time.Unix(11, 0),
	})
	if err != nil {
		t.Fatalf("ClaimForRunner: %v", err)
	}
	return claim, ok
}

// ageAssignmentQueuedAt rewrites an assignment's queued-at stamp into the past,
// measured against the Redis server clock the claim path compares against: what
// waiting does to the stamp, without the wait.
func ageAssignmentQueuedAt(t *testing.T, ctx context.Context, rdb *redis.Client, directory *RedisRunnerDirectory, id AssignmentID, d time.Duration) {
	t.Helper()
	serverNow, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatalf("read the Redis server clock: %v", err)
	}
	aged := strconv.FormatInt(serverNow.Add(-d).UnixMilli(), 10)
	if err := rdb.HSet(ctx, directory.keys.assignmentQueuedAt, string(id), aged).Err(); err != nil {
		t.Fatalf("rewrite the queued-at stamp of %s: %v", id, err)
	}
}

// TestRedisClaimGraceAdvancesPastAnImmaturePage is the head-of-line regression:
// a full page of immature default-mode label misses must not pin the walk to
// itself. The claimable entry queued behind them is reached on the first poll,
// because the fall-through keeps the walk's cursor semantics — the page is
// stepped over and revisited by the next sweep, not re-read by every poll.
func TestRedisClaimGraceAdvancesPastAnImmaturePage(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	observer := newRecordingSelectorRejectObserver()
	directory.observer = observer
	session := registerGraceOrderRunner(t, directory, "grace-order", map[string]string{"cloud": "aws"})

	enqueueImmaturePage(t, ctx, directory)
	claimable := redisDirectoryTestAssignment("exec-grace-order/claimable/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, claimable)

	claim, ok := claimGraceOrderAssignment(t, ctx, directory, session)
	if !ok {
		t.Fatalf("no claim: %d immature default-mode entries pinned the walk to their page, "+
			"so the claimable entry queued behind them was never reached", redisClaimQueuePage)
	}
	if claim.Assignment.AssignmentID != claimable.AssignmentID {
		t.Fatalf("claimed %q, want the second page's %q", claim.Assignment.AssignmentID, claimable.AssignmentID)
	}
	if got := observer.count(SelectorRejectLabelPending); got != redisClaimQueuePage {
		t.Fatalf("label_pending rejections = %d, want %d (one per immature entry on the skipped page)",
			got, redisClaimQueuePage)
	}
	if got := observer.count(SelectorRejectLabelRequired); got != 0 {
		t.Fatalf("label_required rejections = %d, want 0 for default-mode selectors", got)
	}
}

// TestRedisClaimGraceUnstampedPageStillAdvances pins both halves of the
// missing-stamp rule on the walk: an unstamped entry — one an older control
// plane queued before stamps existed — never matures (no stamp, no measurable
// window), and because it never matures it must not pin the poll to its page
// either. The claimable entry behind the unstamped page is still reached on the
// first poll, and the unstamped entries keep counting as pending.
func TestRedisClaimGraceUnstampedPageStillAdvances(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	observer := newRecordingSelectorRejectObserver()
	directory.observer = observer
	session := registerGraceOrderRunner(t, directory, "grace-order", map[string]string{"cloud": "aws"})

	page := enqueueImmaturePage(t, ctx, directory)
	ids := make([]string, 0, len(page))
	for _, a := range page {
		ids = append(ids, string(a.AssignmentID))
	}
	// Simulate the pre-stamp writer.
	if err := rdb.HDel(ctx, directory.keys.assignmentQueuedAt, ids...).Err(); err != nil {
		t.Fatalf("remove the queued-at stamps: %v", err)
	}
	claimable := redisDirectoryTestAssignment("exec-grace-order/claimable/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, claimable)

	claim, ok := claimGraceOrderAssignment(t, ctx, directory, session)
	if !ok {
		t.Fatalf("no claim: %d unstamped entries pinned the walk to their page without bound", redisClaimQueuePage)
	}
	if claim.Assignment.AssignmentID != claimable.AssignmentID {
		t.Fatalf("claimed %q, want the second page's %q", claim.Assignment.AssignmentID, claimable.AssignmentID)
	}
	if got := observer.count(SelectorRejectLabelPending); got != redisClaimQueuePage {
		t.Fatalf("label_pending rejections = %d, want %d (an unstamped entry counts as waiting, not matured)",
			got, redisClaimQueuePage)
	}

	// A later sweep must not find them matured either: with the claimable entry
	// gone, two more polls complete a wrap and re-read the unstamped page, and
	// it still yields nothing claimable.
	for i := 0; i < 2; i++ {
		if claim, ok := claimGraceOrderAssignment(t, ctx, directory, session); ok {
			t.Fatalf("claimed %q on poll %d; an unstamped entry has no measurable window and must never mature",
				claim.Assignment.AssignmentID, i+2)
		}
	}
	if got := observer.count(SelectorRejectLabelPending); got < 2*redisClaimQueuePage {
		t.Fatalf("label_pending rejections after the wrap = %d, want at least %d: the unstamped page must be re-read by the sweep",
			got, 2*redisClaimQueuePage)
	}
}

// TestMatureGraceCandidatesKeepsUnstampedPending pins the unstamped split at
// the helper: with one stamped entry aged past the window and one unstamped
// entry beside it, only the stamped one matures — the unstamped one stays
// pending so it cannot jump ahead of an entry whose wait is measurable.
func TestMatureGraceCandidatesKeepsUnstampedPending(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	stamped := immatureLabelMissAssignment("exec-grace-split/stamped/activation-1")
	unstamped := immatureLabelMissAssignment("exec-grace-split/unstamped/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, stamped)
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, unstamped)
	ageAssignmentQueuedAt(t, ctx, rdb, directory, stamped.AssignmentID, DefaultSelectorFallback+time.Second)
	if err := rdb.HDel(ctx, directory.keys.assignmentQueuedAt, string(unstamped.AssignmentID)).Err(); err != nil {
		t.Fatalf("remove the unstamped entry's stamp: %v", err)
	}

	matured, pending, _, err := directory.matureGraceCandidates(ctx, []redisClaimCandidate{
		{assignmentID: string(stamped.AssignmentID), assignment: stamped},
		{assignmentID: string(unstamped.AssignmentID), assignment: unstamped},
	})
	if err != nil {
		t.Fatalf("matureGraceCandidates: %v", err)
	}
	if len(matured) != 1 || matured[0].assignmentID != string(stamped.AssignmentID) {
		t.Fatalf("matured = %+v, want only the aged stamped entry %q", matured, stamped.AssignmentID)
	}
	if len(pending) != 1 || pending[0].assignmentID != string(unstamped.AssignmentID) {
		t.Fatalf("pending = %+v, want only the unstamped entry %q", pending, unstamped.AssignmentID)
	}
}

// TestRedisClaimGracePartialMaturityStillReportsPending fixes the counter's
// blind spot: when one entry on a page matured and another is still waiting,
// the waiting one must still be reported as label_pending. Reporting only a
// page whose every entry is immature hid exactly the mixed state the counter
// exists to surface.
func TestRedisClaimGracePartialMaturityStillReportsPending(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	observer := newRecordingSelectorRejectObserver()
	directory.observer = observer
	session := registerGraceOrderRunner(t, directory, "grace-order", map[string]string{"cloud": "aws"})

	matured := immatureLabelMissAssignment("exec-grace-partial/matured/activation-1")
	waiting := immatureLabelMissAssignment("exec-grace-partial/waiting/activation-1")
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, matured)
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, waiting)
	ageAssignmentQueuedAt(t, ctx, rdb, directory, matured.AssignmentID, DefaultSelectorFallback+time.Second)

	claim, ok := claimGraceOrderAssignment(t, ctx, directory, session)
	if !ok {
		t.Fatal("the matured entry was not claimed after its window elapsed")
	}
	if claim.Assignment.AssignmentID != matured.AssignmentID {
		t.Fatalf("claimed %q, want the matured entry %q", claim.Assignment.AssignmentID, matured.AssignmentID)
	}
	if got := observer.count(SelectorRejectLabelPending); got != 1 {
		t.Fatalf("label_pending rejections = %d, want 1 (the immature sibling, whose wait is the whole point of the counter)", got)
	}
}
