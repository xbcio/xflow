package control

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine"
)

// fakeOrphanedHandoffDirectory is the directory-side half of the reaper: a real
// memory directory with the orphaned-handoff capability bolted on, so the Core
// test exercises real ledger settlement against a scripted candidate list.
type fakeOrphanedHandoffDirectory struct {
	*MemoryRunnerDirectory
	mu     sync.Mutex
	claims []Claim
	err    error
	calls  int
}

func (f *fakeOrphanedHandoffDirectory) ListOrphanedHandoffs(_ context.Context, limit int) ([]Claim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if limit < len(f.claims) {
		return f.claims[:limit], nil
	}
	return f.claims, nil
}

func (f *fakeOrphanedHandoffDirectory) snapshot() (int, []Claim) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, append([]Claim(nil), f.claims...)
}

// orphanedHandoffFixture builds the debt shape in a real directory and hands
// Core the directory's own view of it: the claim a reaper would have listed,
// with the ledger record still behind it, so a settlement on the fake settles
// the real thing.
func orphanedHandoffFixture(t *testing.T, eng EngineFacade) (*Core, *fakeOrphanedHandoffDirectory, RunnerSession, Assignment) {
	t.Helper()
	ctx := context.Background()

	directory := NewMemoryRunnerDirectory()
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-orphan-handoff", 1)
	assignment := testAssignment("exec-orphan-handoff/node-a/activation-1")
	mustEnqueueAssignment(t, ctx, directory, assignment)
	claim := mustClaimAssignment(t, ctx, directory, session)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist: %v", err)
	}
	if err := directory.MakeClaimHandoffRecoverable(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MakeClaimHandoffRecoverable: %v", err)
	}
	handoff, ok, err := directory.ClaimForRunner(ctx, testClaimRequest(session, 1))
	if err != nil || !ok || handoff.Handoff == nil {
		t.Fatalf("handoff claim = %#v, ok=%v, err=%v; want the recoverable debt", handoff, ok, err)
	}

	fake := &fakeOrphanedHandoffDirectory{MemoryRunnerDirectory: directory, claims: []Claim{handoff}}
	core := &Core{engine: eng, runners: fake}
	return core, fake, session, assignment
}

// TestCoreReapOrphanedHandoffsRequeuesWhenNoLeaseExists covers the engine
// answer that means "the work never started": the assignment goes back to the
// queue, which is the whole point of the pass — a wedged assignment becomes
// claimable again.
func TestCoreReapOrphanedHandoffsRequeuesWhenNoLeaseExists(t *testing.T) {
	ctx := context.Background()
	core, directory, session, _ := orphanedHandoffFixture(t, &fakeControlEngine{recoverErr: engine.ErrLeaseNotRecoverable})

	result, err := core.ReapOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("ReapOrphanedHandoffs() error = %v", err)
	}
	if result != (ReapResult{Inspected: 1, Released: 1}) {
		t.Fatalf("result = %+v, want one inspected and one settled", result)
	}
	// The debt is gone, so the next poll gets an ordinary queue claim rather than
	// a recovery claim that has nothing left to recover.
	reclaimed, ok, err := directory.ClaimForRunner(ctx, testClaimRequest(session, 1))
	if err != nil || !ok {
		t.Fatalf("post-reap claim = %#v, ok=%v, err=%v; want the requeued assignment", reclaimed, ok, err)
	}
	if reclaimed.Handoff != nil {
		t.Fatalf("post-reap handoff = %#v, want the debt settled", reclaimed.Handoff)
	}
}

// TestCoreReapOrphanedHandoffsDropsWhenExecutionIsInactive covers the terminal
// answer: no lease and no execution means there is nothing to run, so the
// assignment is removed rather than requeued.
func TestCoreReapOrphanedHandoffsDropsWhenExecutionIsInactive(t *testing.T) {
	ctx := context.Background()
	core, directory, session, _ := orphanedHandoffFixture(t, &fakeControlEngine{recoverErr: engine.ErrExecutionInactive})

	result, err := core.ReapOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("ReapOrphanedHandoffs() error = %v", err)
	}
	if result.Released != 1 {
		t.Fatalf("released = %d, want 1", result.Released)
	}
	if _, ok, err := directory.ClaimForRunner(ctx, testClaimRequest(session, 1)); err != nil || ok {
		t.Fatalf("post-reap claim ok=%v, err=%v; want the assignment dropped", ok, err)
	}
}

// TestCoreReapOrphanedHandoffsRetainsDebtUnderRecoverableLease is the deliberate
// difference from the poll path, and the one that decides whether this reaper is
// safe to run at all. A recoverable lease is evidence work is still owned
// somewhere; there is no runner here to hand it to, and settling the claim would
// requeue an assignment the engine still holds a lease for — the duplicate
// execution the handoff ledger exists to prevent. The debt must survive, and so
// must the token, so the next pass (or the owner, if it returns) can act.
func TestCoreReapOrphanedHandoffsRetainsDebtUnderRecoverableLease(t *testing.T) {
	ctx := context.Background()
	core, directory, session, assignment := orphanedHandoffFixture(t, nil)
	lease := &engine.TaskLease{
		LeaseID:    "lease-orphan",
		LeaseToken: "token-orphan",
		Task:       assignment.Task,
		NodeType:   assignment.Routing.NodeType,
	}
	core.engine = &fakeControlEngine{recoverLease: lease}

	result, err := core.ReapOrphanedHandoffs(ctx, 8)
	if err != nil {
		t.Fatalf("ReapOrphanedHandoffs() error = %v", err)
	}
	if result != (ReapResult{Inspected: 1, Released: 0}) {
		t.Fatalf("result = %+v, want one inspected and none settled", result)
	}
	retained, ok, err := directory.ClaimForRunner(ctx, testClaimRequest(session, 1))
	if err != nil || !ok || retained.Handoff == nil || retained.Handoff.State != HandoffDebtLeaseMayExist {
		t.Fatalf("retained claim = %#v, ok=%v, err=%v; want the debt and its token back", retained, ok, err)
	}
}

// TestCoreReapOrphanedHandoffsRetainsDebtWhenEngineIsInconclusive covers the
// engine failing to answer. Nothing is settled, the token goes back, and the
// error is reported so the pass reads as failed rather than as a pass that found
// nothing.
func TestCoreReapOrphanedHandoffsRetainsDebtWhenEngineIsInconclusive(t *testing.T) {
	ctx := context.Background()
	inconclusive := errors.New("engine timeout")
	core, directory, session, _ := orphanedHandoffFixture(t, &fakeControlEngine{recoverErr: inconclusive})

	result, err := core.ReapOrphanedHandoffs(ctx, 8)
	if !errors.Is(err, inconclusive) {
		t.Fatalf("ReapOrphanedHandoffs() error = %v, want %v", err, inconclusive)
	}
	if result != (ReapResult{Inspected: 1, Released: 0}) {
		t.Fatalf("result = %+v, want one inspected and none settled", result)
	}
	retained, ok, err := directory.ClaimForRunner(ctx, testClaimRequest(session, 1))
	if err != nil || !ok || retained.Handoff == nil {
		t.Fatalf("retained claim = %#v, ok=%v, err=%v; want the debt retained", retained, ok, err)
	}
}

// TestCoreReapOrphanedHandoffsIsNoOpWithoutCapability keeps a directory that does
// not implement the capability out of this path entirely.
func TestCoreReapOrphanedHandoffsIsNoOpWithoutCapability(t *testing.T) {
	ctx := context.Background()
	core := &Core{engine: &fakeControlEngine{}, runners: NewMemoryRunnerDirectory()}

	result, err := core.ReapOrphanedHandoffs(ctx, 8)
	if err != nil || result != (ReapResult{}) {
		t.Fatalf("ReapOrphanedHandoffs() = %+v, err=%v; want a no-op", result, err)
	}
}

// TestCoreReapOrphanedHandoffsIsBoundedByItsLimit pins that limit<=0 is the
// documented no-op rather than an unbounded read: the sweeper's zero batch would
// otherwise turn one pass into an unbounded ledger settlement.
func TestCoreReapOrphanedHandoffsIsBoundedByItsLimit(t *testing.T) {
	ctx := context.Background()
	core, directory, _, _ := orphanedHandoffFixture(t, &fakeControlEngine{recoverErr: engine.ErrLeaseNotRecoverable})

	if result, err := core.ReapOrphanedHandoffs(ctx, 0); err != nil || result != (ReapResult{}) {
		t.Fatalf("ReapOrphanedHandoffs(0) = %+v, err=%v; want a no-op", result, err)
	}
	if calls, _ := directory.snapshot(); calls != 0 {
		t.Fatalf("list calls = %d, want 0 for a zero limit", calls)
	}
}
