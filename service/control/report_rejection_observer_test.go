package control

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// recordingRejectionObserver captures the rejection reasons the report path
// emits, so a test can assert attribution rather than just a 409.
type recordingRejectionObserver struct {
	mu          sync.Mutex
	reasons     []string
	divergences int
}

func (r *recordingRejectionObserver) OnReportRejected(_ context.Context, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reasons = append(r.reasons, reason)
}

func (r *recordingRejectionObserver) OnReportRejectionDivergence(context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.divergences++
}

func (r *recordingRejectionObserver) snapshot() ([]string, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.reasons...), r.divergences
}

// newReportRejectionTestCore wires a real RedisRunnerDirectory (miniredis) to a
// fake engine, which is the only arrangement that can exercise the report path's
// four fences end to end without a Redis daemon.
func newReportRejectionTestCore(t *testing.T, fake EngineFacade) (context.Context, *Core, *RedisRunnerDirectory, RunnerSession, *engine.TaskLease, *recordingRejectionObserver) {
	t.Helper()

	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Minute))
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-report-reason", 1)

	assignment := redisDirectoryTestAssignment("exec-report-reason/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-report-reason", time.Minute)
	lease.Input = &types.Input{Data: map[string]any{"k": "v"}}
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

	observer := &recordingRejectionObserver{}
	core := &Core{
		engine:                  fake,
		runners:                 directory,
		pollWait:                time.Second,
		reportRejectionObserver: observer,
	}
	return ctx, core, directory, session, lease, observer
}

func reportOnce(t *testing.T, ctx context.Context, core *Core, session RunnerSession, lease *engine.TaskLease) error {
	t.Helper()
	_, err := core.reportResult(ctx, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     lease,
		Result:    engine.TaskResult{},
	}, TransportInfo{})
	return err
}

// TestReportRejectionReasonsAreAttributed is the R6 acceptance criterion: the
// 409 rate must name which fence refused it. Every case here is an HTTP 409 on
// the wire; only the reason separates "the directory lost the lease" from "the
// engine re-leased it under a directory record that is still there".
func TestReportRejectionReasonsAreAttributed(t *testing.T) {
	t.Run("engine stale token", func(t *testing.T) {
		fake := &fakeControlEngine{commitErr: engine.ErrInvalidLeaseToken}
		ctx, core, _, session, lease, observer := newReportRejectionTestCore(t, fake)

		if err := reportOnce(t, ctx, core, session, lease); err == nil {
			t.Fatal("reportResult() error = nil, want ErrInvalidLeaseToken")
		}
		reasons, divergences := observer.snapshot()
		if len(reasons) != 1 || reasons[0] != ReportRejectedEngineStaleToken {
			t.Fatalf("reasons = %v, want [%s]", reasons, ReportRejectedEngineStaleToken)
		}
		// The directory still holds this exact token, so the two views are
		// measurably in disagreement.
		if divergences != 1 {
			t.Fatalf("divergences = %d, want 1: the directory still resolves the "+
				"token the engine just refused", divergences)
		}
	})

	t.Run("engine stale token without divergence", func(t *testing.T) {
		// The directory lets the lease go between the lookup and the probe, which
		// is the ordinary late-report shape: the token is stale on BOTH sides.
		fake := &fakeControlEngine{commitErr: engine.ErrInvalidLeaseToken}
		ctx, core, directory, session, lease, observer := newReportRejectionTestCore(t, fake)
		fake.commitHook = func() {
			if err := directory.ReleaseLeased(ctx, ReleaseLeasedRequest{
				RunnerID:     session.RunnerID,
				SessionID:    session.SessionID,
				AssignmentID: BuildAssignmentID(&lease.Task),
				LeaseID:      lease.LeaseID,
				LeaseToken:   lease.LeaseToken,
			}); err != nil {
				t.Errorf("ReleaseLeased() error = %v", err)
			}
		}

		if err := reportOnce(t, ctx, core, session, lease); err == nil {
			t.Fatal("reportResult() error = nil, want ErrInvalidLeaseToken")
		}
		reasons, divergences := observer.snapshot()
		if len(reasons) != 1 || reasons[0] != ReportRejectedEngineStaleToken {
			t.Fatalf("reasons = %v, want [%s]", reasons, ReportRejectedEngineStaleToken)
		}
		if divergences != 0 {
			t.Fatalf("divergences = %d, want 0: the directory had already released the lease", divergences)
		}
	})

	t.Run("directory lease not found", func(t *testing.T) {
		fake := &fakeControlEngine{}
		ctx, core, _, _, lease, observer := newReportRejectionTestCore(t, fake)

		// A runner that does not own the assignment: the directory resolves the
		// lease by its token index and then refuses it on ownership.
		stranger := registerRedisDirectoryRunner(t, ctx, core.runners.(*RedisRunnerDirectory), "runner-stranger", 1)
		if err := reportOnce(t, ctx, core, stranger, lease); err == nil {
			t.Fatal("reportResult() error = nil, want ErrInvalidLeaseToken")
		}
		reasons, divergences := observer.snapshot()
		if len(reasons) != 1 || reasons[0] != ReportRejectedDirectoryLeaseNotFound {
			t.Fatalf("reasons = %v, want [%s]", reasons, ReportRejectedDirectoryLeaseNotFound)
		}
		if divergences != 0 {
			t.Fatalf("divergences = %d, want 0", divergences)
		}
	})

	t.Run("directory immutable mismatch", func(t *testing.T) {
		fake := &fakeControlEngine{}
		ctx, core, _, session, lease, observer := newReportRejectionTestCore(t, fake)

		// An empty echoed token is the one shape where resolveLeaseAssignmentID
		// falls through to its assignment-ID path, so the persisted lease is read
		// and its identity compared rather than resolved by token. That makes the
		// immutable check reachable for input that never carried a token at all.
		//
		// NOTE: for a NON-empty token the resolver can only return the lease that
		// token indexes, so LeaseID/LeaseToken can never disagree on that path and
		// this guard is unreachable there. A non-zero rate of this reason is
		// therefore a forge/empty-token rate, NOT evidence of divergence — which
		// is exactly why the two are counted separately.
		echoed := *lease
		echoed.LeaseToken = ""
		if err := reportOnce(t, ctx, core, session, &echoed); err == nil {
			t.Fatal("reportResult() error = nil, want ErrInvalidLeaseToken")
		}
		reasons, divergences := observer.snapshot()
		if len(reasons) != 1 || reasons[0] != ReportRejectedDirectoryImmutableMismatch {
			t.Fatalf("reasons = %v, want [%s]", reasons, ReportRejectedDirectoryImmutableMismatch)
		}
		if divergences != 0 {
			t.Fatalf("divergences = %d, want 0", divergences)
		}
	})

	t.Run("directory without lease lookup capability", func(t *testing.T) {
		observer := &recordingRejectionObserver{}
		inner := NewMemoryRunnerDirectory()
		runners := &lookuplessDirectory{inner: inner}
		core := &Core{
			engine:                  &fakeControlEngine{},
			runners:                 runners,
			pollWait:                time.Second,
			reportRejectionObserver: observer,
		}
		ctx := context.Background()
		session, err := runners.Register(ctx, RegisterRunnerRequest{
			RunnerID: "runner-lookupless",
			Capacity: 1,
			Now:      time.Unix(10, 0),
		})
		if err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		if err := reportOnce(t, ctx, core, session, &engine.TaskLease{}); err == nil {
			t.Fatal("reportResult() error = nil, want ErrInvalidLeaseToken")
		}
		reasons, _ := observer.snapshot()
		if len(reasons) != 1 || reasons[0] != ReportRejectedDirectoryUnavailable {
			t.Fatalf("reasons = %v, want [%s]", reasons, ReportRejectedDirectoryUnavailable)
		}
	})
}

// TestReportRejectionObserverIsOptional pins the compatibility promise: an
// unset observer changes nothing, and a panicking one cannot fail a report.
func TestReportRejectionObserverIsOptional(t *testing.T) {
	fake := &fakeControlEngine{commitErr: engine.ErrInvalidLeaseToken}
	ctx, core, _, session, lease, _ := newReportRejectionTestCore(t, fake)
	core.reportRejectionObserver = nil

	if err := reportOnce(t, ctx, core, session, lease); err == nil {
		t.Fatal("reportResult() error = nil, want ErrInvalidLeaseToken")
	}

	core.reportRejectionObserver = panickingRejectionObserver{}
	if err := reportOnce(t, ctx, core, session, lease); err == nil {
		t.Fatal("reportResult() error = nil with a panicking observer, want the report error")
	}
}

type panickingRejectionObserver struct{}

func (panickingRejectionObserver) OnReportRejected(context.Context, string) { panic("observer") }
func (panickingRejectionObserver) OnReportRejectionDivergence(context.Context) {
	panic("observer")
}
