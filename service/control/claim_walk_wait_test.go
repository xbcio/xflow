package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// walkPendingDirectory wraps the in-memory directory to report a pending claim
// walk, standing in for the Redis directory's optional ClaimWalkReporter
// surface. The wrapper also demonstrates discovery by assertion: the embedded
// directory does not implement the interface, only the wrapper does.
type walkPendingDirectory struct {
	*MemoryRunnerDirectory
	pending bool
}

func (d *walkPendingDirectory) ClaimWalkPending(string) bool { return d.pending }

// TestPollTaskUsesTheShortWaitWhileAClaimWalkIsPending pins the walk-wait
// decoupling: a poll that found nothing because the scan stopped at its budget
// must not park the runner at the idle long-poll cadence, because the queue
// behind the budget is the work the runner was woken to cross. A walk that
// reached a definite answer, and a directory without the capability, both keep
// the idle wait exactly.
func TestPollTaskUsesTheShortWaitWhileAClaimWalkIsPending(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID: "runner-walk",
		Capacity: 1,
		Policy:   RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	req := protocol.PollTaskRequest{RunnerID: session.RunnerID, SessionID: session.SessionID}

	directory := &walkPendingDirectory{MemoryRunnerDirectory: dir}
	core := &Core{runners: directory, pollWait: time.Second, pollWalkWait: defaultPollWalkWait}

	// Nothing to claim and no walk pending: the idle cadence.
	resp, err := core.pollTask(ctx, req, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask() error = %v", err)
	}
	if resp.Lease != nil {
		t.Fatal("pollTask() returned a lease from an empty directory")
	}
	if resp.Wait != time.Second {
		t.Fatalf("Wait = %v with no walk pending, want the idle %v", resp.Wait, time.Second)
	}

	// A walk pending: the short cadence.
	directory.pending = true
	resp, err = core.pollTask(ctx, req, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask() with a walk pending error = %v", err)
	}
	if resp.Wait != defaultPollWalkWait {
		t.Fatalf("Wait = %v with a walk pending, want the short %v", resp.Wait, defaultPollWalkWait)
	}

	// A directory without the capability keeps the idle cadence exactly: the
	// bare in-memory directory cannot report a walk at all.
	plain := &Core{runners: dir, pollWait: time.Second, pollWalkWait: defaultPollWalkWait}
	resp, err = plain.pollTask(ctx, req, TransportInfo{})
	if err != nil {
		t.Fatalf("pollTask() on a directory without the capability error = %v", err)
	}
	if resp.Wait != time.Second {
		t.Fatalf("Wait = %v on a directory without the capability, want the idle %v", resp.Wait, time.Second)
	}
}
