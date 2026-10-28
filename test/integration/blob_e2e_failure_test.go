//go:build integration

package integration

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/types"
)

// blobE2EAssertQueuesIdle fails the test if either queue of the run still holds
// a task in any state: pending, active, scheduled, waiting for a retry, or
// archived after its retries ran out.
func blobE2EAssertQueuesIdle(t *testing.T, r *blobE2ERun) {
	t.Helper()
	insp := asynqlib.NewInspector(asynqlib.RedisClientOpt{Addr: r.addr})
	defer func() { _ = insp.Close() }()
	for _, q := range []string{dpDefaultQueue + string(r.ns), dpBatchQueue + string(r.ns)} {
		info, err := insp.GetQueueInfo(q)
		if errors.Is(err, asynqlib.ErrQueueNotFound) {
			continue
		}
		if err != nil {
			t.Fatalf("GetQueueInfo(%s): %v", q, err)
		}
		if info.Size != 0 || info.Active != 0 || info.Retry != 0 || info.Archived != 0 {
			t.Errorf("queue %s is not idle: size=%d active=%d retry=%d archived=%d",
				q, info.Size, info.Active, info.Retry, info.Archived)
		}
	}
}

// blobE2EMaxAttempts is the largest number of times the handler was called for
// one asynq task. A task that is retried shows up as more than one.
func blobE2EMaxAttempts(r *blobE2ERun) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	most := 0
	for _, n := range r.attempts {
		most = max(most, n)
	}
	return most
}

// TestBlobE2EPermanentReadFailure: the blob is lost after the expansion stored
// it, so the batch task can never read its items. The map node must fail with
// the reason, and the queue must see the task finish once, not retry it.
func TestBlobE2EPermanentReadFailure(t *testing.T) {
	addr := requireRedis(t)
	r := newBlobE2ERun(t, addr, true)
	r.serve(t)
	// The first read finds the object files gone and gets the store's own
	// not-found, which the blob layer classifies as permanent.
	r.store.vanishOnRead(r.dir)
	out := r.run(t, blobE2EItems)

	if out.res.Status != types.ExecutionStatusFailed {
		t.Fatalf("status = %s, want failed", out.res.Status)
	}
	if out.fan.Status != types.NodeStatusFailed || !strings.Contains(out.fan.Error, "batch items unavailable") {
		t.Fatalf("fan node = %s %q, want failed with a batch items unavailable reason", out.fan.Status, out.fan.Error)
	}
	// The blob existed when the batch read it: the read itself removed it.
	if n := r.store.removedObjects(); n != 1 {
		t.Errorf("objects removed by the first read = %d, want 1 (the blob must have existed)", n)
	}
	// One read, one handler call: a permanent failure is settled, not retried.
	// A retry would read again (asynq's default is 25 retries).
	if _, gets, fails := r.store.counts(); gets != 1 || fails != 1 {
		t.Errorf("blob reads = %d (failed %d), want exactly 1 failed read", gets, fails)
	}
	if most := blobE2EMaxAttempts(r); most != 1 {
		t.Errorf("a queue task was handled %d times, want 1 (no retry)", most)
	}
	r.mu.Lock()
	failures := len(r.failures)
	r.mu.Unlock()
	if failures != 0 {
		t.Errorf("handler returned %d error(s) %v, want none: the batch is settled, not failed", failures, r.failures)
	}
	blobE2EAssertQueuesIdle(t, r)
}

// TestBlobE2ETransientReadFailure: the first two reads of the blob fail with an
// error the blob layer does not classify, so they are transient. The batch task
// must be retried until a read succeeds, and the run must end exactly as the
// blob-off run does. Every run has a namespace of its own, so the engine's
// process-wide items cache cannot serve this run an array another run loaded.
func TestBlobE2ETransientReadFailure(t *testing.T) {
	addr := requireRedis(t)
	_, want := blobE2EExecute(t, addr, false, blobE2EItems)
	r := newBlobE2ERun(t, addr, true)
	r.serve(t)
	r.store.failReads(2)
	got := r.run(t, blobE2EItems)

	blobE2EAssertSuccess(t, got)
	for _, node := range blobE2ENodes {
		a, b := blobE2ENormalize(t, want.res.Output[node]), blobE2ENormalize(t, got.res.Output[node])
		if !reflect.DeepEqual(a, b) {
			t.Errorf("output of %q differs from the blob-off run:\n off %v\n on  %v", node, a, b)
		}
	}
	// Two failed reads and a third that worked: the task was retried twice.
	if _, gets, fails := r.store.counts(); gets < 3 || fails != 2 {
		t.Errorf("blob reads = %d (failed %d), want at least 3 with exactly the 2 injected failures", gets, fails)
	}
	if most := blobE2EMaxAttempts(r); most < 3 {
		t.Errorf("most-retried task was handled %d times, want at least 3", most)
	}
	r.mu.Lock()
	failures := append([]error(nil), r.failures...)
	r.mu.Unlock()
	if len(failures) < 2 {
		t.Fatalf("handler errors = %v, want the 2 injected failures to have reached the queue", failures)
	}
	for _, err := range failures {
		if !errors.Is(err, errBlobE2ETransient) || errors.Is(err, types.ErrPermanent) {
			t.Errorf("handler error %v is not the injected transient failure", err)
		}
	}
	blobE2EAssertBlobOn(t, r, 1)
	blobE2EAssertQueuesIdle(t, r)
}
