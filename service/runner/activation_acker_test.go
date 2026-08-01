package runner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// 供给长期不可用时，同一 (workflow, entryUnit, generation) 会被反复重试；ack
// 必须按三元组去重，否则会形成 ack 风暴。generation 单调递增，所以每次真正的
// 重派都会产生新三元组并允许一次新的 ack。
//
// The dedup decision (activationAcker.shouldAck) runs synchronously before a
// send goroutine is ever spawned, so calling ackFailed twice back-to-back
// from this goroutine deterministically dispatches exactly one HTTP call —
// there is no race between "check" and "spawn" to fool. What's left to
// verify by polling is that the single dispatched call actually lands on the
// wire with the right path and body.
func TestActivationAckIsSentOncePerGeneration(t *testing.T) {
	var mu sync.Mutex
	var acks []protocol.ActivationAck
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.ActivationAckPath {
			t.Errorf("path = %q, want %q", r.URL.Path, protocol.ActivationAckPath)
		}
		var a protocol.ActivationAck
		if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
			t.Errorf("decode ack: %v", err)
		}
		mu.Lock()
		acks = append(acks, a)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := protocol.NewClient(srv.URL, srv.Client())
	acker := newActivationAcker(client, "runner-1", slog.New(slog.NewTextHandler(io.Discard, nil)))

	d := protocol.ActivateDirective{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 7}
	activateErr := errors.New("supply not ready: rules")

	acker.ackFailed("session-1", d, activateErr)
	acker.ackFailed("session-1", d, activateErr) // repeated same-generation failure

	waitForAcks(t, &mu, &acks, 1)

	mu.Lock()
	defer mu.Unlock()
	if len(acks) != 1 {
		t.Fatalf("acks = %d, want exactly 1 for a repeated same-generation failure", len(acks))
	}
	if acks[0].Status != protocol.ActivationStatusFailed {
		t.Fatalf("status = %q, want %q", acks[0].Status, protocol.ActivationStatusFailed)
	}
	if acks[0].Generation != 7 {
		t.Fatalf("generation = %d, want 7", acks[0].Generation)
	}
	if acks[0].Error == "" {
		t.Fatal("ack must carry the failure reason")
	}
	if acks[0].WorkflowID != "wf-1" || acks[0].GroupID != "grp-a" {
		t.Fatalf("ack identity = %+v, want workflow_id=wf-1 group_id=grp-a", acks[0])
	}
	if acks[0].RunnerID != "runner-1" || acks[0].SessionID != "session-1" {
		t.Fatalf("ack origin = %+v, want runner_id=runner-1 session_id=session-1", acks[0])
	}
}

// A higher generation is a genuine redispatch and must get its own ack, even
// though it shares the same (workflow, entryUnit) pair as an already-acked
// failure.
func TestActivationAckAllowsNewGenerationAfterOldOneWasAcked(t *testing.T) {
	var mu sync.Mutex
	var acks []protocol.ActivationAck
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var a protocol.ActivationAck
		_ = json.NewDecoder(r.Body).Decode(&a)
		mu.Lock()
		acks = append(acks, a)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := protocol.NewClient(srv.URL, srv.Client())
	acker := newActivationAcker(client, "runner-1", slog.New(slog.NewTextHandler(io.Discard, nil)))

	activateErr := errors.New("supply not ready: rules")
	acker.ackFailed("session-1", protocol.ActivateDirective{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 7}, activateErr)
	acker.ackFailed("session-1", protocol.ActivateDirective{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 8}, activateErr)

	waitForAcks(t, &mu, &acks, 2)

	mu.Lock()
	defer mu.Unlock()
	if len(acks) != 2 {
		t.Fatalf("acks = %d, want 2 (one per generation)", len(acks))
	}
	gens := map[uint64]bool{acks[0].Generation: true, acks[1].Generation: true}
	if !gens[7] || !gens[8] {
		t.Fatalf("ack generations = %v, want {7,8}", gens)
	}
}

// A goroutine panic inside the async send must never escape — this is a new
// I/O path running in a runner process, and an unrecovered panic there would
// take the whole runner down over an activation failure it was only trying
// to report.
func TestActivationAckerRecoversFromClientPanic(t *testing.T) {
	client := panicAckClient{}
	acker := newActivationAcker(client, "runner-1", slog.New(slog.NewTextHandler(io.Discard, nil)))

	done := make(chan struct{})
	go func() {
		acker.ackFailed("session-1", protocol.ActivateDirective{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 1}, errors.New("boom"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ackFailed did not return promptly")
	}
	// If the panicking goroutine's recover didn't fire, the test binary
	// itself would have crashed by now (a panic that escapes a goroutine
	// takes the whole process down) — reaching here is the assertion.
}

type panicAckClient struct{}

func (panicAckClient) ActivationAck(context.Context, protocol.ActivationAck) error {
	panic("simulated transport panic")
}

// waitForAcks polls until at least want acks have arrived or the deadline
// expires. Polling (not a fixed sleep) with a bound on iteration count: the
// terminal condition here is genuinely reachable in bounded time because the
// caller has already ensured no more than `want` sends were ever dispatched
// (see the dedup notes on the callers), so waiting longer cannot manufacture
// additional acks — it can only confirm the ones already in flight arrived.
func waitForAcks(t *testing.T, mu *sync.Mutex, acks *[]protocol.ActivationAck, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(*acks)
		mu.Unlock()
		if n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d ack(s), got %d", want, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
