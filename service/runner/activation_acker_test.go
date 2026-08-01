package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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
//
// This test must wait for the SPAWNED goroutine to actually enter the client
// call (and panic) before asserting anything about recovery — not merely for
// ackFailed to return, which happens synchronously right after the goroutine
// is dispatched and proves nothing about what that goroutine did. An earlier
// version of this test only waited on ackFailed's return and was shown (via
// fault injection: deleting the recover() block) to pass ~15/15 runs anyway,
// because most runs finished before the spawned goroutine was even
// scheduled. Fixed by: (1) waiting on a channel the fake client closes
// immediately before it panics, so we know the panic has happened, and (2)
// polling captured log output for the exact line recover() emits, so we know
// the recover() branch itself ran rather than merely "the process didn't
// crash yet".
func TestActivationAckerRecoversFromClientPanic(t *testing.T) {
	entered := make(chan struct{})
	client := &panicAckClient{entered: entered}

	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))
	acker := newActivationAcker(client, "runner-1", logger)

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

	// Wait for the spawned goroutine to actually reach the client call (and
	// thus panic). ackFailed's own return above only proves the synchronous
	// dedup-and-dispatch happened, not that the dispatched goroutine ran.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("spawned send goroutine never reached the client call")
	}

	// Confirm the recover() branch itself executed — not just that the test
	// process is still alive — by polling for the exact log line it emits.
	// This is a terminal condition: once written, that line does not
	// disappear, so a positive match is conclusive and continuing to poll
	// after a negative match cannot manufacture a false pass.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if strings.Contains(logBuf.String(), "activation ack panicked") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recover() never logged the panic-recovery line within 2s; captured log:\n%s", logBuf.String())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The panic must not have corrupted the acker: it must still accept and
	// send a subsequent ack normally (different generation, so dedup doesn't
	// suppress it).
	var mu sync.Mutex
	var sent []protocol.ActivationAck
	ok := &okAckClient{onAck: func(ack protocol.ActivationAck) {
		mu.Lock()
		sent = append(sent, ack)
		mu.Unlock()
	}}
	acker.client = ok
	acker.ackFailed("session-1", protocol.ActivateDirective{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 2}, errors.New("still failing"))
	waitForAcks(t, &mu, &sent, 1)
}

type panicAckClient struct {
	entered chan struct{}
}

func (c *panicAckClient) ActivationAck(context.Context, protocol.ActivationAck) error {
	close(c.entered)
	panic("simulated transport panic")
}

// okAckClient is a non-panicking activationAckClient used to prove the acker
// remains usable after a previous send's panic was recovered.
type okAckClient struct {
	onAck func(protocol.ActivationAck)
}

func (c *okAckClient) ActivationAck(_ context.Context, ack protocol.ActivationAck) error {
	c.onAck(ack)
	return nil
}

// syncBuffer is a concurrency-safe io.Writer/String() pair, used to capture
// slog output from a goroutine while the test goroutine polls it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
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
