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
// The dedup decision (activationAcker.beginActivationAck) runs synchronously
// before a send goroutine is ever spawned, so calling ackFailed twice
// back-to-back from this goroutine deterministically dispatches exactly one
// HTTP call — there is no race between "check" and "spawn" to fool. What's
// left to verify by polling is that the single dispatched call actually lands
// on the wire with the right path and body.
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

// TestActivationAckDedupIncludesVersion verifies that the dedup key includes
// WorkflowVersion: two activations with the same (WorkflowID, EntryUnitID) but
// different versions have independent generation sequences, so a high generation
// on one must not suppress a lower-generation ack for the other.
func TestActivationAckDedupIncludesVersion(t *testing.T) {
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

	// v1 fails at generation 5.
	acker.ackFailed("session-1", protocol.ActivateDirective{
		WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "grp-a", Generation: 5,
	}, activateErr)
	// v2 fails at generation 1 (lower than v1's — independent sequence).
	acker.ackFailed("session-1", protocol.ActivateDirective{
		WorkflowID: "wf-1", WorkflowVersion: "v2", EntryUnitID: "grp-a", Generation: 1,
	}, activateErr)

	waitForAcks(t, &mu, &acks, 2)

	mu.Lock()
	defer mu.Unlock()
	if len(acks) != 2 {
		t.Fatalf("acks = %d, want 2 (one per version); v1 gen=5 must not suppress v2 gen=1", len(acks))
	}
	// Verify both versions are represented.
	versions := map[string]uint64{}
	for _, a := range acks {
		versions[a.WorkflowVersion] = a.Generation
	}
	if versions["v1"] != 5 {
		t.Fatalf("v1 ack generation = %d, want 5", versions["v1"])
	}
	if versions["v2"] != 1 {
		t.Fatalf("v2 ack generation = %d, want 1", versions["v2"])
	}
}

func TestActivationAckDedupIncludesReplicaIndex(t *testing.T) {
	var mu sync.Mutex
	var acks []protocol.ActivationAck
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ack protocol.ActivationAck
		_ = json.NewDecoder(r.Body).Decode(&ack)
		mu.Lock()
		acks = append(acks, ack)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	acker := newActivationAcker(
		protocol.NewClient(srv.URL, srv.Client()),
		"runner-1",
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	activateErr := errors.New("activation failed")
	acker.ackFailed("session-1", protocol.ActivateDirective{
		WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "group-a", ReplicaIndex: 0, Generation: 5,
	}, activateErr)
	acker.ackFailed("session-1", protocol.ActivateDirective{
		WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "group-a", ReplicaIndex: 1, Generation: 1,
	}, activateErr)

	waitForAcks(t, &mu, &acks, 2)
	mu.Lock()
	defer mu.Unlock()
	generations := map[uint32]uint64{}
	for _, ack := range acks {
		generations[ack.ReplicaIndex] = ack.Generation
	}
	if generations[0] != 5 || generations[1] != 1 {
		t.Fatalf("acks by replica = %v, want replica 0/gen5 and replica 1/gen1", generations)
	}
}

func TestDeactivationReceiptRetriesAfterTransportFailure(t *testing.T) {
	client := &flakyDeactivationAckClient{failFirst: true, attempted: make(chan struct{}, 2)}
	acker := newActivationAcker(client, "runner-1", slog.New(slog.NewTextHandler(io.Discard, nil)))
	directive := protocol.DeactivateDirective{WorkflowID: "wf", WorkflowVersion: "v1", EntryUnitID: "entry", Generation: 3}
	acker.ackDeactivated("session-1", directive)
	select {
	case <-client.attempted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for failed receipt attempt")
	}
	acker.ackDeactivated("session-1", directive)
	select {
	case <-client.attempted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retry receipt attempt")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.acks) != 2 {
		t.Fatalf("receipt attempts = %d, want retry after failure", len(client.acks))
	}
	if client.acks[1].Status != protocol.ActivationStatusDeactivated || client.acks[1].SessionID != "session-1" {
		t.Fatalf("retry receipt = %+v", client.acks[1])
	}
}

type flakyDeactivationAckClient struct {
	mu        sync.Mutex
	failFirst bool
	acks      []protocol.ActivationAck
	attempted chan struct{}
}

func (c *flakyDeactivationAckClient) ActivationAck(_ context.Context, ack protocol.ActivationAck) error {
	c.mu.Lock()
	c.acks = append(c.acks, ack)
	fail := c.failFirst
	c.failFirst = false
	c.mu.Unlock()
	c.attempted <- struct{}{}
	if fail {
		return errors.New("temporary receipt failure")
	}
	return nil
}

// TestActivationAckRetriesAfterTransportFailure pins the F1b fix: a failed
// send must NOT count as acked. Before it, the dedup map was written before
// the HTTP call, so one lost request permanently suppressed the failure
// signal for that (activation, generation) — the server never learned why the
// runner could not take the activation. Now the reservation is released on
// failure and the next onActivateFailed (which every redelivery re-triggers)
// sends again, while an in-flight or already-delivered ack still dedups.
func TestActivationAckRetriesAfterTransportFailure(t *testing.T) {
	client := newScriptedActivationAckClient()
	acker := newActivationAcker(client, "runner-1", slog.New(slog.NewTextHandler(io.Discard, nil)))
	d := protocol.ActivateDirective{WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "grp-a", Generation: 5}
	activateErr := errors.New("supply not ready")

	// First failure: the send is observed entering the client, then fails.
	acker.ackFailed("session-1", d, activateErr)
	client.waitForCall(t, 1)

	// A repeated failure while the first send is still in flight must not
	// dispatch a second request.
	acker.ackFailed("session-1", d, activateErr)
	if got := client.callCount(); got != 1 {
		t.Fatalf("calls while one is in flight = %d, want 1", got)
	}
	client.releaseCall(t, 1, errors.New("transport down"))

	// The failed send releases its reservation: the record must not be marked
	// delivered, and the next failure for the same directive must send again.
	waitForActivationAckState(t, acker, d, func(a *activationAcker) bool {
		_, busy := a.activationAckInFlight[activationAckKeyOf(d)]
		return !busy
	}, "in-flight reservation released after a failed send")
	if got := client.callCount(); got != 1 {
		t.Fatalf("calls before the retry = %d, want 1", got)
	}

	acker.ackFailed("session-1", d, activateErr)
	client.waitForCall(t, 2)
	client.releaseCall(t, 2, nil)
	waitForActivationAckState(t, acker, d, func(a *activationAcker) bool {
		return a.acked[activationAckKeyOf(d)] >= d.Generation
	}, "ack recorded after a successful send")

	// A delivered ack still dedups: another failure for the same generation
	// dispatches nothing.
	acker.ackFailed("session-1", d, activateErr)
	if got := client.callCount(); got != 2 {
		t.Fatalf("calls after a delivered ack = %d, want still 2", got)
	}
}

func activationAckKeyOf(d protocol.ActivateDirective) activationAckKey {
	return activationAckKey{
		WorkflowID:      d.WorkflowID,
		WorkflowVersion: d.WorkflowVersion,
		EntryUnitID:     d.EntryUnitID,
		ReplicaIndex:    d.ReplicaIndex,
	}
}

// scriptedActivationAckClient blocks every call until the test releases it, so
// in-flight and post-failure ordering are deterministic rather than polled.
type scriptedActivationAckClient struct {
	mu      sync.Mutex
	calls   int
	release []chan error
	entered chan int
}

func newScriptedActivationAckClient() *scriptedActivationAckClient {
	return &scriptedActivationAckClient{entered: make(chan int, 8)}
}

func (c *scriptedActivationAckClient) ActivationAck(context.Context, protocol.ActivationAck) error {
	c.mu.Lock()
	c.calls++
	call := c.calls
	ch := make(chan error, 1)
	c.release = append(c.release, ch)
	c.mu.Unlock()
	c.entered <- call
	return <-ch
}

func (c *scriptedActivationAckClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *scriptedActivationAckClient) waitForCall(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case n := <-c.entered:
			if n >= want {
				return
			}
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for call %d (observed %d)", want, c.callCount())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (c *scriptedActivationAckClient) releaseCall(t *testing.T, call int, err error) {
	t.Helper()
	c.mu.Lock()
	if call > len(c.release) {
		c.mu.Unlock()
		t.Fatalf("release call %d before it was entered", call)
	}
	ch := c.release[call-1]
	c.mu.Unlock()
	ch <- err
}

// waitForActivationAckState polls the acker's internal bookkeeping under its
// own mutex until cond holds. Every condition used here is a terminal state
// (a reservation is removed, an ack is recorded) reached in bounded time, so
// the poll can only confirm it, never manufacture it.
func waitForActivationAckState(t *testing.T, acker *activationAcker, d protocol.ActivateDirective, cond func(*activationAcker) bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		acker.mu.Lock()
		ok := cond(acker)
		acker.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestActivationAckReservationSurvivesStaleCompletion pins the interleaving
// the conditional delete exists for: an older generation's send completing
// must not erase a newer generation's reservation, which would let a repeated
// failure for the newer generation dispatch a duplicate HTTP call.
func TestActivationAckReservationSurvivesStaleCompletion(t *testing.T) {
	acker := newActivationAcker(&okAckClient{onAck: func(protocol.ActivationAck) {}}, "runner-1", slog.New(slog.NewTextHandler(io.Discard, nil)))
	key := activationAckKey{WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "grp-a"}

	// (b) A newer generation takes the slot while gen 1 is still in flight.
	if !acker.beginActivationAck(key, 1) {
		t.Fatal("gen 1 must be admitted")
	}
	if !acker.beginActivationAck(key, 2) {
		t.Fatal("gen 2 must be admitted while gen 1 is in flight")
	}
	// Gen 1 completes after gen 2 took the slot: it must not erase it.
	acker.finishActivationAck(key, 1, true)
	acker.mu.Lock()
	pending, present := acker.activationAckInFlight[key]
	acker.mu.Unlock()
	if !present || pending != 2 {
		t.Fatalf("in-flight after the stale completion = %d (present=%v), want gen 2", pending, present)
	}
	// A repeated failure for gen 2 stays deduped while its send is in flight.
	if acker.beginActivationAck(key, 2) {
		t.Fatal("gen 2 must stay deduped while its own send is in flight")
	}
	// Its own failure releases the reservation, so the retry is admitted.
	acker.finishActivationAck(key, 2, false)
	if !acker.beginActivationAck(key, 2) {
		t.Fatal("a failed gen 2 send must be retryable")
	}
	acker.finishActivationAck(key, 2, true)
	if acker.beginActivationAck(key, 2) {
		t.Fatal("a delivered gen 2 must stay deduped")
	}

	// (a) A failed generation's own completion releases its reservation, and a
	// stale completion of an older generation cannot re-open a newer one.
	if !acker.beginActivationAck(key, 3) {
		t.Fatal("gen 3 must be admitted")
	}
	acker.finishActivationAck(key, 1, false) // stale completion, must be a no-op
	acker.mu.Lock()
	pending, present = acker.activationAckInFlight[key]
	acker.mu.Unlock()
	if !present || pending != 3 {
		t.Fatalf("in-flight after a stale failure completion = %d (present=%v), want gen 3", pending, present)
	}
	acker.finishActivationAck(key, 3, false)
	if !acker.beginActivationAck(key, 3) {
		t.Fatal("a failed gen 3 send must be retryable")
	}
}
