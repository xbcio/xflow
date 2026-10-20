package redis

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// claimStore is the minimal dedup state a fake runtime needs to tell a released
// claim from one that was left behind. The production primitives hold this in
// Redis (or a map); the trigger only acts on the boolean it answers.
type claimStore struct {
	mu      sync.Mutex
	claimed map[string]struct{}
}

func newClaimStore() *claimStore {
	return &claimStore{claimed: make(map[string]struct{})}
}

func (s *claimStore) dedup(_ context.Context, key string, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.claimed[key]; ok {
		return false, nil
	}
	s.claimed[key] = struct{}{}
	return true, nil
}

func (s *claimStore) release(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.claimed, key)
	return nil
}

// TestEmitMessageEmitFailureReleasesDedupKey: the claim is taken before the
// emit, so an emit failure must give it back. Otherwise the consumer group's
// redelivery of the unacknowledged entry is answered as a duplicate and dropped
// for the whole 24h window — at-most-once instead of at-least-once.
func TestEmitMessageEmitFailureReleasesDedupKey(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	claims := newClaimStore()
	rt.SetDedupFunc(claims.dedup)
	rt.SetReleaseDedupFunc(claims.release)
	var calls int
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		calls++
		if calls == 1 {
			return "", errors.New("boom")
		}
		return "exec-2", nil
	})
	in := &types.TriggerActivateInput{WorkflowID: "wf-1", NodeName: "hub", Runtime: rt}
	msg := Message{Stream: "orders", ID: "1700000000000-0", Payload: []byte("one"), Time: time.Unix(1700000000, 0)}

	if emitMessage(context.Background(), in, "stream", msg) {
		t.Fatal("a failed emit must leave the delivery unfinished so the entry is not acknowledged")
	}
	released := rt.ReleaseCalls()
	if len(released) != 1 {
		t.Fatalf("release calls = %d, want 1: the failed emit left its dedup claim set", len(released))
	}
	if key := rt.DedupCalls()[0].Key; released[0] != key {
		t.Fatalf("released key = %q, want the claimed key %q", released[0], key)
	}

	// The redelivery must reach Emit again, not be folded into the failed
	// attempt's claim.
	if !emitMessage(context.Background(), in, "stream", msg) {
		t.Fatal("redelivery after a failed emit was not finished")
	}
	if calls != 2 {
		t.Fatalf("emit calls = %d, want 2 (one failed, one redelivered)", calls)
	}
}

// TestEmitMessageSuccessfulEmitKeepsDedupKey is the other half of the contract:
// a claim whose emit succeeded must survive, or the redelivery starts a second
// execution.
func TestEmitMessageSuccessfulEmitKeepsDedupKey(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	claims := newClaimStore()
	rt.SetDedupFunc(claims.dedup)
	rt.SetReleaseDedupFunc(claims.release)
	in := &types.TriggerActivateInput{WorkflowID: "wf-1", NodeName: "hub", Runtime: rt}
	msg := Message{Stream: "orders", ID: "1700000000000-1", Payload: []byte("one"), Time: time.Unix(1700000000, 0)}

	for i := 0; i < 2; i++ {
		if !emitMessage(context.Background(), in, "stream", msg) {
			t.Fatalf("delivery %d was left unfinished; a duplicate is finished with", i+1)
		}
	}
	if got := rt.EmitCount(); got != 1 {
		t.Fatalf("emit calls = %d, want 1: the successful emit's claim was released, so the redelivery re-ran the workflow", got)
	}
	if got := rt.ReleaseCalls(); len(got) != 0 {
		t.Fatalf("released %d dedup keys after a successful emit, want 0", len(got))
	}
}
