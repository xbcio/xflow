package control

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// fakeDeadLetterStore is a minimal engine.DeadLetterStore for manager tests.
type fakeDeadLetterStore struct {
	mu       sync.Mutex
	replayed int
	result   engine.ReplayDeadLetterResult
	err      error
}

func (f *fakeDeadLetterStore) ListDeadLetters(context.Context, types.ExecutionID, engine.DeadLetterPage) (engine.DeadLetterList, error) {
	return engine.DeadLetterList{}, nil
}
func (f *fakeDeadLetterStore) ReplayDeadLetter(_ context.Context, req engine.ReplayDeadLetterRequest) (engine.ReplayDeadLetterResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replayed++
	_ = req
	if f.err != nil {
		return engine.ReplayDeadLetterResult{}, f.err
	}
	if f.result.Outcome == "" {
		return engine.ReplayDeadLetterResult{Outcome: engine.ReplayReplayed, AuditID: "audit-1", ExecutionID: req.ExecutionID, NodeID: "review", ActivationID: "1"}, nil
	}
	return f.result, nil
}

// captureOutboxObserver records every OnOutboxReplayed outcome and
// OnOutboxError operation for metric/alarm assertions.
type captureOutboxObserver struct {
	mu       sync.Mutex
	outcomes []engine.DeadLetterReplayOutcome
	errors   int
	errOps   []string
	pending  []int
	dead     []int
}

func (c *captureOutboxObserver) OnOutboxRetry(context.Context, int) {}
func (c *captureOutboxObserver) OnOutboxDeadLetter(context.Context) {}
func (c *captureOutboxObserver) OnOutboxPending(_ context.Context, pending, deadLettered int, _ time.Duration) {
	c.mu.Lock()
	c.pending = append(c.pending, pending)
	c.dead = append(c.dead, deadLettered)
	c.mu.Unlock()
}
func (c *captureOutboxObserver) OnOutboxError(_ context.Context, op string, _ error) {
	c.mu.Lock()
	c.errors++
	c.errOps = append(c.errOps, op)
	c.mu.Unlock()
}
func (c *captureOutboxObserver) OnOutboxReplayed(_ context.Context, outcome engine.DeadLetterReplayOutcome) {
	c.mu.Lock()
	c.outcomes = append(c.outcomes, outcome)
	c.mu.Unlock()
}

func (c *captureOutboxObserver) countErrors(op string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.errOps {
		if e == op {
			n++
		}
	}
	return n
}

type fakeAuditSink struct {
	recorded int
	last     engine.ReplayDeadLetterResult
}

func (f *fakeAuditSink) RecordReplay(_ context.Context, res engine.ReplayDeadLetterResult, _ engine.ReplayDeadLetterRequest) error {
	f.recorded++
	f.last = res
	return nil
}

func principalWithScope() DeadLetterReplayPrincipal {
	return DeadLetterReplayPrincipal{Subject: "alice", Scopes: []string{ScopeDeadLetterReplay}}
}

func TestDeadLetterManagerRecordsMetricAndAudit(t *testing.T) {
	store := &fakeDeadLetterStore{}
	obs := &captureOutboxObserver{}
	audit := &fakeAuditSink{}
	mgr := NewDeadLetterManager(store, obs, audit)

	res, err := mgr.Replay(context.Background(), principalWithScope(), engine.ReplayDeadLetterRequest{
		ExecutionID: "exec-1", EntryID: "entry-1", Reason: "root cause fixed",
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Outcome != engine.ReplayReplayed {
		t.Fatalf("outcome = %q, want replayed", res.Outcome)
	}
	if got := obs.outcomes; len(got) != 1 || got[0] != engine.ReplayReplayed {
		t.Fatalf("metric outcomes = %v, want [replayed]", got)
	}
	if audit.recorded != 1 || audit.last.AuditID != "audit-1" {
		t.Fatalf("audit not projected: recorded=%d last=%+v", audit.recorded, audit.last)
	}
}

func TestDeadLetterManagerRejectsInvalidRequest(t *testing.T) {
	store := &fakeDeadLetterStore{}
	obs := &captureOutboxObserver{}
	mgr := NewDeadLetterManager(store, obs, nil)

	cases := []struct {
		name string
		req  engine.ReplayDeadLetterRequest
	}{
		{"missing entry", engine.ReplayDeadLetterRequest{ExecutionID: "x", Reason: "r"}},
		{"missing reason", engine.ReplayDeadLetterRequest{ExecutionID: "x", EntryID: "e"}},
		{"overlong reason", engine.ReplayDeadLetterRequest{ExecutionID: "x", EntryID: "e", Reason: strings.Repeat("x", maxReplayReasonLen+1)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, _ := mgr.Replay(context.Background(), principalWithScope(), c.req)
			if res.Outcome != engine.ReplayInvalidRequest {
				t.Fatalf("outcome = %q, want invalid_request", res.Outcome)
			}
			if store.replayed != 0 {
				t.Fatalf("store was touched for invalid request %q", c.name)
			}
		})
	}
	// invalid_request must still record a metric at the single outlet.
	if len(obs.outcomes) != len(cases) {
		t.Fatalf("metric outcomes = %d, want %d (one per invalid request)", len(obs.outcomes), len(cases))
	}
}

func TestDeadLetterManagerRejectsUnauthorized(t *testing.T) {
	store := &fakeDeadLetterStore{}
	obs := &captureOutboxObserver{}
	mgr := NewDeadLetterManager(store, obs, nil)

	principal := DeadLetterReplayPrincipal{Subject: "mallory"} // no scope
	res, _ := mgr.Replay(context.Background(), principal, engine.ReplayDeadLetterRequest{
		ExecutionID: "x", EntryID: "e", Reason: "r",
	})
	if res.Outcome != engine.ReplayUnauthorized {
		t.Fatalf("outcome = %q, want unauthorized", res.Outcome)
	}
	if store.replayed != 0 {
		t.Fatalf("unauthorized replay reached the store")
	}
	if len(obs.outcomes) != 1 || obs.outcomes[0] != engine.ReplayUnauthorized {
		t.Fatalf("unauthorized metric not recorded: %v", obs.outcomes)
	}
}

func TestDeadLetterManagerRejectsNamespaceMismatch(t *testing.T) {
	store := &fakeDeadLetterStore{}
	obs := &captureOutboxObserver{}
	mgr := NewDeadLetterManager(store, obs, nil)

	ctx := namespace.WithNamespace(context.Background(), "namespace-a")
	principal := DeadLetterReplayPrincipal{Subject: "alice", Namespace: "namespace-b", Scopes: []string{ScopeDeadLetterReplay}}
	res, _ := mgr.Replay(ctx, principal, engine.ReplayDeadLetterRequest{
		ExecutionID: "x", EntryID: "e", Reason: "r",
	})
	if res.Outcome != engine.ReplayUnauthorized {
		t.Fatalf("outcome = %q, want unauthorized", res.Outcome)
	}
	if store.replayed != 0 {
		t.Fatalf("namespace-mismatched replay reached the store")
	}
	if len(obs.outcomes) != 1 || obs.outcomes[0] != engine.ReplayUnauthorized {
		t.Fatalf("namespace mismatch metric not recorded: %v", obs.outcomes)
	}
}

func TestDeadLetterManagerAllowsNamespaceMatch(t *testing.T) {
	store := &fakeDeadLetterStore{}
	obs := &captureOutboxObserver{}
	mgr := NewDeadLetterManager(store, obs, nil)

	ctx := namespace.WithNamespace(context.Background(), "namespace-a")
	principal := DeadLetterReplayPrincipal{Subject: "alice", Namespace: "namespace-a", Scopes: []string{ScopeDeadLetterReplay}}
	res, err := mgr.Replay(ctx, principal, engine.ReplayDeadLetterRequest{
		ExecutionID: "x", EntryID: "e", Reason: "r",
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Outcome != engine.ReplayReplayed {
		t.Fatalf("outcome = %q, want replayed", res.Outcome)
	}
	if store.replayed != 1 {
		t.Fatalf("store.replayed = %d, want 1", store.replayed)
	}
}

func TestDeadLetterManagerSkipsNamespaceCheckWhenPrincipalNamespaceEmpty(t *testing.T) {
	store := &fakeDeadLetterStore{}
	obs := &captureOutboxObserver{}
	mgr := NewDeadLetterManager(store, obs, nil)

	ctx := namespace.WithNamespace(context.Background(), "namespace-a")
	principal := DeadLetterReplayPrincipal{Subject: "alice", Namespace: "", Scopes: []string{ScopeDeadLetterReplay}}
	res, err := mgr.Replay(ctx, principal, engine.ReplayDeadLetterRequest{
		ExecutionID: "x", EntryID: "e", Reason: "r",
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Outcome != engine.ReplayReplayed {
		t.Fatalf("outcome = %q, want replayed", res.Outcome)
	}
	if store.replayed != 1 {
		t.Fatalf("store.replayed = %d, want 1 (G0 backward compatibility)", store.replayed)
	}
}

func TestDeadLetterManagerPropagatesStoreError(t *testing.T) {
	store := &fakeDeadLetterStore{err: errors.New("redis boom")}
	obs := &captureOutboxObserver{}
	mgr := NewDeadLetterManager(store, obs, nil)

	_, err := mgr.Replay(context.Background(), principalWithScope(), engine.ReplayDeadLetterRequest{
		ExecutionID: "x", EntryID: "e", Reason: "r",
	})
	if err == nil {
		t.Fatal("expected store error to propagate")
	}
	if obs.errors != 1 {
		t.Fatalf("error metric = %d, want 1", obs.errors)
	}
}
