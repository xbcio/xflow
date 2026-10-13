package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// claimRecorder is a scripted IdempotentStateStore: it answers every claim
// with the configured state and records what the engine asked for, so the
// engine's half — the ID it derives, the window it asks for, how it settles —
// can be checked apart from any store's. The store half is the statestore
// contract.
type claimRecorder struct {
	*fakeState
	state     IdempotencyClaimState
	status    types.ExecutionStatus
	claims    []IdempotencyClaimRequest
	confirmed []string
	released  []string
}

func (s *claimRecorder) ClaimIdempotency(_ context.Context, req IdempotencyClaimRequest) (IdempotencyClaim, error) {
	s.claims = append(s.claims, req)
	return IdempotencyClaim{State: s.state, Status: s.status}, nil
}

func (s *claimRecorder) ConfirmIdempotency(_ context.Context, _ types.ExecutionID, token string) error {
	s.confirmed = append(s.confirmed, token)
	return nil
}

func (s *claimRecorder) ReleaseIdempotency(_ context.Context, _ types.ExecutionID, token string) error {
	s.released = append(s.released, token)
	return nil
}

// TestKeyedSubmitUsesTheDeterministicID: a keyed submission persists under the
// key's ID even when the API layer pre-allocated one, because the retry that
// matters has to find it, and confirms the claim it took.
func TestKeyedSubmitUsesTheDeterministicID(t *testing.T) {
	state := &claimRecorder{fakeState: newFakeState(), state: IdempotencyClaimAccepted}
	eng := New(state, &fakeQueue{})
	g := idempotencyTestGraph(t, nil)
	ctx := namespace.WithNamespace(context.Background(), "tenant-a")
	ctx = WithExecutionID(ctx, "exec-preallocated")
	idem := &Idempotency{Key: "k", Scope: WorkflowIdempotencyScope("wf")}
	ctx = WithIdempotency(ctx, idem)

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if want := IdempotentExecutionID("tenant-a", "workflow:wf", "k"); id != want {
		t.Fatalf("id = %q, want %q", id, want)
	}
	if snap, _ := state.GetExecution(context.Background(), id); snap == nil {
		t.Fatal("keyed execution was not created")
	}
	if idem.Duplicate {
		t.Fatal("first keyed Submit reported Duplicate")
	}
	if len(state.claims) != 1 || len(state.confirmed) != 1 || state.confirmed[0] != state.claims[0].Token {
		t.Fatalf("claims=%d confirmed=%v, want one claim confirmed under its own token", len(state.claims), state.confirmed)
	}
	if state.claims[0].Token == "" {
		t.Fatal("claim carried no token")
	}
}

func TestKeyedSubmitDuplicateCreatesNothing(t *testing.T) {
	state := &claimRecorder{fakeState: newFakeState(), state: IdempotencyClaimDuplicate}
	eng := New(state, &fakeQueue{})
	idem := &Idempotency{Key: "k", Scope: WorkflowIdempotencyScope("wf")}

	id, err := eng.Invoke(WithIdempotency(context.Background(), idem), idempotencyTestGraph(t, nil), "start", nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if !idem.Duplicate || id != IdempotentExecutionID(namespace.Default, "workflow:wf", "k") {
		t.Fatalf("duplicate = (%q, %v), want the deterministic id and Duplicate", id, idem.Duplicate)
	}
	if snap, _ := state.GetExecution(context.Background(), id); snap != nil {
		t.Fatal("a duplicate created an execution")
	}
	if len(state.confirmed)+len(state.released) != 0 {
		t.Fatal("a duplicate settled a claim it does not hold")
	}
}

func TestKeyedSubmitRefusals(t *testing.T) {
	cases := map[IdempotencyClaimState]error{
		IdempotencyClaimConflict:   ErrIdempotencyConflict,
		IdempotencyClaimInProgress: ErrIdempotencyInProgress,
	}
	for claimState, want := range cases {
		state := &claimRecorder{fakeState: newFakeState(), state: claimState}
		eng := New(state, &fakeQueue{})
		ctx := WithIdempotency(context.Background(), &Idempotency{Key: "k", Scope: "workflow:wf"})
		if _, err := eng.Submit(ctx, idempotencyTestGraph(t, nil), nil); !errors.Is(err, want) {
			t.Errorf("%s: Submit() error = %v, want %v", claimState, err, want)
		}
	}

	// A store without the capability fails closed rather than ignoring the key.
	eng := New(newFakeState(), &fakeQueue{})
	ctx := WithIdempotency(context.Background(), &Idempotency{Key: "k", Scope: "workflow:wf"})
	if _, err := eng.Submit(ctx, idempotencyTestGraph(t, nil), nil); !errors.Is(err, ErrIdempotencyUnsupported) {
		t.Fatalf("Submit() on an incapable store error = %v, want ErrIdempotencyUnsupported", err)
	}

	// An invalid key is refused before any store is asked.
	state := &claimRecorder{fakeState: newFakeState(), state: IdempotencyClaimAccepted}
	eng = New(state, &fakeQueue{})
	ctx = WithIdempotency(context.Background(), &Idempotency{Key: "bad key", Scope: "workflow:wf"})
	if _, err := eng.Submit(ctx, idempotencyTestGraph(t, nil), nil); !errors.Is(err, ErrIdempotencyKeyInvalid) {
		t.Fatalf("Submit() with an invalid key error = %v, want ErrIdempotencyKeyInvalid", err)
	}
	if len(state.claims) != 0 {
		t.Fatal("an invalid key reached the store")
	}
}

// TestKeyedSubmitReleasesOnRefusal: a refusal that persisted nothing gives the
// claim back, so the retry the 429 invites is not answered as a duplicate.
func TestKeyedSubmitReleasesOnRefusal(t *testing.T) {
	state := &claimRecorder{fakeState: newFakeState(), state: IdempotencyClaimAccepted}
	eng := New(state, &fakeQueue{})
	g := idempotencyTestGraph(t, &types.WorkflowDef{
		Name:     "idem-limited",
		Version:  "1",
		Nodes:    []types.NodeDef{{Name: "start", Type: "xflow.start"}},
		Settings: &types.WorkflowSettings{Concurrency: &types.ConcurrencySettings{Limit: 1, Mode: types.ConcurrencyModeReject}},
	})
	ctx := WithIdempotency(context.Background(), &Idempotency{Key: "k", Scope: "workflow:wf"})
	if _, err := eng.Submit(context.Background(), g, nil); err != nil {
		t.Fatalf("slot holder Submit() error = %v", err)
	}
	if _, err := eng.Submit(ctx, g, nil); !errors.Is(err, ErrConcurrencyLimit) {
		t.Fatalf("keyed Submit() error = %v, want a concurrency refusal", err)
	}
	if len(state.released) != 1 || len(state.confirmed) != 0 {
		t.Fatalf("released=%v confirmed=%v, want the claim released", state.released, state.confirmed)
	}
}
