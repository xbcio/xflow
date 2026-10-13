package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestKeyedRerunUsesTheParentScopedID: the rerun's ID derives from its parent
// and key, never from the caller's pre-allocation, and a duplicate seeds
// nothing.
func TestKeyedRerunUsesTheParentScopedID(t *testing.T) {
	g := rerunDiamond(t)
	base := newFakeState()
	parent := rerunParent(t, base, g, types.ExecutionStatusFailed, diamondResolved())
	claims := &claimRecorder{fakeState: base, state: IdempotencyClaimAccepted}
	seeder := &keyedSeeder{claimRecorder: claims}
	eng := New(seeder, &fakeQueue{})
	ctx := WithExecutionID(context.Background(), parent)

	res, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid", IdempotencyKey: "r-1"})
	if err != nil {
		t.Fatalf("Rerun() error = %v", err)
	}
	want := IdempotentExecutionID(namespace.Default, rerunIdempotencyScope(parent), "r-1")
	if res.ExecutionID != want || res.Duplicate {
		t.Fatalf("rerun = %+v, want a new execution %q", res, want)
	}
	if len(seeder.requests) != 1 || seeder.requests[0].Execution.ID != want {
		t.Fatalf("seed requests = %d, want one under %q", len(seeder.requests), want)
	}
	if len(claims.confirmed) != 1 {
		t.Fatalf("confirmed = %v, want the claim confirmed", claims.confirmed)
	}

	claims.state = IdempotencyClaimDuplicate
	again, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid", IdempotencyKey: "r-1"})
	if err != nil {
		t.Fatalf("retried Rerun() error = %v", err)
	}
	if !again.Duplicate || again.ExecutionID != want || len(seeder.requests) != 1 {
		t.Fatalf("retry = %+v with %d seeds, want a duplicate that seeded nothing", again, len(seeder.requests))
	}

	// The fingerprint covers the from-node: the same key elsewhere is a
	// different request.
	fpMid, _ := rerunFingerprint(RerunRequest{ExecutionID: parent, FromNode: "mid"})
	fpEnd, _ := rerunFingerprint(RerunRequest{ExecutionID: parent, FromNode: "tail"})
	if fpMid == fpEnd {
		t.Fatal("rerun fingerprint ignores the from-node")
	}
}

type keyedSeeder struct {
	*claimRecorder
	requests []SeedExecutionRequest
}

func (s *keyedSeeder) SeedExecution(_ context.Context, req SeedExecutionRequest) (SeedExecutionResponse, error) {
	s.requests = append(s.requests, req)
	return SeedExecutionResponse{}, nil
}
