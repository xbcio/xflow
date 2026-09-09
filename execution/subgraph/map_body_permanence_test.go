package subgraph

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// batchOfOneFailingMember runs a one-item batch whose body is a single member
// node that always fails, and returns that item's result.
//
// It goes in through ExecuteBatchBody on purpose: everything else in this file
// calls itemFailure directly, which cannot tell whether runItem still calls it.
func batchOfOneFailingMember(t *testing.T, nodeType string, handler types.ActionHandler) engine.BatchItemResult {
	t.Helper()

	pkg := buildSingleNodePackage(nodeType)
	reg := execution.NewRegistry()
	reg.RegisterGlobal(nodeType, handler)
	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) })
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	results, err := NewMapBodyExecutor(ex, false, time.Time{}).ExecuteBatchBody(
		context.Background(), engine.BatchBodyRequest{
			Body:      pkg,
			BodyHash:  hash,
			Items:     []any{map[string]any{"seed": 1}},
			BatchSize: 1,
		})
	// A body that could not be RUN at all is a different failure entirely, and it
	// would leave the per-item assertions below with nothing to inspect. Fatal
	// rather than tolerated, so that case can never masquerade as a pass.
	if err != nil {
		t.Fatalf("execute batch body: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want exactly 1", len(results))
	}
	if results[0].Err == nil {
		t.Fatal("the member node always fails, yet the item reported no error")
	}
	return results[0]
}

// TestItemFailure_CarriesPermanence pins the hop where a failed body item's
// permanence used to be flattened into text.
//
// Why it changes behaviour rather than only shape: BatchResultForCommit returns
// the first failed item's Err as the whole BATCH's error, and completeAtomic
// then asks types.IsPermanent(cause) whether to retry. Before this, every item
// failure reached that check as a bare fmt.Errorf, which is transient by
// default -- so a body item that fails identically on every redelivery (a bad
// rule, a malformed package) was re-run to MaxAttempts.
//
// The false arm is not filler. Marking everything permanent would be just as
// wrong in the other direction: a timeout or a cancel is environmental, and
// Result.Permanent is already documented as meaningful only for a genuine
// failure. Asserting both arms is what makes this test able to fail when the
// flag is ignored in either direction.
func TestItemFailure_CarriesPermanence(t *testing.T) {
	tests := []struct {
		name          string
		res           Result
		wantPermanent bool
	}{
		{
			name:          "permanent failure",
			res:           Result{Outcome: OutcomeFailed, Error: "rule compile failed", Permanent: true},
			wantPermanent: true,
		},
		{
			name:          "transient failure",
			res:           Result{Outcome: OutcomeFailed, Error: "connection refused", Permanent: false},
			wantPermanent: false,
		},
		{
			// A timeout is environmental: the same input may well succeed on the
			// next attempt, so it must NOT be stamped permanent even though it is
			// a non-success outcome.
			name:          "timeout is never permanent",
			res:           Result{Outcome: OutcomeTimeout, Error: "deadline exceeded"},
			wantPermanent: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := itemFailure(tt.res)
			if err == nil {
				t.Fatal("itemFailure returned nil for a non-success outcome")
			}
			if got := types.IsPermanent(err); got != tt.wantPermanent {
				t.Fatalf("types.IsPermanent = %v, want %v -- the batch's retry "+
					"decision reads this flag off the item's error", got, tt.wantPermanent)
			}
		})
	}
}

// TestItemFailure_MessageIsUnchangedByPermanence guards a user-visible string,
// not an internal one.
//
// batchResultData writes Err.Error() into each failed item's _error slot in the
// map node's output, so this text is what a workflow author reads and what a
// downstream filter matches on. The obvious way to stamp permanence --
// errors.Join(types.ErrPermanent, err) -- renders as TWO lines, which would
// silently rewrite that field for every permanent failure.
//
// So the assertion is equality against the exact string the old fmt.Errorf
// produced, and it is deliberately identical for both arms: whatever the
// classification, the message a user sees must not move.
func TestItemFailure_MessageIsUnchangedByPermanence(t *testing.T) {
	for _, permanent := range []bool{true, false} {
		t.Run(fmt.Sprintf("permanent=%v", permanent), func(t *testing.T) {
			res := Result{Outcome: OutcomeFailed, Error: "boom", Permanent: permanent}
			want := fmt.Sprintf("%s: %s", res.Outcome, res.Error)
			if got := itemFailure(res).Error(); got != want {
				t.Fatalf("Error() = %q, want %q -- this string lands in the map "+
					"node's _error field, so stamping permanence must not alter it "+
					"(errors.Join would make it two lines)", got, want)
			}
		})
	}
}

// TestExecuteBatchBody_ItemFailureCarriesPermanence drives the WIRING that the
// two tests above cannot see.
//
// Both of them call itemFailure directly. That proves the function classifies
// correctly, and proves nothing about whether anyone still calls it: reverting
// map_body.go's `itemResult.Err = itemFailure(res)` to the original
// `fmt.Errorf("%s: %s", res.Outcome, res.Error)` leaves them both green, which
// means the entire fix can be undone with no red signal. This test enters
// through ExecuteBatchBody, so that call site is on the path.
//
// Both arms are load-bearing. A lone permanent arm would also pass an
// implementation that stamps EVERY item failure permanent -- the more damaging
// direction of the two, since it commits the Kafka offsets of batches a retry
// would have handled, discarding real messages instead of merely re-running
// them.
func TestExecuteBatchBody_ItemFailureCarriesPermanence(t *testing.T) {
	tests := []struct {
		name          string
		nodeType      string
		handler       types.ActionHandler
		wantPermanent bool
	}{
		{"member marks itself permanent", "test.permfail", sentinelPermanentHandler{}, true},
		{"member failure is unclassified", "test.transientfail", transientHandler{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := batchOfOneFailingMember(t, tt.nodeType, tt.handler)
			if got := types.IsPermanent(item.Err); got != tt.wantPermanent {
				t.Fatalf("types.IsPermanent(item err) = %v, want %v -- this is the "+
					"flag BatchResultForCommit hands up as the batch's error, and "+
					"completeAtomic's retry-vs-skip decision reads it", got, tt.wantPermanent)
			}
		})
	}
}

// TestExecuteBatchBody_PermanentItemErrorSurvivesTheWire pins the one field
// every other assertion in this file is blind to.
//
// types.IsPermanent resolves through ClassifiedError.Is, which consults
// Permanent alone -- Kind is not part of that decision. So Kind:
// ErrorKindTransient on an error that is Permanent: true passes every test
// above, and changes no retry behaviour anywhere in this repo.
//
// Kind is not dead weight, though. It leaves the process: BatchResultForCommit
// returns the item's error unwrapped, service/runner assigns it to
// TaskResult.Error unwrapped, and protocol.MarshalTaskResult then serialises the
// whole DTO into error_detail. A wrong Kind arrives at the control plane as a
// classification that contradicts its own Permanent flag, with no local symptom
// to give it away.
//
// The type assertion is deliberate and is NOT interchangeable with errors.As.
// MarshalTaskResult type-switches on *types.ClassifiedError, and a type switch
// does not see through %w. Were someone to wrap this error anywhere on the way
// up, errors.As would keep passing here while the production switch silently
// missed and fell back to synthesising a DTO from the error text. Asserting the
// way the consumer consumes is what keeps the two from drifting apart.
func TestExecuteBatchBody_PermanentItemErrorSurvivesTheWire(t *testing.T) {
	item := batchOfOneFailingMember(t, "test.permfail", sentinelPermanentHandler{})

	classified, ok := item.Err.(*types.ClassifiedError)
	if !ok {
		t.Fatalf("item error is %T, not *types.ClassifiedError -- "+
			"protocol.MarshalTaskResult type-switches on exactly this type, so it "+
			"would fall back to rebuilding a DTO out of the error text", item.Err)
	}
	if classified.Kind != types.ErrorKindPermanent {
		t.Fatalf("Kind = %q, want %q -- error_detail.kind is what the control "+
			"plane reads; a transient kind on a permanent error contradicts "+
			"itself and no assertion on retry behaviour would notice",
			classified.Kind, types.ErrorKindPermanent)
	}
}
