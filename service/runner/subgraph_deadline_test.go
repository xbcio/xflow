package runner

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// blockingBodyHandler holds until its context is cancelled or a long timer
// fires, so a test can tell "stopped by the deadline" from "ran to completion".
type blockingBodyHandler struct {
	completed chan struct{}
}

func (h *blockingBodyHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.body_item"}
}

func (h *blockingBodyHandler) Execute(ctx context.Context, _ *types.Input) (*types.Output, error) {
	select {
	case <-time.After(5 * time.Second):
		close(h.completed)
		return &types.Output{Data: map[string]any{}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// The runner's SubgraphRuntime is built once at startup with no deadline of
// its own, so the per-lease payload.Deadline is the only thing that can stop a
// body outliving its map node's timeout on a distributed deployment.
func TestSubgraphRuntimeStopsABodyPastThePayloadDeadline(t *testing.T) {
	reg := execution.NewRegistry()
	handler := &blockingBodyHandler{completed: make(chan struct{})}
	reg.RegisterGlobal("test.body_item", handler)
	rt := NewSubgraphRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 4}))

	lease := batchBodyLease(t, bodyPackageForTest())
	lease.SubgraphPayload.Items = []any{"c"}
	lease.SubgraphPayload.Deadline = time.Now().Add(200 * time.Millisecond)

	start := time.Now()
	result, err := rt.Execute(context.Background(), lease)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("batch took %v, want it stopped near the 200ms payload deadline", elapsed)
	}
	if result.Error == nil {
		t.Fatalf("batch reported success after its body was cut off by the deadline: %+v", result)
	}
	select {
	case <-handler.completed:
		t.Fatal("body member ran to completion past the payload deadline")
	default:
	}
}
