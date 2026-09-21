package xflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestFAFBindingUsesStoredGraphAfterTimeoutOnlyReregistration(t *testing.T) {
	ctx := context.Background()
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	handler := &fafBindingRegressionHandler{calls: make(chan int, 1)}
	const firstTimeout = time.Minute
	const secondTimeout = 2 * time.Minute

	first := Workflow("faf-binding-timeout-idempotent").FAF()
	first.LocalNode("only", handler).Timeout(firstTimeout)
	workflowID, err := eng.AddWorkflow(ctx, first)
	if err != nil {
		t.Fatalf("AddWorkflow(first) error = %v", err)
	}

	second := Workflow("faf-binding-timeout-idempotent").FAF()
	second.LocalNode("only", handler).Timeout(secondTimeout)
	secondID, err := eng.AddWorkflow(ctx, second)
	if err != nil {
		t.Fatalf("AddWorkflow(second) error = %v", err)
	}
	if secondID != workflowID {
		t.Fatalf("idempotent AddWorkflow() id = %q, want %q", secondID, workflowID)
	}

	stored, err := eng.workflowRegistry.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatalf("GetWorkflow() error = %v", err)
	}
	if got := stored.Graph.NodeAt(0).Timeout; got != firstTimeout {
		t.Fatalf("stored graph timeout = %s, want first registration timeout %s", got, firstTimeout)
	}

	if err := eng.FireAndForget(ctx, workflowID, nil); err != nil {
		t.Fatalf("FireAndForget() error = %v", err)
	}
	if got := waitFAFBindingCall(t, handler.calls); got != 1 {
		t.Fatalf("dispatched handler marker = %d, want 1", got)
	}
}

func TestFAFBindingResolvesVersionPinnedTypedHandler(t *testing.T) {
	const nodeType = "test.faf.binding.versioned"
	calls := make(chan int, 2)
	v1 := &fafBindingRegressionHandler{nodeType: nodeType, version: 1, calls: calls}
	v2 := &fafBindingRegressionHandler{nodeType: nodeType, version: 2, calls: calls}
	registry.Register(v1)
	registry.Register(v2)

	eng, err := NewLocal(WithNodes(v2))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	for _, tc := range []struct {
		name    string
		version int
	}{
		{name: "v1", version: 1},
		{name: "v2", version: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := Workflow("faf-binding-version-" + tc.name).FAF()
			wf.Node("only", &fafVersionPinnedBuilder{nodeType: nodeType, version: tc.version})
			workflowID, err := eng.AddWorkflow(context.Background(), wf)
			if err != nil {
				t.Fatalf("AddWorkflow() error = %v", err)
			}

			stored, err := eng.workflowRegistry.GetWorkflow(context.Background(), workflowID)
			if err != nil {
				t.Fatalf("GetWorkflow() error = %v", err)
			}
			if got := stored.Graph.NodeAt(0).Version; got != tc.version {
				t.Fatalf("stored node version = %d, want %d", got, tc.version)
			}

			if err := eng.FireAndForget(context.Background(), workflowID, nil); err != nil {
				t.Fatalf("FireAndForget() error = %v", err)
			}
			if got := waitFAFBindingCall(t, calls); got != tc.version {
				t.Fatalf("dispatched handler version = %d, want %d", got, tc.version)
			}
		})
	}
}

func TestFAFBindingRejectsMissingExactVersion(t *testing.T) {
	const nodeType = "test.faf.binding.missing-exact-version"
	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	latest := &fafBindingRegressionHandler{nodeType: nodeType, version: 2, calls: make(chan int, 1)}
	wf := Workflow("faf-binding-missing-exact-version")
	// Both type-keyed mirrors offer v2, but the definition explicitly requires
	// v1 and no exact registry entry exists. FAF must fail rather than bind v2.
	wf.handlers[nodeType] = latest
	eng.mu.Lock()
	eng.globalHandlers[nodeType] = latest
	_, err = eng.resolveFAFWorkflowHandlerLocked(wf, &types.WorkflowDef{
		Name: "faf-binding-missing-exact-version",
		Nodes: []types.NodeDef{{
			Name:    "only",
			Type:    nodeType,
			Kind:    types.NodeKindAction,
			Version: 1,
		}},
	})
	eng.mu.Unlock()
	if !errors.Is(err, ErrFAFHandlerBindingRequired) {
		t.Fatalf("resolveFAFWorkflowHandlerLocked() error = %v, want ErrFAFHandlerBindingRequired", err)
	}
}

func waitFAFBindingCall(t *testing.T, calls <-chan int) int {
	t.Helper()
	select {
	case got := <-calls:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("FAF handler did not execute")
		return 0
	}
}

type fafBindingRegressionHandler struct {
	nodeType string
	version  int
	calls    chan int
}

func (h *fafBindingRegressionHandler) Descriptor() types.Descriptor {
	nodeType := h.nodeType
	if nodeType == "" {
		nodeType = "test.faf.binding.timeout"
	}
	return types.Descriptor{Type: nodeType, Kind: types.NodeKindAction}
}

func (h *fafBindingRegressionHandler) NodeVersion() int {
	if h.version == 0 {
		return 1
	}
	return h.version
}

func (h *fafBindingRegressionHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	h.calls <- h.NodeVersion()
	return &types.Output{}, nil
}

// fafVersionPinnedBuilder deliberately provides metadata and a version but no
// local handler. It makes the registry's type-only v2 mirror observable while
// keeping the requested node version explicit in the workflow definition.
type fafVersionPinnedBuilder struct {
	nodeType string
	version  int
	onError  types.OnError
}

func (b *fafVersionPinnedBuilder) NodeType() string { return b.nodeType }
func (*fafVersionPinnedBuilder) RawParams() any     { return map[string]any{} }
func (b *fafVersionPinnedBuilder) OnError(strategy types.OnError) types.Builder {
	b.onError = strategy
	return b
}
func (b *fafVersionPinnedBuilder) OnErrorStrategy() types.OnError { return b.onError }
func (b *fafVersionPinnedBuilder) NodeVersion() int               { return b.version }
func (b *fafVersionPinnedBuilder) Descriptor() types.Descriptor {
	return types.Descriptor{Type: b.nodeType, Kind: types.NodeKindAction}
}
