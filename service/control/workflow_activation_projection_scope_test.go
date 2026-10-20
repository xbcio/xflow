package control

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
)

// scopeRecordingOutbox reports a fixed namespace list and records which
// namespaces the worker asked for pending refs in. It never has pending work,
// so the worker's projector is never reached.
type scopeRecordingOutbox struct {
	mu         sync.Mutex
	namespaces []namespace.Namespace
	listed     []namespace.Namespace
}

var _ backend.WorkflowActivationProjectionOutbox = (*scopeRecordingOutbox)(nil)

func (o *scopeRecordingOutbox) ListWorkflowActivationProjectionNamespaces(context.Context) ([]namespace.Namespace, error) {
	return append([]namespace.Namespace(nil), o.namespaces...), nil
}

func (o *scopeRecordingOutbox) ListPendingWorkflowActivationProjections(_ context.Context, ns namespace.Namespace, _ uint64, _ int) ([]backend.WorkflowActivationProjectionRef, uint64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.listed = append(o.listed, ns)
	return nil, 0, nil
}

func (o *scopeRecordingOutbox) ClaimWorkflowActivationProjection(context.Context, namespace.Namespace, string, string, time.Duration) (backend.WorkflowActivationProjectionClaim, error) {
	return backend.WorkflowActivationProjectionClaim{}, nil
}

func (o *scopeRecordingOutbox) AckWorkflowActivationProjection(context.Context, namespace.Namespace, string, string) (bool, error) {
	return false, nil
}

func TestWorkflowActivationProjectionWorkerScopesNamespaces(t *testing.T) {
	all := nsList("default", "local-jack", "team-a")
	tests := []struct {
		name  string
		scope []namespace.Namespace
		want  []namespace.Namespace
	}{
		// Mutation: filtering when no scope is configured would stop legacy
		// deployments from draining any namespace.
		{"nil scope visits every namespace", nil, all},
		{"only-empty scope is unscoped", nsList(""), all},
		// Mutation: ignoring the scope drains and advances another deployment's
		// projection intents.
		{"scope visits its own namespace only", nsList("local-jack"), nsList("local-jack")},
		{"scope keeps outbox order", nsList("team-a", "default"), nsList("default", "team-a")},
		{"scope outside the outbox visits nothing", nsList("unknown"), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outbox := &scopeRecordingOutbox{namespaces: all}
			worker := NewWorkflowActivationProjectionWorker(WorkflowActivationProjectionWorkerConfig{
				Outbox: outbox, Leader: backend.AlwaysLeader{}, Namespaces: tc.scope,
			})
			worker.ReconcileOnce(context.Background())
			if !reflect.DeepEqual(outbox.listed, tc.want) {
				t.Fatalf("listed namespaces = %v, want %v", outbox.listed, tc.want)
			}
		})
	}
}

// The recording outbox proves which namespaces are visited; this proves the
// consequence — an intent outside the scope stays pending and unprojected,
// while the owning scope projects and acknowledges it.
func TestWorkflowActivationProjectionWorkerScopeLeavesForeignIntentPending(t *testing.T) {
	registry := newWorkflowProjectionTestRegistry(t)
	result := commitWorkflowProjectionTestIntent(t, registry.registry, "scoped", singleTriggerGraph(t, nil))
	entries := NewMemoryEntryActivationStore()

	foreign := NewWorkflowActivationProjectionWorker(WorkflowActivationProjectionWorkerConfig{
		Outbox:     registry.outbox,
		Projector:  NewWorkflowActivationProjector(NewEntryActivationManager(entries)),
		Leader:     backend.AlwaysLeader{},
		Namespaces: nsList("some-other-deployment"),
	})
	if applied := foreign.ReconcileOnce(context.Background()); applied != 0 {
		t.Fatalf("foreign-scope ReconcileOnce applied = %d, want 0", applied)
	}
	requireNoWorkflowProjectionActivation(t, entries, result.Current.ID)
	if pending := pendingWorkflowProjectionMutationIDs(t, registry.outbox); !reflect.DeepEqual(pending, []string{"scoped"}) {
		t.Fatalf("pending after foreign-scope pass = %v, want [scoped]", pending)
	}

	owner := NewWorkflowActivationProjectionWorker(WorkflowActivationProjectionWorkerConfig{
		Outbox:     registry.outbox,
		Projector:  NewWorkflowActivationProjector(NewEntryActivationManager(entries)),
		Leader:     backend.AlwaysLeader{},
		Namespaces: []namespace.Namespace{workflowProjectionTestNamespace},
	})
	if applied := owner.ReconcileOnce(context.Background()); applied != 1 {
		t.Fatalf("owner-scope ReconcileOnce applied = %d, want 1", applied)
	}
	requireWorkflowProjectionActivation(t, entries, result.Current)
	if pending := pendingWorkflowProjectionMutationIDs(t, registry.outbox); len(pending) != 0 {
		t.Fatalf("pending after owner-scope pass = %v, want none", pending)
	}
}
