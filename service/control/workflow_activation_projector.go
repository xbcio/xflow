package control

import (
	"context"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
)

// WorkflowActivationProjector applies one durable workflow-registry projection
// intent to the entry-activation desired-state store. The registry revision on
// the intent is the fence that makes duplicate and out-of-order delivery safe.
type WorkflowActivationProjector struct {
	manager *EntryActivationManager
}

// NewWorkflowActivationProjector constructs a projector. A nil manager is a
// valid no-op configuration for deployments that do not host remote entry
// activations; their durable intents can still be acknowledged safely.
func NewWorkflowActivationProjector(manager *EntryActivationManager) *WorkflowActivationProjector {
	return &WorkflowActivationProjector{manager: manager}
}

// Project applies intent once. Callers own retry, timeout, claim, and ack
// policy. When workflow identity or version changed, the previous projection
// is retired before the current graph is installed. Both writes carry the
// committed current revision so a delayed intent cannot overwrite newer state.
func (p *WorkflowActivationProjector) Project(ctx context.Context, intent backend.WorkflowActivationProjectionIntent) error {
	if p == nil || p.manager == nil {
		return nil
	}
	ns := intent.Namespace
	if ns == "" {
		ns = namespace.Namespace(intent.Current.Namespace)
	}
	if ns == "" {
		ns = namespace.Default
	}
	ctx = namespace.WithNamespace(ctx, ns)
	if intent.Previous.ID != intent.Current.ID || intent.Previous.Version != intent.Current.Version {
		if err := p.manager.RemoveWorkflowRevision(
			ctx,
			ns,
			intent.Previous.ID,
			intent.Previous.Version,
			intent.Current.RegistryRevision,
			nil,
		); err != nil {
			return err
		}
	}
	return p.manager.AddOrUpdateWorkflowRevision(
		ctx,
		ns,
		intent.Current.ID,
		intent.Current.Version,
		intent.Current.RegistryRevision,
		intent.Current.Graph,
	)
}
