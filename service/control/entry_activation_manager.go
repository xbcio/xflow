package control

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// EntryActivationManager translates workflow add/update/remove lifecycle events
// into durable EntryActivation desired-state records. A trigger entry unit
// (single trigger node OR a group node whose entry member is a trigger) that
// carries a RunnerSelector is meant to be hosted by a remote runner; the manager
// creates a desired activation for it, fences the old generation on an update
// (new selector / package hash), and deactivates it on removal.
//
// The manager only writes desired-state; the EntryActivationReconciler assigns a
// live runner and manages generation. Separating them keeps this a pure,
// leader-safe upsert path (spec §11.6, §4.4).
type EntryActivationManager struct {
	store engine.EntryActivationStore
}

// NewEntryActivationManager constructs a manager over the given store.
func NewEntryActivationManager(store engine.EntryActivationStore) *EntryActivationManager {
	return &EntryActivationManager{store: store}
}

// EntryUnitActivation is a derived description of one trigger entry unit that
// needs a remote-hosted activation.
type EntryUnitActivation struct {
	EntryUnitID  string
	NodeType     string
	Params       map[string]any
	PackageHash  string
	Selector     *types.RunnerSelector
	Requirements []engine.CapabilityRequirement
}

// projectGroupPackage indirects graph.ProjectGroupPackage so the derivation's
// fail-closed error path can be exercised in tests. Production always uses the
// real projection.
var projectGroupPackage = graph.ProjectGroupPackage

// DeriveEntryActivations extracts the trigger entry units from a compiled graph
// that carry a RunnerSelector (i.e. are meant to run on a remote runner). A
// single trigger node and a group node whose entry is a trigger both qualify.
// Units without a selector are skipped — they run inline and need no activation.
//
// It returns an error when a group entry unit's capability requirements cannot
// be derived (its package fails to project). Propagating rather than swallowing
// is a fail-closed guarantee: a fresh group activation always carries non-empty
// requirements (xflow.group + group.exec.v1 at minimum), so an empty-requirements
// group would otherwise be stored as selector-only and could be placed on a
// runner that cannot host it (violating the capability contract).
func DeriveEntryActivations(g *graph.Graph) ([]EntryUnitActivation, error) {
	if g == nil {
		return nil, nil
	}
	var out []EntryUnitActivation
	for i := 0; i < g.UnitCount(); i++ {
		switch g.UnitKindAt(i) {
		case graph.UnitGroup:
			gm := g.GroupMetaAt(i)
			if !gm.Trigger || gm.RunnerSelector == nil {
				continue
			}
			// Derive the group's capability requirements from the projected
			// package (union of member node requirements + the group exec
			// feature), reusing engine.RequirementsFromGraphPackage. A projection
			// failure is fatal: do not downgrade to a selector-only activation.
			pkg, _, err := projectGroupPackage(g, i)
			if err != nil {
				return nil, fmt.Errorf("derive requirements for group entry unit %q: %w", gm.Name, err)
			}
			reqs := engine.RequirementsFromGraphPackage(pkg.Requirements)
			out = append(out, EntryUnitActivation{
				EntryUnitID:  gm.Name,
				NodeType:     "xflow.group",
				PackageHash:  gm.PackageHash,
				Selector:     gm.RunnerSelector,
				Requirements: reqs,
			})
		case graph.UnitNode:
			nodeIdx := g.UnitNodeIndex(i)
			nm := g.NodeAt(nodeIdx)
			if nm.Kind != types.NodeKindTrigger || nm.RunnerSelector == nil {
				continue
			}
			out = append(out, EntryUnitActivation{
				EntryUnitID: nm.Name,
				NodeType:    nm.Type,
				Params:      nm.Parameters,
				Selector:    nm.RunnerSelector,
				Requirements: engine.NormalizeRequirements([]engine.CapabilityRequirement{{
					NodeType:    nm.Type,
					NodeVersion: nm.Version,
				}}),
			})
		}
	}
	return out, nil
}

// AddOrUpdateWorkflow reconciles the desired EntryActivations for a workflow
// version. For each remote-hosted trigger entry unit it upserts a desired
// activation; when an existing record's package hash or selector changed it
// fences the current generation first so the reconciler must issue a strictly
// higher generation (and the stale runner is fenced off). Entry units that no
// longer carry a selector, or workflows with none, are left untouched here (a
// full remove is handled by RemoveWorkflow).
func (m *EntryActivationManager) AddOrUpdateWorkflow(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, workflowVersion string, g *graph.Graph) error {
	if m.store == nil {
		return nil
	}
	if ns == "" {
		ns = namespace.Default
	}
	units, err := DeriveEntryActivations(g)
	if err != nil {
		return err
	}
	for _, eu := range units {
		key := engine.EntryActivationKey{
			Namespace:       ns,
			WorkflowID:      workflowID,
			WorkflowVersion: workflowVersion,
			EntryUnitID:     eu.EntryUnitID,
		}
		existing, ok, err := m.store.Get(ctx, key)
		if err != nil {
			return err
		}
		// On a material change (selector or package hash) to an already-assigned
		// activation, fence the old generation so the currently-hosting runner is
		// invalidated before the reconciler reassigns the new desired state.
		if ok && existing.RunnerID != "" && changedActivation(existing, eu) {
			if err := m.store.Fence(ctx, key, existing.Generation); err != nil {
				return err
			}
		}
		if err := m.store.Upsert(ctx, engine.EntryActivation{
			Namespace:       ns,
			WorkflowID:      workflowID,
			WorkflowVersion: workflowVersion,
			EntryUnitID:     eu.EntryUnitID,
			NodeType:        eu.NodeType,
			Params:          eu.Params,
			PackageHash:     eu.PackageHash,
			Selector:        eu.Selector,
			Requirements:    eu.Requirements,
			Desired:         true,
		}); err != nil {
			return err
		}
	}
	return nil
}

// RemoveWorkflow deactivates every remote-hosted trigger entry unit of the given
// workflow version: it marks the activation non-desired and fences the current
// generation so any hosting runner is told to stop (via a subsequent reconcile /
// generation bump) and no new runner is assigned.
func (m *EntryActivationManager) RemoveWorkflow(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, workflowVersion string, g *graph.Graph) error {
	if m.store == nil {
		return nil
	}
	if ns == "" {
		ns = namespace.Default
	}
	units, err := DeriveEntryActivations(g)
	if err != nil {
		return err
	}
	for _, eu := range units {
		key := engine.EntryActivationKey{
			Namespace:       ns,
			WorkflowID:      workflowID,
			WorkflowVersion: workflowVersion,
			EntryUnitID:     eu.EntryUnitID,
		}
		existing, ok, err := m.store.Get(ctx, key)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := m.store.Upsert(ctx, engine.EntryActivation{
			Namespace:       ns,
			WorkflowID:      workflowID,
			WorkflowVersion: workflowVersion,
			EntryUnitID:     eu.EntryUnitID,
			NodeType:        existing.NodeType,
			Params:          existing.Params,
			PackageHash:     existing.PackageHash,
			Selector:        existing.Selector,
			Requirements:    existing.Requirements,
			Desired:         false,
		}); err != nil {
			return err
		}
		if err := m.store.Fence(ctx, key, existing.Generation); err != nil {
			return err
		}
	}
	return nil
}

// changedActivation reports whether the desired package hash or selector differs
// from the stored record, requiring a fence + reassignment.
func changedActivation(existing engine.EntryActivation, eu EntryUnitActivation) bool {
	if existing.PackageHash != eu.PackageHash {
		return true
	}
	return !selectorsEqual(existing.Selector, eu.Selector)
}

func selectorsEqual(a, b *types.RunnerSelector) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Mode != b.Mode {
		return false
	}
	if len(a.MatchLabels) != len(b.MatchLabels) {
		return false
	}
	for k, v := range a.MatchLabels {
		if b.MatchLabels[k] != v {
			return false
		}
	}
	return true
}
