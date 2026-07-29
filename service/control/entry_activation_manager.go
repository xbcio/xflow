package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// nodeTriggerPackageHashPrefix namespaces the single-trigger-node content
// fingerprint so it never collides with a group package hash.
const nodeTriggerPackageHashPrefix = "node-sha256:v1:"

// nodeTriggerPackageHash computes a deterministic content fingerprint of a single
// trigger node's hostable identity: its node type, version, and params. A change
// to any of these (within the same workflow version) changes the hash, which is
// how the reconciler detects a material within-version content change to an
// already-assigned activation and re-delivers the new params at a new generation.
// encoding/json sorts map keys, so the encoding is deterministic.
func nodeTriggerPackageHash(nodeType string, version int, params map[string]any) string {
	payload := struct {
		NodeType string         `json:"node_type"`
		Version  int            `json:"version"`
		Params   map[string]any `json:"params,omitempty"`
	}{NodeType: nodeType, Version: version, Params: params}
	data, err := json.Marshal(payload)
	if err != nil {
		// Fall back to a type+version-only fingerprint if params are not
		// JSON-encodable (should not happen for validated node params). This still
		// changes on a type/version change; params drift would be missed, but the
		// input is already invalid workflow content.
		data = []byte(fmt.Sprintf("%s:%d", nodeType, version))
	}
	sum := sha256.Sum256(data)
	return nodeTriggerPackageHashPrefix + hex.EncodeToString(sum[:])
}

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
				PackageHash: nodeTriggerPackageHash(nm.Type, nm.Version, nm.Parameters),
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
// version. For each remote-hosted trigger entry unit it upserts the desired
// state (NodeType/Params/Requirements/Selector/PackageHash). It writes
// desired-state ONLY: it never touches the assignment fields (RunnerID/
// SessionID/Generation/LeaseDeadline) and never fences. The
// EntryActivationReconciler is the single fence+deactivate authority — it
// observes a stale/mismatched owner (selector/capability/liveness/expiry) or a
// cleared record, deactivates the previously-hosting runner, and then fences
// before reassigning. Keeping the fence off this path is what lets the reconciler
// still see the old owner (RunnerID set) so it can deliver the Deactivate; a
// pre-fence here would clear RunnerID and orphan the old runner's subscription.
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
// workflow version by marking the activation non-desired. It writes
// desired-state ONLY (Desired=false) and never fences: the
// EntryActivationReconciler observes the non-desired record with its owner still
// set, delivers a Deactivate to the hosting runner, and then fences (clears the
// owner + advances the generation floor) so no new runner is assigned. Fencing
// here would clear RunnerID first and orphan the hosting runner's subscription.
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
	}
	return nil
}
