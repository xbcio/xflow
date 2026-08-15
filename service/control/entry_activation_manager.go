package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

// nodeTriggerPackageHashPrefix namespaces the single-trigger-node content
// fingerprint so it never collides with a group package hash.
const nodeTriggerPackageHashPrefix = "node-sha256:v1:"

// scriptNodeType and wasmScriptLanguage identify a wasm script node in a compiled
// graph. Only such a node can consume a supply through the reactor's pool-rebuild
// path, so only it produces a SupplyConsumerBinding.
const (
	scriptNodeType     = "xflow.script"
	wasmScriptLanguage = "wasm"
)

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
	Supplies     []engine.SupplyRequirement
	// SupplyConsumers routes fetched supply content to the wasm modules that
	// consume it; Supplies only says which content the unit needs.
	SupplyConsumers []engine.SupplyConsumerBinding
}

// projectGroupPackage indirects graph.ProjectGroupPackage so the derivation's
// fail-closed error path can be exercised in tests. Production always uses the
// real projection.
var projectGroupPackage = graph.ProjectGroupPackage

// SuppliesForEntryUnit collects the supply requirements of one entry unit: for
// every node reachable from the entry unit in the flow graph, the supplies its
// dependency edges point at.
//
// The result is sorted by supply node name and deduplicated, so two nodes in the
// same unit depending on one supply yield one requirement. It returns nil (not an
// empty slice) when the unit depends on nothing — the wire format's omitempty and
// the byte-for-byte stability of existing directives both depend on that.
func SuppliesForEntryUnit(g *graph.Graph, unitIdx int) []engine.SupplyRequirement {
	if g == nil {
		return nil
	}
	byNode := map[string]engine.SupplyRequirement{}
	walkEntryUnitNodes(g, unitIdx, func(nodeIdx int) {
		for _, supplyName := range g.SupplyRefsFor(nodeIdx) {
			if _, seen := byNode[supplyName]; seen {
				continue
			}
			si, ok := g.NodeIndex(supplyName)
			if !ok {
				// buildDependencyEdges already rejected a dangling edge, so this
				// is unreachable; skip rather than panic if it ever is.
				continue
			}
			params := g.NodeAt(si).Parameters
			byNode[supplyName] = engine.SupplyRequirement{
				Node:         supplyName,
				Resource:     supply.ResourceName(supplyName, params),
				RequireReady: supply.RequireReady(params),
			}
		}
	})

	if len(byNode) == 0 {
		return nil
	}
	names := make([]string, 0, len(byNode))
	for n := range byNode {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]engine.SupplyRequirement, 0, len(names))
	for _, n := range names {
		out = append(out, byNode[n])
	}
	return out
}

// walkEntryUnitNodes calls visit once for every node reachable from entry unit
// unitIdx through flow edges, seeded with the unit's own node(s). Both supply
// derivations share it so they can never disagree about which nodes an entry
// unit owns.
func walkEntryUnitNodes(g *graph.Graph, unitIdx int, visit func(nodeIdx int)) {
	// Seed the BFS with the node(s) directly in the entry unit.
	var seeds []int
	switch g.UnitKindAt(unitIdx) {
	case graph.UnitGroup:
		seeds = g.GroupMetaAt(unitIdx).Members
	case graph.UnitNode:
		seeds = []int{g.UnitNodeIndex(unitIdx)}
	}

	// BFS downstream from the seeds through flow edges to find all reachable
	// nodes. Supply dependencies on any reachable node belong to this entry unit.
	visited := make(map[int]bool, len(seeds))
	queue := make([]int, 0, len(seeds))
	for _, s := range seeds {
		if !visited[s] {
			visited[s] = true
			queue = append(queue, s)
		}
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		visit(cur)
		for _, e := range g.NodeOutEdges(cur) {
			if !visited[e.DstIdx] {
				visited[e.DstIdx] = true
				queue = append(queue, e.DstIdx)
			}
		}
	}
}

// SupplyConsumerBindingsForEntryUnit pairs each wasm script node in an entry unit
// with the supply nodes it depends on. It is the routing half of what
// SuppliesForEntryUnit collects: the same dependency edges, but keeping the
// CONSUMER's identity rather than only the supply's.
//
// The pairing must be derived here because nothing downstream retains it — the
// activation directive carries a flat supply list, and a projected group package
// flattens every member's supply refs into one deduplicated name set.
//
// A wasm node carrying inline code instead of artifact_digest yields no binding.
// Computing a digest server-side would create a second module-identity source
// that could drift from the runtime's own; ScriptFile-built nodes always have
// artifact_digest by the time the graph compiles (see sdk/xflow.resolveArtifacts).
//
// The result is sorted and deduplicated, and nil (not empty) when there is
// nothing to bind, so the wire format's omitempty keeps existing directives
// byte-for-byte stable.
func SupplyConsumerBindingsForEntryUnit(g *graph.Graph, unitIdx int) []engine.SupplyConsumerBinding {
	if g == nil {
		return nil
	}
	seen := map[engine.SupplyConsumerBinding]bool{}
	walkEntryUnitNodes(g, unitIdx, func(nodeIdx int) {
		refs := g.SupplyRefsFor(nodeIdx)
		if len(refs) == 0 {
			return
		}
		nm := g.NodeAt(nodeIdx)
		collectWasmBindings(nm.Name, nm.Type, nm.Parameters, refs, seen)

		// A body-bearing node (xflow.map) is walked one level deeper. Its members
		// are not graph nodes, so the BFS above can never reach them, and they
		// carry no dependency edges of their own — a body inherits its parent's
		// visible supplies (ProjectNodeBodyPackage's visibleSupplies). Skipping
		// this is not a missing optimisation: the module would be fetched-for but
		// never registered as a consumer, so it would stay on the legacy globals
		// path and evaluate every record against an empty rule set, silently.
		body := g.BodyAt(nodeIdx)
		if body == nil || body.Package == nil || body.Package.Def == nil {
			return
		}
		for _, member := range body.Package.Def.Nodes {
			collectWasmBindings(member.Name, member.Type, member.Parameters, refs, seen)
		}
	})

	if len(seen) == 0 {
		return nil
	}
	out := make([]engine.SupplyConsumerBinding, 0, len(seen))
	for b := range seen {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ModuleDigest != out[j].ModuleDigest {
			return out[i].ModuleDigest < out[j].ModuleDigest
		}
		return out[i].SupplyNode < out[j].SupplyNode
	})
	return out
}

// collectWasmBindings adds one binding per supply name for a node that is an
// artifact-backed wasm script, and does nothing for anything else. It is shared
// by the outer-graph walk and the body walk so a body member is judged bindable
// by exactly the same rules as a top-level node — a second copy of these three
// checks is how the two layers would drift.
func collectWasmBindings(name, nodeType string, params map[string]any, refs []string, seen map[engine.SupplyConsumerBinding]bool) {
	if nodeType != scriptNodeType {
		return
	}
	if lang, _ := params["language"].(string); lang != wasmScriptLanguage {
		return
	}
	digest, _ := params["artifact_digest"].(string)
	if digest == "" {
		// Inline-code wasm node: no stable module identity to bind against.
		// The node name is safe to log; params are not (they may carry
		// credential references), so only the name appears here.
		slog.Warn("supply consumer binding skipped: wasm node has no artifact_digest",
			"node", name, "supplies", refs)
		return
	}
	for _, supplyName := range refs {
		seen[engine.SupplyConsumerBinding{ModuleDigest: digest, SupplyNode: supplyName}] = true
	}
}

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
				EntryUnitID:     gm.Name,
				NodeType:        "xflow.group",
				PackageHash:     gm.PackageHash,
				Selector:        gm.RunnerSelector,
				Requirements:    reqs,
				Supplies:        SuppliesForEntryUnit(g, i),
				SupplyConsumers: SupplyConsumerBindingsForEntryUnit(g, i),
			})
		case graph.UnitNode:
			nodeIdx := g.UnitNodeIndex(i)
			nm := g.NodeAt(nodeIdx)
			if nm.Kind != types.NodeKindTrigger || nm.RunnerSelector == nil {
				continue
			}
			// A trigger is an entry index, never a scheduled task, so its
			// parameters never reach execution/params.go. Render them here or
			// the runner subscribes to the literal "${{ $config.topic }}".
			//
			// The hash is computed over the RENDERED params on purpose: a
			// $config change that moves a rendered value must move the hash so
			// the reconciler re-delivers, and two workflow versions whose
			// templates differ but render identically must NOT.
			params, err := graph.EvaluateActivationParams(g, nm.Name, nm.Type, nm.Parameters)
			if err != nil {
				return nil, err
			}
			out = append(out, EntryUnitActivation{
				EntryUnitID: nm.Name,
				NodeType:    nm.Type,
				Params:      params,
				PackageHash: nodeTriggerPackageHash(nm.Type, nm.Version, params),
				Selector:    nm.RunnerSelector,
				Requirements: engine.NormalizeRequirements([]engine.CapabilityRequirement{{
					NodeType:    nm.Type,
					NodeVersion: nm.Version,
				}}),
				Supplies:        SuppliesForEntryUnit(g, i),
				SupplyConsumers: SupplyConsumerBindingsForEntryUnit(g, i),
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
			Supplies:        eu.Supplies,
			SupplyConsumers: eu.SupplyConsumers,
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
			Supplies:        existing.Supplies,
			SupplyConsumers: existing.SupplyConsumers,
			Desired:         false,
		}); err != nil {
			return err
		}
	}
	return nil
}
