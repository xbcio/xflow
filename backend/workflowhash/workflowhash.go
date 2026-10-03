// Package workflowhash computes the hashes a workflow registry stores for a
// workflow definition. Every registration path (the SDK Engine, the HTTP API,
// the embedded Server) hashes through this package, so one definition stores
// one hash whichever path registered it.
//
// The package has three hash responsibilities:
//   - Runtime: runtime-semantic fields only. Used for registry conflict
//     detection. Prefix: RuntimePrefixV1 or RuntimePrefixV2.
//   - Audit: the full WorkflowDef plus its WorkflowEditorMetadata sibling.
//     Used for audit/export traceability and the replace no-op check, never
//     for conflict detection. Prefix: AuditPrefix.
//   - graph.Graph.Hash() (in package engine/graph, not here): structural
//     compile hash over the compiled graph IR; orthogonal to the JSON
//     definition form.
package workflowhash

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/xbcio/xflow/types"
)

// Hash format prefixes are baked into the stored DefinitionHash so future
// format changes are self-describing and the registry can reconcile legacy
// records without a separate migration pass.
const (
	// RuntimePrefixV1 marks the runtime-semantic hash produced by Runtime for
	// a definition that sets no node Timeout or private Output. Such a
	// definition hashes to the same bytes under both algorithms, so it keeps
	// the v1 prefix and its historical hash.
	//
	// A stored v1 hash is not trusted as current: v1 hashes were also written
	// for definitions whose node Timeout/Output the v1 algorithm ignored, so
	// reconciliation recomputes them from the stored definition.
	RuntimePrefixV1 = "runtime-sha256:v1:"
	// RuntimePrefixV2 marks a runtime hash over a definition that sets a node
	// Timeout or private Output -- fields the v1 algorithm left out. A v2 hash
	// is always current.
	RuntimePrefixV2 = "runtime-sha256:v2:"
	// AuditPrefix marks the audit fingerprint produced by Audit (full
	// definition plus editor metadata). It is stored in
	// WorkflowRecord.AuditFingerprint and must NOT be used as the
	// conflict-detection hash.
	AuditPrefix = "sha256:audit:v1:"
)

// Runtime produces a canonical hash over the runtime-semantic fields of def.
// It excludes:
//   - descriptive fields (WorkflowDef.Description) -- human documentation, no
//     execution effect;
//   - stable editor identity (NodeDef.ID) -- durable editor-assigned handle
//     that survives re-imports and must not invalidate a workflow;
//   - instance identifiers (WorkflowDef.ID) -- runtime instance pointers, not
//     part of the workflow definition.
//
// Editor-only visual state (position, UI theme, author notes) needs no
// exclusion here: it is not a field of types.NodeDef or types.WorkflowDef at
// all, and lives solely in types.WorkflowEditorMetadata, which Runtime never
// reads.
//
// pin_data IS included because it fixes node inputs and therefore affects
// execution output.
//
// Node Timeout and Output are included (v2). A definition that sets neither
// on any node keeps the "runtime-sha256:v1:<hex>" form, byte-identical to the
// pre-v2 hash; otherwise the form is "runtime-sha256:v2:<hex>".
//
// The hash is taken over Canonical(def, specs), so an omitted param of a type
// specs knows hashes like its Default. A nil specs hashes def as written.
func Runtime(def *types.WorkflowDef, specs ParamSpecLookup) (string, error) {
	def = Canonical(def, specs)
	payload := RuntimePayload{
		Namespace:       def.Namespace,
		Name:            def.Name,
		Version:         def.Version,
		Spec:            def.Spec,
		RunnerSelector:  toHashSelector(def.RunnerSelector),
		Context:         def.Context,
		Settings:        def.Settings,
		Options:         def.Options,
		Credentials:     def.Credentials,
		Params:          def.Params,
		NodeTemplates:   def.NodeTemplates,
		Connections:     def.Connections,
		Outputs:         def.Outputs,
		PinData:         def.PinData,
		Nodes:           make([]runtimeNodeHashPayload, len(def.Nodes)),
		Groups:          canonicalizeGroups(def.Groups),
		DependencyEdges: canonicalizeDependencyEdges(def.DependencyEdges),
	}
	prefix := RuntimePrefixV1
	for i, n := range def.Nodes {
		output := toHashOutputPolicy(n.Output)
		if n.Timeout != 0 || output != nil {
			prefix = RuntimePrefixV2
		}
		payload.Nodes[i] = runtimeNodeHashPayload{
			Name:               n.Name,
			Type:               n.Type,
			Kind:               n.Kind,
			Version:            n.Version,
			Template:           n.Template,
			Disabled:           n.Disabled,
			OnError:            n.OnError,
			RunnerSelector:     toHashSelector(n.RunnerSelector),
			Inputs:             n.Inputs,
			OutputSchema:       n.OutputSchema,
			Parameters:         n.Parameters,
			Retry:              n.Retry,
			ActivationReplicas: n.ActivationReplicas,
			Timeout:            n.Timeout,
			Output:             output,
		}
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal runtime hash payload: %w", err)
	}
	sum := sha256.Sum256(data)
	return prefix + hex.EncodeToString(sum[:]), nil
}

// Audit returns a SHA-256 fingerprint over the full WorkflowDef and its
// editor metadata (ADR-D4 §2.3/§3). It is kept for audit/export traceability
// and for the replace no-op check (sameAuditFingerprint), and must never be
// used for registry conflict detection. Including metadata is deliberate: a
// metadata-only PUT must still differ from the stored fingerprint so the
// replace path writes it as a new revision (ADR §3.1, D3).
//
// md may be nil, meaning no editor metadata was ever supplied; this is
// distinct from a non-nil &types.WorkflowEditorMetadata{}, which marshals its
// own fields away (all omitempty) but still occupies the "editor_metadata"
// JSON slot with {} rather than null, so the two inputs fingerprint
// differently.
//
// The returned string has the form "sha256:audit:v1:<hex>".
func Audit(def *types.WorkflowDef, md *types.WorkflowEditorMetadata) (string, error) {
	data, err := json.Marshal(auditPayload{Definition: def, EditorMetadata: md})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return AuditPrefix + hex.EncodeToString(sum[:]), nil
}

// auditPayload pairs the definition with its editor metadata sibling so the
// audit fingerprint covers both without embedding EditorMetadata on
// types.WorkflowDef itself (which would also require Runtime to grow an
// exclusion for it).
type auditPayload struct {
	Definition     *types.WorkflowDef             `json:"definition,omitempty"`
	EditorMetadata *types.WorkflowEditorMetadata `json:"editor_metadata,omitempty"`
}

// RuntimePayload is the normalized, struct-based runtime identity Runtime
// marshals. Struct field order is fixed at compile time, which makes the JSON
// encoding stable without relying on map-key sorting. It is exported only so
// encoding pins can inspect it; Runtime is the sole producer.
//
// Description is intentionally excluded -- it is human documentation and does
// not affect execution semantics. See Runtime.
type RuntimePayload struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	Version   string `json:"version,omitempty"`
	Spec      string `json:"spec,omitempty"`
	// RunnerSelector is converted to runtimeSelectorHashPayload at the payload
	// boundary (see toHashSelector). The hash-local mirror's tags are FROZEN at
	// the pre-§9.4 wire bytes ("runnerSelector"/"matchLabels"/"mode") so the
	// snake_case wire rename on types.RunnerSelector cannot move the hash. Do
	// NOT "tidy" these to match the wire tags -- see runtimeSelectorHashPayload.
	RunnerSelector *runtimeSelectorHashPayload     `json:"runnerSelector,omitempty"`
	Context        *types.WorkflowContext          `json:"context,omitempty"`
	Settings       *types.WorkflowSettings         `json:"settings,omitempty"`
	Options        *types.WorkflowOptions          `json:"options,omitempty"`
	Credentials    map[string]types.CredentialDef  `json:"credentials,omitempty"`
	Params         map[string]types.ParamDef       `json:"params,omitempty"`
	NodeTemplates  map[string]types.NodeTemplate   `json:"node_templates,omitempty"`
	Nodes          []runtimeNodeHashPayload        `json:"nodes,omitempty"`
	Connections    types.Connections               `json:"connections,omitempty"`
	Outputs        map[string]types.WorkflowOutput `json:"outputs,omitempty"`
	PinData        map[string]any                  `json:"pin_data,omitempty"`
	Groups         []runtimeHashGroupPayload       `json:"Groups,omitempty"`
	// DependencyEdges is appended last and carries omitempty on purpose: struct
	// field order fixes the JSON encoding order, so appending here keeps every
	// pre-existing definition's runtime hash byte-identical while still making a
	// changed supply dependency a runtime-semantic change.
	DependencyEdges []types.DependencyEdge `json:"dependency_edges,omitempty"`
}

// runtimeNodeHashPayload is the runtime-semantic subset of NodeDef used by
// Runtime. The stable editor identity (ID) is intentionally omitted: it is a
// durable editor-assigned handle, and re-importing a workflow must not
// invalidate its registry record just because the editor assigned a
// different stable ID this time. NodeDef.Name carries the runtime identity
// used by connections and pin_data, and IS included. NodeDef itself carries
// no position/UI/notes fields to omit -- those live solely in
// types.WorkflowEditorMetadata.
type runtimeNodeHashPayload struct {
	Name           string                      `json:"name,omitempty"`
	Type           string                      `json:"type,omitempty"`
	Kind           types.NodeKind              `json:"kind,omitempty"`
	Version        int                         `json:"version,omitempty"`
	Template       string                      `json:"template,omitempty"`
	Disabled       bool                        `json:"disabled,omitempty"`
	OnError        string                      `json:"on_error,omitempty"`
	RunnerSelector *runtimeSelectorHashPayload `json:"runnerSelector,omitempty"`
	Inputs         []types.PortDecl            `json:"inputs,omitempty"`
	OutputSchema   map[string]any              `json:"output_schema,omitempty"`
	Parameters     map[string]any              `json:"parameters,omitempty"`
	Retry          *types.RetrySettings        `json:"retry,omitempty"`
	// Appended with omitempty so zero-valued definitions retain their historical
	// runtime hash bytes.
	ActivationReplicas uint32 `json:"activation_replicas,omitempty"`
	// Timeout and Output are the v2 additions, appended with omitempty so a
	// node that sets neither keeps its v1 bytes. Output is the hash-local
	// mirror below, nil unless it changes behaviour.
	Timeout time.Duration             `json:"timeout,omitempty"`
	Output  *runtimeOutputHashPayload `json:"output,omitempty"`
}

// runtimeOutputHashPayload is the hash-local mirror of types.NodeOutputPolicy,
// kept separate for the same reason as runtimeSelectorHashPayload: a wire
// change to NodeOutputPolicy must not move the hash.
type runtimeOutputHashPayload struct {
	Private bool `json:"private,omitempty"`
}

// toHashOutputPolicy returns nil for a nil or zero policy, so that an explicit
// empty `output: {}` hashes like an absent one -- both run the same way.
func toHashOutputPolicy(p *types.NodeOutputPolicy) *runtimeOutputHashPayload {
	if p == nil || !p.Private {
		return nil
	}
	return &runtimeOutputHashPayload{Private: true}
}

// runtimeSelectorHashPayload is the hash-local mirror of types.RunnerSelector.
// Its JSON tags are FROZEN at the pre-§9.4 wire bytes -- "runnerSelector",
// "matchLabels", "mode" -- and MUST NOT be updated to match the snake_case wire
// tags on types.RunnerSelector.
//
// The runtime hash is computed by marshalling RuntimePayload, which (before
// this mirror) held *types.RunnerSelector directly in three places: workflow-
// level, per-node, and per-group. A nested struct marshals with its OWN tags,
// so renaming the wire tags on types.RunnerSelector would change the marshalled
// bytes and therefore the hash of every definition carrying a selector. The
// registry compares the fresh hash against the stored one and returns
// ErrWorkflowConflict; the legacy-hash recovery path returns early for any
// hash already in runtime-sha256:v1: format, so a pre-rename record would
// surface a permanent, un-resolvable conflict on every re-registration.
//
// This mirror is the same pattern runtimeNodeHashPayload already establishes:
// it exists precisely so editor-facing type churn (here, a wire-tag rename for
// API-SPECIFICATION.md §9.4) cannot move the hash. toHashSelector is the single
// conversion point at the payload boundary.
type runtimeSelectorHashPayload struct {
	Mode        string            `json:"mode,omitempty"`
	MatchLabels map[string]string `json:"matchLabels,omitempty"`
}

// toHashSelector converts a wire-facing *types.RunnerSelector to its hash-local
// mirror. nil passes through as nil so the omitempty tag takes effect and a
// selector-free definition produces an identical hash.
func toHashSelector(s *types.RunnerSelector) *runtimeSelectorHashPayload {
	if s == nil {
		return nil
	}
	return &runtimeSelectorHashPayload{
		Mode:        string(s.Mode),
		MatchLabels: s.MatchLabels,
	}
}

type runtimeHashGroupPayload struct {
	Name    string   `json:"name,omitempty"`
	Members []string `json:"members,omitempty"`
	// RunnerSelector is converted to the hash-local mirror (see
	// runtimeSelectorHashPayload). Tags frozen at pre-§9.4 bytes.
	RunnerSelector *runtimeSelectorHashPayload `json:"runnerSelector,omitempty"`
	OnError        string                      `json:"on_error,omitempty"`
	Retry          *types.RetrySettings        `json:"retry,omitempty"`
	Timeout        time.Duration               `json:"timeout,omitempty"`
	Mode           string                      `json:"mode,omitempty"`
	// Appended with omitempty so zero-valued groups retain their historical hash.
	ActivationReplicas uint32 `json:"activation_replicas,omitempty"`
}

// canonicalizeGroups returns a sorted, stable group payload; empty input returns
// nil so that ungrouped definitions produce an identical hash (omitempty + nil).
func canonicalizeGroups(groups []types.GroupDef) []runtimeHashGroupPayload {
	if len(groups) == 0 {
		return nil
	}
	out := make([]runtimeHashGroupPayload, 0, len(groups))
	for _, g := range groups {
		members := append([]string(nil), g.Members...)
		sort.Strings(members)
		out = append(out, runtimeHashGroupPayload{
			Name: g.Name, Members: members, RunnerSelector: toHashSelector(g.RunnerSelector),
			OnError: g.OnError, Retry: g.Retry, Timeout: g.Timeout, Mode: g.Mode,
			ActivationReplicas: g.ActivationReplicas,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// canonicalizeDependencyEdges sorts dependency edges by (Node, Supply) so that
// declaration order -- which carries no semantics -- does not change the
// runtime hash. Returns nil for an empty input so the omitempty tag takes
// effect.
func canonicalizeDependencyEdges(edges []types.DependencyEdge) []types.DependencyEdge {
	if len(edges) == 0 {
		return nil
	}
	out := make([]types.DependencyEdge, len(edges))
	copy(out, edges)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Node != out[j].Node {
			return out[i].Node < out[j].Node
		}
		return out[i].Supply < out[j].Supply
	})
	return out
}
