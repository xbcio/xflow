package xflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/xbcio/xflow/types"
)

// workflowKey returns the human-meaningful identity used by registries.
// It intentionally uses Namespace+Name+Version — WorkflowDef.ID is an instance
// identifier, not a runtime identity key.
func workflowKey(def *types.WorkflowDef) string {
	return fmt.Sprintf("%s/%s@%s", def.Namespace, def.Name, def.Version)
}

// Hash format prefixes are baked into the stored DefinitionHash so future
// format changes are self-describing and the registry can reconcile legacy
// records without a separate migration pass.
const (
	// runtimeHashPrefix marks the canonical runtime-semantic hash produced by
	// runtimeHash. Always current for new registrations.
	runtimeHashPrefix = "runtime-sha256:v1:"
	// auditHashPrefix marks the audit fingerprint produced by
	// legacyDefinitionHash (full-definition, includes editor metadata). It is
	// stored in WorkflowRecord.AuditFingerprint and must NOT be used as the
	// conflict-detection hash.
	auditHashPrefix = "sha256:audit:v1:"
)

// runtimeHash produces a canonical hash over the runtime-semantic fields of
// def. It excludes:
//   - editor metadata (NodeDef.Position, NodeDef.UI, NodeDef.Notes) — purely
//     visual, never affects execution output;
//   - descriptive fields (WorkflowDef.Description) — human documentation, no
//     execution effect;
//   - stable editor identity (NodeDef.ID) — durable editor-assigned handle
//     that survives re-imports and must not invalidate a workflow;
//   - instance identifiers (WorkflowDef.ID) — runtime instance pointers, not
//     part of the workflow definition.
//
// pin_data IS included because it fixes node inputs and therefore affects
// execution output.
//
// The three hash responsibilities in this package are:
//   - runtimeHash (this function): runtime-semantic fields only. Used for
//     registry conflict detection. Prefix: runtimeHashPrefix.
//   - legacyDefinitionHash (audit fingerprint): full WorkflowDef including
//     editor metadata, description, and node IDs. Used for audit/export
//     traceability; never for conflict detection. Prefix: auditHashPrefix.
//   - graph.Graph.Hash() (in package engine/graph): structural compile hash
//     over the compiled graph IR (nodes/edges/order); orthogonal to the JSON
//     definition form.
//
// The returned string has the form "runtime-sha256:v1:<hex>".
func runtimeHash(def *types.WorkflowDef) (string, error) {
	payload := runtimeHashPayload{
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
	for i, n := range def.Nodes {
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
		}
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal runtime hash payload: %w", err)
	}
	sum := sha256.Sum256(data)
	return runtimeHashPrefix + hex.EncodeToString(sum[:]), nil
}

// runtimeHashPayload is the normalized, struct-based runtime identity used by
// runtimeHash. Struct field order is fixed at compile time, which makes the
// JSON encoding stable without relying on map-key sorting.
//
// Description is intentionally excluded — it is human documentation and does
// not affect execution semantics. See runtimeHash doc comment.
type runtimeHashPayload struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	Version   string `json:"version,omitempty"`
	Spec      string `json:"spec,omitempty"`
	// RunnerSelector is converted to runtimeSelectorHashPayload at the payload
	// boundary (see toHashSelector). The hash-local mirror's tags are FROZEN at
	// the pre-§9.4 wire bytes ("runnerSelector"/"matchLabels"/"mode") so the
	// snake_case wire rename on types.RunnerSelector cannot move the hash. Do
	// NOT "tidy" these to match the wire tags — see runtimeSelectorHashPayload.
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
// runtimeHash. Editor metadata fields (Position, UI, Notes) and the stable
// editor identity (ID) are intentionally omitted:
//   - Position/UI/Notes are visual and never affect execution output.
//   - ID is a durable editor-assigned handle. Re-importing a workflow must
//     not invalidate its registry record just because the editor assigned a
//     different stable ID this time. NodeDef.Name carries the runtime
//     identity used by connections and pin_data, and IS included.
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
}

// runtimeSelectorHashPayload is the hash-local mirror of types.RunnerSelector.
// Its JSON tags are FROZEN at the pre-§9.4 wire bytes — "runnerSelector",
// "matchLabels", "mode" — and MUST NOT be updated to match the snake_case wire
// tags on types.RunnerSelector.
//
// The runtime hash is computed by marshalling runtimeHashPayload, which (before
// this mirror) held *types.RunnerSelector directly in three places: workflow-
// level, per-node, and per-group. A nested struct marshals with its OWN tags,
// so renaming the wire tags on types.RunnerSelector would change the marshalled
// bytes and therefore the hash of every definition carrying a selector. The
// registry compares the fresh hash against the stored one (AddWorkflow) and
// returns ErrWorkflowConflict; the legacy-hash recovery path returns early for
// any hash already in runtime-sha256:v1: format, so a pre-rename record would
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
// declaration order — which carries no semantics — does not change the runtime
// hash. Returns nil for an empty input so the omitempty tag takes effect.
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

// legacyDefinitionHash returns a SHA-256 fingerprint over the entire
// WorkflowDef (including editor metadata). It is kept for audit/export
// traceability and must not be used for conflict detection.
//
// The returned string has the form "sha256:audit:v1:<hex>".
func legacyDefinitionHash(def *types.WorkflowDef) (string, error) {
	data, err := json.Marshal(def)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return auditHashPrefix + hex.EncodeToString(sum[:]), nil
}

// reconcileDefinitionHash returns the effective runtime hash to compare
// against a new registration, given the currently stored DefinitionHash and
// the stored Definition.
//
// If storedHash is already in runtime-sha256:v1: format it is returned as-is
// with needsUpgrade=false — the record is current and no rewrite is required.
//
// Otherwise (storedHash is in any legacy format — bare "sha256:", or
// "sha256:audit:v1:", or any unrecognized prefix), the runtime hash is
// recomputed from storedDef and returned with needsUpgrade=true. Callers
// should then persist the recomputed hash via the registry's
// UpdateDefinitionHash to atomically upgrade the record.
//
// storedDef may be nil only if storedHash is already in runtime format; if a
// recompute is required and storedDef is nil, an error is returned. This
// guards against registries that store the hash without the definition.
func reconcileDefinitionHash(storedHash string, storedDef *types.WorkflowDef) (effectiveHash string, needsUpgrade bool, err error) {
	if strings.HasPrefix(storedHash, runtimeHashPrefix) {
		return storedHash, false, nil
	}
	if storedDef == nil {
		return "", false, fmt.Errorf("reconcile definition hash: stored definition is nil for legacy hash %q", storedHash)
	}
	recomputed, err := runtimeHash(storedDef)
	if err != nil {
		return "", false, fmt.Errorf("reconcile definition hash: %w", err)
	}
	return recomputed, true, nil
}
