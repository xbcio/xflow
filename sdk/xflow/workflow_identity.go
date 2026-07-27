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
		Namespace:      def.Namespace,
		Name:           def.Name,
		Version:        def.Version,
		Spec:           def.Spec,
		RunnerSelector: def.RunnerSelector,
		Context:        def.Context,
		Settings:       def.Settings,
		Options:        def.Options,
		Credentials:    def.Credentials,
		Params:         def.Params,
		NodeTemplates:  def.NodeTemplates,
		Connections:    def.Connections,
		Outputs:        def.Outputs,
		PinData:        def.PinData,
		Nodes:          make([]runtimeNodeHashPayload, len(def.Nodes)),
		Groups:         canonicalizeGroups(def.Groups),
	}
	for i, n := range def.Nodes {
		payload.Nodes[i] = runtimeNodeHashPayload{
			Name:           n.Name,
			Type:           n.Type,
			Kind:           n.Kind,
			Version:        n.Version,
			Template:       n.Template,
			Disabled:       n.Disabled,
			OnError:        n.OnError,
			RunnerSelector: n.RunnerSelector,
			Inputs:         n.Inputs,
			OutputSchema:   n.OutputSchema,
			Parameters:     n.Parameters,
			Retry:          n.Retry,
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
	Namespace      string                          `json:"namespace,omitempty"`
	Name           string                          `json:"name,omitempty"`
	Version        string                          `json:"version,omitempty"`
	Spec           string                          `json:"spec,omitempty"`
	RunnerSelector *types.RunnerSelector           `json:"runnerSelector,omitempty"`
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
	Name           string                `json:"name,omitempty"`
	Type           string                `json:"type,omitempty"`
	Kind           types.NodeKind        `json:"kind,omitempty"`
	Version        int                   `json:"version,omitempty"`
	Template       string                `json:"template,omitempty"`
	Disabled       bool                  `json:"disabled,omitempty"`
	OnError        string                `json:"on_error,omitempty"`
	RunnerSelector *types.RunnerSelector `json:"runnerSelector,omitempty"`
	Inputs         []types.PortDecl      `json:"inputs,omitempty"`
	OutputSchema   map[string]any        `json:"output_schema,omitempty"`
	Parameters     map[string]any        `json:"parameters,omitempty"`
	Retry          *types.RetrySettings  `json:"retry,omitempty"`
}

type runtimeHashGroupPayload struct {
	Name           string                `json:"name,omitempty"`
	Members        []string              `json:"members,omitempty"`
	RunnerSelector *types.RunnerSelector `json:"runnerSelector,omitempty"`
	OnError        string                `json:"on_error,omitempty"`
	Retry          *types.RetrySettings  `json:"retry,omitempty"`
	Timeout        time.Duration         `json:"timeout,omitempty"`
	Mode           string                `json:"mode,omitempty"`
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
			Name: g.Name, Members: members, RunnerSelector: g.RunnerSelector,
			OnError: g.OnError, Retry: g.Retry, Timeout: g.Timeout, Mode: g.Mode,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
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
