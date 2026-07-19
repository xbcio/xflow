package xflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/xbcio/xflow/types"
)

// workflowKey returns the human-meaningful identity used by registries.
// It intentionally uses Namespace+Name+Version — WorkflowDef.ID is an instance
// identifier, not a runtime identity key.
func workflowKey(def *types.WorkflowDef) string {
	return fmt.Sprintf("%s/%s@%s", def.Namespace, def.Name, def.Version)
}

// runtimeHashVersion is the canonical runtime-hash algorithm version.
// It is baked into the hash prefix so future format changes are self-describing.
const runtimeHashVersion = "v1"

// runtimeHash produces a canonical hash over the runtime-semantic fields of def.
// It excludes editor metadata (NodeDef.Position, NodeDef.UI, NodeDef.Notes) and
// instance identifiers (WorkflowDef.ID, WorkflowDef.TenantID) while including
// pin_data because it fixes node inputs and therefore affects execution output.
//
// The returned string has the form "runtime-sha256:v1:<hex>".
func runtimeHash(def *types.WorkflowDef) (string, error) {
	payload := runtimeHashPayload{
		Namespace:      def.Namespace,
		Name:           def.Name,
		Version:        def.Version,
		Description:    def.Description,
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
	}
	for i, n := range def.Nodes {
		payload.Nodes[i] = runtimeNodeHashPayload{
			ID:             n.ID,
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
	return fmt.Sprintf("runtime-sha256:%s:%s", runtimeHashVersion, hex.EncodeToString(sum[:])), nil
}

// runtimeHashPayload is the normalized, struct-based runtime identity used by
// runtimeHash. Struct field order is fixed at compile time, which makes the
// JSON encoding stable without relying on map-key sorting.
type runtimeHashPayload struct {
	Namespace      string                          `json:"namespace,omitempty"`
	Name           string                          `json:"name,omitempty"`
	Version        string                          `json:"version,omitempty"`
	Description    string                          `json:"description,omitempty"`
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
}

// runtimeNodeHashPayload is the runtime-semantic subset of NodeDef used by
// runtimeHash. Editor metadata fields (Position, UI, Notes) are intentionally
// omitted.
type runtimeNodeHashPayload struct {
	ID             string                `json:"id,omitempty"`
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
	return "sha256:audit:v1:" + hex.EncodeToString(sum[:]), nil
}
