package types

import "time"

// WorkflowDefinition is the management resource envelope for a workflow.
// It is identified by a server-assigned UUID (ID) and scoped to a namespace.
// The runtime identity remains Namespace+Name+Version, but API paths use the
// stable ID.
type WorkflowDefinition struct {
	ID            string    `json:"id,omitempty"`
	Namespace     string    `json:"namespace,omitempty"`
	Name          string    `json:"name,omitempty"`
	Description   string    `json:"description,omitempty"`
	CreatedAt     time.Time `json:"createdAt,omitempty"`
	UpdatedAt     time.Time `json:"updatedAt,omitempty"`
	CreatedBy     string    `json:"createdBy,omitempty"`
	LatestVersion string    `json:"latestVersion,omitempty"`
	DraftRevision int64     `json:"draftRevision,omitempty"`
}

// WorkflowDraft is the mutable child resource under a WorkflowDefinition.
// It stores the runtime definition (editor-only fields stripped) together with
// the editor metadata that the server keeps for UI convenience.
type WorkflowDraft struct {
	Definition       WorkflowDef            `json:"definition"`
	EditorMetadata   WorkflowEditorMetadata `json:"editorMetadata,omitempty"`
	Revision         int64                  `json:"revision"`
	DefinitionHash   string                 `json:"definitionHash,omitempty"`
	AuditFingerprint string                 `json:"auditFingerprint,omitempty"`
	UpdatedAt        time.Time              `json:"updatedAt,omitempty"`
}

// WorkflowDefinitionVersion is an immutable snapshot produced by publishing a
// draft. The Version field is the client-supplied version string; DefinitionHash
// is the canonical runtime hash used as the execution identity.
type WorkflowDefinitionVersion struct {
	WorkflowDraft `json:",inline"`
	Version       string    `json:"version"`
	PublishedAt   time.Time `json:"publishedAt,omitempty"`
	PublishedBy   string    `json:"publishedBy,omitempty"`
	Immutable     bool      `json:"immutable"`
}

// WorkflowEditorMetadata holds editor-only data that does not affect execution
// semantics and is therefore excluded from the runtime hash.
type WorkflowEditorMetadata struct {
	Positions map[string]Position `json:"positions,omitempty"`
	Viewport  *Viewport           `json:"viewport,omitempty"`
	UI        map[string]any      `json:"ui,omitempty"`
	Notes     map[string]string   `json:"notes,omitempty"`
	// PinData is a read-only derived cache of WorkflowDef.PinData for UI display
	// convenience. WorkflowDef.PinData remains the authoritative source.
	PinData map[string]any `json:"pinData,omitempty"`
}

// Viewport holds the editor canvas pan/zoom state.
type Viewport struct {
	X    float64 `json:"x,omitempty"`
	Y    float64 `json:"y,omitempty"`
	Zoom float64 `json:"zoom,omitempty"`
}
