package types

// WorkflowEditorMetadata is the editor-only sibling of WorkflowDef (ADR-D4
// §2.2-§2.3). It carries fields with no execution effect: visual node
// positions, canvas viewport, per-node UI theme/configuration, and author
// notes. None of these fields exist on NodeDef or WorkflowDef any more --
// they live exclusively here.
//
// Every node-level map (Positions, UI, Notes) is keyed by the stable node
// identity: NodeDef.ID when the node has one, else NodeDef.Name (ADR §2.4).
type WorkflowEditorMetadata struct {
	Positions map[string]Position `json:"positions,omitempty"`
	Viewport  *Viewport           `json:"viewport,omitempty"`
	UI        map[string]any      `json:"ui,omitempty"`
	Notes     map[string]string   `json:"notes,omitempty"`
}

// Viewport holds the editor canvas pan/zoom state.
type Viewport struct {
	X    float64 `json:"x,omitempty"`
	Y    float64 `json:"y,omitempty"`
	Zoom float64 `json:"zoom,omitempty"`
}

// DiagNodeMetadataKeyedByName is the diagnostic code emitted when an editor
// metadata key is resolved against a node that has no NodeDef.ID, falling
// back to NodeDef.Name. Diagnostics surface as plain warning strings
// alongside graph.Compile's own warnings (registrationDiagnostics.Warnings),
// so the code is embedded in the message text rather than carried as a
// separate structured field.
const DiagNodeMetadataKeyedByName = "NODE_METADATA_KEYED_BY_NAME"
