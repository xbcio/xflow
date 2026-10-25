package types

// WorkflowEditorMetadata is the editor-only sibling of WorkflowDef (ADR-D4
// §2.2-§2.3). It carries fields with no execution effect: visual node
// positions, canvas viewport, per-node UI theme/configuration, author notes,
// and visual canvas sections. None of these fields exist on NodeDef or
// WorkflowDef any more -- they live exclusively here.
//
// Every node-level map (Positions, UI, Notes) and every CanvasSection.NodeKeys
// entry is keyed by the stable node identity: NodeDef.ID when the node has
// one, else NodeDef.Name (ADR §2.4).
type WorkflowEditorMetadata struct {
	Positions map[string]Position `json:"positions,omitempty"`
	Viewport  *Viewport           `json:"viewport,omitempty"`
	UI        map[string]any      `json:"ui,omitempty"`
	Notes     map[string]string   `json:"notes,omitempty"`
	// Sections is appended last so adding the field does not reorder the JSON
	// encoding of any pre-existing field; with omitempty a metadata value that
	// has no sections encodes byte-identically to before this field existed.
	Sections []CanvasSection `json:"sections,omitempty"`
}

// CanvasSection is one purely visual grouping box drawn behind the canvas
// nodes: a dashed border plus a title label. It has no execution effect -- the
// engine never reads it, it is excluded from the runtime conflict hash, and it
// participates only in the audit fingerprint because it lives in editor
// metadata.
//
// NodeKeys uses the same stable node identity as Positions and UI: NodeDef.ID
// when the node has an ID, else NodeDef.Name. ValidateEditorMetadata drops
// keys that match no node, drops a section left with no keys, and drops
// sections with an empty or duplicate ID.
//
// A section is deliberately NOT the same thing as GroupDef (WorkflowDef.Groups):
// groups are scheduling semantics -- they co-locate members on one runner and
// carry a runner_selector and group-level on_error -- while a section only
// tells the editor which nodes to draw inside one visual box. Never reuse one
// for the other.
//
// No colour is stored: the design derives a section's accent bar from the
// dominant category of its member nodes, keeping the palette a frontend
// concern.
type CanvasSection struct {
	ID       string   `json:"id,omitempty"`
	Title    string   `json:"title,omitempty"`
	NodeKeys []string `json:"node_keys,omitempty"`
	X        float64  `json:"x,omitempty"`
	Y        float64  `json:"y,omitempty"`
	Width    float64  `json:"width,omitempty"`
	Height   float64  `json:"height,omitempty"`
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
