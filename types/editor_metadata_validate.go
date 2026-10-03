package types

import (
	"fmt"
	"sort"
)

// ValidateEditorMetadata checks md's node-keyed maps (Positions, UI, Notes)
// against def's nodes and returns a sanitized copy plus any diagnostics.
//
// A key that matches no node (by ID, or by Name when the node has no ID) is
// dropped silently from the returned copy; no diagnostic is emitted for a
// dropped key, since an unknown key is routine debris from a renamed or
// removed node, not a hazard worth warning about on every save.
//
// A key that matches a node by Name while that node also has a non-empty ID
// is dropped (it is not addressable by name once the node has a stable ID)
// without a diagnostic, since matching a different node's identity is the
// same "stale key" case as matching no node.
//
// A key that matches a node BY NAME because that node has no ID at all is
// kept, and a DiagNodeMetadataKeyedByName warning is emitted naming the node,
// so hosts can warn authors that renaming or duplicating a node may silently
// cross-contaminate its metadata.
//
// A nil md returns (nil, nil).
func ValidateEditorMetadata(def *WorkflowDef, md *WorkflowEditorMetadata) (*WorkflowEditorMetadata, []string) {
	if md == nil {
		return nil, nil
	}
	// idKeys are nodes addressable by a stable ID; nameKeys are nodes
	// addressable only by name (no ID set), which also need the diagnostic.
	idKeys := make(map[string]struct{})
	nameKeys := make(map[string]struct{})
	if def != nil {
		for _, n := range def.Nodes {
			if n.ID != "" {
				idKeys[n.ID] = struct{}{}
			} else if n.Name != "" {
				nameKeys[n.Name] = struct{}{}
			}
		}
	}

	var diagnostics []string
	warnedNames := make(map[string]struct{})
	checkKey := func(key string) bool {
		if _, ok := idKeys[key]; ok {
			return true
		}
		if _, ok := nameKeys[key]; ok {
			if _, warned := warnedNames[key]; !warned {
				warnedNames[key] = struct{}{}
				diagnostics = append(diagnostics, fmt.Sprintf(
					"%s: editor metadata key %q is keyed by node name because the node has no stable id; "+
						"renaming or duplicating the node may cross-contaminate its metadata",
					DiagNodeMetadataKeyedByName, key))
			}
			return true
		}
		return false
	}

	out := &WorkflowEditorMetadata{Viewport: md.Viewport}
	if len(md.Positions) > 0 {
		out.Positions = make(map[string]Position, len(md.Positions))
		for k, v := range md.Positions {
			if checkKey(k) {
				out.Positions[k] = v
			}
		}
		if len(out.Positions) == 0 {
			out.Positions = nil
		}
	}
	if len(md.UI) > 0 {
		out.UI = make(map[string]any, len(md.UI))
		for k, v := range md.UI {
			if checkKey(k) {
				out.UI[k] = v
			}
		}
		if len(out.UI) == 0 {
			out.UI = nil
		}
	}
	if len(md.Notes) > 0 {
		out.Notes = make(map[string]string, len(md.Notes))
		for k, v := range md.Notes {
			if checkKey(k) {
				out.Notes[k] = v
			}
		}
		if len(out.Notes) == 0 {
			out.Notes = nil
		}
	}

	sort.Strings(diagnostics)
	return out, diagnostics
}
