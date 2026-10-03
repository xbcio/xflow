package apiserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/types"
)

// MaxEditorMetadataBytes bounds the JSON-encoded size of one WorkflowDef's
// editor_metadata object. There is no general request body size limit on the
// workflow registration routes today (POST/PUT /v1/workflows decode straight
// off r.Body), so editor_metadata gets a dedicated cap rather than inheriting
// one. 1 MiB comfortably covers a canvas of several thousand positioned nodes
// plus notes and UI state, while still bounding a pathological payload well
// under the kind of size that would strain the registry's Redis hash.
const MaxEditorMetadataBytes = 1 << 20

// errEditorMetadataTooLarge is wrapped by validateEditorMetadataSize so HTTP
// handlers can map it to a 400 the same way they map a compile error, without
// a type assertion.
var errEditorMetadataTooLarge = errors.New("editor_metadata exceeds the maximum allowed size")

// validateEditorMetadataSize rejects an editor_metadata object whose encoded
// JSON form exceeds MaxEditorMetadataBytes. nil is always valid.
func validateEditorMetadataSize(md *types.WorkflowEditorMetadata) error {
	if md == nil {
		return nil
	}
	data, err := json.Marshal(md)
	if err != nil {
		return fmt.Errorf("encode editor_metadata: %w", err)
	}
	if len(data) > MaxEditorMetadataBytes {
		return fmt.Errorf("%w: %d bytes (max %d)", errEditorMetadataTooLarge, len(data), MaxEditorMetadataBytes)
	}
	return nil
}

// workflowDefinitionWithMetadata is the wire shape of POST/PUT /v1/workflows
// (and PUT /v1/workflows/{id}) request bodies and the GET/PUT response
// bodies: every current WorkflowDef field, flattened by embedding, plus an
// optional sibling editor_metadata object (ADR-D4 §2.3/§4, D2).
//
// EditorMetadata deliberately is NOT a field on types.WorkflowDef itself: the
// runtime hash (workflowhash.Runtime) walks WorkflowDef directly, and adding
// a field there would need an explicit exclusion the same way Description and
// the per-node editor fields once did. Keeping it on a wrapper means the
// hash's input type never has to know editor metadata exists.
type workflowDefinitionWithMetadata struct {
	*types.WorkflowDef
	EditorMetadata *types.WorkflowEditorMetadata `json:"editor_metadata,omitempty"`
}

// decodeWorkflowDefinitionWithMetadata decodes body into a WorkflowDef plus
// its optional editor_metadata sibling, enforcing MaxEditorMetadataBytes
// before any other validation. decodeJSON's own JSON-syntax error path is
// reused (bad_request / "invalid JSON"); only the size check gets a
// dedicated, named failure the caller can distinguish.
func decodeWorkflowDefinitionWithMetadata(w http.ResponseWriter, r *http.Request) (*types.WorkflowDef, *types.WorkflowEditorMetadata, bool) {
	var body workflowDefinitionWithMetadata
	body.WorkflowDef = &types.WorkflowDef{}
	if !decodeJSON(w, r, &body) {
		return nil, nil, false
	}
	if err := validateEditorMetadataSize(body.EditorMetadata); err != nil {
		writeFail(w, r, http.StatusBadRequest, "editor_metadata_too_large", err.Error())
		return nil, nil, false
	}
	return body.WorkflowDef, body.EditorMetadata, true
}

// workflowResponseBody builds the GET/PUT/POST response wire shape: rec's
// definition with its stored editor metadata attached. Namespace-scoped
// read checks and 404 mapping happen in the caller; this only shapes the body.
func workflowResponseBody(rec backend.WorkflowRecord) workflowDefinitionWithMetadata {
	return workflowDefinitionWithMetadata{WorkflowDef: rec.Definition, EditorMetadata: rec.EditorMetadata}
}
