package apiserver

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/xbcio/xflow/node/registry"
)

// nodeDescriptorSource returns the registered descriptors the node-types
// endpoints project; nil means registry.Descriptors (this server process's
// registry — types registered only on runners are not visible).
type nodeDescriptorSource func() []registry.RegisteredDescriptor

func (m *workflowControlModule) nodeTypeDescriptors() []registry.RegisteredDescriptor {
	if m.nodeDescriptors != nil {
		return m.nodeDescriptors()
	}
	return registry.Descriptors()
}

// handleListNodeTypes serves GET /v1/node-types: every (type, version) in the
// server process's registry projected to NodeFormSchema v1, plus the server's
// param_validation_mode so the editor's blocking semantics follow the backend.
//
// The projection is recomputed per request (an embedded host may register
// types at run time), and so is the ETag.
func (m *workflowControlModule) handleListNodeTypes(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	writeNodeTypesData(w, r, nodeTypesResponse{
		ParamValidationMode: m.paramValidation.OrDefault(),
		NodeTypes:           projectNodeTypes(m.nodeTypeDescriptors(), builtinNodeFormFallbacks()),
	})
}

// handleGetNodeType serves GET /v1/node-types/{type}?version=N: one schema.
// An omitted (or 0) version selects the latest registered version, matching
// registry Lookup; an unknown type or version is 404.
func (m *workflowControlModule) handleGetNodeType(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	typ := r.PathValue("type")
	version := 0
	if raw := r.URL.Query().Get("version"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 {
			writeFail(w, r, http.StatusBadRequest, "node_type_version_invalid", "version must be a non-negative integer")
			return
		}
		version = v
	}
	var (
		found registry.RegisteredDescriptor
		ok    bool
	)
	for _, rd := range m.nodeTypeDescriptors() {
		if rd.Type != typ {
			continue
		}
		if version == 0 {
			if !ok || rd.Version > found.Version {
				found, ok = rd, true
			}
		} else if rd.Version == version {
			found, ok = rd, true
			break
		}
	}
	if !ok {
		writeFail(w, r, http.StatusNotFound, "node_type_not_found", "node type not found")
		return
	}
	writeNodeTypesData(w, r, projectNodeForm(found, builtinNodeFormFallbacks()))
}

// writeNodeTypesData writes data in the success envelope with a strong ETag
// over the marshalled data payload (not the envelope, whose trace_id differs
// per request), answering a matching If-None-Match with 304 and no body.
func writeNodeTypesData(w http.ResponseWriter, r *http.Request, data any) {
	raw, err := json.Marshal(data)
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	sum := sha256.Sum256(raw)
	etag := `"sha256:` + hex.EncodeToString(sum[:]) + `"`
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "no-cache")
	if ifNoneMatch(r.Header.Get("If-None-Match"), etag) {
		if rid := sanitizeRequestID(r.Header.Get("X-Request-Id")); rid != "" {
			w.Header().Set("X-Request-Id", rid)
		}
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeData(w, r, http.StatusOK, json.RawMessage(raw))
}

// ifNoneMatch reports whether an If-None-Match header value matches etag,
// using the weak comparison RFC 9110 §13.1.2 prescribes for GET: "*" matches,
// and a W/ prefix is ignored.
func ifNoneMatch(header, etag string) bool {
	if header == "" {
		return false
	}
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || strings.TrimPrefix(candidate, "W/") == etag {
			return true
		}
	}
	return false
}
