package apiserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
)

// nodeDescriptorSource returns the registered descriptors the node-types
// endpoints project; nil means registry.Descriptors (this server process's
// registry).
type nodeDescriptorSource func() []registry.RegisteredDescriptor

// runnerNodeTypeSource returns the descriptors the live runners serving ns
// reported, aggregated by (type, version). newWorkflowControlModule sets it
// from the control plane (ControlPlane.LiveRunnerNodeTypes); nil means no
// runner types.
type runnerNodeTypeSource func(ctx context.Context, ns namespace.Namespace) ([]control.AggregatedDescriptor, error)

// builtinNodeTypePrefix marks builtin node types. A runner never contributes
// one, even when the server registry lacks it.
const builtinNodeTypePrefix = "xflow."

func (m *workflowControlModule) nodeTypeDescriptors() []registry.RegisteredDescriptor {
	if m.nodeDescriptors != nil {
		return m.nodeDescriptors()
	}
	return registry.Descriptors()
}

// nodeTypesNamespaceResolver carries ?namespace= as the resource namespace so
// the authorizer decides a scoped node-types read against that namespace. An
// unscoped read resolves nothing and is decided on the operation alone.
func nodeTypesNamespaceResolver() func(*http.Request) (string, string, string, string) {
	return func(r *http.Request) (string, string, string, string) {
		return "", "", "", r.URL.Query().Get("namespace")
	}
}

// nodeTypesScope reads the optional ?namespace= of a node-types request.
// scoped=false means the request asked for the server registry only. It
// writes the failure itself and returns ok=false on a malformed namespace
// (400), or on any namespace other than the request's own (403). The request's
// own namespace is the verified principal's on the authz branch and the
// configured one on the legacy bearer branch. The check does not defer to the
// authorizer: only NamespaceAwareAuthorizer compares ResourceNamespace, and
// unlike other routes no namespace-scoped store read stands behind this one,
// so under ScopeAuthorizer or a custom authorizer a foreign namespace would
// otherwise disclose another tenant's runner schemas and pool names.
func (m *workflowControlModule) nodeTypesScope(w http.ResponseWriter, r *http.Request) (ns namespace.Namespace, scoped, ok bool) {
	q := r.URL.Query()
	if !q.Has("namespace") {
		return "", false, true
	}
	ns = namespace.Namespace(q.Get("namespace"))
	if err := namespace.Validate(ns); err != nil {
		writeFail(w, r, http.StatusBadRequest, "namespace_invalid", "namespace is invalid")
		return "", false, false
	}
	if ns != namespace.FromContext(r.Context()) {
		writeFail(w, r, http.StatusForbidden, "forbidden", "forbidden")
		return "", false, false
	}
	return ns, true, true
}

// nodeTypeSchemas is the merged node-types view: every server registry entry,
// plus, for a scoped request, the runner entries that survive
// runnerNodeTypeSchemas. The server entries keep registry order; runner
// entries are merged into the same type-then-version order.
func (m *workflowControlModule) nodeTypeSchemas(ctx context.Context, ns namespace.Namespace, scoped bool) []nodeFormSchema {
	fallbacks := builtinNodeFormFallbacks()
	server := m.nodeTypeDescriptors()
	out := projectNodeTypes(server, fallbacks)
	if !scoped {
		return out
	}
	serverTypes := make(map[string]bool, len(server))
	for _, rd := range server {
		serverTypes[rd.Type] = true
	}
	runner := m.runnerNodeTypeSchemas(ctx, ns, serverTypes, fallbacks)
	if len(runner) == 0 {
		return out
	}
	out = append(out, runner...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].NodeType != out[j].NodeType {
			return out[i].NodeType < out[j].NodeType
		}
		return out[i].NodeVersion < out[j].NodeVersion
	})
	return out
}

// runnerNodeTypeSchemas projects the runner-reported descriptors visible to
// ns. The server wins by type, not by (type, version): an entry whose type
// the server registry has in any version is dropped, as is any xflow.* type,
// so builtin and server schemas stay authoritative and a runner cannot shadow
// or extend them. An entry not attributed to ns (or "*") is dropped as
// defense in depth over the source's own filtering. A source error, or an
// entry that fails to decode or project, is logged and skipped: runner types
// are editor metadata and never fail the request.
func (m *workflowControlModule) runnerNodeTypeSchemas(ctx context.Context, ns namespace.Namespace, serverTypes map[string]bool, fallbacks nodeFormFallbacks) []nodeFormSchema {
	if m.runnerNodeTypes == nil {
		return nil
	}
	aggregated, err := m.runnerNodeTypes(ctx, ns)
	if err != nil {
		m.warn("node_types_runner_descriptors_unavailable", "namespace", string(ns), "err", err)
		return nil
	}
	var out []nodeFormSchema
	for _, a := range aggregated {
		if serverTypes[a.Type] || strings.HasPrefix(a.Type, builtinNodeTypePrefix) || !namespaceVisible(a.Namespaces, ns) {
			continue
		}
		s, err := projectRunnerNodeForm(a, fallbacks)
		if err != nil {
			m.warn("node_types_runner_descriptor_skipped",
				"node_type", a.Type, "node_version", a.Version, "err", err)
			continue
		}
		out = append(out, s)
	}
	return out
}

// projectRunnerNodeForm decodes one aggregated runner descriptor and projects
// it, marking the schema runner-sourced. A projection panic on runner-supplied
// data is returned as an error rather than failing the request.
func projectRunnerNodeForm(a control.AggregatedDescriptor, fallbacks nodeFormFallbacks) (s nodeFormSchema, err error) {
	d, err := protocol.DecodeRunnerDescriptor(a.JSON)
	if err != nil {
		return nodeFormSchema{}, err
	}
	defer func() {
		if p := recover(); p != nil {
			s, err = nodeFormSchema{}, fmt.Errorf("project runner descriptor: %v", p)
		}
	}()
	s = projectNodeForm(registry.RegisteredDescriptor{Type: a.Type, Version: a.Version, Descriptor: d}, fallbacks)
	s.Source = nodeFormSourceRunner
	s.RunnerPools = append([]string(nil), a.Pools...)
	return s, nil
}

func namespaceVisible(namespaces []namespace.Namespace, ns namespace.Namespace) bool {
	for _, candidate := range namespaces {
		if candidate == ns || candidate == "*" {
			return true
		}
	}
	return false
}

func (m *workflowControlModule) warn(msg string, args ...any) {
	if m.log != nil {
		m.log.Warn(msg, args...)
	}
}

// handleListNodeTypes serves GET /v1/node-types: every (type, version) in the
// server process's registry projected to NodeFormSchema v1, plus the server's
// param_validation_mode so the editor's blocking semantics follow the backend.
// With ?namespace= it also carries the runner-reported types visible to that
// namespace (see runnerNodeTypeSchemas).
//
// The projection is recomputed per request (an embedded host may register
// types at run time, and runners come and go), and so is the ETag.
func (m *workflowControlModule) handleListNodeTypes(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	ns, scoped, ok := m.nodeTypesScope(w, r)
	if !ok {
		return
	}
	writeNodeTypesData(w, r, nodeTypesResponse{
		ParamValidationMode: m.paramValidation.OrDefault(),
		NodeTypes:           m.nodeTypeSchemas(r.Context(), ns, scoped),
	})
}

// handleGetNodeType serves GET /v1/node-types/{type}?version=N: one schema
// from the same merged view as the list. An omitted (or 0) version selects
// the latest version, matching registry Lookup; an unknown type or version is
// 404.
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
	ns, scoped, ok := m.nodeTypesScope(w, r)
	if !ok {
		return
	}
	var (
		found nodeFormSchema
		hit   bool
	)
	for _, s := range m.nodeTypeCandidates(r.Context(), typ, ns, scoped) {
		if version == 0 {
			if !hit || s.NodeVersion > found.NodeVersion {
				found, hit = s, true
			}
		} else if s.NodeVersion == version {
			found, hit = s, true
			break
		}
	}
	if !hit {
		writeFail(w, r, http.StatusNotFound, "node_type_not_found", "node type not found")
		return
	}
	writeNodeTypesData(w, r, found)
}

// nodeTypeCandidates is the merged view restricted to one type. The server
// wins by type, so the runner source is consulted only when the server
// registry has no version of typ.
func (m *workflowControlModule) nodeTypeCandidates(ctx context.Context, typ string, ns namespace.Namespace, scoped bool) []nodeFormSchema {
	fallbacks := builtinNodeFormFallbacks()
	var out []nodeFormSchema
	for _, rd := range m.nodeTypeDescriptors() {
		if rd.Type == typ {
			out = append(out, projectNodeForm(rd, fallbacks))
		}
	}
	if len(out) > 0 || !scoped {
		return out
	}
	for _, s := range m.runnerNodeTypeSchemas(ctx, ns, nil, fallbacks) {
		if s.NodeType == typ {
			out = append(out, s)
		}
	}
	return out
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
