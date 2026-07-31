package apiserver

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// maxSupplyContentBytes bounds one supply snapshot. It matches the webhook body
// limit (node/internal/trigger/webhook.go). Larger rule sets belong in object
// storage referenced by a small descriptor, not in this endpoint.
const maxSupplyContentBytes = 1 << 20

// supplyModule mounts PUT/GET /v1/supplies/{name}: the push-mode ingress for
// supply content and the read endpoint runners use to fetch it.
//
// The route deliberately carries NO namespace segment. The namespace is always
// namespace.FromContext(ctx), injected by authzWrap from the authenticated
// principal; a path segment would only invite a future change to read the
// tenant from the client, which is exactly the IDOR this endpoint must not have.
//
// The module registers its routes ONLY when a PrincipalAuthenticator is
// configured. There is intentionally no unauthenticated fallback: the legacy
// WorkflowAuthenticator path degrades to DisabledWorkflowAuth (allow all), and
// an endpoint that can rewrite production cleansing rules must 404 rather than
// serve unauthenticated.
type supplyModule struct {
	authzHolder
	supplies store.Supplies
}

func newSupplyModule(s store.Supplies) *supplyModule {
	return &supplyModule{supplies: s}
}

func (m *supplyModule) Name() string { return "supply" }

func (m *supplyModule) RegisterHTTP(mux *http.ServeMux) {
	if m.principalAuth == nil || m.supplies == nil {
		return
	}
	mux.HandleFunc("/v1/supplies/", m.authzWrapResolved(m.handleSupply, func(r *http.Request) (resolvedRoute, bool) {
		name := supplyNameFromPath(r.URL.Path)
		if name == "" {
			return resolvedRoute{}, false
		}
		switch r.Method {
		case http.MethodPut:
			return resolvedRoute{operation: OpSupplyWrite, resource: "supply/" + name, isMutation: true}, true
		case http.MethodGet:
			return resolvedRoute{operation: OpSupplyRead, resource: "supply/" + name}, true
		default:
			return resolvedRoute{}, false
		}
	}))
}

// supplyNameFromPath extracts the single name segment. A nested path or an empty
// name yields "" so the route resolver rejects it — a client cannot smuggle a
// namespace in as "tenant-b/rules".
func supplyNameFromPath(path string) string {
	name := strings.Trim(strings.TrimPrefix(path, "/v1/supplies/"), "/")
	if name == "" || strings.Contains(name, "/") {
		return ""
	}
	return name
}

func (m *supplyModule) handleSupply(w http.ResponseWriter, r *http.Request) {
	name := supplyNameFromPath(r.URL.Path)
	ns := string(namespace.FromContext(r.Context()))
	switch r.Method {
	case http.MethodPut:
		m.handlePut(w, r, ns, name)
	case http.MethodGet:
		m.handleGet(w, r, ns, name)
	default:
		writeError(w, http.StatusNotFound, "route not found")
	}
}

func (m *supplyModule) handlePut(w http.ResponseWriter, r *http.Request, ns, name string) {
	// Read one byte past the limit so an oversized body is detected without
	// buffering an unbounded request.
	body, err := io.ReadAll(io.LimitReader(r.Body, maxSupplyContentBytes+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot read request body")
		return
	}
	if len(body) > maxSupplyContentBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "supply content exceeds limit")
		return
	}

	var ifMatch *uint64
	if raw := r.Header.Get("If-Match"); raw != "" {
		rev, perr := strconv.ParseUint(strings.Trim(raw, `"`), 10, 64)
		if perr != nil {
			writeError(w, http.StatusBadRequest, "malformed If-Match")
			return
		}
		ifMatch = &rev
	}

	rec, err := m.supplies.PutSupply(r.Context(), &store.SupplyResource{
		Namespace:   ns,
		Name:        name,
		Content:     body,
		ContentType: r.Header.Get("Content-Type"),
		UpdatedBy:   supplyPrincipalSubject(r),
	}, ifMatch)
	if errors.Is(err, store.ErrRevisionConflict) {
		writeError(w, http.StatusConflict, "revision_conflict")
		return
	}
	if err != nil {
		// Never surface the driver error: it can carry SQL and server paths.
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	w.Header().Set("ETag", rec.ContentHash)
	writeJSON(w, http.StatusOK, map[string]any{
		"revision":     rec.Revision,
		"content_hash": rec.ContentHash,
	})
}

func (m *supplyModule) handleGet(w http.ResponseWriter, r *http.Request, ns, name string) {
	rec, err := m.supplies.GetSupply(r.Context(), ns, name)
	if errors.Is(err, store.ErrNotFound) {
		// 404 for a name owned by another namespace too: the read is
		// namespace-scoped, so existence in another tenant never leaks.
		writeError(w, http.StatusNotFound, "supply not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	w.Header().Set("ETag", rec.ContentHash)
	w.Header().Set("X-Supply-Revision", strconv.FormatUint(rec.Revision, 10))
	if match := r.Header.Get("If-None-Match"); match != "" && match == rec.ContentHash {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	ct := rec.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(rec.Content)
}

// supplyPrincipalSubject returns the authenticated principal's subject for the
// UpdatedBy audit field, or "" when unavailable. It reads the principal authzWrap
// stored in the request context; the client body is never consulted.
func supplyPrincipalSubject(r *http.Request) string {
	if p, ok := principalFromRequest(r); ok {
		return p.Subject
	}
	return ""
}
