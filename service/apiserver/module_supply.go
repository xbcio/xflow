package apiserver

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// supplyModule mounts GET /v1/supplies/{name}: the read endpoint runners use
// to fetch supply content. The write verb (PUT) is sealed (spec appendix
// Z.5); the only write entry point is in-process:
// sdk/xflow.Server.UpdateSupply / UpdateSupplyIfMatch.
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
	supplies  store.Supplies
	encryptor SupplyContentEncryptor
}

// SupplyContentEncryptor is the interface the supply module uses to encrypt
// content for runners that request it. Implemented by control.SupplyEncryptor.
type SupplyContentEncryptor interface {
	Encrypt(plaintext []byte) ([]byte, error)
}

// AcceptEncrypted is the Accept header value a runner sends to request
// encrypted supply content.
const AcceptEncrypted = "application/x-xflow-encrypted"

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
		// PUT is sealed (spec appendix Z.5). Supply content is what decides which
		// wasm module every runner loads, and an HTTP write verb is a path around
		// the embedder's session + policy + audit. The only write entry point is
		// in-process: sdk/xflow.Server.UpdateSupply / UpdateSupplyIfMatch. There
		// is deliberately no config flag to bring this back — a flag would BE the
		// bypass this seal exists to remove.
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
	case http.MethodGet:
		m.handleGet(w, r, ns, name)
	default:
		writeFail(w, r, http.StatusNotFound, "route_not_found", "route not found")
	}
}

func (m *supplyModule) handleGet(w http.ResponseWriter, r *http.Request, ns, name string) {
	rec, err := m.supplies.GetSupply(r.Context(), ns, name)
	if errors.Is(err, store.ErrNotFound) {
		// 404 for a name owned by another namespace too: the read is
		// namespace-scoped, so existence in another tenant never leaks.
		writeFail(w, r, http.StatusNotFound, "supply_not_found", "supply not found")
		return
	}
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	w.Header().Set("ETag", rec.ContentHash)
	w.Header().Set("X-Supply-Revision", strconv.FormatUint(rec.Revision, 10))
	if match := r.Header.Get("If-None-Match"); match != "" && match == rec.ContentHash {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Encrypt if the runner requests it and an encryptor is configured.
	if m.encryptor != nil && r.Header.Get("Accept") == AcceptEncrypted {
		ciphertext, encErr := m.encryptor.Encrypt(rec.Content)
		if encErr != nil {
			writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
		w.Header().Set("Content-Type", AcceptEncrypted)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(ciphertext)
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
