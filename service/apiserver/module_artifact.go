package apiserver

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
)

// artifactModule mounts GET/HEAD /v1/artifacts/{digest}: the read endpoint a
// runner uses to fetch a script artifact it does not have cached locally.
//
// Like supplyModule, the route carries NO namespace segment — the namespace is
// always namespace.FromContext(ctx), injected by authzWrap from the
// authenticated principal. Here that matters more than it does for supply: the
// digest in the path is globally unique across every tenant (bytes are
// deduplicated globally, design §4.2), so the ONLY thing standing between
// tenant A and tenant B's artifact is the identity-table lookup this handler
// performs. A namespace read from the request would defeat it entirely.
//
// The endpoint is strictly read-only. Uploads happen at publish time through
// store.ArtifactStore directly (design §6.1, §6.3); there is deliberately no
// write verb here, so a compromised runner token cannot introduce bytes.
type artifactModule struct {
	authzHolder
	artifacts *store.ArtifactStore
}

func newArtifactModule(a *store.ArtifactStore) *artifactModule {
	return &artifactModule{artifacts: a}
}

func (m *artifactModule) Name() string { return "artifact" }

// RegisterHTTP mounts the route only when a PrincipalAuthenticator and an
// artifact store are both configured. There is intentionally no unauthenticated
// fallback: without a verified principal there is no namespace, and without a
// namespace the tenant check below cannot run — an open route would serve any
// digest to anyone who learned it.
func (m *artifactModule) RegisterHTTP(mux *http.ServeMux) {
	if m.principalAuth == nil || m.artifacts == nil {
		return
	}
	mux.HandleFunc("/v1/artifacts/", m.authzWrapResolved(m.handleArtifact, func(r *http.Request) (resolvedRoute, bool) {
		digest := artifactDigestFromPath(r.URL.Path)
		if digest == "" {
			return resolvedRoute{}, false
		}
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			return resolvedRoute{operation: OpArtifactRead, resource: "artifact/" + digest}, true
		default:
			return resolvedRoute{}, false
		}
	}))
}

// artifactDigestFromPath extracts and validates the single digest segment,
// returning "" for anything the retrieval path must not see.
//
// Validation happens here, in the route resolver, rather than in the handler:
// a malformed digest then yields 404 "route not found" from authzWrapResolved,
// which is the same answer an unreferenced digest gets. A 400 would tell a
// prober that its digest was at least well-formed — the shape of the key space
// is not secret, but there is no reason to confirm it either, and keeping every
// rejection identical is what makes the tenant check below unobservable.
func artifactDigestFromPath(path string) string {
	digest := strings.Trim(strings.TrimPrefix(path, "/v1/artifacts/"), "/")
	if digest == "" || strings.Contains(digest, "/") {
		return ""
	}
	if store.ValidateDigest(digest) != nil {
		return ""
	}
	return digest
}

// handleArtifact serves the bytes for a digest the caller's namespace
// references. Every rejection is 404: see the comment on the HasReference
// branch.
func (m *artifactModule) handleArtifact(w http.ResponseWriter, r *http.Request) {
	digest := artifactDigestFromPath(r.URL.Path)
	ns := string(namespace.FromContext(r.Context()))

	// Authorization is an identity-table lookup, not a capability check
	// (design §7.1). A digest referenced only by another tenant answers 404,
	// NOT 403: a 403 would confirm the digest exists, which is exactly the
	// existence leak the dual-table split is there to prevent. Knowing a digest
	// must not be sufficient to read it.
	allowed, err := m.artifacts.HasReference(r.Context(), ns, digest)
	if err != nil {
		// Never surface the driver error: it can carry SQL and server paths.
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	if !allowed {
		writeError(w, http.StatusNotFound, "artifact not found")
		return
	}

	// Stat before Get so a HEAD never pulls the body, and so a GET can set
	// Content-Length from the authoritative size rather than from whatever it
	// happens to have copied. That header is load-bearing on the client:
	// objectstore.ReadThrough validates the received byte count against the
	// origin's reported size and refuses a mismatch, and a chunked response
	// (ContentLength == -1) would silently skip that check.
	ref, err := m.artifacts.Stat(r.Context(), digest)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			// An identity row exists but the bytes do not. This is the orphan
			// direction §6.1 rules out by ordering, so reaching it means the
			// blob was removed out of band. 404 is still the honest answer.
			writeError(w, http.StatusNotFound, "artifact not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	// Content-Type is deliberately fixed rather than echoed from the stored
	// value: the platform does not interpret artifact content (§2.1 constraint
	// 5), and the client addresses purely by digest. ETag carries the digest,
	// which for this store IS the content hash (unlike real S3 — see
	// objectstore.Object.ETag).
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", digest)
	w.Header().Set("Content-Length", strconv.FormatInt(ref.Size, 10))

	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	body, _, err := m.artifacts.Open(r.Context(), digest)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			writeError(w, http.StatusNotFound, "artifact not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	defer func() { _ = body.Close() }()

	w.WriteHeader(http.StatusOK)
	// A copy error here is unreportable — the 200 and the headers are already
	// on the wire. The client detects the truncation from the Content-Length
	// mismatch, which is the other reason that header is set explicitly.
	_, _ = io.Copy(w, body)
}
