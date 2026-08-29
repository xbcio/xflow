package apiserver

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
)

// artifactModule mounts GET/HEAD /v1/artifacts/{digest}: the read endpoint a
// runner uses to fetch a script artifact it does not have cached locally.
//
// The namespace used for the identity-table lookup below defaults to
// namespace.FromContext(ctx), injected by authzWrap from the authenticated
// principal — this is what a bare token-holder (e.g. a human calling the API
// directly) gets checked against. A runner MAY instead declare the namespace
// of the task it is currently executing via the X-Xflow-Namespace header
// (objectstore.NamespaceHeader): a runner's principal is provisioned once for
// the union of namespaces it may ever serve, but any single task lease belongs
// to exactly one of them, and without this declaration a runner would use its
// broad principal namespace to read another namespace's artifacts (the gap
// docs/superpowers/specs/2026-08-30-runner-artifact-namespace-authorization-design.md
// exists to close). The declared namespace is trusted ONLY when runnerAuth is
// a genuinely configured Authenticator (control.IsConfigured) AND that
// authenticator's AuthenticateOngoing verdict for the caller's runner ID +
// bearer token grants a RunnerPolicy whose AllowsNamespace(declared) is true.
// When no verdict is obtainable at all — no header, auth disabled, unknown
// token — the principal's own namespace is used, exactly as a request with no
// header would be. When a verdict IS obtainable and it denies the declared
// namespace, the request is refused outright rather than falling back: see
// resolveNamespace's doc comment for why that fallback would re-open the very
// gap this design closes.
//
// The digest in the path is globally unique across every tenant (bytes are
// deduplicated globally, design §4.2), so the identity-table lookup this
// handler performs is what actually stands between tenant A and tenant B's
// artifact regardless of which namespace was selected above.
//
// The endpoint is strictly read-only. Uploads happen at publish time through
// store.ArtifactStore directly (design §6.1, §6.3); there is deliberately no
// write verb here, so a compromised runner token cannot introduce bytes.
type artifactModule struct {
	authzHolder
	artifacts *store.ArtifactStore
	// runnerAuth is the runner-protocol Authenticator (service/control), used
	// only to validate a declared X-Xflow-Namespace header. Nil is treated
	// identically to control.DisabledAuthenticator: the declaration is never
	// trusted (see control.IsConfigured).
	runnerAuth control.Authenticator
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

// resolveNamespace returns the namespace to check the digest reference
// against, plus whether the request must be refused outright.
//
// There are three outcomes here, not two, and collapsing the third into the
// first is a live vulnerability rather than a style choice:
//
//   - No header, runnerAuth not configured, or AuthenticateOngoing cannot
//     produce a verdict → fall back to the principal's namespace. These are
//     the "we learned nothing about the caller's runner policy" cases: a
//     human calling the API with a plain principal token reaches every one of
//     them, and for such a caller the principal namespace is the only
//     identity there is. Honouring an unbacked declaration instead would let
//     any token-holder name any tenant.
//
//   - Policy is available and grants the declared namespace → use it. This is
//     the whole point of the header: narrowing a broadly-provisioned runner
//     down to the single namespace of the task it is running right now.
//
//   - Policy is available and explicitly does NOT grant the declared
//     namespace → refuse. Falling back to the principal here looks safe
//     ("we just ignore the header") but is the exact gap this design closes:
//     the caller told us it is executing a task in B, and we would answer it
//     with A's authority — the over-privileged read of design §2(a), merely
//     re-entered through the denial path. The window is reachable in
//     production: a runner legitimately claims a task in B, an operator then
//     narrows or revokes that runner's policy, and every subsequent artifact
//     fetch for the in-flight B task would silently be authorized against A.
//     Refusing is also strictly narrower than the alternative of honouring
//     the declaration, which would hand out B's artifacts to a runner the
//     policy just said may not serve B.
//
// A declared namespace the policy denies is anomalous by construction — a
// runner only ever declares the namespace of a task it was allowed to claim
// (service/control's canServeNamespace gate) — so refusing costs nothing a
// correctly-configured deployment relies on.
func (m *artifactModule) resolveNamespace(r *http.Request) (ns string, refuse bool) {
	principalNS := string(namespace.FromContext(r.Context()))

	declared := r.Header.Get(objectstore.NamespaceHeader)
	if declared == "" {
		return principalNS, false
	}
	// A runner declaring a namespace with no runner-protocol auth configured
	// to back it up must not be trusted — control.IsConfigured distinguishes
	// a real Authenticator from nil/DisabledAuthenticator (see its doc
	// comment for why a plain nil check cannot).
	if !control.IsConfigured(m.runnerAuth) {
		return principalNS, false
	}

	runnerID := r.Header.Get(protocol.RunnerIDHeader)
	token := bearerToken(r)
	policy, err := m.runnerAuth.AuthenticateOngoing(runnerID, token, httpTransportInfoFromRequest(r))
	if err != nil {
		return principalNS, false
	}
	if !policy.AllowsNamespace(namespace.Namespace(declared)) {
		return "", true
	}
	return declared, false
}

// bearerToken extracts the token from an "Authorization: Bearer <token>"
// header, mirroring BearerPrincipalAuth.Authenticate's own extraction (authz.go)
// so runnerAuth validates the exact same credential the principal layer already
// authenticated with — there is deliberately no second, independent credential
// channel for the namespace declaration.
func bearerToken(r *http.Request) string {
	hdr := r.Header.Get("Authorization")
	if !strings.HasPrefix(hdr, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(hdr, "Bearer ")
}

// httpTransportInfoFromRequest extracts TLS peer identity from the request,
// mirroring service/control/server.go's unexported httpTransportInfo (not
// exported from that package, so duplicated here at the same small size
// rather than justifying a cross-package export for one helper). Returns an
// empty struct on plaintext HTTP so an mTLS-bound policy correctly refuses.
func httpTransportInfoFromRequest(r *http.Request) control.TransportInfo {
	info := control.TransportInfo{}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return info
	}
	cert := r.TLS.PeerCertificates[0]
	info.TLSPeerCN = cert.Subject.String()
	info.TLSPeerSAN = append(info.TLSPeerSAN, cert.DNSNames...)
	return info
}

// handleArtifact serves the bytes for a digest the caller's namespace
// references. Every rejection is 404: see the comment on the HasReference
// branch.
func (m *artifactModule) handleArtifact(w http.ResponseWriter, r *http.Request) {
	digest := artifactDigestFromPath(r.URL.Path)
	ns, refuse := m.resolveNamespace(r)
	if refuse {
		// Same 404 shape as an unreferenced digest: a distinct status here
		// would tell a prober that its declared namespace was the thing that
		// got rejected, which leaks the policy's shape.
		writeFail(w, r, http.StatusNotFound, "artifact_not_found", "artifact not found")
		return
	}

	// Authorization is an identity-table lookup, not a capability check
	// (design §7.1). A digest referenced only by another tenant answers 404,
	// NOT 403: a 403 would confirm the digest exists, which is exactly the
	// existence leak the dual-table split is there to prevent. Knowing a digest
	// must not be sufficient to read it.
	allowed, err := m.artifacts.HasReference(r.Context(), ns, digest)
	if err != nil {
		// Never surface the driver error: it can carry SQL and server paths.
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	if !allowed {
		writeFail(w, r, http.StatusNotFound, "artifact_not_found", "artifact not found")
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
			writeFail(w, r, http.StatusNotFound, "artifact_not_found", "artifact not found")
			return
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
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
			writeFail(w, r, http.StatusNotFound, "artifact_not_found", "artifact not found")
			return
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	defer func() { _ = body.Close() }()

	w.WriteHeader(http.StatusOK)
	// A copy error here is unreportable — the 200 and the headers are already
	// on the wire. The client detects the truncation from the Content-Length
	// mismatch, which is the other reason that header is set explicitly.
	_, _ = io.Copy(w, body)
}
