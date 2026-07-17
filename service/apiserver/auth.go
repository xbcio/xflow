package apiserver

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

// WorkflowAuthenticator authenticates inbound calls to the workflow/control API
// (/v1/workflows, /v1/executions/*). Return nil to allow the request or a
// non-nil error to reject it with 401 Unauthorized.
//
// Implementations can enforce bearer tokens, JWT validation, mTLS subject
// checks, or any composable chain. The interface is intentionally minimal so
// callers can wrap it with tenant, workflow, or operation-level checks.
type WorkflowAuthenticator interface {
	AuthenticateRequest(r *http.Request) error
}

// ErrWorkflowUnauthenticated is the canonical rejection error returned by
// built-in WorkflowAuthenticator implementations.
var ErrWorkflowUnauthenticated = errors.New("unauthenticated")

// DisabledWorkflowAuth allows every request unconditionally. Use this in
// development or when the workflow API is deliberately left open behind an
// external gateway. It must NOT be used as the default in production; set
// Config.RequireWorkflowAuth to enforce that a real authenticator is provided.
type DisabledWorkflowAuth struct{}

func (DisabledWorkflowAuth) AuthenticateRequest(_ *http.Request) error { return nil }

// BearerTokenAuth validates the Authorization: Bearer <token> header against
// a configured static token using constant-time comparison, preventing
// timing-based token enumeration.
type BearerTokenAuth struct {
	// tokenHash stores the sha256 of the expected token so the plaintext
	// is never retained in memory after construction.
	tokenHash [32]byte
}

// NewBearerTokenAuth creates a BearerTokenAuth that accepts requests carrying
// the given static bearer token. The token should have at least 128 bits of
// random entropy (e.g. 32 hex or base64url chars) to resist brute force.
func NewBearerTokenAuth(token string) *BearerTokenAuth {
	h := sha256.Sum256([]byte(token))
	return &BearerTokenAuth{tokenHash: h}
}

// AuthenticateRequest returns nil if the request carries the expected bearer
// token in the Authorization header, or ErrWorkflowUnauthenticated otherwise.
// The comparison is constant-time to prevent token enumeration via timing.
func (a *BearerTokenAuth) AuthenticateRequest(r *http.Request) error {
	hdr := r.Header.Get("Authorization")
	if !strings.HasPrefix(hdr, "Bearer ") {
		return ErrWorkflowUnauthenticated
	}
	token := strings.TrimPrefix(hdr, "Bearer ")
	if token == "" {
		return ErrWorkflowUnauthenticated
	}
	candidate := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(candidate[:], a.tokenHash[:]) != 1 {
		return ErrWorkflowUnauthenticated
	}
	return nil
}
