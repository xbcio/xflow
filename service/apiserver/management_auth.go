package apiserver

import (
	"net/http"
	"strings"
)

// managementAuthMiddleware gates the read-only management API
// (/v1/management/*) behind a WorkflowAuthenticator while leaving the liveness
// and readiness probes (/healthz, /readyz) unauthenticated so Kubernetes and
// load balancers can probe without credentials.
//
// The management surface exposes runner directory and execution inspect data
// (see module_management.go) and must not be left open in production. When
// management is enabled via WithManagement, callers should wrap the handler
// chain with this middleware using the same BearerTokenAuth (or any
// WorkflowAuthenticator) that gates the workflow API.
//
// A nil authenticator allows all requests (dev / behind-an-external-gateway
// deployments); production should pass a real authenticator.
func ManagementAuthMiddleware(auth WorkflowAuthenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasPrefix(r.URL.Path, "/v1/management/") {
				next.ServeHTTP(w, r)
				return
			}
			// /healthz and /readyz do not match the /v1/management/ prefix,
			// so they pass through unauthenticated. Only the management API
			// surface is gated here.
			a := auth
			if a == nil {
				a = DisabledWorkflowAuth{}
			}
			if err := a.AuthenticateRequest(r); err != nil {
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
