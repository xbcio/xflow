package apiserver

import (
	"net/http"

	"github.com/xbcio/xflow/service/control"
)

// Option configures an APIServer.
type Option func(*APIServer)

// WithModule registers a Module (HTTPModule, GRPCModule, or both) with the
// APIServer. Modules are aggregated in registration order.
func WithModule(m Module) Option {
	return func(s *APIServer) {
		s.modules = append(s.modules, m)
	}
}

// WithHTTPMiddleware appends HTTP middleware to the APIServer's handler
// chain. Middleware is applied outermost-last so the first registered
// middleware wraps the outermost layer and runs first on incoming requests.
func WithHTTPMiddleware(mw ...func(http.Handler) http.Handler) Option {
	return func(s *APIServer) {
		s.middleware = append(s.middleware, mw...)
	}
}

// WithControlPlane injects an externally-owned ControlPlane. The APIServer
// will not own its lifecycle (Shutdown will not tear it down) — useful when
// the host program constructs the ControlPlane itself.
func WithControlPlane(cp *control.ControlPlane) Option {
	return func(s *APIServer) {
		s.cp = cp
		s.ownsCP = false
	}
}

// WithManagement registers the management (ops read-only) module. Opt-in:
// management is NOT registered by default to avoid exposing the runner
// directory, leader state, and execution inspect surface without an auth
// middleware. When enabled, mount an authz middleware via WithHTTPMiddleware
// that gates /v1/management/*. The module is assembled at the end of New
// (after the ControlPlane is guaranteed to be non-nil), so this option is
// safe to combine with either an injected ControlPlane or a built one.
func WithManagement() Option {
	return func(s *APIServer) {
		s.enableManagement = true
	}
}
