package apiserver

import (
	"net/http"

	"google.golang.org/grpc"
)

// Module is a registrable API unit. A module must implement at least one of
// HTTPModule or GRPCModule.
//
// The apiserver aggregates modules into a single HTTP handler and/or gRPC
// service registration, so new API surfaces (control, management, host-custom)
// are added by writing a Module rather than editing the transport-hosting core.
type Module interface {
	// Name identifies the module for diagnostics and de-duplication.
	Name() string
}

// HTTPModule mounts HTTP routes onto a shared mux. Control and Management APIs
// are HTTP/JSON only and implement this.
type HTTPModule interface {
	Module
	// RegisterHTTP mounts the module's routes onto mux. Modules own their own
	// path prefixes; the apiserver does not rewrite paths.
	RegisterHTTP(mux *http.ServeMux)
}

// GRPCModule registers gRPC services onto a server.
//
// Per the apiserver design, gRPC is used only by the Runner Protocol
// (server<->runner today, gateway<->runner later), so in practice only the
// runner-protocol module implements this. The interface exists so that same
// gRPC service can be registered by both the server and a future gateway
// process without duplicating gRPC wiring.
type GRPCModule interface {
	Module
	// RegisterGRPC registers the module's gRPC services onto reg.
	RegisterGRPC(reg grpc.ServiceRegistrar)
}

var _ = grpc.ServiceRegistrar(nil)
