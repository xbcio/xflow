// Package apiserver is the user-facing HTTP layer of xflow. It aggregates a
// service/control.ControlPlane, a workflow-control module, and optional
// feature modules (supply, artifact, management) behind a single Handler()
// and Start()/Shutdown() lifecycle, exposing the API described in
// docs/design/API-SPECIFICATION.md.
//
// # Position in the main line
//
// apiserver sits above service/control as the public API boundary:
//
//	caller (HTTP client / in-process host)
//	       │  POST /v1/workflows/register
//	       │  POST /v1/workflows/{id}/execute
//	       │  GET  /v1/executions/{id}
//	       │  …
//	       ▼
//	[service/apiserver]  ← THIS PACKAGE
//	  APIServer.Handler(): auth → authz → audit admission → module route
//	       │  delegates workflow control, execution, supply, management
//	       ▼
//	[service/control]   ControlPlane: engine + dispatcher + sweeper
//	       │
//	       ▼
//	[engine / rstate]   scheduling and Redis authoritative state
//
// Runner-facing routes (/v1/runners/*) are also served by APIServer.Handler()
// (via the runner-protocol module) but their path constants live in
// service/protocol, not here.
//
// # Core invariants and boundaries
//
// Path constants and UserFacingPaths must be kept in lockstep. paths.go
// declares every user-facing constant (PathWorkflows, PathExecutionByID, …)
// and UserFacingPaths enumerates them for the §2.2 dead-constant guard and
// the §10 OpenAPI contract guard. Adding a constant without also adding it to
// UserFacingPaths is the same defect class the guards exist to catch.
//
// Runner-facing paths are deliberately absent from both the constants and
// UserFacingPaths — they live in service/protocol.RunnerFacingPaths.
//
// POST /v1/executions is a runner-protocol-face entry-seed endpoint, not a
// user-facing one. It does not appear in UserFacingPaths and is excluded from
// the OpenAPI contract (its bare 409 shape is a load-bearing offset-safety
// contract, not an OpenAPI schema).
//
// apiserver does NOT import service/runner. The dependency runs one way:
// service/control ← service/apiserver; service/runner is a separate process.
//
// # Module architecture
//
//	New(cfg) builds a ControlPlane (or accepts one via WithControlPlane),
//	then prepends two always-present modules:
//	  1. runnerProtocolModule — mounts service/protocol RunnerHTTPHandler routes
//	  2. workflowControlModule — /v1/workflows, /v1/executions, /v1/executions/{id}/*
//	Optional modules added when configured:
//	  3. managementModule — /v1/management/* (WithManagement)
//	  4. supplyModule — /v1/supplies/{name} (PrincipalAuth + Supplies required)
//	  5. artifactModule — /v1/artifacts/{digest} (PrincipalAuth + Artifacts required)
//
// Each module implements HTTPModule.RegisterHTTP(*http.ServeMux). Modules
// never share a mux; routing is by path prefix with no overlap.
//
// # Pitfalls when modifying this package
//
//   - Typed-nil interface trap: supplyEncryptorFor and metricsInboxFor both
//     check the concrete pointer (cp.SupplyEncryptor(), cp.MetricsInbox())
//     BEFORE assigning to an interface. Assigning a nil *control.SupplyEncryptor
//     to a SupplyContentEncryptor interface yields a non-nil interface whose
//     methods panic. The same applies to *control.MetricsInbox vs.
//     prometheus.Gatherer. Always nil-check the concrete pointer first.
//
//   - paths.go constant/enum drift: adding a PathFoo constant without adding it
//     to UserFacingPaths will be caught by the §2.2 guard at test time, but
//     failing the guard also blocks the CI build. Keep the two in lockstep.
//
//   - Modules must not import each other. Auth state (principal, namespace) is
//     passed through the request context (namespace.FromContext, the authz
//     wrapper). A module that needs data from another should go through the
//     ControlPlane facade, not through a direct import.
//
//   - Sensitive log content: error bodies returned to callers must never expose
//     stack traces, SQL text, or internal paths. Sanitization functions that
//     only truncate — without actually removing credential-pattern strings —
//     can leave tokens in log lines or error responses while appearing to
//     handle them. A function named "redact" or "sanitize" is not proof of
//     redaction; verify the output, not the name.
package apiserver
