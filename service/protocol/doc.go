// Package protocol defines the wire contract between the server (service/control)
// and the runner (service/runner). It is the single source of truth for every
// runner-facing path constant and every message type that crosses the process
// boundary, so both sides of a multi-replica deployment always agree on the
// shape of the traffic.
//
// # Position in the main line
//
// In the cross-process variant of the execution path:
//
//	[service/control]  server side — dispatch, lease, commit
//	       │  HTTP (primary) / gRPC (experimental)
//	       ▼
//	[service/protocol] ← path constants + wire types (THIS PACKAGE)
//	       │
//	       ▼
//	[service/runner]   runner side — poll, execute, report
//
// # Core invariant: path constants are the single authority
//
// Every runner-facing HTTP path is declared in this package and nowhere else.
// Adding a path constant anywhere in the package without also registering a
// handler is a defect class that the dead-constant guard in paths_test.go
// catches at test time (it parses the package sources for "/v1/" constants and
// asserts each one appears in RunnerFacingPaths). User-facing paths are
// declared in service/apiserver — they are deliberately absent here.
//
// What this package owns:
//   - Path constants in server.go, group.go, activation.go, metrics.go
//   - RunnerFacingPaths — the enumerable list the dead-constant guard reads
//   - All request/response wire types: RegisterRunnerRequest/Response,
//     HeartbeatRequest/Response, PollTaskRequest/Response,
//     ReportResultRequest/Response, RenewLeaseRequest/Response,
//     ActivateDirective, DeactivateDirective, ActivationAck,
//     GroupLeaseWire/GroupResultWire, MetricsPayloadStamp/Unstamp
//   - The concrete HTTP Client (register/heartbeat/poll/report/renew/ack/metrics)
//   - The RunnerHTTPHandler interface consumed by service/control
//   - The gRPC client (grpc_client.go) and protobuf mappings (runnerpb/)
//
// What this package does NOT own:
//   - Handler logic (lives in service/control)
//   - Execution logic (lives in service/runner)
//   - User-facing paths (live in service/apiserver)
//
// # Transport shape
//
//	Primary transport: HTTP/1.1 JSON (all seven runner-facing routes)
//	   Register   POST /v1/runners/register
//	   Heartbeat  POST /v1/runners/heartbeat
//	   Poll       POST /v1/runners/poll
//	   Result     POST /v1/runners/result
//	   RenewLease POST /v1/runners/lease/renew   (HTTP only)
//	   ActAck     POST /v1/runners/activation/ack
//	   Metrics    POST /v1/runners/metrics        (HTTP only)
//
//	Experimental transport: gRPC (grpc_client.go + runnerpb/)
//	   Only Register/Heartbeat/Poll/Result mapped; RenewLease, ActivationAck
//	   and Metrics are HTTP-only — gRPC is not the target deployment shape
//	   (cross-cloud traffic goes through the Relay Gateway, which is not yet
//	   implemented).
//
// # Pitfalls when modifying this package
//
//   - Adding a "/v1/" constant without registering a handler: the
//     dead-constant guard in paths_test.go will fail loudly. Add to
//     RunnerFacingPaths AND register a handler in service/control before
//     shipping.
//
//   - Reintroducing path constants for routes that were never implemented
//     (ActivatePath, DeactivatePath, ActivationListPath once existed and were
//     removed): a constant with no handler gives every caller a 404. See the
//     comment in activation.go.
//
//   - Assuming gRPC parity: the gRPC transport deliberately does not implement
//     RenewLease, ActivationAck, or Metrics. A gRPC-transport runner therefore
//     never renews leases (service/runner.renewLeaseLoop is gated on a type
//     assertion), never sends acks, and never proxies metrics. Do not add HTTP-
//     only capability checks that compare the client type against the concrete
//     *Client — use the narrow interface approach (leaseRenewClient,
//     MetricsReportClient, activationAckClient in service/runner) instead.
package protocol
