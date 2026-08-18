// Package runner is the runner-process side of the xflow worker protocol. It
// drives the full session lifecycle — Register → heartbeat loop → N parallel
// poll/execute/report workers — while talking exclusively through the
// ProtocolClient interface. The runner process never connects directly to
// Redis, Asynq, or any queue; all coordination is mediated by the server via
// HTTP (primary transport) or gRPC (experimental, incomplete).
//
// # Position in the main line
//
// In the cross-process execution path this package is the remote executor:
//
//	[service/control]  server: dispatches tasks, holds leases, commits results
//	       │  HTTP (primary) / gRPC (experimental)
//	       ▼
//	[service/protocol] wire contract — ProtocolClient satisfies it
//	       │
//	       ▼
//	[service/runner]   ← THIS PACKAGE
//	  Runner.Run: Register → sessionID
//	             heartbeatLoop (goroutine)  ← piggybacked directives + hints
//	             metricsReporter (goroutine) ← optional, HTTP only
//	             N × workerLoop (goroutines) ← poll → executeAndReport
//	       │
//	       ▼
//	[execution]  execution.Runner.Execute → ActionHandler
//
// # Core invariants and boundaries
//
// ProtocolClient is the ONLY network interface the runner uses. It exposes
// four required methods (Register, Heartbeat, Poll, ReportResult) and several
// optional capabilities detected via type assertion at startup:
//
//   - leaseRenewClient (RenewLease) — HTTP client only; gRPC client does not
//     implement it, so gRPC-transport runners never renew leases and rely on the
//     server-side sweeper for reclaim after a crash.
//   - MetricsReportClient (ReportMetrics) — HTTP client only; allows the runner
//     to proxy its Prometheus registry through the server when cross-domain
//     scraping is not possible.
//   - activationAckClient (ActivationAck) — HTTP client only; sends a negative
//     ack when an ActivateDirective could not be applied.
//
// The runner does NOT import redis, asynq, or any storage driver. Any
// connectivity it needs is satisfied by a custom *http.Client passed at
// construction.
//
// # Session lifecycle
//
//	Register(RunnerID, Concurrency, Capabilities, Labels, Namespaces, Activations)
//	      │  → sessionID (required for all subsequent calls)
//	      │  → optional SupplyKey (AES-256-GCM transport key)
//	      ▼
//	heartbeatLoop ──────────────────────────────────────────────┐
//	      │  Heartbeat(RunnerID, SessionID, InFlight, Observed) │
//	      │  ← HeartbeatResponse: Activations, SupplyHints,    │
//	      │                        SupplyKeyRotation,           │
//	      │                        MetricsReportIntervalSeconds │
//	      │                                                      │
//	metricsReporter (optional, HTTP only, shares heartbeatCtx)  │
//	      │  POST /v1/runners/metrics (gzip protobuf)           │
//	      ▼                                                      │
//	N × workerLoop ◄────────────────────────────────────────────┘
//	      │  Poll → lease → executeAndReport → ReportResult
//	      │  optional: renewLeaseLoop (background, HTTP only)
//	      ▼
//	Runner.Run returns (transport error or ctx.Err())
//	      → caller reconnects (cmd/runner reconnect loop)
//
// # Pitfalls when modifying this package
//
//   - Transport fault vs. shutdown: runContextError tests ctx.Err() to
//     distinguish a context cancellation (clean shutdown, return nil) from a
//     real transport fault (return the error so the caller reconnects). Testing
//     against specific error strings instead will silently swallow faults and
//     keep the reconnect loop from firing — the runner appears to shut down
//     cleanly while actually stuck.
//
//   - Supply TLS: if a private CA is configured for the server, the SAME
//     custom *http.Client must be used for BOTH the ProtocolClient AND the
//     HTTPSupplyFetcher inside SupplyGate. Passing a TLS-configured client
//     only to the ProtocolClient leaves supply fetches on http.DefaultTransport,
//     which rejects the private-CA certificate. The supply gate never resolves,
//     activation never completes, and trigger start-up silently hangs. See the
//     installSupplyKey path and HTTPSupplyFetcher.
//
//   - Encryption assembly: tests that assign a fake key directly to a fetcher's
//     Keyring field bypass the real assembly path (installSupplyKey →
//     HTTPSupplyFetcher.Keyring). All tests that pass such an assembly must also
//     exercise the real path (Register → SupplyKey → installSupplyKey) in at
//     least one integration-level test; a unit test where you set the field
//     directly proves nothing about the wiring.
//
//   - gRPC capability gaps: gRPC-transport runners never renew leases, send
//     activation acks, or report metrics. Do not add fallback logic that assumes
//     those calls succeeded on the gRPC path — they are skipped by the
//     leaseRenewClient / MetricsReportClient / activationAckClient type
//     assertions at startup, and adding silent fallbacks would hide the gap.
package runner
