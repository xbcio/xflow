// Package xflow provides the public Go SDK for defining, embedding, and
// operating xflow workflows. NewLocal and NewCluster construct an Engine for
// in-process workflow execution, while Workflow and its related builders define
// workflow graphs and Engine exposes the runtime control operations.
//
// # Embedded control plane and runner
//
// NewServer is the supported embedded control-plane facade. Unlike an Engine,
// it dispatches work to remote runners rather than executing node handlers
// itself. It has the same runner-protocol and workflow-control surface as the
// standalone server while letting a host own its HTTP and gRPC listeners through
// Handler and RegisterGRPC, or let Server.Run own configured listeners.
//
// NewRunner is the corresponding embedded execution-plane facade for a process
// that registers node handlers and executes remote leases. Its assembly keeps
// runtime dependencies consistent across task, group, subgraph, trigger, and
// artifact execution paths; in particular, declare every node capability the
// runner can execute and use one TLS-aware HTTP client configuration for every
// connection to the control plane.
//
// The management API is intentionally opt-in: pass WithServerManagement when
// constructing a Server, then secure that privileged surface with the host's
// authentication and authorization wiring. Its HTTP contract is maintained in
// api/openapi/xflow-v1.yaml. To expose Prometheus instrumentation, create a
// metrics collector with observability/metrics.New or NewWithRegistry and pass
// it through WithServerMetrics; WithServerMetricsAddr optionally serves that
// collector from Server.Run when a collector was supplied.
//
// # Runner operation control
//
// The management drain operation stops new task claims for one runner while
// allowing recovery of already-finalized pre-drain leases from its current
// session. It is not a shutdown request. A complete drain phase means the
// control plane has observed convergence; it does not terminate the runner
// process. A timed_out drain remains draining with the admission gate closed
// and may later become complete after blockers settle and the control plane
// receives a fresh valid quiet observation. Hosts should use their process
// supervisor or orchestrator to stop a runner only after applying their own
// completion policy to the drain projection, rather than assuming either phase
// exits it automatically.
package xflow
