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
// Registering the host's own workflow at startup is recoverable. AddWorkflow
// and ReplaceWorkflow are idempotent and safe to call again, and neither retries
// for you: a host whose registration fails should retry it — see
// Server.ReplaceWorkflow for that contract and IsRetryableRegistrationError for
// which failures a retry can clear — or decide to exit rather than keep serving
// its API with the xflow feature off. Attempts are counted in
// xflow_workflow_registration_total, by operation and outcome.
//
// ReplaceWorkflow always clears a record's existing editor_metadata, since
// the embedded builder carries none to supply; ReplaceWorkflowWithMetadata is
// the explicit opt-in for an embedded host that wants to carry one through.
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
// metrics collector with observability/metrics.New, NewWithRegistry, or
// NewWithRegisterer and pass it through WithServerMetrics; WithServerMetricsAddr
// optionally serves that collector from Server.Run when a collector was
// supplied.
//
// # Remote control client
//
// NewRemote is the out-of-process client for a standalone server: it drives the
// public /v1 HTTP surface (register, invoke, wait, signal, cancel, pause,
// resume, rerun, inspect, list) over an endpoint and a bearer token. It is the
// fourth mode next to NewLocal, NewCluster and NewServer, and owns none of what
// those own: no engine, no store, no queue, no Redis, no node registration. The
// server is the only holder of state.
//
// Identity is the token's, server-side: a Remote carries no namespace knob, so
// two tenants are two tokens. Every Invoke carries an Idempotency-Key —
// WithIdempotencyKey's when the caller set one, a fresh random key otherwise —
// and a transport failure (no response at all) is retried once with the same
// key, so a lost response cannot create a second execution. A response of any
// status is never retried: the server has decided. Failures are *RemoteError,
// whose Is maps the server's stable codes onto the SDK's sentinels
// (store.ErrNotFound, engine.ErrConcurrencyLimit, ...), so errors.Is reads the
// same against a Remote as against an Engine.
//
// Three methods deviate from the in-process faces because the HTTP routes
// force it, each documented on the method: ReplaceWorkflow takes the workflow
// id explicitly (server-minted ids are uuids and PUT addresses the resource by
// id), Invoke refuses WithExecutionTTL, WithRuntime and WithTraceID (the
// execute route has no fields for them, and silently dropping one would
// promise a run with different semantics), and ListExecutions exists only here,
// because an embedded engine has no execution collection to page.
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
