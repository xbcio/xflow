// Package metrics provides xflow's Prometheus instrumentation primitives and
// observers. New creates an isolated registry; NewWithRegistry attaches the
// collector to a caller-owned Prometheus registry. Metrics.Registry returns the
// registry to expose through a host's scrape endpoint.
//
// A control plane configured with service/control.Config.Metrics automatically
// wires runner-control observation when its runner directory supports operation
// control. Embedded servers do this by passing the same collector through
// xflow.WithServerMetrics.
//
// # Runner operation control metrics
//
// Runner-control metrics are fleet aggregates, not a per-runner diagnostic API:
// use the management runner snapshot to inspect one drain. The exported series
// are:
//
//   - xflow_runner_control_transitions_total{action,result}: drain and resume
//     mutation outcomes.
//   - xflow_runner_draining_count: runners whose desired state is draining,
//     including quiescing, complete, and timed-out phases.
//   - xflow_runner_drain_duration_seconds: duration histogram recorded only
//     when a drain reaches complete.
//   - xflow_runner_drain_blockers{kind}: fleet totals for handoff_debt,
//     replay_debt, activation_receipt, runner_ack, and deadline.
//
// A timed-out drain contributes to the deadline blocker projection; it does not
// produce a drain-duration observation. Labels are deliberately bounded: never
// add runner IDs, namespaces, request IDs, actors, reasons, or other
// user-provided values to these metric families.
package metrics
