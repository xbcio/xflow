// cmd/server is the xflow management server (Master node).
//
// Responsibilities:
//   - Accept workflow submissions via HTTP/gRPC API
//   - Compile WorkflowDef into Graph IR
//   - Dispatch node tasks to workers via TaskQueue
//   - Track execution lifecycle (status, completion, cancellation)
//   - Deliver signals to suspended nodes
//   - Serve query APIs (execution status, pending approvals)
//   - Run TimeoutSweep for suspended node expiration
//
// It does NOT execute node handlers — that is the worker's job.
package main

func main() {
	// TODO: xflow management server
}
