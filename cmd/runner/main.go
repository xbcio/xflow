// cmd/runner is the xflow task runner (Execution node).
//
// Responsibilities:
//   - Connect to xflow-server or xflow-gateway via Runner Protocol
//   - Receive runner-assigned node tasks from the server-side dispatcher
//   - Resolve handler via HandlerRegistry (global type→handler map)
//   - Execute node handlers (ActionHandler / SuspendingHandler)
//   - Report results back through Runner Protocol
//
// It does NOT accept external API requests and does NOT connect to Redis,
// Asynq, or StateStore directly — those are server-side responsibilities.
// Scale horizontally by running multiple runner instances.
//
// Node handlers must be registered before the runner starts.
// Import handler packages in an init() block or directly in this file.
package main

func main() {
	// TODO: xflow task runner
}
