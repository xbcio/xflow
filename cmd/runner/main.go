// cmd/runner is the xflow task runner (Execution node).
//
// Responsibilities:
//   - Consume node tasks from TaskQueue (Asynq)
//   - Resolve handler via HandlerRegistry (global type→handler map)
//   - Execute node handlers (TaskHandler / SuspendingHandler)
//   - Report results back to StateBackend
//   - Trigger downstream scheduling via Engine.OnNodeComplete
//
// It does NOT accept external API requests — that is the server's job.
// Scale horizontally by running multiple runner instances.
//
// Node handlers must be registered before the runner starts.
// Import handler packages in an init() block or directly in this file.
package main

func main() {
	// TODO: xflow task runner
}
