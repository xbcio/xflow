package metrics

import "context"

// Workflow registration metric name.
const metricWorkflowRegistration = "xflow_workflow_registration_total"

// WorkflowRegistrationMetrics counts workflow registration attempts made
// through the control plane's registration path: POST/PUT /v1/workflows and the
// in-process calls an embedded host reaches through xflow.Server.AddWorkflow /
// ReplaceWorkflow, which share that path by construction.
//
// It exists because a registration that fails at startup is otherwise invisible.
// The host process keeps running and keeps serving its own HTTP API with the
// feature it failed to start switched off — in the case this series was added
// for, its runner endpoint answered 503 until the process was restarted. The
// failure itself was a log line the host had to remember to grep for, and
// nothing in /metrics moved: R4's process that is up while its xflow feature is
// permanently off.
//
// Labels are the namespace, the operation, and the outcome:
//
//   - namespace is the registration target, asserted by the registration path
//     rather than read from the request body. Low-cardinality by construction.
//   - operation is a closed enum: "add" (AddWorkflow / POST), "replace"
//     (ReplaceWorkflow / PUT on the collection), or "replace_by_id"
//     (PUT /v1/workflows/{id}).
//   - outcome is a closed enum naming what happened: "registered",
//     "invalid_definition" (the compiler or the SDK refused the definition),
//     "conflict" (a different definition occupies the key), or "error"
//     (backend or transport failure). The split matters because the four call
//     for different responses — only "error" can clear on a retry, which is the
//     distinction xflow.IsRetryableRegistrationError makes for a host.
//
// Deliberately absent: the workflow name, version, and ID. Those are bounded
// per host, but they are also unbounded per fleet (a host that registers a
// workflow per tenant would mint a series per tenant), and every operator
// question this counter answers — "did registration succeed", "is it failing
// again" — is answered without them.
type WorkflowRegistrationMetrics struct {
	Metrics *Metrics
}

// NewWorkflowRegistrationMetrics creates a registration observer backed by
// Metrics. A nil Metrics counts nothing.
func NewWorkflowRegistrationMetrics(m *Metrics) WorkflowRegistrationMetrics {
	return WorkflowRegistrationMetrics{Metrics: m}
}

// ObserveRegistration counts one registration attempt. operation and outcome
// are the closed enum values documented above; the counter cannot enforce them,
// so a new value here is a new series and belongs in this comment first.
func (w WorkflowRegistrationMetrics) ObserveRegistration(ctx context.Context, operation, outcome string) {
	w.Metrics.Inc(metricWorkflowRegistration, withNamespace(ctx, map[string]string{
		"operation": operation,
		"outcome":   outcome,
	}))
}
