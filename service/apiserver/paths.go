package apiserver

// User-facing HTTP paths. Every constant here MUST be registered on the mux —
// see paths_test.go, which fails on any that is not.
//
// runner-face paths live in service/protocol, not here.
const (
	// PathWorkflows is registered for POST only — the GET list route is not
	// implemented, see API-SPECIFICATION.md §9.
	PathWorkflows           = "/v1/workflows"
	PathWorkflowByID        = "/v1/workflows/{id}"
	PathWorkflowExecute     = "/v1/workflows/execute"
	PathWorkflowExecuteByID = "/v1/workflows/{id}/execute"

	// PathExecutions is registered for POST only — the GET list route is not
	// implemented, see API-SPECIFICATION.md §9.
	PathExecutions          = "/v1/executions"
	PathExecutionByID       = "/v1/executions/{id}"
	PathExecutionCancel     = "/v1/executions/{id}/cancel"
	PathExecutionSignals    = "/v1/executions/{id}/signals"
	PathExecutionSignalByID = "/v1/executions/{id}/signals/{name}"
	PathExecutionWait       = "/v1/executions/{id}/wait"

	PathSupplyByName     = "/v1/supplies/{name}"
	PathArtifactByDigest = "/v1/artifacts/{digest}"

	PathManagementLeader      = "/v1/management/leader"
	PathManagementRunnerByID  = "/v1/management/runners/{id}"
	PathManagementExecByID    = "/v1/management/executions/{id}"
	PathManagementDeadLetters = "/v1/management/dead-letters/{execID}"
	PathManagementDLReplay    = "/v1/management/dead-letters/{execID}/replay"

	PathHealthz = "/healthz"
	PathReadyz  = "/readyz"
)
