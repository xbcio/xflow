package apiserver

// User-facing HTTP path constants. These are the spec §7 target shapes;
// some are not yet registered — they are pending §9 route-migration targets,
// not dead constants in the ActivatePath sense. The §2.2 dead-constant guard
// (every exported constant must have a matching mux registration) lands in a
// later task; until then, treat a constant here as a contract, not proof of
// registration.
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
