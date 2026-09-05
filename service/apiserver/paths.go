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

	// PathManagementRegistrationCodes serves both POST (create) and GET (list) —
	// the two are disambiguated by method, not by a separate path constant. See
	// Task 8 addendum Ruling A: Produces is three Path* constants, not four.
	PathManagementRegistrationCodes     = "/v1/management/registration-codes"
	PathManagementRegistrationCodeByID  = "/v1/management/registration-codes/{id}"
	PathManagementRegistrationCodeAudit = "/v1/management/registration-codes/{id}/audit"

	PathHealthz = "/healthz"
	PathReadyz  = "/readyz"
)

// UserFacingPaths enumerates every user-facing HTTP path this server exposes.
// Go cannot reflect over constants, so the enumerable form is what both the
// §2.2 dead-constant guard and the §10 contract guard read. Adding a constant
// above without adding it here is the same defect class the guards exist to
// catch, so keep the two in lockstep.
//
// runner-face paths are deliberately absent: per spec §10 the OpenAPI contract
// describes only the user face. POST /v1/executions (entry-seed) is a runner
// protocol-face endpoint (§0.1) and is likewise absent — its bare 409 body
// shape is a load-bearing offset-safety contract, not an OpenAPI schema.
var UserFacingPaths = []string{
	PathWorkflows,
	PathWorkflowByID,
	PathWorkflowExecute,
	PathWorkflowExecuteByID,

	PathExecutionByID,
	PathExecutionCancel,
	PathExecutionSignals,
	PathExecutionSignalByID,
	PathExecutionWait,

	PathSupplyByName,
	PathArtifactByDigest,

	PathManagementLeader,
	PathManagementRunnerByID,
	PathManagementExecByID,
	PathManagementDeadLetters,
	PathManagementDLReplay,

	PathManagementRegistrationCodes,
	PathManagementRegistrationCodeByID,
	PathManagementRegistrationCodeAudit,

	PathHealthz,
	PathReadyz,
}
