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
	// PathWorkflows serves both POST (register a definition) and GET (the
	// offset-paginated page list, spec §3.3) — the two are disambiguated by
	// method, not by a separate path constant. Both hang off the principal's
	// own namespace; neither reads a namespace from the request.
	PathWorkflows           = "/v1/workflows"
	PathWorkflowByID        = "/v1/workflows/{id}"
	PathWorkflowExecute     = "/v1/workflows/execute"
	PathWorkflowExecuteByID = "/v1/workflows/{id}/execute"

	// PathExecutions serves both POST (the entry-seed endpoint, runner protocol
	// face, spec §0.1) and GET (the offset-paginated execution collection read,
	// spec §3.3) — the two are disambiguated by method, not by a separate path
	// constant. The two are not merely different verbs on one resource: POST is a runner admission mutation (OpExecutionSeed) and
	// GET is a tenant-scoped read (OpExecutionRead), and the authz wrapper takes
	// that distinction from the registered pattern rather than from the request.
	PathExecutions          = "/v1/executions"
	PathExecutionByID       = "/v1/executions/{id}"
	PathExecutionCancel     = "/v1/executions/{id}/cancel"
	PathExecutionPause      = "/v1/executions/{id}/pause"
	PathExecutionResume     = "/v1/executions/{id}/resume"
	PathExecutionRerun      = "/v1/executions/{id}/rerun"
	PathExecutionSignals    = "/v1/executions/{id}/signals"
	PathExecutionSignalByID = "/v1/executions/{id}/signals/{name}"
	PathExecutionWait       = "/v1/executions/{id}/wait"

	// Schedules are the runtime overlays on a workflow's cron and timer trigger
	// nodes: what an operator may retime or pause WITHOUT registering a new
	// workflow version. They live under the workflow because a schedule has no
	// existence apart from the node that declares it — there is no /v1/schedules
	// collection, and no way to address one without naming its workflow. The
	// {node} segment is the trigger node's name, the same identity the
	// definition and the entry unit use.
	//
	// PathWorkflowScheduleByNode serves GET (one schedule) and PATCH (write its
	// overlay); the verbs that are not the overlay itself — pause, resume,
	// backfill, dropping the overlay — are separate paths, because each is a
	// distinct operation with its own scope decision in the audit trail.
	PathWorkflowSchedules        = "/v1/workflows/{id}/schedules"
	PathWorkflowScheduleByNode   = "/v1/workflows/{id}/schedules/{node}"
	PathWorkflowScheduleOverride = "/v1/workflows/{id}/schedules/{node}/override"
	PathWorkflowSchedulePause    = "/v1/workflows/{id}/schedules/{node}/pause"
	PathWorkflowScheduleResume   = "/v1/workflows/{id}/schedules/{node}/resume"
	PathWorkflowScheduleBackfill = "/v1/workflows/{id}/schedules/{node}/backfill"

	// PathNodeTypes lists the NodeFormSchema of every node type registered in
	// the server process; PathNodeTypeByType serves one (?version= selects a
	// version, latest when omitted).
	PathNodeTypes      = "/v1/node-types"
	PathNodeTypeByType = "/v1/node-types/{type}"

	// PathCurrentUser reports the caller's own verified identity (subject,
	// namespace, scopes). It is registered only on the principal-auth path: the
	// response is a verified subject, and a server without a
	// PrincipalAuthenticator has none to report.
	PathCurrentUser = "/v1/current-user"

	PathSupplyByName     = "/v1/supplies/{name}"
	PathArtifactByDigest = "/v1/artifacts/{digest}"

	// PathWebhookEntry is the server-hosted webhook entry route. The workflow
	// id and entry unit are in the PATH rather than in a header or a query so
	// the sender's URL is the whole address of one entry — the shape every
	// webhook provider's configuration form expects. The HTTP method is NOT part
	// of the pattern: it belongs to the entry's own webhook node (its `method`
	// parameter), so the mux cannot select on it and the handler compares.
	//
	// It is registered OUTSIDE the authz wrapper on purpose: the authentication
	// for this route is the per-entry HMAC signature, which is what an external
	// sender can actually produce. See module_webhook.go.
	PathWebhookEntry = "/v1/hooks/{namespace}/{workflow_id}/{entry_unit}"

	PathManagementLeader     = "/v1/management/leader"
	PathManagementRunners    = "/v1/management/runners"
	PathManagementRunnerByID = "/v1/management/runners/{id}"
	// PathManagementRunnerRevokeIdentity kills one runner's issued identity
	// (T6). It reuses the {id} wildcard name PathManagementRunnerByID already
	// uses, rather than {runnerID} — the two are read by different mux
	// registrations, and the wildcard NAME in the registration pattern must
	// match r.PathValue's argument byte-for-byte or PathValue silently returns
	// "" with no compile error, no panic, and no log.
	PathManagementRunnerRevokeIdentity = "/v1/management/runners/{id}/revoke-identity"
	// Drain / resume are platform-operator controls. They deliberately do not
	// imply remote process shutdown or force cancellation of leased tasks.
	PathManagementRunnerDrain  = "/v1/management/runners/{id}/drain"
	PathManagementRunnerResume = "/v1/management/runners/{id}/resume"
	PathManagementExecByID     = "/v1/management/executions/{id}"
	PathManagementDeadLetters  = "/v1/management/dead-letters/{execID}"
	PathManagementDLReplay     = "/v1/management/dead-letters/{execID}/replay"

	PathManagementRunnerPools          = "/v1/management/runner-pools"
	PathManagementRunnerPoolByID       = "/v1/management/runner-pools/{id}"
	PathManagementRunnerPoolTokens     = "/v1/management/runner-pools/{id}/tokens"
	PathManagementRunnerPoolTokenByID  = "/v1/management/runner-pools/{id}/tokens/{token_id}"
	PathManagementRunnerPoolTokenAudit = "/v1/management/runner-pools/{id}/tokens/{token_id}/audit"
	PathManagementRunnerPoolRunners    = "/v1/management/runner-pools/{id}/runners"

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
//
// PathExecutions IS listed, and the asymmetry with the paragraph above is the
// point: the constant is shared by two methods, and membership here is decided
// per path. The POST on it stays out of the contract (runner protocol face, as
// just described); the GET on it is the offset-paginated collection read, which
// is as user-facing as GET /v1/workflows and MUST appear in the contract. The
// contract's operations decide, not this list — this list only says the path is
// part of the user face.
var UserFacingPaths = []string{
	PathWorkflows,
	PathWorkflowByID,
	PathWorkflowExecute,
	PathWorkflowExecuteByID,

	PathWorkflowSchedules,
	PathWorkflowScheduleByNode,
	PathWorkflowScheduleOverride,
	PathWorkflowSchedulePause,
	PathWorkflowScheduleResume,
	PathWorkflowScheduleBackfill,

	PathExecutions,
	PathExecutionByID,
	PathExecutionCancel,
	PathExecutionPause,
	PathExecutionResume,
	PathExecutionRerun,
	PathExecutionSignals,
	PathExecutionSignalByID,
	PathExecutionWait,

	PathNodeTypes,
	PathNodeTypeByType,

	PathCurrentUser,

	PathSupplyByName,
	PathArtifactByDigest,

	PathWebhookEntry,

	PathManagementLeader,
	PathManagementRunners,
	PathManagementRunnerByID,
	PathManagementRunnerRevokeIdentity,
	PathManagementRunnerDrain,
	PathManagementRunnerResume,
	PathManagementExecByID,
	PathManagementDeadLetters,
	PathManagementDLReplay,

	PathManagementRunnerPools,
	PathManagementRunnerPoolByID,
	PathManagementRunnerPoolTokens,
	PathManagementRunnerPoolTokenByID,
	PathManagementRunnerPoolTokenAudit,
	PathManagementRunnerPoolRunners,

	PathHealthz,
	PathReadyz,
}
