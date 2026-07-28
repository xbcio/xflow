package tracing

// Group lifecycle span names. These follow OTel semantic conventions
// for xflow node-group co-location operations.
const (
	SpanGroupDispatch  = "xflow.group.dispatch"
	SpanGroupExecute   = "xflow.group.execute"
	SpanGroupMember    = "xflow.group.member"
	SpanGroupEmit      = "xflow.group.emit"
	SpanGroupAdmission = "xflow.group.admission"
	SpanGroupCommit    = "xflow.group.commit"
	SpanGroupActivate  = "xflow.group.activate"
	SpanGroupRenew     = "xflow.group.renew"
	SpanGroupSuspend   = "xflow.group.suspend"
	SpanGroupResume    = "xflow.group.resume"
)

// Standard attribute keys for group spans.
const (
	AttrGroupID           = "xflow.group.id"
	AttrGroupWorkflow     = "xflow.group.workflow_id"
	AttrGroupExecID       = "xflow.group.execution_id"
	AttrGroupRunnerID     = "xflow.group.runner_id"
	AttrGroupGeneration   = "xflow.group.generation"
	AttrGroupOutcome      = "xflow.group.outcome"
	AttrGroupMemberCount  = "xflow.group.member_count"
	AttrGroupBatchSize    = "xflow.group.batch_size"
	AttrGroupAdmissionKey = "xflow.group.admission_key"
	AttrGroupPackageHash  = "xflow.group.package_hash"
	AttrGroupSignalName   = "xflow.group.signal_name"
)
