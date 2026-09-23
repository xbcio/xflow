package workflows

import (
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

// Runtime variables and identities the high tier reads.
const (
	// VarGRPCHost is the host:port HighWorkflow's gRPC node dials.
	VarGRPCHost = "grpc_host"
	// VarDBTable is the table HighWorkflow inserts into and selects from. The
	// caller creates it; the definition never issues DDL.
	VarDBTable = "db_table"
	// VarDBRowID is the primary key HighWorkflow inserts. It must be unique per
	// run: the insert is a plain INSERT and a repeated key is a duplicate-key
	// error, not an upsert.
	VarDBRowID = "db_row_id"
)

// HighApprover is the approver identity HighWorkflow requires. The caller must
// send the approval signal as this identity, with signal name
// HighApprovalSignal.
const HighApprover = "qa-approver@example.test"

// HighApprovalSignal is the signal name HighWorkflow's approval node listens on.
// An xflow.approval in "any" mode accepts a single decision addressed to the
// node, so the name carries no approver segment — that form is for the "all" and
// "sequential" modes, which address each approver separately.
const HighApprovalSignal = "gate/approval"

// HighReleaseSignal is the signal name HighWorkflow's wait node listens on.
const HighReleaseSignal = "hold/release"

// HighWorkflow is the high-complexity tier: credential-backed IO, a gRPC call, a
// human approval, a signal wait with a timer fallback, and two supply
// declarations.
//
//	policy(static supply) ─┐
//	rules(external supply)─┤ (dependency-declared, never executed)
//	start → db_insert → db_load → risk(gRPC, error→ risk_gap) → gate(approval)
//	     ├ approved → hold(wait signal, 5s timeout) ─ main → tick(timer) ─┐
//	     │                                          └ timeout → timed_out ┤
//	     └ rejected → reject ────────────────────────────────────────────┴→ summarize → done
//
// The gRPC node is wired through its error port because its success path is
// currently broken: node/internal/action/grpc.go unmarshals the response into an
// empty *dynamicpb.Message, which carries no message descriptor, and protobuf
// panics on the nil descriptor before the node can return anything. A panic is
// not something a workflow can route around, so the tier drives the path that
// works — a NotFound status is classified permanent and reaches the error port —
// and the success path is covered separately as a known defect rather than
// quietly dropped.
//
// supply.external and supply.static are declaration-only node types: neither
// registers a handler, and the engine skips both at execution. Their whole
// contribution is graph-visible dependency, which is what the activation-time
// readiness gate and the server's reverse index read. Declaring them here is
// therefore the complete exercise of their behaviour, and their presence in the
// compiled graph is the assertion.
//
// The wait node needs a timeout, not for its own sake but for the workflow's:
// without one, a lost signal parks the execution forever and the suite would
// report a timeout instead of naming the node that never released.
//
// Node types: xflow.start, xflow.supply.static, xflow.supply.external,
// xflow.database (insert and select), xflow.grpc, xflow.approval, xflow.wait
// (signal and timer), xflow.transform.set, xflow.merge, xflow.end.
//
// Input: none. Runtime vars: VarGRPCHost, VarDBTable, VarDBRowID.
// Credential: CredentialDB.
func HighWorkflow() *xflow.WorkflowBuilder {
	wf := xflow.Workflow("high-tier")

	policy := wf.Node("policy", node.SupplyStatic([]byte(`{"max_amount": 1000}`)))
	rules := wf.Node("rules", node.SupplyExternal("qa_rules"))

	start := wf.Node("start", node.Start())
	dbInsert := wf.Node("db_insert", node.Database("insert", `{{ $vars.db_table }}`, CredentialDB).
		SetData(map[string]any{
			"id":  `${{ $vars.db_row_id }}`,
			"qty": 7,
		}))
	dbLoad := wf.Node("db_load", node.Database("select", `{{ $vars.db_table }}`, CredentialDB))
	risk := wf.Node("risk", node.GRPC("qa.RiskService", "Score", `{{ $vars.grpc_host }}`).
		SetRequest(map[string]any{"subject": "qa-tier"}).
		Timeout("5s")).
		OnError(types.OnErrorOutput)
	riskGap := wf.Node("risk_gap", node.Set(map[string]any{"risk_scored": false}))
	gate := wf.Node("gate", node.Approval([]string{HighApprover}, node.ApprovalAny))
	hold := wf.Node("hold", node.Wait(HighReleaseSignal).WithTimeout("5s"))
	tick := wf.Node("tick", node.WaitDuration("200ms"))
	approved := wf.Node("approved", node.Set(map[string]any{"decision": "approved"}))
	timedOut := wf.Node("timed_out", node.Set(map[string]any{"decision": "wait_timeout"}))
	rejected := wf.Node("rejected", node.Set(map[string]any{"decision": "rejected"}))
	join := wf.Node("join", node.Merge(node.MergeWaitAny))
	summarize := wf.Node("summarize", node.Set(map[string]any{
		"rows_loaded": `${{ $nodes['db_load'].rows }}`,
		"risk_ok":     `${{ $nodes['risk_gap'].risk_scored }}`,
	}))
	done := wf.Node("done", node.End())

	wf.Connect(start, dbInsert).
		Connect(dbInsert.Output("main"), dbLoad).
		Connect(dbLoad.Output("main"), risk).
		Connect(risk.Output("main"), gate).
		Connect(risk.Output("error"), riskGap).
		Connect(riskGap.Output("main"), gate).
		Connect(gate.Output("approved"), hold).
		Connect(gate.Output("rejected"), rejected).
		Connect(hold.Output("main"), approved).
		Connect(hold.Output("timeout"), timedOut).
		Connect(approved.Output("main"), tick).
		Connect(tick.Output("main"), join).
		Connect(timedOut.Output("main"), join).
		Connect(rejected.Output("main"), join).
		Connect(join.Output("main"), summarize).
		Connect(summarize.Output("main"), done)

	// Supply nodes carry no dataflow edge — they have no output ports at all.
	// DependsOn records that summarize is the consumer, which is the edge the
	// readiness gate and the reverse index follow.
	wf.DependsOn(summarize, policy)
	wf.DependsOn(summarize, rules)
	return wf
}

// HighVars returns the runtime variables HighWorkflow requires.
func HighVars(grpcHost, table, rowID string) map[string]any {
	return map[string]any{
		VarGRPCHost: grpcHost,
		VarDBTable:  table,
		VarDBRowID:  rowID,
	}
}
