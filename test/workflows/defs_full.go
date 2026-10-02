package workflows

import (
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/trigger"
	triggerredis "github.com/xbcio/xflow/node/trigger/redis"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

// Signal names and identities FullWorkflow's suspending nodes listen on. They
// follow the same addressing as the high tier: an "any"-mode approval takes one
// decision addressed to the node, and a wait node listens on its literal name.
const (
	// FullApprover is the only identity whose decision FullWorkflow's gate
	// counts.
	FullApprover = "qa-full-approver@example.test"
	// FullApprovalSignal is the signal name of FullWorkflow's approval node.
	FullApprovalSignal = "gate/approval"
	// FullReleaseSignal is the signal name of FullWorkflow's signal wait.
	FullReleaseSignal = "hold/release"
	// FullWebhookPath is the route FullWorkflow's webhook entry registers. It
	// differs from WebhookPath so the full and the single-trigger definitions
	// can be hosted by the same engine without a route conflict.
	FullWebhookPath = "/qa/full/webhook"
)

// Entry node names of FullWorkflow.
const (
	FullEntryStart   = "start"
	FullEntryTimer   = "on_timer"
	FullEntryCron    = "on_cron"
	FullEntryWebhook = "on_webhook"
	FullEntryRedis   = "on_redis"
	FullEntryKafka   = "on_kafka"
)

// FullTriggerConfig parameterizes FullWorkflow's trigger entries. Broker
// addresses and stream/topic names are parameters for the same reason they are
// on the single-trigger tiers (see RedisTriggerWorkflow): a consumer group
// remembers its offsets, so each run needs its own.
type FullTriggerConfig struct {
	TimerInterval  time.Duration
	CronExpression string
	RedisAddr      string
	RedisStream    string
	RedisGroup     string
	KafkaBrokers   []string
	KafkaTopic     string
	KafkaGroup     string
}

// DefaultFullTriggerConfig returns a config whose schedules are slow enough not
// to fire during a short run and whose brokers are the local defaults. It is
// what the static coverage and validation suites compile.
func DefaultFullTriggerConfig() FullTriggerConfig {
	return FullTriggerConfig{
		TimerInterval:  time.Hour,
		CronExpression: "@every 1h",
		RedisAddr:      "localhost:6379",
		RedisStream:    RedisStreamName + ":full",
		RedisGroup:     RedisStreamGroup + "-full",
		KafkaBrokers:   KafkaQABrokers(),
		KafkaTopic:     KafkaQATopic + "-full",
		KafkaGroup:     "xflow-qa-full",
	}
}

// FullWorkflow is one definition that carries every registered node type the
// compiler accepts and every registered trigger kind. The other tiers split
// coverage across several definitions to keep each one about a single concern;
// this one exists to prove the types also compose — one graph, one compile, and
// executions threading data through all of them.
//
// Main lane (entry: start):
//
//	policy(supply.static) ┐ rules(supply.external) ┐  (DependsOn summarize)
//	start → seed(function) → tag(set) → enrich(http) → rehydrate(set)
//	  → normalize(filter) → dedupe(remove_duplicates) → ordered(sort)
//	  → capped(limit) → rollup(aggregate) → renamed(rename) → script(js)
//	  → route(switch) ─ bulk   → fan(map, body: dq) ─┐
//	                  └ single → single_lane(set) ───┴→ join_lane(merge any)
//	  → db_insert(database) → db_load(database)
//	  → risk(grpc) ─ main ─────────────┐
//	              └ error → risk_gap ──┴→ browse(browser.cdp) ─ main ──────────────┐
//	                                                          └ error → browser_denied ┴→ gate(approval)
//	gate ─ approved → hold(wait signal) ─ main → tick(wait timer) → approved(set) ─┐
//	     │                              └ timeout → timed_out(set) ─────────────────┤
//	     └ rejected → rejected(set) ────────────────────────────────────────────────┴→ join_gate(merge any)
//	  → summarize(set) → brief(pick) → verdict(if) ─ true  → notify(notification) ─┐
//	                                               └ false → quiet(set) ───────────┴→ join_out(merge any) → done(end)
//
// Trigger lane (entries: on_timer, on_cron, on_webhook, on_redis, on_kafka):
//
//	on_* ─→ trigger_join(merge any) → trigger_record(function) → trigger_done(end)
//
// An execution starts from exactly one of the six entries; the engine resolves
// the other five as skipped, and the skip cascades through every node only they
// reach. A start run therefore skips the whole trigger lane, and a trigger run
// skips the whole main lane and the four sibling triggers — which is what lets
// one wait-any merge and one recording node serve all five kinds.
//
// Branch coverage per run: the switch and the if both branch on amount, so
// AboveThresholdInput takes bulk+notify and BelowThresholdInput takes
// single+quiet. The gRPC and browser nodes take their error ports on every run
// (see defs_high.go and defs_browser.go for why those are the reachable
// outcomes); the approval decision and the wait release decide the gate arm.
//
// Input: `amount` (number). Runtime vars: VarHTTPURL, VarRecipient,
// VarGRPCHost, VarDBTable, VarDBRowID (see FullVars). Credential: CredentialDB.
func FullWorkflow(tc FullTriggerConfig) *xflow.WorkflowBuilder {
	wf := xflow.Workflow("full-coverage")

	// --- declarations ---
	policy := wf.Node("policy", node.SupplyStatic([]byte(`{"max_amount": 1000}`)))
	rules := wf.Node("rules", node.SupplyExternal("qa_rules"))

	// --- main lane: data shaping ---
	start := wf.Node(FullEntryStart, node.Start())
	seed := wf.Node("seed", node.Expr(`{"amount": $input.amount, "orders": [
		{"id": "a", "qty": 3}, {"id": "b", "qty": 5},
		{"id": "a", "qty": 2}, {"id": "c", "qty": 0}, {"id": "d", "qty": 7}]}`))
	tag := wf.Node("tag", node.Set(map[string]any{"tier": "full"}).
		SetExpr(map[string]string{"double_amount": "$input.amount * 2"}))
	enrich := wf.Node("enrich", node.HTTP(string(node.HTTPGet), `{{ $vars.http_url }}/enrich`))
	// The HTTP node returns its response envelope, not the seeded collection,
	// so the collection is copied back before the data-driven transforms read it.
	rehydrate := wf.Node("rehydrate", node.Set(map[string]any{
		"orders":    `${{ $nodes['seed'].orders }}`,
		"amount":    `${{ $nodes['seed'].amount }}`,
		"enrich_ok": `${{ $nodes['enrich'].status == 200 }}`,
	}))
	normalize := wf.Node("normalize", node.Filter("orders", `item.qty > 0`))
	dedupe := wf.Node("dedupe", node.RemoveDuplicates("orders", "id"))
	ordered := wf.Node("ordered", node.Sort("orders", node.SortDesc("qty")))
	capped := wf.Node("capped", node.Limit("orders", 3))
	rollup := wf.Node("rollup", node.Aggregate("orders").Count("n").Sum("qty", "total_qty"))
	renamed := wf.Node("renamed", node.Rename(map[string]string{"n": "count"}))
	// A script's return value replaces the node's data, so it re-emits amount
	// and orders for the switch and the map downstream.
	scr := wf.Node("script", node.Script(
		`({script_ok: true, n: $input.orders.length, amount: $input.amount, orders: $input.orders})`).
		Language("js").Runtime("goja"))

	// --- main lane: branch + fan-out ---
	route := wf.Node("route", Switch([]node.SwitchRule{
		{Condition: BranchCondition(), Output: "bulk"},
	}, "single", "bulk", "single"))
	fan := wf.Node("fan", node.Map("orders", 2))
	fanBody := xflow.Workflow("full-fan-body")
	fanBody.Node("dq", node.Expr(`{"id": $item.id, "doubled": $item.qty * 2}`))
	fan.Body(fanBody)
	single := wf.Node("single_lane", node.Set(map[string]any{"lane": "single"}))
	joinLane := wf.Node("join_lane", node.Merge(node.MergeWaitAny))

	// --- main lane: IO ---
	dbInsert := wf.Node("db_insert", node.Database("insert", `{{ $vars.db_table }}`, CredentialDB).
		SetData(map[string]any{
			"id":  `${{ $vars.db_row_id }}`,
			"qty": 7,
		}))
	dbLoad := wf.Node("db_load", node.Database("select", `{{ $vars.db_table }}`, CredentialDB).
		SetWhere(map[string]any{"id": `${{ $vars.db_row_id }}`}))
	risk := wf.Node("risk", node.GRPC("qa.RiskService", "Score", `{{ $vars.grpc_host }}`).
		SetRequest(map[string]any{"subject": "qa-full"}).
		Timeout("5s")).
		OnError(types.OnErrorOutput)
	riskGap := wf.Node("risk_gap", node.Set(map[string]any{"risk_scored": false}))
	browse := wf.Node("browse", node.BrowserCDP(map[string]any{
		"debugging_url": BrowserDebuggingURL,
		"entry_url":     BrowserEntryURL,
		"target_host":   BrowserTargetHost,
	})).OnError(types.OnErrorOutput)
	browserDenied := wf.Node("browser_denied", node.Set(map[string]any{"denied": true}))

	// --- main lane: human gate ---
	gate := wf.Node("gate", node.Approval([]string{FullApprover}, node.ApprovalAny))
	hold := wf.Node("hold", node.Wait(FullReleaseSignal).WithTimeout("10s"))
	tick := wf.Node("tick", node.WaitDuration("200ms"))
	approved := wf.Node("approved", node.Set(map[string]any{"decision": "approved"}))
	timedOut := wf.Node("timed_out", node.Set(map[string]any{"decision": "wait_timeout"}))
	rejected := wf.Node("rejected", node.Set(map[string]any{"decision": "rejected"}))
	joinGate := wf.Node("join_gate", node.Merge(node.MergeWaitAny))

	// --- main lane: report ---
	summarize := wf.Node("summarize", node.Set(map[string]any{
		"amount":         `${{ $nodes['seed'].amount }}`,
		"count":          `${{ $nodes['renamed'].count }}`,
		"script_ok":      `${{ $nodes['script'].script_ok }}`,
		"rows_loaded":    `${{ $nodes['db_load'].rows }}`,
		"risk_ok":        `${{ $nodes['risk_gap'].risk_scored }}`,
		"browser_denied": `${{ $nodes['browser_denied'].denied }}`,
	}))
	brief := wf.Node("brief", node.Pick("amount", "count", "script_ok", "rows_loaded", "risk_ok", "browser_denied"))
	verdict := wf.Node("verdict", node.IF(BranchCondition()))
	notify := wf.Node("notify", node.Notification("email", `${{ $vars.recipient }}`).
		Subject("full coverage done").
		Message(`${{ sprintf("count=%v", $nodes['renamed'].count) }}`))
	quiet := wf.Node("quiet", node.Set(map[string]any{"notified": false}))
	joinOut := wf.Node("join_out", node.Merge(node.MergeWaitAny))
	done := wf.Node("done", node.End())

	wf.Connect(start, seed).
		Connect(seed.Output("main"), tag).
		Connect(tag.Output("main"), enrich).
		Connect(enrich.Output("main"), rehydrate).
		Connect(rehydrate.Output("main"), normalize).
		Connect(normalize.Output("main"), dedupe).
		Connect(dedupe.Output("main"), ordered).
		Connect(ordered.Output("main"), capped).
		Connect(capped.Output("main"), rollup).
		Connect(rollup.Output("main"), renamed).
		Connect(renamed.Output("main"), scr).
		Connect(scr.Output("main"), route).
		Connect(route.Output("bulk"), fan).
		Connect(route.Output("single"), single).
		Connect(fan.Output("main"), joinLane).
		Connect(single.Output("main"), joinLane).
		Connect(joinLane.Output("main"), dbInsert).
		Connect(dbInsert.Output("main"), dbLoad).
		Connect(dbLoad.Output("main"), risk).
		Connect(risk.Output("main"), browse).
		Connect(risk.Output("error"), riskGap).
		Connect(riskGap.Output("main"), browse).
		Connect(browse.Output("main"), gate).
		Connect(browse.Output("error"), browserDenied).
		Connect(browserDenied.Output("main"), gate).
		Connect(gate.Output("approved"), hold).
		Connect(gate.Output("rejected"), rejected).
		Connect(hold.Output("main"), tick).
		Connect(hold.Output("timeout"), timedOut).
		Connect(tick.Output("main"), approved).
		Connect(approved.Output("main"), joinGate).
		Connect(timedOut.Output("main"), joinGate).
		Connect(rejected.Output("main"), joinGate).
		Connect(joinGate.Output("main"), summarize).
		Connect(summarize.Output("main"), brief).
		Connect(brief.Output("main"), verdict).
		Connect(verdict.Output("true"), notify).
		Connect(verdict.Output("false"), quiet).
		Connect(notify.Output("main"), joinOut).
		Connect(quiet.Output("main"), joinOut).
		Connect(joinOut.Output("main"), done)

	// Supply nodes have no output ports; the dependency edge is their whole
	// contribution (see defs_high.go).
	wf.DependsOn(summarize, policy)
	wf.DependsOn(summarize, rules)

	// --- trigger lane ---
	onTimer := wf.Node(FullEntryTimer, trigger.Timer().Every(tc.TimerInterval))
	onCron := wf.Node(FullEntryCron, trigger.Cron().Cron(tc.CronExpression))
	onWebhook := wf.Node(FullEntryWebhook, trigger.Webhook().Method("POST").Path(FullWebhookPath))
	// StartID "0" for the reason RedisTriggerWorkflow documents.
	onRedis := wf.Node(FullEntryRedis, trigger.Redis().
		Addr(tc.RedisAddr).
		Mode("stream").
		Stream(tc.RedisStream).
		Group(tc.RedisGroup).
		Tuning(triggerredis.Tuning{StartID: "0"}))
	onKafka := wf.Node(FullEntryKafka, trigger.Kafka().
		Brokers(tc.KafkaBrokers...).
		Topic(tc.KafkaTopic).
		Group(tc.KafkaGroup).
		StartOffset("earliest"))
	// The join declares one input port per entry and each edge names it, so the
	// merge publishes the arriving entry's payload under that port. The four
	// entries that never fire arrive as empty inputs, which is why the record
	// node filters for the non-empty one.
	triggerJoin := wf.Node("trigger_join", node.Merge(node.MergeWaitAny)).
		Inputs("on_timer", "on_cron", "on_webhook", "on_redis", "on_kafka")
	triggerRecord := wf.Node("trigger_record", node.Expr(`{"lane": "trigger", "event": first(filter(
		[$input.on_timer, $input.on_cron, $input.on_webhook, $input.on_redis, $input.on_kafka],
		len(#) > 0)).trigger}`))
	triggerDone := wf.Node("trigger_done", node.End())
	for _, arm := range []struct {
		entry *xflow.NodeRef
		port  string
	}{
		{onTimer, "on_timer"}, {onCron, "on_cron"}, {onWebhook, "on_webhook"},
		{onRedis, "on_redis"}, {onKafka, "on_kafka"},
	} {
		wf.Connect(arm.entry.Output("main"), triggerJoin.Input(arm.port))
	}
	wf.Connect(triggerJoin.Output("main"), triggerRecord).
		Connect(triggerRecord.Output("main"), triggerDone)

	return wf
}

// FullTriggerNodeNames lists the trigger lane's nodes, entries first.
func FullTriggerNodeNames() []string {
	return []string{
		FullEntryTimer, FullEntryCron, FullEntryWebhook, FullEntryRedis, FullEntryKafka,
		"trigger_join", "trigger_record", "trigger_done",
	}
}

// FullVars returns the runtime variables FullWorkflow's main lane requires.
func FullVars(httpURL, recipient, grpcHost, table, rowID string) map[string]any {
	return map[string]any{
		VarHTTPURL:   httpURL,
		VarRecipient: recipient,
		VarGRPCHost:  grpcHost,
		VarDBTable:   table,
		VarDBRowID:   rowID,
	}
}

// FullMainNodeNames lists the main lane's nodes in execution order, so a failed
// run can name the node it stuck on.
func FullMainNodeNames() []string {
	return []string{
		"start", "seed", "tag", "enrich", "rehydrate", "normalize", "dedupe",
		"ordered", "capped", "rollup", "renamed", "script", "route", "fan", "dq",
		"single_lane", "join_lane", "db_insert", "db_load", "risk", "risk_gap",
		"browse", "browser_denied", "gate", "hold", "tick", "approved", "timed_out",
		"rejected", "join_gate", "summarize", "brief", "verdict", "notify", "quiet",
		"join_out", "done",
	}
}
