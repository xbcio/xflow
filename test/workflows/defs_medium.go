package workflows

import (
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
)

// Runtime variable names the medium and high tiers read at execution time. They
// are supplied by the caller through xflow.WithRuntimeVars so one definition can
// run against a local test server and a deployed one.
const (
	// VarHTTPURL is the base URL of the HTTP endpoint MediumWorkflow calls. The
	// node appends /enrich.
	VarHTTPURL = "http_url"
	// VarRecipient is the notification recipient MediumWorkflow reports to.
	VarRecipient = "recipient"
)

// CredentialDB is the credential name HighWorkflow's database nodes resolve. The
// caller supplies the values (driver, dsn) through the runner's credential
// resolver.
const CredentialDB = "qa_db"

// MediumWorkflow is the medium-complexity tier: an HTTP call, a script, and the
// full transform family over a seeded collection, closed by a switch that fans
// one arm out through a map body.
//
//	start → seed → tag → enrich(HTTP) → rehydrate → normalize(filter)
//	      → dedupe → ordered(sort) → capped(limit) → rollup(aggregate)
//	      → renamed → script ─ bulk → fan(map body)
//	                          └ single → single_lane ─┴→ join → notify → done
//
// Two details are load-bearing and easy to get wrong:
//
//   - rehydrate exists because the transform nodes are data-driven: each one
//     reads its `items` expression out of the node's own input data, and the
//     expression is a field name that must already be present. The HTTP node
//     returns the response envelope rather than the seeded collection, so the
//     collection is copied back with an explicit ${{ }} reference to the seed
//     node before the first transform runs.
//
//   - the script node sits before the switch and re-emits amount alongside its
//     own result. A script's return value replaces the node's data wholesale, so
//     a script that returned only its own fields would leave the switch's
//     `$input.amount` nil and the rule would fail on a comparison against nil.
//
// Node types: xflow.start, xflow.function, xflow.transform.set, xflow.http,
// xflow.transform.filter, xflow.transform.remove_duplicates,
// xflow.transform.sort, xflow.transform.limit, xflow.transform.aggregate,
// xflow.transform.rename, xflow.script, xflow.switch, xflow.map (with an
// xflow.subgraph body), xflow.merge, xflow.notification, xflow.end.
//
// Input: `amount` (number) decides the switch arm. Runtime vars:
// VarHTTPURL, VarRecipient.
func MediumWorkflow() *xflow.WorkflowBuilder {
	wf := xflow.Workflow("medium-tier")
	start := wf.Node("start", node.Start())
	seed := wf.Node("seed", node.Expr(`{"amount": $input.amount, "orders": [
		{"id": "a", "qty": 3}, {"id": "b", "qty": 5},
		{"id": "a", "qty": 2}, {"id": "c", "qty": 0}, {"id": "d", "qty": 7}]}`))
	tag := wf.Node("tag", node.Set(map[string]any{"tier": "medium"}))
	enrich := wf.Node("enrich", node.HTTP(string(node.HTTPGet), `{{ $vars.http_url }}/enrich`))
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
	scr := wf.Node("script", node.Script(
		`({script_ok: true, n: $input.orders.length, amount: $input.amount, orders: $input.orders})`).
		Language("js").Runtime("goja"))
	route := wf.Node("route", Switch([]node.SwitchRule{
		{Condition: `$input.amount >= 100`, Output: "bulk"},
	}, "single", "bulk", "single"))

	fan := wf.Node("fan", node.Map("orders", 2))
	// The map body is a nested builder, so the body itself carries an
	// xflow.subgraph node and the body's members.
	fanBody := xflow.Workflow("fan-body")
	fanBody.Node("dq", node.Expr(`{"id": $item.id, "doubled": $item.qty * 2}`))
	fan.Body(fanBody)

	single := wf.Node("single_lane", node.Set(map[string]any{"lane": "single"}))
	join := wf.Node("join", node.Merge(node.MergeWaitAny))
	notify := wf.Node("notify", node.Notification("email", `${{ $vars.recipient }}`).
		Subject("medium tier done").
		Message(`${{ sprintf("count=%v", $nodes['renamed'].count) }}`))
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
		Connect(fan.Output("main"), join).
		Connect(single.Output("main"), join).
		Connect(join.Output("main"), notify).
		Connect(notify.Output("main"), done)
	return wf
}

// MediumBulkInput takes the map arm of MediumWorkflow's switch.
func MediumBulkInput() map[string]any { return map[string]any{"amount": 500.0} }

// MediumSingleInput takes the single-node arm of MediumWorkflow's switch.
func MediumSingleInput() map[string]any { return map[string]any{"amount": 10.0} }

// MediumVars returns the runtime variables MediumWorkflow requires.
func MediumVars(httpURL, recipient string) map[string]any {
	return map[string]any{VarHTTPURL: httpURL, VarRecipient: recipient}
}
