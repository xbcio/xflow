package workflows

import (
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

// Trigger identities shared between the definitions and the suites that fire
// them.
//
// The broker endpoints are parameters, not $vars: a trigger's parameters are
// rendered when the workflow is REGISTERED, and the embedded AddWorkflow
// (graph-builder) path builds the WorkflowDef itself with no Context.Vars
// setter, so an in-process host has no way to supply them. Only the server's
// submit path carries a Context. A $vars form would therefore be a definition
// nothing could ever fire, which is why the coverage list and the firing suites
// share these parameterised builders instead of one declaration-only shape and
// one runnable shape.
const (
	// TriggerEntryName is the node name every trigger tier gives its entry
	// node, so a suite can address it without knowing which kind it is.
	TriggerEntryName = "entry"
	// WebhookPath is the path WebhookTriggerWorkflow registers.
	WebhookPath = "/qa/webhook"
	// RedisStreamName is the default stream the redis tier consumes. A firing
	// suite passes its own: a consumer group remembers its last-delivered ID, so
	// a reused stream/group pair resumes from a previous run's offsets.
	RedisStreamName = "xflow:qa:stream"
	// RedisStreamGroup is the default consumer group it joins.
	RedisStreamGroup = "xflow-qa"
	// KafkaQATopic is the default topic the kafka tier consumes, and
	// KafkaQAGroup the consumer group it joins.
	KafkaQATopic = "xflow-qa-events"
	KafkaQAGroup = "xflow-qa"
)

// KafkaQABrokers is the default broker list the kafka tier dials. Nothing in
// this package connects: the coverage list only compiles the definition, and a
// firing suite supplies its own brokers.
func KafkaQABrokers() []string { return []string{"localhost:9092"} }

// TimerTriggerWorkflow fires on a fixed interval and copies the scheduled time
// out of the trigger event.
//
//	entry(timer) → mark → done
//
// Node types: xflow.trigger.timer, xflow.function, xflow.end.
func TimerTriggerWorkflow(interval time.Duration) *xflow.WorkflowBuilder {
	return triggerWorkflow("trigger-timer", trigger.Timer().Every(interval),
		`{"kind": "timer", "event": $input.trigger}`)
}

// CronTriggerWorkflow fires on a cron expression.
//
// Node types: xflow.trigger.cron, xflow.function, xflow.end.
func CronTriggerWorkflow(expression string) *xflow.WorkflowBuilder {
	return triggerWorkflow("trigger-cron", trigger.Cron().Cron(expression),
		`{"kind": "cron", "event": $input.trigger}`)
}

// RedisTriggerWorkflow consumes a Redis stream.
//
//	entry(redis) → record → done
//
// stream and group are parameters rather than the shared constants because a
// consumer group remembers its last-delivered ID: a suite that reused the
// shared names would resume from wherever a previous run left off, and would
// then be asserting against that run's offsets instead of its own messages.
//
// Node types: xflow.trigger.redis, xflow.function, xflow.end.
func RedisTriggerWorkflow(addr, stream, group string) *xflow.WorkflowBuilder {
	return triggerWorkflow("trigger-redis",
		trigger.Redis().
			Addr(addr).
			Mode("stream").
			Stream(stream).
			Group(group),
		`{"kind": "redis", "event": $input.trigger}`)
}

// KafkaTriggerWorkflow consumes a Kafka topic.
//
//	Node types: xflow.trigger.kafka, xflow.function, xflow.end.
func KafkaTriggerWorkflow(brokers []string, topic, group string) *xflow.WorkflowBuilder {
	return triggerWorkflow("trigger-kafka",
		trigger.Kafka().
			Brokers(brokers...).
			Topic(topic).
			Group(group).
			StartOffset("earliest"),
		`{"kind": "kafka", "event": $input.trigger}`)
}

// WebhookTriggerWorkflow fires on an inbound HTTP request.
//
// Node types: xflow.trigger.webhook, xflow.function, xflow.end.
//
// The webhook trigger registers its route against the runtime's WebhookRuntime,
// which nothing mounts on a listener by default — the control-plane API has no
// webhook route. An embedded host must mount Engine.WebhookHandler() itself;
// the trigger suite does exactly that, so this tier is exercised through a real
// HTTP request rather than by calling the handler directly.
func WebhookTriggerWorkflow() *xflow.WorkflowBuilder {
	return triggerWorkflow("trigger-webhook",
		trigger.Webhook().Method("POST").Path(WebhookPath),
		`{"kind": "webhook", "event": $input.trigger}`)
}

// triggerWorkflow is the shared shape of every trigger tier: the trigger entry
// fans into one xflow.function node that records the trigger event, then ends.
//
// The recording node is a function rather than a transform because the event
// arrives as part of the node's input map, and a function's expression is
// evaluated against that whole map. A transform node would look for its data
// where a transform expects it, which for a trigger entry is not where the
// event lands.
//
// Every tier records the event through `$input.trigger` rather than through the
// `trigger` root's fields (trigger.data.value and friends). That is not a style
// choice. The environment spreads the input map's keys as roots, so `trigger`
// is the raw value stored under that key — and that value's Go type depends on
// the topology: in-process the trigger runtime hands the engine a
// *types.TriggerEvent, while over the wire the event is stored as JSON and read
// back as a map[string]any. expr resolves struct fields at COMPILE time and
// caches the compiled program by source text alone (see exprx.CompileExpr), so
// `trigger.data.value` compiles only against the decoded-map shape and
// `trigger.Data.X` only against the struct — a definition written for one
// topology fails on the other. `$input` is a map[string]any in both, so
// `$input.trigger` is an untyped index in both: it compiles everywhere and the
// consumer decides how to read the value it gets.
func triggerWorkflow(name string, entry types.Builder, recordExpr string) *xflow.WorkflowBuilder {
	wf := xflow.Workflow(name)
	entryRef := wf.Node(TriggerEntryName, entry)
	record := wf.Node("record", node.Expr(recordExpr))
	done := wf.Node("done", node.End())
	wf.Connect(entryRef, record).
		Connect(record.Output("main"), done)
	return wf
}
