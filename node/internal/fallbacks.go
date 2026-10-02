package internal

import "slices"

// Fallback records a builtin param whose handler substitutes a value when the
// param is absent although its ParamSpec declares no Default.
//
// These are deliberately NOT promoted to ParamSpec.Default: the SDK writes
// Default into a workflow's params, so adding one moves the definition hash of
// every SDK workflow using the node and the next redeploy of an unchanged
// workflow conflicts (descriptor-contract design, "Default 不可改"). The table
// exists so an editor can still show the effective value as a hint (the
// node-types DTO's "fallback" field) without the value being written into
// anybody's definition.
//
// Every entry is exercised against the handler that applies it by a contract
// test next to that handler (see node/default_contract_coverage_test.go for
// the index), and node/internal/fallbacks_test.go checks each Param resolves to
// a declared ParamSpec that still has no Default.
type Fallback struct {
	// Type and Version identify the builtin descriptor.
	Type    string
	Version int
	// Param is the dotted ParamSpec path ("tuning.start_id"): nested Fields are
	// joined with ".", matching how the node-form projection walks Fields.
	Param string
	// Value is the JSON-shaped fallback: string, float64 or bool. Durations are
	// Go duration strings, the form the param itself accepts. Nil when Derived
	// is set.
	Value any
	// Derived describes a fallback that is computed rather than constant (from
	// the node name, another param, or the activation path). Empty for a
	// constant Value.
	Derived string
}

var builtinFallbacks = []Fallback{
	// action / flow
	{Type: "xflow.browser.cdp", Version: 1, Param: "target_host", Derived: "the host of entry_url"},
	{Type: "xflow.http", Version: 1, Param: "mode", Value: "json"},
	{Type: "xflow.merge", Version: 1, Param: "mode", Value: "wait_all"},
	{Type: "xflow.switch", Version: 1, Param: "default_output", Value: "default"},
	{Type: "xflow.switch", Version: 1, Param: "mode", Value: "rules"},
	{Type: "xflow.wait", Version: 1, Param: "signal_name", Derived: "<node name>/signal, when signals is empty"},

	// trigger.kafka
	{Type: "xflow.trigger.kafka", Version: 1, Param: "aggregate.by", Value: "partition"},
	{Type: "xflow.trigger.kafka", Version: 1, Param: "aggregate.dedup", Value: "message"},
	{Type: "xflow.trigger.kafka", Version: 1, Param: "aggregate.flush_interval", Derived: "100ms; 1s when the activation admits through entry seeds"},
	{Type: "xflow.trigger.kafka", Version: 1, Param: "aggregate.max_size", Value: float64(100)},
	{Type: "xflow.trigger.kafka", Version: 1, Param: "aggregate.on_overflow", Value: "discard"},
	{Type: "xflow.trigger.kafka", Version: 1, Param: "message_schema.on_invalid", Value: "discard"},
	{Type: "xflow.trigger.kafka", Version: 1, Param: "tuning.dial_timeout", Value: "10s"},
	{Type: "xflow.trigger.kafka", Version: 1, Param: "tuning.fetch_max_bytes", Value: float64(10_000_000)},
	{Type: "xflow.trigger.kafka", Version: 1, Param: "tuning.fetch_min_bytes", Value: float64(1)},
	{Type: "xflow.trigger.kafka", Version: 1, Param: "tuning.heartbeat_interval", Value: "3s"},
	{Type: "xflow.trigger.kafka", Version: 1, Param: "tuning.max_wait", Value: "10s"},
	{Type: "xflow.trigger.kafka", Version: 1, Param: "tuning.rebalance_timeout", Value: "30s"},
	{Type: "xflow.trigger.kafka", Version: 1, Param: "tuning.session_timeout", Value: "30s"},

	// trigger.redis
	{Type: "xflow.trigger.redis", Version: 1, Param: "tuning.claim_min_idle", Value: "1m"},
	{Type: "xflow.trigger.redis", Version: 1, Param: "tuning.consumer", Derived: "the node name"},
	{Type: "xflow.trigger.redis", Version: 1, Param: "tuning.db", Value: float64(0)},
	{Type: "xflow.trigger.redis", Version: 1, Param: "tuning.dial_timeout", Value: "10s"},
	{Type: "xflow.trigger.redis", Version: 1, Param: "tuning.start_id", Value: "$"},
}

// BuiltinFallbacks returns a copy of the fallback table. Values are scalars,
// so the copy is independent of the table.
func BuiltinFallbacks() []Fallback { return slices.Clone(builtinFallbacks) }

// BuiltinFallbacksFor returns the entries for one type@version.
func BuiltinFallbacksFor(typ string, version int) []Fallback {
	var out []Fallback
	for _, f := range builtinFallbacks {
		if f.Type == typ && f.Version == version {
			out = append(out, f)
		}
	}
	return out
}
