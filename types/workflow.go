package types

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

type WorkflowID string

// WorkflowDef is the top-level DSL data structure representing a workflow definition.
type WorkflowDef struct {
	ID string `json:"id,omitempty"`
	// Namespace is the server-authoritative isolation scope and workflow
	// namespace. API handlers must inject or validate it against the
	// authenticated principal; clients cannot use it to cross namespaces.
	Namespace       string                    `json:"namespace,omitempty"`
	Name            string                    `json:"name,omitempty"`
	Version         string                    `json:"version,omitempty"`
	Description     string                    `json:"description,omitempty"`
	Spec            string                    `json:"spec,omitempty"`
	RunnerSelector  *RunnerSelector           `json:"runner_selector,omitempty"`
	Context         *WorkflowContext          `json:"context,omitempty"`
	Settings        *WorkflowSettings         `json:"settings,omitempty"`
	Options         *WorkflowOptions          `json:"options,omitempty"`
	Credentials     map[string]CredentialDef  `json:"credentials,omitempty"`
	Params          map[string]ParamDef       `json:"params,omitempty"`
	NodeTemplates   map[string]NodeTemplate   `json:"node_templates,omitempty"`
	Nodes           []NodeDef                 `json:"nodes,omitempty"`
	Groups          []GroupDef                `json:"groups,omitempty"`
	Connections     Connections               `json:"connections,omitempty"`
	Outputs         map[string]WorkflowOutput `json:"outputs,omitempty"`
	PinData         map[string]any            `json:"pin_data,omitempty"`
	DependencyEdges []DependencyEdge          `json:"dependency_edges,omitempty"`
}

// WorkflowOptions controls advanced workflow-level runtime behavior.
type WorkflowOptions struct {
	// AllowCycles opts the workflow into cyclic execution mode.
	//
	// Default false keeps the original DAG semantics and rejects any cycle at
	// compile/build time. When true, the workflow must contain exactly one
	// xflow.start node, scheduling follows the active output port directly, and
	// a node may run more than once. The engine still stores only the latest
	// state/output for each node; business history and custom-node side effects
	// remain the caller's responsibility.
	AllowCycles bool `json:"allow_cycles,omitempty"`

	// MaxAutoDepth caps one uninterrupted automatic scheduling chain in cyclic
	// mode. It prevents unattended loops from running forever. Manual resume
	// points such as signals and timeouts reset the automatic depth counter, so
	// human-driven approval loops are not limited by the total number of rounds.
	//
	// Values <= 0 use the engine default.
	MaxAutoDepth int `json:"max_auto_depth,omitempty"`

	// ExperimentalNodeGroup opts a workflow into the node-group co-location
	// protocol. When true, the server advertises group-aware scheduling to
	// runners that report the FeatureGroupProtocolV1 capability, and the
	// compiler emits group packages. Setting this to false on a workflow that
	// contains groups is currently allowed (groups compile normally) but the
	// scheduler will not co-locate them — individual nodes dispatch as before.
	ExperimentalNodeGroup bool `json:"experimental_node_group,omitempty"`

	// Transient opts a single workflow into transient (fire-and-forget) execution
	// mode. When true, executions of this workflow skip the SQL audit projection
	// and use TTL-bounded Redis state, regardless of the engine-wide transient
	// setting. This allows mixing transient and durable workflows in the same
	// engine instance.
	Transient bool `json:"transient,omitempty"`

	// TransientTTL is the active retention TTL for transient execution keys. Its
	// active window starts when the execution is created; ordinary state mutations
	// do not renew it, so it must exceed the execution's maximum end-to-end
	// wall-clock duration. Supported suspension handling may explicitly extend
	// affected keys while waiting; that is not general sliding retention. When
	// zero, the engine-wide transient TTL (or the default exec TTL) is used.
	TransientTTL time.Duration `json:"transient_ttl,omitempty"`

	// TransientCompletionTTL is the retention TTL applied to transient execution
	// keys after the execution reaches a terminal state. When zero, the engine-wide
	// transient completion TTL is used.
	TransientCompletionTTL time.Duration `json:"transient_completion_ttl,omitempty"`

	// FAF is best-effort, non-persistent fire-and-forget: it creates no Redis/MySQL
	// execution, node, output, lease, outbox, retry, audit, or result state; it provides
	// no durable delivery, retry/recovery, cross-process dataflow, status inspection/wait/cancel,
	// or workflow result.
	FAF bool `json:"faf,omitempty"`
}

// NodeOutputPolicy controls how a node's runtime output is exposed.
// Private output remains available to runtime consumers but is redacted from
// public representations.
type NodeOutputPolicy struct {
	Private bool `json:"private,omitempty"`
}

// NodeDef describes a single node in the workflow graph.
//
// NodeDef carries no editor-only fields. Visual position, UI theme, and
// author notes live exclusively in WorkflowEditorMetadata (ADR-D4 §2.2),
// keyed by ID (or Name when ID is empty). ID itself stays on NodeDef: it is
// the metadata key, not metadata.
type NodeDef struct {
	ID             string            `json:"id,omitempty"`
	Name           string            `json:"name,omitempty"`
	Type           string            `json:"type,omitempty"`
	Kind           NodeKind          `json:"kind,omitempty"`
	Version        int               `json:"version,omitempty"`
	Template       string            `json:"template,omitempty"`
	Disabled       bool              `json:"disabled,omitempty"`
	OnError        string            `json:"on_error,omitempty"`
	RunnerSelector *RunnerSelector   `json:"runner_selector,omitempty"`
	Inputs         []PortDecl        `json:"inputs,omitempty"`
	OutputSchema   map[string]any    `json:"output_schema,omitempty"`
	Output         *NodeOutputPolicy `json:"output,omitempty"`
	Parameters     map[string]any    `json:"parameters,omitempty"`
	// Retry overrides WorkflowSettings.Retry for this node. Nil means inherit
	// the workflow default; the workflow default of nil means no retries.
	Retry *RetrySettings `json:"retry,omitempty"`
	// Timeout bounds a single execution of this node. Zero inherits the
	// engine's default (engine.DefaultNodeTimeout); a negative value means no
	// limit and must be written explicitly. It bounds one attempt, not the sum
	// of retries -- each retry attempt gets the full budget.
	Timeout time.Duration `json:"timeout,omitempty"`
	// ActivationReplicas is the desired number of distinct runners that host a
	// trigger entry. Zero preserves the legacy single-activation behavior; one is
	// therefore equivalent to zero. Values greater than one are useful for
	// partitioned sources such as Kafka, where every activation joins the same
	// consumer group. Group members must configure this on GroupDef instead.
	ActivationReplicas uint32 `json:"activation_replicas,omitempty"`
}

type RunnerSelectorMode string

const (
	RunnerSelectorModeDefault  RunnerSelectorMode = "default"
	RunnerSelectorModeRequired RunnerSelectorMode = "required"
)

type RunnerSelector struct {
	Mode        RunnerSelectorMode `json:"mode,omitempty"`
	MatchLabels map[string]string  `json:"match_labels,omitempty"`
}

// NodeKind describes a node's runtime role.
type NodeKind string

const (
	NodeKindAction  NodeKind = "action"
	NodeKindTrigger NodeKind = "trigger"
	// NodeKindSupply marks a node that maintains long-lived shared data for
	// other nodes to read. A supply node never advances an execution: it is
	// registered in the graph and referable by dependency edges, but it is
	// deliberately excluded from the unit layer, so it never counts toward the
	// remaining-unit denominator.
	NodeKindSupply NodeKind = "supply"
)

// Position holds the visual coordinates of a node in the workflow editor.
type Position struct {
	X float64 `json:"x,omitempty"`
	Y float64 `json:"y,omitempty"`
}

// DefaultInputPort is the input port a node has when it declares no `inputs:`,
// and the port a connection targets when it carries no `input:` label.
//
// Fan-in inputs are keyed by port (Input.Inputs, exposed as $inputs), so every
// unlabelled edge of a node lands on this one key. A node that fans in without
// declaring its ports is warned about at compile time: two upstreams sharing a
// key means only one of them can be read.
const DefaultInputPort = "main"

// PortDecl declares an input port on a node.
type PortDecl struct {
	Name     string `json:"name,omitempty"`
	Required bool   `json:"required,omitempty"`
}

// Connection represents a single incoming edge to a node from a source port.
type Connection struct {
	Node  string `json:"node,omitempty"`
	Input string `json:"input,omitempty"`
}

// ConnectionType is the channel class of a source port. It is declared on the
// port rather than on each link because the class is a property of what the
// port emits, not of who listens. n8n puts a type on the link object too, but
// that one is a redundant echo forced by index-based port location; xflow ports
// have names, so the port-level declaration is the only load-bearing one.
type ConnectionType string

const (
	// ConnectionTypeData carries node output downstream. The zero value is
	// equivalent, which is what lets the legacy array form decode unchanged.
	ConnectionTypeData ConnectionType = "data"
	// ConnectionTypeDependency declares that the targets read the source supply
	// node's content through $supplies.<name>. It carries no data and does not
	// participate in topology.
	ConnectionTypeDependency ConnectionType = "dependency"
)

// PortConnections is every edge leaving one source port. Type describes the
// channel class and is uniform across all targets of that port.
type PortConnections struct {
	Type    ConnectionType `json:"type,omitempty"`
	Targets []Connection   `json:"targets"`
}

// Connections maps source_node → output_port → the edges leaving that port.
type Connections map[string]map[string]PortConnections

// DependencyEdge declares that Node reads the shared data maintained by the
// supply node named Supply. It is deliberately separate from Connections:
// a dependency edge carries no data and takes no part in unit-edge
// construction, so it must not pollute the dataflow topology.
type DependencyEdge struct {
	Node   string `json:"node"`
	Supply string `json:"supply"`
}

// WorkflowContext holds runtime variables and configuration available to all nodes.
type WorkflowContext struct {
	Vars   map[string]any `json:"vars,omitempty"`
	Config map[string]any `json:"config,omitempty"`
}

// WorkflowSettings controls execution behaviour of the workflow.
type WorkflowSettings struct {
	// Concurrency limits how many executions of this workflow may be in flight
	// at once. A bare int (`concurrency: 10`) means QUEUE mode: submissions over
	// the limit wait in FIFO order. Reject mode must be written explicitly —
	// see ConcurrencySettings.
	Concurrency *ConcurrencySettings `json:"concurrency,omitempty"`
	Timezone    string               `json:"timezone,omitempty"`
	OnError     string               `json:"on_error,omitempty"`
	PinDataMode string               `json:"pin_data_mode,omitempty"`
	Retry       *RetrySettings       `json:"retry,omitempty"`
}

// Values of ConcurrencySettings.Mode.
const (
	// ConcurrencyModeQueue admits submissions over the limit into a FIFO queue
	// and promotes them as slots free up. It is the implicit mode of a bare int
	// `concurrency: N`.
	ConcurrencyModeQueue = "queue"
	// ConcurrencyModeReject refuses a submission over the limit with 429 rather
	// than queueing it. Never persists anything for the refused execution.
	ConcurrencyModeReject = "reject"
)

// ConcurrencySettings limits how many executions of one workflow run at once.
//
// It accepts two wire forms. A bare int is the shorthand for
// `{limit: N, mode: queue}` — the mode the doc settled on as the meaning of the
// pre-existing `settings.concurrency` field, which had no reader before this
// type existed. The object form carries the explicit mode plus the optional
// queue controls.
//
// Limit 0 means no limit and is rejected as a meaningless setting only when
// combined with queue controls; the field's absence is the normal
// "unlimited" spelling.
type ConcurrencySettings struct {
	// Limit is the maximum number of concurrent executions. 0 means unlimited.
	Limit int `json:"limit,omitempty"`
	// Mode is ConcurrencyModeQueue or ConcurrencyModeReject. Empty is QUEUE:
	// a bare int carries no mode and must decode to the documented default.
	Mode string `json:"mode,omitempty"`
	// MaxQueued caps the QUEUE-mode backlog. A submission that would exceed it
	// is refused with the same 429 as reject mode. Ignored outside queue mode.
	MaxQueued int `json:"max_queued,omitempty"`
	// QueueTimeout bounds how long a queued execution may wait for a slot. The
	// clock starts at admission (it is not an execution budget — see the design
	// note on promotion). Ignored outside queue mode.
	QueueTimeout time.Duration `json:"queue_timeout,omitempty"`

	// modeSet records that the wire form carried an explicit mode, so a
	// round-trip of an object-form setting does not collapse to the bare int
	// shorthand and silently lose the author's explicit "queue".
	modeSet bool
}

// EffectiveMode returns the mode the settings actually mean: an unset mode is
// QUEUE, because that is what the bare-int shorthand denotes.
func (c *ConcurrencySettings) EffectiveMode() string {
	if c == nil || c.Mode == "" {
		return ConcurrencyModeQueue
	}
	return c.Mode
}

// EffectiveLimit returns the configured limit, or 0 for a nil settings.
func (c *ConcurrencySettings) EffectiveLimit() int {
	if c == nil {
		return 0
	}
	return c.Limit
}

// UnmarshalJSON accepts both the bare-int shorthand and the object form.
//
// The branch is taken on the JSON shape before any semantics are read, the same
// rule PortConnections.UnmarshalJSON follows: a decision that depends on a
// field value would give that value two meanings and make a later parse error
// indistinguishable from legacy input.
func (c *ConcurrencySettings) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 {
		return fmt.Errorf("concurrency settings: empty value")
	}
	if trimmed[0] != '{' {
		var limit int
		if err := json.Unmarshal(data, &limit); err != nil {
			return fmt.Errorf("concurrency settings (int form): %w", err)
		}
		*c = ConcurrencySettings{Limit: limit}
		return nil
	}
	type alias ConcurrencySettings
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return fmt.Errorf("concurrency settings (object form): %w", err)
	}
	*c = ConcurrencySettings(a)
	c.modeSet = true
	return nil
}

// MarshalJSON emits the bare-int shorthand whenever the setting carries nothing
// an int cannot express — the default queue mode with no queue controls. That
// keeps a definition authored as `concurrency: 10` byte-identical across a
// decode/encode round trip, and therefore keeps its definition hash stable; an
// unconditional object encoding would re-hash every workflow that already
// declares the shorthand.
func (c ConcurrencySettings) MarshalJSON() ([]byte, error) {
	if !c.modeSet && c.Mode == "" && c.MaxQueued == 0 && c.QueueTimeout == 0 {
		return json.Marshal(c.Limit)
	}
	type alias ConcurrencySettings
	return json.Marshal(alias(c))
}

// Values of WorkflowSettings.PinDataMode. The empty string means
// PinDataModeTestOnly.
const (
	// PinDataModeTestOnly applies pin_data only to executions started as test
	// runs (xflow.WithTestRun, or "test": true on the HTTP invoke body).
	PinDataModeTestOnly = "test_only"
	// PinDataModeAlways applies pin_data to every execution.
	PinDataModeAlways = "always"
	// PinDataModeDisabled ignores pin_data entirely.
	PinDataModeDisabled = "disabled"
)

// RetrySettings configures automatic retry behaviour for the workflow.
// Enabled is tri-state: nil means enabled whenever MaxAttempts > 0; only an
// explicit false disables retries.
type RetrySettings struct {
	Enabled         *bool   `json:"enabled,omitempty"`
	MaxAttempts     int     `json:"max_attempts,omitempty"`
	Strategy        string  `json:"strategy,omitempty"`
	InitialInterval int     `json:"initial_interval,omitempty"`
	MaxInterval     int     `json:"max_interval,omitempty"`
	Multiplier      float64 `json:"multiplier,omitempty"`
}

// Values of RetrySettings.Strategy. The empty string means
// RetryStrategyExponential.
const (
	RetryStrategyFixed       = "fixed"
	RetryStrategyExponential = "exponential"
)

// IsEnabled reports whether r requests retries: non-nil, not explicitly
// disabled, and MaxAttempts > 0.
func (r *RetrySettings) IsEnabled() bool {
	return r != nil && (r.Enabled == nil || *r.Enabled) && r.MaxAttempts > 0
}

// Clone returns a deep copy of r.
func (r *RetrySettings) Clone() *RetrySettings {
	if r == nil {
		return nil
	}
	cp := *r
	if r.Enabled != nil {
		v := *r.Enabled
		cp.Enabled = &v
	}
	return &cp
}

// CredentialDef references a named credential stored in a secrets manager.
type CredentialDef struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
}

// ParamDef declares a workflow-level input parameter.
type ParamDef struct {
	Type        string         `json:"type,omitempty"`
	Required    bool           `json:"required,omitempty"`
	DisplayName string         `json:"display_name,omitempty"`
	Default     any            `json:"default,omitempty"`
	Validation  map[string]any `json:"validation,omitempty"`
}

// NodeTemplate is a reusable node configuration snippet referenced by NodeDef.Template.
type NodeTemplate struct {
	Type       string         `json:"type,omitempty"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

// WorkflowOutput declares a named output exposed by the workflow.
type WorkflowOutput struct {
	Value       any    `json:"value,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
}
