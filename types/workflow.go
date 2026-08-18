package types

import "time"

type WorkflowID string

// WorkflowDef is the top-level DSL data structure representing a workflow definition.
type WorkflowDef struct {
	ID string `json:"id,omitempty"`
	// Namespace is the server-authoritative isolation scope and workflow
	// namespace. API handlers must inject or validate it against the
	// authenticated principal; clients cannot use it to cross namespaces.
	Namespace      string                    `json:"namespace,omitempty"`
	Name           string                    `json:"name,omitempty"`
	Version        string                    `json:"version,omitempty"`
	Description    string                    `json:"description,omitempty"`
	Spec           string                    `json:"spec,omitempty"`
	RunnerSelector *RunnerSelector           `json:"runner_selector,omitempty"`
	Context        *WorkflowContext          `json:"context,omitempty"`
	Settings       *WorkflowSettings         `json:"settings,omitempty"`
	Options        *WorkflowOptions          `json:"options,omitempty"`
	Credentials    map[string]CredentialDef  `json:"credentials,omitempty"`
	Params         map[string]ParamDef       `json:"params,omitempty"`
	NodeTemplates  map[string]NodeTemplate   `json:"node_templates,omitempty"`
	Nodes          []NodeDef                 `json:"nodes,omitempty"`
	Groups         []GroupDef                `json:"groups,omitempty"`
	Connections    Connections               `json:"connections,omitempty"`
	Outputs        map[string]WorkflowOutput `json:"outputs,omitempty"`
	PinData        map[string]any            `json:"pin_data,omitempty"`
	DependencyEdges []DependencyEdge         `json:"dependency_edges,omitempty"`
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

	// TransientTTL is the sliding active TTL for transient execution keys.
	// When zero, the engine-wide transient TTL (or the default exec TTL) is used.
	TransientTTL time.Duration `json:"transient_ttl,omitempty"`

	// TransientCompletionTTL is the shortened TTL applied to all execution keys
	// once the execution reaches a terminal state. When zero, the engine-wide
	// transient completion TTL is used.
	TransientCompletionTTL time.Duration `json:"transient_completion_ttl,omitempty"`
}

// NodeDef describes a single node in the workflow graph.
type NodeDef struct {
	ID             string          `json:"id,omitempty"`
	Name           string          `json:"name,omitempty"`
	Type           string          `json:"type,omitempty"`
	Kind           NodeKind        `json:"kind,omitempty"`
	Version        int             `json:"version,omitempty"`
	Template       string          `json:"template,omitempty"`
	Position       *Position       `json:"position,omitempty"`
	Disabled       bool            `json:"disabled,omitempty"`
	OnError        string          `json:"on_error,omitempty"`
	RunnerSelector *RunnerSelector `json:"runner_selector,omitempty"`
	Notes          string          `json:"notes,omitempty"`
	Inputs         []PortDecl      `json:"inputs,omitempty"`
	OutputSchema   map[string]any  `json:"output_schema,omitempty"`
	Parameters     map[string]any  `json:"parameters,omitempty"`
	UI             map[string]any  `json:"ui,omitempty"`
	// Retry overrides WorkflowSettings.Retry for this node. Nil means inherit
	// the workflow default; the workflow default of nil means no retries.
	Retry *RetrySettings `json:"retry,omitempty"`
	// Timeout bounds a single execution of this node. Zero inherits the
	// engine's default (engine.DefaultNodeTimeout); a negative value means no
	// limit and must be written explicitly. It bounds one attempt, not the sum
	// of retries -- each retry attempt gets the full budget.
	Timeout time.Duration `json:"timeout,omitempty"`
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
	Concurrency int            `json:"concurrency,omitempty"`
	Timezone    string         `json:"timezone,omitempty"`
	OnError     string         `json:"on_error,omitempty"`
	PinDataMode string         `json:"pin_data_mode,omitempty"`
	Retry       *RetrySettings `json:"retry,omitempty"`
}

// RetrySettings configures automatic retry behaviour for the workflow.
type RetrySettings struct {
	Enabled         bool    `json:"enabled,omitempty"`
	MaxAttempts     int     `json:"max_attempts,omitempty"`
	Strategy        string  `json:"strategy,omitempty"`
	InitialInterval int     `json:"initial_interval,omitempty"`
	MaxInterval     int     `json:"max_interval,omitempty"`
	Multiplier      float64 `json:"multiplier,omitempty"`
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
