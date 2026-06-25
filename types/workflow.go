package types

// WorkflowDef is the top-level DSL data structure representing a workflow definition.
type WorkflowDef struct {
	ID            string                    `json:"id,omitempty"`
	Name          string                    `json:"name,omitempty"`
	Version       string                    `json:"version,omitempty"`
	Description   string                    `json:"description,omitempty"`
	Spec          string                    `json:"spec,omitempty"`
	Context       *WorkflowContext          `json:"context,omitempty"`
	Settings      *WorkflowSettings         `json:"settings,omitempty"`
	Credentials   map[string]CredentialDef  `json:"credentials,omitempty"`
	Params        map[string]ParamDef       `json:"params,omitempty"`
	NodeTemplates map[string]NodeTemplate   `json:"node_templates,omitempty"`
	Nodes         []NodeDef                 `json:"nodes,omitempty"`
	Connections   Connections               `json:"connections,omitempty"`
	Outputs       map[string]WorkflowOutput `json:"outputs,omitempty"`
	PinData       map[string]any            `json:"pin_data,omitempty"`
}

// NodeDef describes a single node in the workflow graph.
type NodeDef struct {
	ID           string         `json:"id,omitempty"`
	Name         string         `json:"name,omitempty"`
	Type         string         `json:"type,omitempty"`
	Kind         NodeKind       `json:"kind,omitempty"`
	Version      int            `json:"version,omitempty"`
	Template     string         `json:"template,omitempty"`
	Position     *Position      `json:"position,omitempty"`
	Disabled     bool           `json:"disabled,omitempty"`
	OnError      string         `json:"on_error,omitempty"`
	Notes        string         `json:"notes,omitempty"`
	Inputs       []PortDecl     `json:"inputs,omitempty"`
	OutputSchema map[string]any `json:"output_schema,omitempty"`
	Parameters   map[string]any `json:"parameters,omitempty"`
	UI           map[string]any `json:"ui,omitempty"`
}

// NodeKind describes a node's runtime role.
type NodeKind string

const (
	NodeKindAction NodeKind = "action"
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

// Connections maps source_node → output_port → list of target connections.
type Connections map[string]map[string][]Connection

// WorkflowContext holds runtime variables and configuration available to all nodes.
type WorkflowContext struct {
	Vars   map[string]any `json:"vars,omitempty"`
	Config map[string]any `json:"config,omitempty"`
}

// WorkflowSettings controls execution behaviour of the workflow.
type WorkflowSettings struct {
	Timeout     int            `json:"timeout,omitempty"`
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
