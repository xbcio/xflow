package types

import (
	"context"
	"time"
)

// DescriptorProvider provides type identity, credential declarations, and schema
// information for typed node handlers.
type DescriptorProvider interface {
	Descriptor() Descriptor
}

// Handler is implemented by all typed node handlers that provide metadata.
type Handler interface {
	DescriptorProvider
}

// Builder is implemented by typed node builders returned from node factory
// functions and custom node definitions.
type Builder interface {
	NodeType() string
	RawParams() any
	OnError(strategy OnError) Builder
	OnErrorStrategy() OnError
}

// HandlerProvider is implemented by builders that can expose a process-local
// action handler instance for embedded SDK execution. It is the public
// counterpart of the internal HandlerCarrier interface so that SDK callers can
// assert it via a named interface instead of an anonymous one.
type HandlerProvider interface {
	Handler() ActionHandler
}

// TriggerHandlerProvider is implemented by trigger builders that can expose a
// process-local trigger handler instance for embedded SDK activation.
type TriggerHandlerProvider interface {
	TriggerHandler() TriggerHandler
}

// OnError is the error handling strategy for a workflow node.
type OnError string

const (
	OnErrorStop       OnError = "stop"
	OnErrorOutput     OnError = "error_output"
	OnErrorMainOutput OnError = "main_output"
	OnErrorContinue   OnError = "continue"
)

// ActionHandler is the runtime interface for action nodes.
// Implementations must be stateless and safe for concurrent use.
type ActionHandler interface {
	Handler
	Execute(ctx context.Context, input *Input) (*Output, error)
}

// Input holds the execution context passed to a node handler.
type Input struct {
	Params      map[string]any // evaluated node parameters
	Data        map[string]any // upstream data from the main input port ($input)
	Inputs      map[string]any // multi-port inputs keyed by port name ($inputs)
	Vars        map[string]any // workflow-level variables ($vars)
	Config      map[string]any // workflow-level config ($config)
	Runtime     *Runtime       // per-execution runtime context ($runtime)
	ExecutionID string
	NodeName    string
	TraceID     string
	SpanID      string
	Timeout     time.Duration // zero means no limit

	// credential resolver injected by the engine; accessed via Credential().
	credential func(name string) map[string]any
}

// Credential returns the credential values for the given name.
// In platform mode the values come from the credential store;
// in embedded mode they come from the WithCredential local map.
func (n *Input) Credential(name string) map[string]any {
	if n.credential == nil {
		return nil
	}
	return n.credential(name)
}

// SetCredentialResolver sets the credential resolver function.
// Called by engine implementations before Execute is invoked.
func (n *Input) SetCredentialResolver(fn func(name string) map[string]any) {
	n.credential = fn
}

// Output is the result produced by a node handler.
type Output struct {
	Data      map[string]any
	Error     *Error // non-nil routes to the error output port (business error, routable)
	Port      string // output port name; defaults to "main" if empty
	Resuspend bool   // if true, node re-enters suspended state after producing output
}

// Error is a business-level error that can be routed to the error output port.
type Error struct {
	Message    string
	StatusCode int
	NodeName   string
	Timestamp  time.Time
}

// Descriptor contains the metadata for a node type used for registration,
// editor rendering, and compile-time schema validation.
type Descriptor struct {
	Type        string
	Kind        NodeKind
	DisplayName string
	Credentials []string    // declared credential names required by this node
	Params      []ParamSpec // parameter schema for this node
	Inputs      []PortSpec
	Outputs     []PortSpec
	// Capabilities is an open-set list of capability tags. The compiler may use
	// these to gate experimental or implementation-incomplete features. See
	// CapBodySubgraphRequired for the canonical example.
	Capabilities []string
}

// Node capability tags. Open-set strings carried on Descriptor.Capabilities.
// The compiler inspects these to refuse workflows that depend on
// incomplete subsystems unless the workflow explicitly opts in.
const (
	// CapBodySubgraphRequired marks node types that need a compiled body
	// sub-graph at runtime. Currently xflow.loop and xflow.split. The
	// compiler rejects workflows referencing such nodes unless
	// WorkflowOptions.ExperimentalExpand is true.
	CapBodySubgraphRequired = "body_subgraph_required"
)

// ParamSpec defines the schema for a single node parameter.
type ParamSpec struct {
	Name        string
	DisplayName string
	Type        ParamType
	Required    bool
	Default     any
	Description string
}

// ParamType enumerates the supported parameter types.
type ParamType string

const (
	ParamString ParamType = "string"
	ParamNumber ParamType = "number"
	ParamBool   ParamType = "bool"
	ParamArray  ParamType = "array"
	ParamObject ParamType = "object"
)

// PortSpec defines the schema for a single node input or output port.
type PortSpec struct {
	Name        string
	DisplayName string
}

// OutputPort is a reference to a node output port.
type OutputPort struct {
	Node string
	Port string
}

// InputPort is a reference to a node input port.
type InputPort struct {
	Node string
	Port string
}
