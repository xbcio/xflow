package node

import (
	"context"
	"fmt"
	"time"
)

// TaskHandler is the core execution interface for workflow nodes.
// Implementations must be stateless and safe for concurrent use.
type TaskHandler interface {
	Execute(ctx context.Context, input *Input) (*Output, error)
}

// DescriptorProvider is an optional interface implemented by handlers that
// need to provide type identity, credential declarations, and schema information.
// Both node.Register and node.New require the handler to implement this interface.
type DescriptorProvider interface {
	Descriptor() Descriptor
}

// Builder is returned by built-in node factory functions and node.New.
// It is passed to WorkflowBuilder.AddNode.
type Builder interface {
	NodeType() string
	RawParams() any
	OnError(strategy OnError) Builder
	OnErrorStrategy() OnError
}

// Input holds the execution context passed to a node handler.
type Input struct {
	Params      map[string]any // evaluated node parameters
	Data        map[string]any // upstream data from the main input port ($input)
	Inputs      map[string]any // multi-port inputs keyed by port name ($inputs)
	Vars        map[string]any // workflow-level variables ($vars)
	Config      map[string]any // workflow-level config ($config)
	ExecutionID string
	NodeName    string
	TraceID     string        // empty string in embedded mode
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
// Called by engine implementations (e.g. MemoryRunner) before Execute is invoked.
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
	DisplayName string
	Credentials []string    // declared credential names required by this node
	Params      []ParamSpec // parameter schema for this node
	Inputs      []PortSpec
	Outputs     []PortSpec
}

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

// OnError is the error handling strategy for a workflow node.
type OnError string

const (
	OnErrorStop       OnError = "stop"
	OnErrorOutput     OnError = "error_output"
	OnErrorMainOutput OnError = "main_output"
	OnErrorContinue   OnError = "continue"
)

// MergeMode is the merge strategy for a join node waiting on multiple upstreams.
type MergeMode string

const (
	MergeWaitAll MergeMode = "wait_all"
	MergeWaitAny MergeMode = "wait_any"
)

// HTTPMethod is the HTTP verb used in an HTTP request node.
type HTTPMethod string

const (
	HTTPGet    HTTPMethod = "GET"
	HTTPPost   HTTPMethod = "POST"
	HTTPPut    HTTPMethod = "PUT"
	HTTPDelete HTTPMethod = "DELETE"
	HTTPPatch  HTTPMethod = "PATCH"
)

// nodeRef is the Builder implementation returned by node.New().
type nodeRef struct {
	nodeType string
	params   any
	onError  OnError
}

func (r *nodeRef) NodeType() string         { return r.nodeType }
func (r *nodeRef) RawParams() any           { return r.params }
func (r *nodeRef) OnErrorStrategy() OnError { return r.onError }
func (r *nodeRef) OnError(s OnError) Builder {
	r.onError = s
	return r
}

// New creates a Builder that references h's type in the global registry.
// h must not be nil and must implement DescriptorProvider; panics otherwise.
// At execution time the handler is resolved from the registry, not the h instance passed here.
func New(h TaskHandler, params any) Builder {
	if h == nil {
		panic("node.New: h must not be nil")
	}
	p, ok := h.(DescriptorProvider)
	if !ok {
		panic(fmt.Sprintf("node.New: handler %T must implement DescriptorProvider", h))
	}
	t := p.Descriptor().Type
	if t == "" {
		panic(fmt.Sprintf("node.New: handler %T has empty Descriptor().Type", h))
	}
	return &nodeRef{nodeType: t, params: params}
}
