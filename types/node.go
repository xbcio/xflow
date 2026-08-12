package types

import (
	"context"
	"time"

	"github.com/xbcio/xflow/namespace"
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
	Params  map[string]any // evaluated node parameters
	Data    map[string]any // upstream data from the main input port ($input)
	Inputs  map[string]any // multi-port inputs keyed by port name ($inputs)
	Vars    map[string]any // workflow-level variables ($vars)
	Config  map[string]any // workflow-level config ($config)
	Runtime *Runtime       // per-execution runtime context ($runtime)
	// Nodes holds the outputs of nodes referenced via $nodes['name'] in the
	// node's parameters. Populated at input assembly from the compile-time
	// reference set (Graph.NodesRefsFor).
	//
	// Typed-nil contract for unexecuted nodes:
	//   - A node that HAS executed: Nodes["x"] = its output (map[string]any).
	//   - A node that has NOT executed: Nodes["x"] = map[string]any(nil).
	//     The key MUST exist and the value MUST be a typed nil map.
	//
	// Why typed nil and not absent key or untyped nil:
	//   | value form          | $nodes['x'] ?? 'D' | $nodes['x'].f ?? 'D' |
	//   |---------------------|--------------------|-----------------------|
	//   | key absent          | "D"                | ERROR                 |
	//   | untyped nil         | "D"                | ERROR "cannot fetch"  |
	//   | map[string]any(nil) | "D"                | "D" ✓                 |
	//
	// Spec §4.2 recommends $nodes['optional'].field ?? 'default' — only the
	// typed-nil form makes .field access succeed (returning nil) so ?? can fire.
	Nodes       map[string]any
	ExecutionID string
	NodeName    string
	TraceID     string
	SpanID      string
	// WorkflowName and WorkflowVersion carry the compiled graph's identity so
	// expressions can read $workflow.name / $workflow.version. Populated at
	// input assembly from Graph.Name() / Graph.WorkflowVersion().
	//
	// There is deliberately no WorkflowID. WorkflowDef.ID is an instance
	// identifier with no production writer, and sdk/xflow/workflow_identity.go
	// excludes it from workflow identity as "a runtime instance pointer, not
	// part of the workflow definition" -- exposing it would put a permanently
	// empty field into the DSL.
	//
	// Inside a sub-graph body these hold the INNER graph's identity, which for
	// a map body is the map node's name (ProjectNodeBodyPackage builds the
	// inner def with Name = the map node's name). A body member asking for
	// $workflow.name gets the body it belongs to, not the outer workflow.
	WorkflowName    string
	WorkflowVersion string
	Timeout         time.Duration // zero means no limit

	// credential resolver injected by the engine; accessed via Credential().
	credential func(namespace namespace.Namespace, name string) map[string]any
	// namespace scopes the credential resolver to the execution's namespace.
	namespace namespace.Namespace

	// artifactCode resolves a script artifact by digest, returning the raw bytes
	// (e.g. wasm binary). Injected by the runner or embedded dispatcher before
	// Execute so ScriptNode can load code from the artifact store rather than
	// requiring it inline in parameters.
	artifactCode func(ctx context.Context, digest string) ([]byte, error)
}

// Credential returns the credential values for the given name.
// In platform mode the values come from the credential store;
// in embedded mode they come from the WithCredential local map.
func (n *Input) Credential(name string) map[string]any {
	if n.credential == nil {
		return nil
	}
	return n.credential(n.namespace, name)
}

// SetCredentialResolver sets the credential resolver function.
// Called by engine implementations before Execute is invoked. The resolver is
// namespace-scoped; the namespace is bound separately via SetNamespace.
func (n *Input) SetCredentialResolver(fn func(namespace namespace.Namespace, name string) map[string]any) {
	n.credential = fn
}

// SetNamespace scopes the credential resolver to the given namespace.
func (n *Input) SetNamespace(t namespace.Namespace) {
	n.namespace = t
}

// ArtifactCode resolves a script artifact by its content-addressable digest
// (e.g. "sha256:<hex>") and returns the raw bytes. Returns nil, nil when no
// resolver is configured — the caller must treat that as "feature unavailable".
func (n *Input) ArtifactCode(ctx context.Context, digest string) ([]byte, error) {
	if n.artifactCode == nil {
		return nil, nil
	}
	return n.artifactCode(ctx, digest)
}

// SetArtifactCodeResolver sets the function that resolves script artifacts by
// digest. Called by the runner or embedded dispatcher before Execute.
func (n *Input) SetArtifactCodeResolver(fn func(ctx context.Context, digest string) ([]byte, error)) {
	n.artifactCode = fn
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
	// these to gate experimental or implementation-incomplete features.
	Capabilities []string
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
