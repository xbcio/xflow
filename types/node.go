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
	// State is this node's own private state, restored from its own stored
	// output. It is the one input channel no other node and no caller can write:
	// the engine fills it only from what this node itself committed, and never
	// from upstream data, submission params, or an execution scope.
	//
	// A suspending node keeps its bookkeeping here rather than in Data. Data is
	// the merged output of every upstream node, so a key a node stores there can
	// be written by a node upstream of it, and a node that reads such a key back
	// as its own memory is trusting data it did not produce. State has no such
	// path: a node that needs to remember something across a resumption writes it
	// to Output.State and reads it back from here.
	//
	// Nil means the node has no stored state — a first activation, or a node that
	// never suspends. Values must be encodable by the state backend's output
	// codec (JSON-shaped), because State is persisted inside the node's output.
	//
	// This is not the same thing as a private OUTPUT (Graph.NodeOutputPrivate):
	// that flag decides whether an operator may inspect a node's output, while
	// State is a channel between a node and its own next activation.
	State map[string]any
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

// Namespace returns the namespace this execution is scoped to. Node handlers
// that cache anything resolved through a namespace-scoped resolver must key
// that cache by this value: a digest is content-addressable, but the right to
// read it is not, and a cache shared across namespaces would let one tenant
// serve another's artifact without the store's reference check.
func (n *Input) Namespace() namespace.Namespace {
	return n.namespace
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

// HasArtifactResolver reports whether an artifact resolver was injected.
//
// Handlers that cache resolved artifacts must consult this BEFORE their cache:
// a cache hit would otherwise let an execution with no resolver run code that
// some other execution happened to fetch first. That turns a wiring defect —
// a dispatch path that never calls SetArtifactCodeResolver — into a failure
// that depends on process history and execution order, which is strictly worse
// than failing outright every time.
func (n *Input) HasArtifactResolver() bool {
	return n.artifactCode != nil
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
	// State is this node's private state, persisted with its output and handed
	// back as Input.State on that node's own next resumption. It travels through
	// the same commit as Data but down a separate channel: unlike Data it is
	// never merged into a downstream node's input, and unlike Data it cannot be
	// seeded by an upstream node or a caller. See Input.State.
	//
	// Setting State on a result that is not a resuspend stores it just the same,
	// and a result that carries only State — nil Data — still counts as an output
	// worth storing, which is what lets a node update its state without
	// republishing data.
	State map[string]any
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
	// Groups declares the editor sections ParamSpec.Group refers to, in
	// display order.
	Groups []GroupSpec
	// Docs is long-form documentation for the node type (Markdown).
	Docs string
	// OneOf declares cross-parameter "set" cardinality rules over top-level
	// params. Hidden params (VisibleWhen false) still count, because a handler
	// reads any value that is present.
	OneOf []OneOfGroup
}

// ParamSpec defines the schema for a single node parameter.
//
// A param is "set" when its key is present and its value is neither nil nor
// "". A param whose VisibleWhen evaluates false is hidden: it is exempt from
// Required, RequiredWhen, Enum, EnumWhen, and Constraints validation, but it
// still counts toward Descriptor.OneOf.
type ParamSpec struct {
	Name        string
	DisplayName string
	Type        ParamType
	Required    bool
	Default     any
	Description string

	// Enum lists the allowed values. Empty means unrestricted.
	Enum []EnumOption
	// EnumWhen overrides Enum depending on other params: the first entry whose
	// When holds wins; when none holds, Enum applies.
	EnumWhen []ConditionalEnum
	// Item is the element schema when Type is ParamArray.
	Item *ParamSpec
	// Fields lists the known sub-fields when Type is ParamObject. Empty means
	// a free-form map.
	Fields []ParamSpec
	// Secret marks a value that must be masked in editors and logs.
	Secret bool
	// Constraints bounds the value; nil means no constraints.
	Constraints *Constraints
	// VisibleWhen hides the param when it evaluates false; nil means always
	// visible.
	VisibleWhen *Condition
	// RequiredWhen makes the param required while it evaluates true. It is
	// ignored when Required is true.
	RequiredWhen *Condition
	// Group is the Key of a Descriptor.Groups entry; empty means ungrouped.
	Group string
	// Order sorts params within a group (ascending, ties keep declaration order).
	Order int
	// Widget names an editor widget for types the editor cannot infer; empty
	// means infer from Type.
	Widget string
	// Deprecated, when non-empty, marks the param deprecated and explains the
	// replacement.
	Deprecated string
}

// EnumOption is one allowed value of a ParamSpec.
type EnumOption struct {
	Value       any
	DisplayName string
	Description string
}

// ConditionalEnum is an Enum that applies while When holds.
type ConditionalEnum struct {
	When Condition
	Enum []EnumOption
}

// Constraints bounds a param value. Nil pointers mean unbounded.
type Constraints struct {
	Min, Max             *float64 // numeric bounds, inclusive
	MinLength, MaxLength *int     // string length bounds, inclusive
	Pattern              string   // RE2 regular expression the string must match
	// Format names a value format. The backend enforces duration, cron,
	// expression, sha256-digest, and json; url, host-port, and code are
	// advisory editor hints only.
	Format             string
	MinItems, MaxItems *int // array length bounds, inclusive
	UniqueItems        bool
}

// Condition is a predicate over sibling params: top-level params of the same
// Descriptor, or fields of the same ParamSpec.Fields. All non-zero clauses
// must hold (implicit AND); a Condition with no clauses holds.
//
// A missing param compares as null. Eq and In compare numbers numerically,
// so a JSON float64 equals an int literal of the same value. Truthy follows
// JavaScript truthiness: "", 0, false, and null are false; [], {}, and "0"
// are true.
type Condition struct {
	Param  string // the sibling param the Eq, In, and Truthy clauses test
	Eq     any
	In     []any
	Truthy *bool // non-nil: the param's truthiness must equal *Truthy
	AllOf  []Condition
	AnyOf  []Condition
	Not    *Condition
}

// OneOfGroup constrains how many of Params may be set; see ParamSpec for the
// definition of "set".
type OneOfGroup struct {
	Params []string
	Mode   string // OneOfExactly when empty
}

// OneOfGroup modes.
const (
	OneOfExactly = "exactly"  // exactly one param set (the default)
	OneOfAtMost  = "at_most"  // zero or one param set
	OneOfAtLeast = "at_least" // one or more params set
)

// GroupSpec declares an editor section that params join via ParamSpec.Group.
type GroupSpec struct {
	Key         string
	DisplayName string
	Description string
	Collapsed   bool // initially collapsed in the editor
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
