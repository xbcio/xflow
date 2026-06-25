package node

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"
)

type Handler = types.Handler
type DescriptorProvider = types.DescriptorProvider
type ActionHandler = types.ActionHandler
type Input = types.Input
type Output = types.Output
type Error = types.Error
type Descriptor = types.Descriptor
type ParamSpec = types.ParamSpec
type ParamType = types.ParamType
type PortSpec = types.PortSpec

const (
	ParamString = types.ParamString
	ParamNumber = types.ParamNumber
	ParamBool   = types.ParamBool
	ParamArray  = types.ParamArray
	ParamObject = types.ParamObject
)

// Builder is returned by built-in node factory functions and Definition.New.
// It is passed to WorkflowBuilder.AddNode.
type Builder interface {
	NodeType() string
	RawParams() any
	OnError(strategy OnError) Builder
	OnErrorStrategy() OnError
}

// HandlerCarrier is implemented by builders that can expose a process-local
// handler instance for embedded SDK execution.
type HandlerCarrier interface {
	Handler() ActionHandler
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

// nodeRef is the Builder implementation returned by Definition.New().
type nodeRef struct {
	nodeType string
	params   any
	onError  OnError
	handler  ActionHandler
}

func (r *nodeRef) NodeType() string         { return r.nodeType }
func (r *nodeRef) RawParams() any           { return r.params }
func (r *nodeRef) OnErrorStrategy() OnError { return r.onError }
func (r *nodeRef) OnError(s OnError) Builder {
	r.onError = s
	return r
}
func (r *nodeRef) Handler() ActionHandler { return r.handler }
func (r *nodeRef) Descriptor() Descriptor { return r.handler.Descriptor() }

func newBuilder(h ActionHandler, params any) Builder {
	if h == nil {
		panic("node.Definition.New: handler must not be nil")
	}
	t := h.Descriptor().Type
	if t == "" {
		panic(fmt.Sprintf("node.Definition.New: handler %T has empty Descriptor().Type", h))
	}
	return &nodeRef{nodeType: t, params: params, handler: h}
}

// ExecuteFunc is the function signature used by Define for custom action nodes.
type ExecuteFunc func(ctx context.Context, input *Input) (*Output, error)

// Definition is a reusable custom node type. Define it once, register it with
// xflow.WithNodes in consumer processes, and instantiate it in workflows with
// New(params).
type Definition struct {
	descriptor Descriptor
	execute    ExecuteFunc
}

// Define creates a reusable custom action node definition.
func Define(nodeType string, execute ExecuteFunc) *Definition {
	if nodeType == "" {
		panic("node.Define: nodeType must not be empty")
	}
	if execute == nil {
		panic("node.Define: execute must not be nil")
	}
	return &Definition{
		descriptor: Descriptor{
			Type: nodeType,
			Kind: types.NodeKindAction,
		},
		execute: execute,
	}
}

// New instantiates this custom node definition in a workflow with params.
func (d *Definition) New(params any) Builder {
	return newBuilder(d, params)
}

// Descriptor returns this node definition's metadata.
func (d *Definition) Descriptor() Descriptor { return d.descriptor }

// Execute runs this node definition's action.
func (d *Definition) Execute(ctx context.Context, input *Input) (*Output, error) {
	return d.execute(ctx, input)
}

// DisplayName sets the human-readable name used by tooling.
func (d *Definition) DisplayName(name string) *Definition {
	d.descriptor.DisplayName = name
	return d
}

// Param appends a parameter schema entry.
func (d *Definition) Param(spec ParamSpec) *Definition {
	d.descriptor.Params = append(d.descriptor.Params, spec)
	return d
}

// Input appends an input port by name.
func (d *Definition) Input(name string) *Definition {
	d.descriptor.Inputs = append(d.descriptor.Inputs, PortSpec{Name: name})
	return d
}

// Output appends an output port by name.
func (d *Definition) Output(name string) *Definition {
	d.descriptor.Outputs = append(d.descriptor.Outputs, PortSpec{Name: name})
	return d
}

// Credential declares a credential dependency by name.
func (d *Definition) Credential(name string) *Definition {
	d.descriptor.Credentials = append(d.descriptor.Credentials, name)
	return d
}
