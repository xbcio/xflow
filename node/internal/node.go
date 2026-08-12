package internal

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"
)

// Builder is returned by built-in node factory functions and Definition.New.
// It is passed to WorkflowBuilder.AddNode.
// HandlerCarrier is implemented by builders that can expose a process-local
// handler instance for embedded SDK execution. It is an alias for the public
// types.HandlerProvider so internal callers and the SDK share one named
// contract instead of duplicating anonymous interface assertions.
type HandlerCarrier = types.HandlerProvider

// TriggerHandlerCarrier is implemented by trigger builders that can expose a
// process-local trigger handler instance for embedded SDK activation. It is an
// alias for the public types.TriggerHandlerProvider.
type TriggerHandlerCarrier = types.TriggerHandlerProvider

const (
	OnErrorStop       = types.OnErrorStop
	OnErrorOutput     = types.OnErrorOutput
	OnErrorMainOutput = types.OnErrorMainOutput
	OnErrorContinue   = types.OnErrorContinue
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
	onError  types.OnError
	handler  types.ActionHandler
}

func (r *nodeRef) NodeType() string               { return r.nodeType }
func (r *nodeRef) RawParams() any                 { return r.params }
func (r *nodeRef) OnErrorStrategy() types.OnError { return r.onError }
func (r *nodeRef) OnError(s types.OnError) types.Builder {
	r.onError = s
	return r
}
func (r *nodeRef) Handler() types.ActionHandler { return r.handler }
func (r *nodeRef) Descriptor() types.Descriptor { return r.handler.Descriptor() }

func newBuilder(h types.ActionHandler, params any) types.Builder {
	if h == nil {
		panic("node.Definition.New: handler must not be nil")
	}
	t := h.Descriptor().Type
	if t == "" {
		panic(fmt.Sprintf("node.Definition.New: handler %T has empty Descriptor().Type", h))
	}
	return &nodeRef{nodeType: t, params: params, handler: h}
}

type triggerRef struct {
	nodeType string
	params   any
	onError  types.OnError
	handler  types.TriggerHandler
}

func (r *triggerRef) NodeType() string               { return r.nodeType }
func (r *triggerRef) RawParams() any                 { return r.params }
func (r *triggerRef) OnErrorStrategy() types.OnError { return r.onError }
func (r *triggerRef) OnError(s types.OnError) types.Builder {
	r.onError = s
	return r
}
func (r *triggerRef) TriggerHandler() types.TriggerHandler { return r.handler }
func (r *triggerRef) Descriptor() types.Descriptor         { return r.handler.Descriptor() }

func newTriggerBuilder(h types.TriggerHandler, params any) types.Builder {
	if h == nil {
		panic("node.TriggerDefinition.New: handler must not be nil")
	}
	t := h.Descriptor().Type
	if t == "" {
		panic(fmt.Sprintf("node.TriggerDefinition.New: handler %T has empty Descriptor().Type", h))
	}
	return &triggerRef{nodeType: t, params: params, handler: h}
}

// ExecuteFunc is the function signature used by Define for custom action nodes.
type ExecuteFunc func(ctx context.Context, input *types.Input) (*types.Output, error)

// Definition is a reusable custom node type. Define it once, register it with
// xflow.WithNodes in consumer processes, and instantiate it in workflows with
// New(params).
type Definition struct {
	descriptor types.Descriptor
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
		descriptor: types.Descriptor{
			Type: nodeType,
			Kind: types.NodeKindAction,
		},
		execute: execute,
	}
}

// New instantiates this custom node definition in a workflow with params.
func (d *Definition) New(params any) types.Builder {
	return newBuilder(d, params)
}

// Descriptor returns this node definition's metadata.
func (d *Definition) Descriptor() types.Descriptor { return d.descriptor }

// Execute runs this node definition's action.
func (d *Definition) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	return d.execute(ctx, input)
}

// DisplayName sets the human-readable name used by tooling.
func (d *Definition) DisplayName(name string) *Definition {
	d.descriptor.DisplayName = name
	return d
}

// Param appends a parameter schema entry.
func (d *Definition) Param(spec types.ParamSpec) *Definition {
	d.descriptor.Params = append(d.descriptor.Params, spec)
	return d
}

// Input appends an input port by name.
func (d *Definition) Input(name string) *Definition {
	d.descriptor.Inputs = append(d.descriptor.Inputs, types.PortSpec{Name: name})
	return d
}

// Output appends an output port by name.
func (d *Definition) Output(name string) *Definition {
	d.descriptor.Outputs = append(d.descriptor.Outputs, types.PortSpec{Name: name})
	return d
}

// Credential declares a credential dependency by name.
func (d *Definition) Credential(name string) *Definition {
	d.descriptor.Credentials = append(d.descriptor.Credentials, name)
	return d
}

type TriggerActivateFunc func(ctx context.Context, input *types.TriggerActivateInput) (types.TriggerSubscription, error)

type TriggerDefinition struct {
	descriptor types.Descriptor
	activate   TriggerActivateFunc
}

// ExecuteTriggerEntry is the shared entry handler for trigger nodes: it wraps
// the trigger event into the node's main output port.
func ExecuteTriggerEntry(input *types.Input) (*types.Output, error) {
	data := map[string]any{}
	if input != nil && input.Data != nil {
		for k, v := range input.Data {
			data[k] = v
		}
	}
	if _, ok := data["trigger"]; !ok {
		data["trigger"] = &types.TriggerEvent{}
	}
	return &types.Output{Data: data, Port: "main"}, nil
}

func DefineTrigger(nodeType string, activate TriggerActivateFunc) *TriggerDefinition {
	if nodeType == "" {
		panic("trigger.Define: nodeType must not be empty")
	}
	if activate == nil {
		panic("trigger.Define: activate must not be nil")
	}
	return &TriggerDefinition{
		descriptor: types.Descriptor{
			Type:    nodeType,
			Kind:    types.NodeKindTrigger,
			Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		},
		activate: activate,
	}
}

func (d *TriggerDefinition) New(params any) types.Builder { return newTriggerBuilder(d, params) }
func (d *TriggerDefinition) Descriptor() types.Descriptor { return d.descriptor }
func (d *TriggerDefinition) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return ExecuteTriggerEntry(input)
}
func (d *TriggerDefinition) Activate(ctx context.Context, input *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	return d.activate(ctx, input)
}
