package node

import (
	"context"
	"fmt"
	"sync"

	"github.com/expr-lang/expr"
)

// FunctionNode implements xflow.function — executes a named Go function or an inline Expr expression.
type FunctionNode struct {
	BaseNode
	FunctionName string
	Code         string
	Params       map[string]any
}

// Function creates a node that calls a named registered Go function.
//
//	node.Function("calculate_tax")
func Function(name string) *FunctionNode {
	return &FunctionNode{FunctionName: name}
}

// Expr creates a node that evaluates an inline expression.
//
//	node.Expr("price * quantity")
func Expr(code string) *FunctionNode {
	return &FunctionNode{Code: code}
}

func (n *FunctionNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.function",
		DisplayName: "Function",
		Params: []ParamSpec{
			{Name: "function_name", DisplayName: "Function Name", Type: ParamString, Required: false, Description: "Name of a pre-registered Go function to call"},
			{Name: "code", DisplayName: "Code", Type: ParamString, Required: false, Description: "Inline Expr expression to evaluate"},
			{Name: "params", DisplayName: "Params", Type: ParamObject, Required: false, Description: "Extra parameters passed to the function or expression"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *FunctionNode) NodeType() string { return "xflow.function" }
func (n *FunctionNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}

func (n *FunctionNode) RawParams() any {
	params := map[string]any{}
	if n.FunctionName != "" {
		params["function_name"] = n.FunctionName
	}
	if n.Code != "" {
		params["code"] = n.Code
	}
	if n.Params != nil {
		params["params"] = n.Params
	}
	return params
}

func (n *FunctionNode) Execute(ctx context.Context, input *Input) (*Output, error) {
	fnName, _ := input.Params["function_name"].(string)
	code, _ := input.Params["code"].(string)

	if fnName == "" && code == "" {
		return nil, fmt.Errorf("xflow.function: either function_name or code is required")
	}

	if fnName != "" {
		return n.executeNamed(ctx, input, fnName)
	}
	return n.executeExpr(ctx, input, code)
}

func (n *FunctionNode) executeNamed(ctx context.Context, input *Input, name string) (*Output, error) {
	fn, ok := LookupFunc(name)
	if !ok {
		return nil, fmt.Errorf("xflow.function: function %q not registered", name)
	}

	result, err := fn(ctx, input)
	if err != nil {
		return &Output{
			Data: map[string]any{"error": err.Error()},
			Port: "error",
		}, nil
	}
	return result, nil
}

func (n *FunctionNode) executeExpr(_ context.Context, input *Input, code string) (*Output, error) {
	env := buildExprEnv(input)

	if extra, ok := input.Params["params"].(map[string]any); ok {
		for k, v := range extra {
			env[k] = v
		}
	}

	program, err := expr.Compile(code, expr.Env(env))
	if err != nil {
		return nil, fmt.Errorf("xflow.function: compile expression: %w", err)
	}

	result, err := expr.Run(program, env)
	if err != nil {
		return &Output{
			Data: map[string]any{"error": err.Error()},
			Port: "error",
		}, nil
	}

	switch v := result.(type) {
	case map[string]any:
		return &Output{Data: v}, nil
	default:
		return &Output{Data: map[string]any{"result": v}}, nil
	}
}

// UserFunc is the signature for user-registered Go functions callable from xflow.function nodes.
type UserFunc func(ctx context.Context, input *Input) (*Output, error)

var (
	funcRegistryMu sync.RWMutex
	funcRegistry   = make(map[string]UserFunc)
)

// RegisterFunc registers a named Go function for use in xflow.function nodes.
func RegisterFunc(name string, fn UserFunc) {
	funcRegistryMu.Lock()
	defer funcRegistryMu.Unlock()
	funcRegistry[name] = fn
}

// LookupFunc returns a registered function by name.
func LookupFunc(name string) (UserFunc, bool) {
	funcRegistryMu.RLock()
	defer funcRegistryMu.RUnlock()
	fn, ok := funcRegistry[name]
	return fn, ok
}

func init() { Register(&FunctionNode{}) }
