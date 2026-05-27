package node

import "context"

// GRPCHandler implements xflow.grpc — executes a gRPC call.
// Execute is a stub; the real implementation lives in the Worker layer.
type GRPCHandler struct{}

func (h *GRPCHandler) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.grpc",
		DisplayName: "gRPC Call",
		Params: []ParamSpec{
			{Name: "service", DisplayName: "Service", Type: ParamString, Required: true, Description: "Fully-qualified service name (e.g. inventory.InventoryService)"},
			{Name: "method", DisplayName: "Method", Type: ParamString, Required: true, Description: "RPC method name"},
			{Name: "host", DisplayName: "Host", Type: ParamString, Required: true, Description: "gRPC server host:port"},
			{Name: "request", DisplayName: "Request", Type: ParamObject, Required: false, Description: "Request message payload"},
			{Name: "options", DisplayName: "Options", Type: ParamObject, Required: false, Description: "Dial/call options (timeout, TLS, etc.)"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (h *GRPCHandler) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": "xflow.grpc", "_stub": true}}, nil
}

func init() { Register(&GRPCHandler{}) }
