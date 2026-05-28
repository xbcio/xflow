package node

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// GRPCNode implements xflow.grpc — executes a gRPC unary call.
type GRPCNode struct {
	BaseNode
	Service  string
	Method   string
	Host     string
	Request  map[string]any
	Meta     map[string]any
	Options  map[string]any
}

// GRPC creates a gRPC unary call node.
//
//	node.GRPC("inventory.InventoryService", "GetStock", "localhost:50051")
func GRPC(service, method, host string) *GRPCNode {
	return &GRPCNode{Service: service, Method: method, Host: host}
}

func (n *GRPCNode) SetRequest(r map[string]any) *GRPCNode  { n.Request = r; return n }
func (n *GRPCNode) SetMetadata(m map[string]any) *GRPCNode { n.Meta = m; return n }
func (n *GRPCNode) TLS(enabled bool) *GRPCNode {
	if n.Options == nil {
		n.Options = map[string]any{}
	}
	n.Options["tls"] = enabled
	return n
}
func (n *GRPCNode) Timeout(d string) *GRPCNode {
	if n.Options == nil {
		n.Options = map[string]any{}
	}
	n.Options["timeout"] = d
	return n
}

func (n *GRPCNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.grpc",
		DisplayName: "gRPC Call",
		Params: []ParamSpec{
			{Name: "service", DisplayName: "Service", Type: ParamString, Required: true, Description: "Fully-qualified service name (e.g. inventory.InventoryService)"},
			{Name: "method", DisplayName: "Method", Type: ParamString, Required: true, Description: "RPC method name"},
			{Name: "host", DisplayName: "Host", Type: ParamString, Required: true, Description: "gRPC server host:port"},
			{Name: "request", DisplayName: "Request", Type: ParamObject, Required: false, Description: "Request message payload"},
			{Name: "metadata", DisplayName: "Metadata", Type: ParamObject, Required: false, Description: "gRPC metadata (headers)"},
			{Name: "options", DisplayName: "Options", Type: ParamObject, Required: false, Description: "Dial/call options (timeout, tls, etc.)"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *GRPCNode) NodeType() string { return "xflow.grpc" }
func (n *GRPCNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}

func (n *GRPCNode) RawParams() any {
	params := map[string]any{
		"service": n.Service,
		"method":  n.Method,
		"host":    n.Host,
	}
	if n.Request != nil {
		params["request"] = n.Request
	}
	if n.Meta != nil {
		params["metadata"] = n.Meta
	}
	if n.Options != nil {
		params["options"] = n.Options
	}
	return params
}

func (n *GRPCNode) Execute(ctx context.Context, input *Input) (*Output, error) {
	host, _ := input.Params["host"].(string)
	if host == "" {
		return nil, fmt.Errorf("xflow.grpc: host parameter is required")
	}
	service, _ := input.Params["service"].(string)
	if service == "" {
		return nil, fmt.Errorf("xflow.grpc: service parameter is required")
	}
	method, _ := input.Params["method"].(string)
	if method == "" {
		return nil, fmt.Errorf("xflow.grpc: method parameter is required")
	}

	timeout := 30 * time.Second
	useTLS := false
	if options, ok := input.Params["options"].(map[string]any); ok {
		if t, ok := options["timeout"].(string); ok {
			if d, err := time.ParseDuration(t); err == nil {
				timeout = d
			}
		}
		if tls, ok := options["tls"].(bool); ok {
			useTLS = tls
		}
	}
	if input.Timeout > 0 {
		timeout = input.Timeout
	}

	dialCtx, dialCancel := context.WithTimeout(ctx, timeout)
	defer dialCancel()

	var dialOpts []grpc.DialOption
	if !useTLS {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(host, dialOpts...)
	if err != nil {
		return &Output{Data: map[string]any{"error": fmt.Sprintf("dial: %v", err)}, Port: "error"}, nil
	}
	defer conn.Close()

	if md, ok := input.Params["metadata"].(map[string]any); ok {
		pairs := make([]string, 0, len(md)*2)
		for k, v := range md {
			pairs = append(pairs, k, fmt.Sprintf("%v", v))
		}
		dialCtx = metadata.AppendToOutgoingContext(dialCtx, pairs...)
	}

	fullMethod := fmt.Sprintf("/%s/%s", service, method)

	reqData, _ := input.Params["request"].(map[string]any)
	reqBytes, err := json.Marshal(reqData)
	if err != nil {
		return nil, fmt.Errorf("xflow.grpc: marshal request: %w", err)
	}

	reqMsg, err := structFromJSON(reqBytes)
	if err != nil {
		return nil, fmt.Errorf("xflow.grpc: build request message: %w", err)
	}

	respMsg := &dynamicpb.Message{}
	err = conn.Invoke(dialCtx, fullMethod, reqMsg, respMsg)
	if err != nil {
		return &Output{Data: map[string]any{"error": err.Error()}, Port: "error"}, nil
	}

	respBytes, err := protojson.Marshal(proto.Message(respMsg))
	if err != nil {
		return &Output{Data: map[string]any{"error": fmt.Sprintf("marshal response: %v", err)}, Port: "error"}, nil
	}

	var respData map[string]any
	if err := json.Unmarshal(respBytes, &respData); err != nil {
		return &Output{Data: map[string]any{"raw": string(respBytes)}}, nil
	}
	return &Output{Data: respData}, nil
}

func structFromJSON(data []byte) (proto.Message, error) {
	s := &structpb.Struct{}
	if err := protojson.Unmarshal(data, s); err != nil {
		return nil, err
	}
	return s, nil
}

func init() { Register(&GRPCNode{}) }
