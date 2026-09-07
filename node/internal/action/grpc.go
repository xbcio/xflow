package action

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"

	"github.com/xbcio/xflow/types"

	"time"

	"github.com/spf13/cast"
	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// GRPCNode implements xflow.grpc — executes a gRPC unary call.
type GRPCNode struct {
	nodeinternal.BaseNode
	Service string
	Method  string
	Host    string
	Request map[string]any
	Meta    map[string]any
	Options map[string]any
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

func (n *GRPCNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.grpc",
		DisplayName: "gRPC Call",
		Params: []types.ParamSpec{
			{Name: "service", DisplayName: "Service", Type: types.ParamString, Required: true, Description: "Fully-qualified service name (e.g. inventory.InventoryService)"},
			{Name: "method", DisplayName: "Method", Type: types.ParamString, Required: true, Description: "RPC method name"},
			{Name: "host", DisplayName: "Host", Type: types.ParamString, Required: true, Description: "gRPC server host:port"},
			{Name: "request", DisplayName: "Request", Type: types.ParamObject, Required: false, Description: "Request message payload"},
			{Name: "metadata", DisplayName: "Metadata", Type: types.ParamObject, Required: false, Description: "gRPC metadata (headers)"},
			{Name: "options", DisplayName: "Options", Type: types.ParamObject, Required: false, Description: "Dial/call options (timeout, tls, etc.)"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *GRPCNode) NodeType() string { return "xflow.grpc" }
func (n *GRPCNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
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

func (n *GRPCNode) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	host := cast.ToString(input.Params["host"])
	if host == "" {
		return nil, types.NewPermanentError("grpc.missing_host", "xflow.grpc: host parameter is required")
	}
	service := cast.ToString(input.Params["service"])
	if service == "" {
		return nil, types.NewPermanentError("grpc.missing_service", "xflow.grpc: service parameter is required")
	}
	method := cast.ToString(input.Params["method"])
	if method == "" {
		return nil, types.NewPermanentError("grpc.missing_method", "xflow.grpc: method parameter is required")
	}

	timeout := 30 * time.Second
	useTLS := false
	if options, ok := input.Params["options"].(map[string]any); ok {
		if t := cast.ToString(options["timeout"]); t != "" {
			if d, err := time.ParseDuration(t); err == nil {
				timeout = d
			}
		}
		useTLS = cast.ToBool(options["tls"])
	}
	if input.Timeout > 0 {
		timeout = input.Timeout
	}

	dialCtx, dialCancel := context.WithTimeout(ctx, timeout)
	defer dialCancel()

	var dialOpts []grpc.DialOption
	if useTLS {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, release, err := acquireGRPC(ctx, host, useTLS, dialOpts...)
	if err != nil {
		return nil, err
	}
	defer release()

	if md, ok := input.Params["metadata"].(map[string]any); ok {
		pairs := make([]string, 0, len(md)*2)
		for k, v := range md {
			pairs = append(pairs, k, cast.ToString(v))
		}
		dialCtx = metadata.AppendToOutgoingContext(dialCtx, pairs...)
	}

	fullMethod := fmt.Sprintf("/%s/%s", service, method)

	reqData, _ := input.Params["request"].(map[string]any)
	reqBytes, err := json.Marshal(reqData)
	if err != nil {
		return nil, types.NewPermanentError("grpc.marshal_request", fmt.Sprintf("xflow.grpc: marshal request: %v", err))
	}

	reqMsg, err := structFromJSON(reqBytes)
	if err != nil {
		return nil, types.NewPermanentError("grpc.build_request", fmt.Sprintf("xflow.grpc: build request message: %v", err))
	}

	respMsg := &dynamicpb.Message{}
	err = conn.Invoke(dialCtx, fullMethod, reqMsg, respMsg)
	if err != nil {
		if st, ok := grpcstatus.FromError(err); ok {
			switch st.Code() {
			case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied,
				codes.Unauthenticated, codes.AlreadyExists, codes.Unimplemented,
				codes.FailedPrecondition, codes.OutOfRange:
				return nil, types.NewPermanentError("grpc."+st.Code().String(), st.Message())
			default:
				return nil, types.NewTransientError("grpc."+st.Code().String(), st.Message())
			}
		}
		return nil, types.NewTransientError("grpc.invoke", err.Error())
	}

	respBytes, err := protojson.Marshal(proto.Message(respMsg))
	if err != nil {
		return nil, types.NewTransientError("grpc.marshal_response", fmt.Sprintf("marshal response: %v", err))
	}

	var respData map[string]any
	if err := json.Unmarshal(respBytes, &respData); err != nil {
		return &types.Output{Data: map[string]any{"raw": string(respBytes)}}, nil
	}
	return &types.Output{Data: respData}, nil
}

func structFromJSON(data []byte) (proto.Message, error) {
	s := &structpb.Struct{}
	if err := protojson.Unmarshal(data, s); err != nil {
		return nil, err
	}
	return s, nil
}

// acquireGRPC fetches a pooled *grpc.ClientConn keyed by (host, tls). Returns
// a permanent error when no pool is attached to ctx (config issue that won't
// self-heal on retry), or a transient error when the pool dial fails.
func acquireGRPC(ctx context.Context, host string, secure bool, opts ...grpc.DialOption) (*grpc.ClientConn, func(), error) {
	pool := types.ResourcePoolFromContext(ctx)
	if pool == nil {
		return nil, nil, types.NewPermanentError("grpc.no_pool", "xflow.grpc: no resource pool configured")
	}
	conn, err := pool.GRPC(ctx, host, secure, opts...)
	if err != nil {
		return nil, func() {}, types.NewTransientError("grpc.dial", fmt.Sprintf("dial %s: %v", host, err))
	}
	return conn, func() {}, nil
}

func init() { registry.Register(&GRPCNode{}) }
