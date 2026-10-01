package control

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/xbcio/xflow/service/protocol"
)

// limitProbeUnlimited is far above every runner limit, so a peer built with it
// never trips first and the side under test is the only one that can.
const limitProbeUnlimited = 1 << 30

const (
	limitProbeService = "xflow.test.LimitProbe"
	limitProbeSend    = "/" + limitProbeService + "/Send"
	limitProbeReceive = "/" + limitProbeService + "/Receive"
)

// limitProbeDesc is a two-method service: Send accepts a request of any size
// and answers empty; Receive answers with a message of the encoded size the
// request asks for.
var limitProbeDesc = grpc.ServiceDesc{
	ServiceName: limitProbeService,
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Send",
			Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				if err := dec(new(wrapperspb.BytesValue)); err != nil {
					return nil, err
				}
				return &wrapperspb.BytesValue{}, nil
			},
		},
		{
			MethodName: "Receive",
			Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				size := new(wrapperspb.Int64Value)
				if err := dec(size); err != nil {
					return nil, err
				}
				return bytesValueOfEncodedSize(int(size.GetValue())), nil
			},
		},
	},
}

// bytesValueOfEncodedSize returns a BytesValue whose protobuf encoding — the
// size grpc-go compares against its message limits — is exactly n bytes.
func bytesValueOfEncodedSize(n int) *wrapperspb.BytesValue {
	// Field 1, length-delimited: one tag byte plus the varint length prefix.
	for payload := n - 1; payload >= 0 && payload >= n-1-binary.MaxVarintLen64; payload-- {
		if 1+protowire.SizeBytes(payload) == n {
			return &wrapperspb.BytesValue{Value: make([]byte, payload)}
		}
	}
	panic(fmt.Sprintf("no BytesValue encodes to exactly %d bytes", n))
}

func startLimitProbe(t *testing.T, serverOpts []grpc.ServerOption, dialOpts []grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(serverOpts...)
	srv.RegisterService(&limitProbeDesc, struct{}{})
	go func() { _ = srv.Serve(lis) }()

	opts := append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, dialOpts...)
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})
	return conn
}

// TestRunnerGRPCMessageLimitsAreEqual pins that all four runner gRPC message
// limits — server receive, server send, client send, client receive — are
// MaxRegisterRunnerBodyBytes, and that the HTTP body caps agree. A message at
// the limit must round-trip and one byte over must fail with
// ResourceExhausted. Each side is also probed against an unlimited peer, so a
// change to one limit alone cannot hide behind the other side's.
func TestRunnerGRPCMessageLimitsAreEqual(t *testing.T) {
	if protocol.MaxRunnerRequestBodyBytes != MaxRegisterRunnerBodyBytes ||
		protocol.MaxRunnerResponseBodyBytes != MaxRegisterRunnerBodyBytes {
		t.Fatalf("protocol.MaxRunnerRequestBodyBytes = %d, protocol.MaxRunnerResponseBodyBytes = %d, want both = MaxRegisterRunnerBodyBytes %d",
			protocol.MaxRunnerRequestBodyBytes, protocol.MaxRunnerResponseBodyBytes, MaxRegisterRunnerBodyBytes)
	}

	unlimitedServer := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(limitProbeUnlimited),
		grpc.MaxSendMsgSize(limitProbeUnlimited),
	}
	unlimitedDial := []grpc.DialOption{grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(limitProbeUnlimited),
		grpc.MaxCallSendMsgSize(limitProbeUnlimited),
	)}
	pairs := []struct {
		name       string
		serverOpts []grpc.ServerOption
		dialOpts   []grpc.DialOption
	}{
		{"RunnerServerAndRunnerDial", RunnerGRPCServerOptions(), RunnerGRPCDialOptions()},
		{"RunnerServerOnly", RunnerGRPCServerOptions(), unlimitedDial},
		{"RunnerDialOnly", unlimitedServer, RunnerGRPCDialOptions()},
	}
	sizes := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{"AtLimit", MaxRegisterRunnerBodyBytes, false},
		{"OneByteOver", MaxRegisterRunnerBodyBytes + 1, true},
	}
	for _, pair := range pairs {
		t.Run(pair.name, func(t *testing.T) {
			conn := startLimitProbe(t, pair.serverOpts, pair.dialOpts)
			for _, sz := range sizes {
				t.Run("Request"+sz.name, func(t *testing.T) {
					req := bytesValueOfEncodedSize(sz.size)
					if got := proto.Size(req); got != sz.size {
						t.Fatalf("request encodes to %d bytes, want %d", got, sz.size)
					}
					err := conn.Invoke(context.Background(), limitProbeSend, req, new(wrapperspb.BytesValue))
					checkLimitProbe(t, "request", sz.size, sz.wantErr, err)
				})
				t.Run("Response"+sz.name, func(t *testing.T) {
					resp := new(wrapperspb.BytesValue)
					err := conn.Invoke(context.Background(), limitProbeReceive, wrapperspb.Int64(int64(sz.size)), resp)
					checkLimitProbe(t, "response", sz.size, sz.wantErr, err)
					if err == nil {
						if got := proto.Size(resp); got != sz.size {
							t.Fatalf("response encodes to %d bytes, want %d", got, sz.size)
						}
					}
				})
			}
		})
	}
}

func checkLimitProbe(t *testing.T, direction string, size int, wantErr bool, err error) {
	t.Helper()
	if !wantErr {
		if err != nil {
			t.Fatalf("%d-byte %s error = %v, want success at limit %d", size, direction, err, MaxRegisterRunnerBodyBytes)
		}
		return
	}
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("%d-byte %s code = %v (err %v), want ResourceExhausted over limit %d",
			size, direction, got, err, MaxRegisterRunnerBodyBytes)
	}
}
