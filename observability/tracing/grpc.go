package tracing

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// GRPCUnaryServerInterceptor returns a grpc.UnaryServerInterceptor that
// extracts W3C traceparent/tracestate from incoming gRPC metadata and starts
// a server span for the RPC, mirroring the HTTP Middleware on the gRPC path.
//
// This complements (does not replace) the payload-embedded TraceCarrier that
// already flows dispatch→runner→execute→commit. The gRPC metadata carrier lets
// the runner-protocol unary RPCs (Register/Heartbeat/PollTask/ReportResult)
// carry a remote parent when a caller injects traceparent into metadata, so
// the server-side spans they start (e.g. xflow.task.dispatch) have the correct
// remote parent instead of becoming root spans.
//
// When tracer is nil the interceptor is a pass-through (tracing disabled).
func GRPCUnaryServerInterceptor(tracer Tracer) grpc.UnaryServerInterceptor {
	if tracer == nil {
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = extractFromGRPCMetadata(ctx)
		ctx, span := tracer.Start(ctx, "grpc."+info.FullMethod)
		defer span.End()
		resp, err := handler(ctx, req)
		if err != nil {
			span.RecordError(err)
		}
		return resp, err
	}
}

// extractFromGRPCMetadata pulls W3C tracecontext from incoming gRPC metadata
// (lowercased keys per gRPC spec) into ctx so downstream spans are parented to
// the caller's trace. It mirrors tracing.Middleware's HTTP header extraction.
func extractFromGRPCMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	carrier := map[string]string{}
	for k, vals := range md {
		if len(vals) == 0 {
			continue
		}
		switch k {
		case "traceparent", "tracestate", "baggage":
			carrier[k] = vals[0]
		}
	}
	return ExtractCarrier(ctx, carrier)
}

// GRPCStreamServerInterceptor returns a grpc.StreamServerInterceptor that
// extracts W3C traceparent/tracestate from incoming gRPC metadata for every
// streamed message and starts a server span covering the stream RPC. It is the
// stream-RPC analogue of GRPCUnaryServerInterceptor and mirrors the HTTP
// Middleware on the gRPC path.
//
// The extraction runs once per stream (on the incoming context) so bidi
// streams like the runner protocol's Connect inherit the caller's trace across
// the whole lifecycle. When tracer is nil the interceptor is a pass-through
// (tracing disabled).
func GRPCStreamServerInterceptor(tracer Tracer) grpc.StreamServerInterceptor {
	if tracer == nil {
		return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, ss)
		}
	}
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := extractFromGRPCMetadata(ss.Context())
		ctx, span := tracer.Start(ctx, "grpc."+info.FullMethod)
		defer span.End()
		// Wrap the stream so handlers see the trace-enriched context.
		wrapped := &tracedServerStream{ServerStream: ss, ctx: ctx}
		err := handler(srv, wrapped)
		if err != nil {
			span.RecordError(err)
		}
		return err
	}
}

// tracedServerStream forwards a grpc.ServerStream while replacing its context
// with one carrying the extracted traceparent and the started server span.
type tracedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *tracedServerStream) Context() context.Context { return s.ctx }
