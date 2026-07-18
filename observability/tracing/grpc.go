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
