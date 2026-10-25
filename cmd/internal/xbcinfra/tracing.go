package xbcinfra

import (
	"context"
	"fmt"

	xbctracing "github.com/xbcio/xbc/extensions/observability/tracing"
	xbclog "github.com/xbcio/xbc/log"

	xflowtracing "github.com/xbcio/xflow/observability/tracing"
)

// instrumentationName matches xflow's own tracing instrumentation name, so
// spans started through xbc carry the same name an embedded xflow tracer would
// have used.
const instrumentationName = "github.com/xbcio/xflow"

// NewTracer constructs xbc's private OpenTelemetry provider from cfg and adapts
// it to xflow's Tracer. The returned shutdown function flushes and tears the
// provider down; the caller owns it and must call it once the server has
// drained. A flush failure is recorded through the xbc log facade because
// xflow's shutdown hook has no error return.
//
// xbc's plugin never installs a process-global provider, so xflow's
// TraceIDFromContext still reads the span through the context it was given
// rather than through ambient state.
func NewTracer(cfg xbctracing.Config) (xflowtracing.Tracer, func(context.Context), error) {
	plugin, err := xbctracing.New(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("xbcinfra: initialize tracing: %w", err)
	}
	shutdown := func(ctx context.Context) {
		if err := plugin.Shutdown(ctx); err != nil {
			xbclog.L().Error("xbc tracing shutdown failed", "error", err)
		}
	}
	return xflowtracing.NewOTelTracer(plugin.Tracer(instrumentationName)), shutdown, nil
}
