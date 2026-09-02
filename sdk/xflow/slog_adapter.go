package xflow

import (
	"fmt"
	"log/slog"

	"github.com/xbcio/xflow/engine"
)

// slogLogger adapts a *slog.Logger to engine.Logger, the interface the engine
// and the backend providers accept.
//
// It exists because the runner's logging surface and the engine's are different
// types that no package bridges: WithRunnerLogger takes a *slog.Logger, while
// local.WithQueueLogger takes an engine.Logger. The one other adapter in the
// tree (backend/providers/distributed/internal/timeout's slogAdapter) is
// unexported inside an internal package AND hardcodes slog.Default(), so it can
// neither be imported nor carry the logger a caller actually configured.
//
// observability/logger is the other candidate home, and was rejected: it pulls
// in zap, which sdk/xflow does not depend on today (`go list -deps ./sdk/xflow |
// grep go.uber.org/zap` -> 0). Every host that embeds the SDK would acquire that
// dependency to gain ten forwarding methods.
type slogLogger struct{ l *slog.Logger }

// newSlogLogger wraps l. A nil logger falls back to slog.Default() rather than
// returning nil, because the caller passes the result straight into an option
// whose whole purpose is to make dropped work visible — handing it a nil would
// silently restore the behaviour the option exists to fix.
func newSlogLogger(l *slog.Logger) slogLogger {
	if l == nil {
		l = slog.Default()
	}
	return slogLogger{l: l}
}

func (a slogLogger) Debug(msg string, args ...any) { a.l.Debug(msg, args...) }
func (a slogLogger) Info(msg string, args ...any)  { a.l.Info(msg, args...) }
func (a slogLogger) Warn(msg string, args ...any)  { a.l.Warn(msg, args...) }
func (a slogLogger) Error(msg string, args ...any) { a.l.Error(msg, args...) }

func (a slogLogger) Debugf(format string, args ...any) { a.l.Debug(fmt.Sprintf(format, args...)) }
func (a slogLogger) Infof(format string, args ...any)  { a.l.Info(fmt.Sprintf(format, args...)) }
func (a slogLogger) Warnf(format string, args ...any)  { a.l.Warn(fmt.Sprintf(format, args...)) }
func (a slogLogger) Errorf(format string, args ...any) { a.l.Error(fmt.Sprintf(format, args...)) }

// Panic and Panicf log at Error and RETURN — they do not panic. slog has no
// panic level, and this adapter's only consumers are the embedded queues in the
// per-group-attempt and per-map-item backends: killing the runner process
// because one item's task was undeliverable would turn one stuck item into a
// stopped runner. This matches the timeout monitor's slogAdapter, and differs
// from observability/logger's ZapLogger, whose Panic does panic because zap's
// does.
func (a slogLogger) Panic(msg string, args ...any) { a.l.Error(msg, args...) }
func (a slogLogger) Panicf(format string, args ...any) {
	a.l.Error(fmt.Sprintf(format, args...))
}

// Compile-time proof the adapter satisfies the interface it exists for. Without
// it, a method added to engine.Logger would surface as a failure at the call
// site in runner.go rather than here.
var _ engine.Logger = slogLogger{}
