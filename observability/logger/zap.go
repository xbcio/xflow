package logger

import (
	"fmt"

	"go.uber.org/zap"
)

// ZapLogger adapts a zap logger to engine.Logger.
type ZapLogger struct {
	logger *zap.Logger
}

// NewZapLogger returns a zap-backed structured logger adapter. A nil logger
// falls back to zap.NewNop().
func NewZapLogger(logger *zap.Logger) ZapLogger {
	if logger == nil {
		logger = zap.NewNop()
	}
	return ZapLogger{logger: logger}
}

func (l ZapLogger) Error(msg string, args ...any) { l.logger.Error(msg, zapFields(args...)...) }
func (l ZapLogger) Warn(msg string, args ...any)  { l.logger.Warn(msg, zapFields(args...)...) }
func (l ZapLogger) Info(msg string, args ...any)  { l.logger.Info(msg, zapFields(args...)...) }
func (l ZapLogger) Debug(msg string, args ...any) { l.logger.Debug(msg, zapFields(args...)...) }
func (l ZapLogger) Panic(msg string, args ...any) { l.logger.Panic(msg, zapFields(args...)...) }
func (l ZapLogger) Debugf(format string, args ...any) {
	l.logger.Sugar().Debugf(format, args...)
}
func (l ZapLogger) Infof(format string, args ...any) {
	l.logger.Sugar().Infof(format, args...)
}
func (l ZapLogger) Warnf(format string, args ...any) {
	l.logger.Sugar().Warnf(format, args...)
}
func (l ZapLogger) Errorf(format string, args ...any) {
	l.logger.Sugar().Errorf(format, args...)
}
func (l ZapLogger) Panicf(format string, args ...any) {
	l.logger.Sugar().Panicf(format, args...)
}

func zapFields(args ...any) []zap.Field {
	if len(args) == 0 {
		return nil
	}
	fields := make([]zap.Field, 0, (len(args)+1)/2)
	for i := 0; i < len(args); i += 2 {
		key := fmt.Sprint(args[i])
		var value any
		if i+1 < len(args) {
			value = args[i+1]
		}
		if err, ok := value.(error); ok {
			fields = append(fields, zap.NamedError(key, err))
			continue
		}
		fields = append(fields, zap.Any(key, value))
	}
	return fields
}
