package xbcinfra

import (
	"fmt"
	"log/slog"

	xbclog "github.com/xbcio/xbc/log"

	"github.com/xbcio/xflow/engine"
)

// NewLogger initializes xbc's process-wide log facade from cfg and returns the
// engine.Logger that writes through it. Initialization is eager: an invalid
// level, format, or file setting fails startup here rather than dropping
// records later.
//
// The facade is process-wide by design, so a binary calls this once at its
// composition root. The returned adapter carries the same backend, which means
// masking, rotation, and level filtering apply to xflow's structured records
// exactly as they do to records written by xbc components.
func NewLogger(cfg xbclog.Config) (engine.Logger, error) {
	if err := xbclog.Init(cfg); err != nil {
		return nil, fmt.Errorf("xbcinfra: initialize log: %w", err)
	}
	return EngineLogger(xbclog.L()), nil
}

// InstallSlog configures xbc's log facade from cfg and installs a slog default
// that writes through it. It is the runner's entry point: sdk/runner uses the
// process-wide slog logger throughout, so setting the default once here routes
// every runner record through xbc without changing the SDK. The bridge is
// host-side by design -- xbc deliberately ships no slog package -- and lives in
// this package beside the composition root that installs it.
func InstallSlog(cfg xbclog.Config) error {
	if err := xbclog.Init(cfg); err != nil {
		return fmt.Errorf("xbcinfra: initialize log: %w", err)
	}
	slog.SetDefault(slog.New(newSlogBridge(xbclog.L())))
	return nil
}

// CloseLog flushes and releases the sinks xbc's log facade opened, including
// rotated files. A binary that installed the facade calls it once on the way
// out; it is a no-op when this process never initialized the facade.
func CloseLog() error {
	return xbclog.Close()
}

// EngineLogger adapts an xbc logger to engine.Logger. A nil logger becomes
// xbc's Nop backend.
//
// The adapter adds one stack frame between xflow and the facade, so a backend
// that offers xbc's CallerSkipper capability is asked to skip it; without that
// the caller recorded for every xflow entry would be this file's forwarding
// method instead of the code that logged.
func EngineLogger(logger xbclog.Logger) engine.Logger {
	if logger == nil {
		logger = xbclog.Nop()
	}
	if skipper, ok := logger.(xbclog.CallerSkipper); ok {
		logger = skipper.WithCallerSkip(1)
	}
	return engineLogger{backend: logger}
}

type engineLogger struct {
	backend xbclog.Logger
}

func (l engineLogger) Debug(msg string, args ...any) { l.backend.Debug(msg, args...) }
func (l engineLogger) Info(msg string, args ...any)  { l.backend.Info(msg, args...) }
func (l engineLogger) Warn(msg string, args ...any)  { l.backend.Warn(msg, args...) }
func (l engineLogger) Error(msg string, args ...any) { l.backend.Error(msg, args...) }

func (l engineLogger) Debugf(format string, args ...any) {
	l.backend.Debug(fmt.Sprintf(format, args...))
}

func (l engineLogger) Infof(format string, args ...any) {
	l.backend.Info(fmt.Sprintf(format, args...))
}

func (l engineLogger) Warnf(format string, args ...any) {
	l.backend.Warn(fmt.Sprintf(format, args...))
}

func (l engineLogger) Errorf(format string, args ...any) {
	l.backend.Error(fmt.Sprintf(format, args...))
}

// Panic records the entry at Error level and then panics with the message.
// xbc's facade deliberately has no Panic level; the panic itself is the
// contract engine.Logger callers rely on, and recording before it keeps the
// reason visible.
func (l engineLogger) Panic(msg string, args ...any) {
	l.backend.Error(msg, args...)
	panic(msg)
}

func (l engineLogger) Panicf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.backend.Error(msg)
	panic(msg)
}
