package logger

import (
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestZapLoggerImplementsEngineLogger(t *testing.T) {
	var _ engine.Logger = ZapLogger{}

	core, observed := observer.New(zapcore.DebugLevel)
	log := NewZapLogger(zap.New(core))

	log.Debug("debug message", "node", "a")
	log.Debugf("formatted %s", "debug")
	log.Info("info message", "node", "b")
	log.Infof("formatted %s", "info")
	log.Warn("warn message", "node", "c")
	log.Warnf("formatted %s", "warn")
	log.Error("error message", "node", "d")
	log.Errorf("formatted %s", "error")
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("Panic did not panic after logging")
			}
		}()
		log.Panic("panic message", "node", "e")
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("Panicf did not panic after logging")
			}
		}()
		log.Panicf("formatted %s", "panic")
	}()

	entries := observed.All()
	if len(entries) != 10 {
		t.Fatalf("entry count = %d, want 10", len(entries))
	}
	wantLevels := []zapcore.Level{
		zapcore.DebugLevel,
		zapcore.DebugLevel,
		zapcore.InfoLevel,
		zapcore.InfoLevel,
		zapcore.WarnLevel,
		zapcore.WarnLevel,
		zapcore.ErrorLevel,
		zapcore.ErrorLevel,
		zapcore.PanicLevel,
		zapcore.PanicLevel,
	}
	for i, level := range wantLevels {
		if entries[i].Level != level {
			t.Fatalf("entry[%d] level = %s, want %s", i, entries[i].Level, level)
		}
	}
	if got := entries[0].ContextMap()["node"]; got != "a" {
		t.Fatalf("debug node field = %v, want a", got)
	}
	if got := entries[1].Message; got != "formatted debug" {
		t.Fatalf("formatted message = %q, want formatted debug", got)
	}
	// The five non-formatted methods (Debug/Info/Warn/Error/Panic) and the four
	// formatted ones share one code shape, but only Debug's field and Debugf's
	// message were ever checked above. The level checks in wantLevels catch a
	// method that logs at the wrong level, but they do not catch one that logs
	// at the *right* level while silently dropping its own args: e.g. `func (l
	// ZapLogger) Info(msg string, args ...any) { l.logger.Info(msg) }` compiles,
	// keeps entry[2] at InfoLevel, and left every assertion in this test green
	// before these checks were added. Pin the "node" field for Info/Warn/Error
	// non-formatted calls and the interpolated message for their *f siblings, so
	// a dropped args parameter on any one of them fails here instead of only in
	// whatever call site happens to notice missing fields in production logs.
	if got := entries[2].ContextMap()["node"]; got != "b" {
		t.Fatalf("info node field = %v, want b", got)
	}
	if got := entries[3].Message; got != "formatted info" {
		t.Fatalf("formatted message = %q, want formatted info", got)
	}
	if got := entries[4].ContextMap()["node"]; got != "c" {
		t.Fatalf("warn node field = %v, want c", got)
	}
	if got := entries[5].Message; got != "formatted warn" {
		t.Fatalf("formatted message = %q, want formatted warn", got)
	}
	if got := entries[6].ContextMap()["node"]; got != "d" {
		t.Fatalf("error node field = %v, want d", got)
	}
	if got := entries[7].Message; got != "formatted error" {
		t.Fatalf("formatted message = %q, want formatted error", got)
	}
}

// TestZapFieldsErrorValueIsWrittenOnce pins the `continue` guard in
// zapFields: when a field's value is an error, it must be converted to a
// zap.NamedError field and NOT also fall through to the trailing
// zap.Any(key, value) append below the type switch. Real call sites pass
// exactly this shape — "err", err — e.g. engine/atomic.go:742,749 and
// engine/initial_outbox.go:116. Deleting the `continue` would append both a
// NamedError field AND an Any field under the same key "err", so every one
// of those log lines would carry the error twice; JSON output would then
// have a duplicate "err" key (log shippers keep only one, silently dropping
// whichever value they consider redundant) and log volume/cost double for
// every error-logging call site.
func TestZapFieldsErrorValueIsWrittenOnce(t *testing.T) {
	core, observed := observer.New(zapcore.DebugLevel)
	log := NewZapLogger(zap.New(core))

	boom := errors.New("boom")
	log.Error("commit failed", "err", boom)

	entries := observed.All()
	if len(entries) != 1 {
		t.Fatalf("entry count = %d, want 1", len(entries))
	}
	if got := len(entries[0].Context); got != 1 {
		t.Fatalf("field count for a single \"err\" arg pair = %d, want 1 "+
			"(the error must be written once, not once as NamedError and again as Any)", got)
	}
	field := entries[0].Context[0]
	if field.Key != "err" {
		t.Fatalf("field key = %q, want err", field.Key)
	}
	if field.Type != zapcore.ErrorType {
		t.Fatalf("field type = %v, want zapcore.ErrorType (zap.NamedError)", field.Type)
	}
	if field.Interface != boom {
		t.Fatalf("field error value = %v, want %v", field.Interface, boom)
	}
}
