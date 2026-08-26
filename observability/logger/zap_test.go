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
