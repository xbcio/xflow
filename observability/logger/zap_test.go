package logger

import (
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
