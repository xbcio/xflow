package logger

import (
	"runtime"
	"testing"

	"github.com/xbcio/xflow/engine"
)

// NewZapLogger's doc comment states a contract — "A nil logger falls back to
// zap.NewNop()" — that nothing asserted on. Both tests in zap_test.go build
// their adapter from zap.New(observer core), so the nil branch is never taken;
// delete it and this package plus its one importer, cmd/server, stay green.
//
// The guard is defensive rather than load-bearing for cmd/server: buildLogger
// (main.go:600) returns early when zapCfg.Build() errors, so the only in-repo
// call site cannot pass nil. What makes it worth pinning anyway is that
// NewZapLogger is exported from a public module — the callers the fallback
// exists for are SDK embedders, who are exactly the callers no in-repo test
// stands in for. Removing it turns a documented no-op into a nil pointer
// dereference inside zap.(*Logger).check, i.e. the logging call itself crashes
// the process that was trying to report something.

// TestNewZapLoggerNilFallsBackToNop pins the nil branch through the only
// consequence it has: whether calling the adapter works at all.
func TestNewZapLoggerNilFallsBackToNop(t *testing.T) {
	// The fallback is only useful if the result is still usable as the
	// interface the engine takes, so bind it as one rather than as ZapLogger.
	var log engine.Logger = NewZapLogger(nil)

	t.Run("non-terminal levels are silent no-ops", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("logging through a nil-backed adapter panicked: %v; the "+
					"nil fallback is gone, so every log call dereferences a nil "+
					"*zap.Logger and takes down the process that was reporting", r)
			}
		}()
		log.Error("error", "k", "v")
		log.Warn("warn", "k", "v")
		log.Info("info", "k", "v")
		log.Debug("debug", "k", "v")
		log.Errorf("errorf %d", 1)
		log.Warnf("warnf %d", 2)
		log.Infof("infof %d", 3)
		log.Debugf("debugf %d", 4)
	})

	// Panic and Panicf are excluded from the sweep above because a nop core
	// still panics at PanicLevel — that is zap's documented terminal behavior,
	// not a symptom. "It panicked" therefore cannot distinguish the two
	// outcomes here, so these two sub-tests assert on *what* was panicked with:
	// zap panics with the message, a missing fallback panics with a
	// runtime.Error about a nil pointer.
	t.Run("Panic panics with zap's own terminal behavior, not a nil deref", func(t *testing.T) {
		assertPanicsWithMessage(t, "boom", func() { log.Panic("boom", "k", "v") })
	})

	t.Run("Panicf panics with zap's own terminal behavior, not a nil deref", func(t *testing.T) {
		assertPanicsWithMessage(t, "boomf 7", func() { log.Panicf("boomf %d", 7) })
	})
}

func assertPanicsWithMessage(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("did not panic; zap panics at PanicLevel even on a nop core")
		}
		if _, isRuntime := r.(runtime.Error); isRuntime {
			t.Fatalf("panicked with a runtime error (%v) rather than the logged "+
				"message: the nil fallback is gone, so the adapter dereferenced a "+
				"nil *zap.Logger before it ever reached zap's terminal hook", r)
		}
		if got, ok := r.(string); !ok || got != want {
			t.Fatalf("panicked with %#v, want the logged message %q", r, want)
		}
	}()
	fn()
}
