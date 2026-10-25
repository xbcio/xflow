package xbcinfra

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	xbclog "github.com/xbcio/xbc/log"
)

type loggedEntry struct {
	level xbclog.Level
	msg   string
	kv    []any
}

type recordingLogger struct {
	entries []loggedEntry
	min     xbclog.Level
}

func (l *recordingLogger) log(level xbclog.Level, msg string, kv ...any) {
	if !l.Enabled(level) {
		return
	}
	l.entries = append(l.entries, loggedEntry{level: level, msg: msg, kv: append([]any(nil), kv...)})
}

func (l *recordingLogger) Debug(msg string, kv ...any) { l.log(xbclog.DebugLevel, msg, kv...) }
func (l *recordingLogger) Info(msg string, kv ...any)  { l.log(xbclog.InfoLevel, msg, kv...) }
func (l *recordingLogger) Warn(msg string, kv ...any)  { l.log(xbclog.WarnLevel, msg, kv...) }
func (l *recordingLogger) Error(msg string, kv ...any) { l.log(xbclog.ErrorLevel, msg, kv...) }
func (l *recordingLogger) Fatal(msg string, kv ...any) { l.log(xbclog.ErrorLevel, msg, kv...) }
func (l *recordingLogger) With(_ ...any) xbclog.Logger { return l }
func (l *recordingLogger) Enabled(level xbclog.Level) bool {
	return level >= l.min
}

func TestEngineLoggerDelegatesEveryMethod(t *testing.T) {
	backend := &recordingLogger{min: xbclog.DebugLevel}
	logger := EngineLogger(backend)

	logger.Debug("debug", "k", "v")
	logger.Debugf("debugf %d", 1)
	logger.Info("info")
	logger.Infof("infof %s", "x")
	logger.Warn("warn")
	logger.Warnf("warnf %t", true)
	logger.Error("error")
	logger.Errorf("errorf %v", "e")

	want := []loggedEntry{
		{level: xbclog.DebugLevel, msg: "debug", kv: []any{"k", "v"}},
		{level: xbclog.DebugLevel, msg: "debugf 1"},
		{level: xbclog.InfoLevel, msg: "info"},
		{level: xbclog.InfoLevel, msg: "infof x"},
		{level: xbclog.WarnLevel, msg: "warn"},
		{level: xbclog.WarnLevel, msg: "warnf true"},
		{level: xbclog.ErrorLevel, msg: "error"},
		{level: xbclog.ErrorLevel, msg: "errorf e"},
	}
	if len(backend.entries) != len(want) {
		t.Fatalf("recorded %d entries, want %d: %#v", len(backend.entries), len(want), backend.entries)
	}
	for i := range want {
		if backend.entries[i].level != want[i].level || backend.entries[i].msg != want[i].msg {
			t.Fatalf("entry %d = %#v, want %#v", i, backend.entries[i], want[i])
		}
		if len(want[i].kv) == 0 {
			if len(backend.entries[i].kv) != 0 {
				t.Fatalf("entry %d kv = %#v, want none", i, backend.entries[i].kv)
			}
			continue
		}
		if len(backend.entries[i].kv) != 2 || backend.entries[i].kv[0] != want[i].kv[0] || backend.entries[i].kv[1] != want[i].kv[1] {
			t.Fatalf("entry %d kv = %#v, want %#v", i, backend.entries[i].kv, want[i].kv)
		}
	}
}

func TestEngineLoggerPanicRecordsThenPanics(t *testing.T) {
	backend := &recordingLogger{min: xbclog.DebugLevel}
	logger := EngineLogger(backend)

	func() {
		defer func() {
			if recovered := recover(); recovered != "boom" {
				t.Fatalf("recovered %v, want boom", recovered)
			}
		}()
		logger.Panic("boom", "k", "v")
	}()

	if len(backend.entries) != 1 {
		t.Fatalf("recorded %d entries, want 1", len(backend.entries))
	}
	if backend.entries[0].msg != "boom" || backend.entries[0].level != xbclog.ErrorLevel {
		t.Fatalf("entry = %#v, want an error entry for boom", backend.entries[0])
	}

	func() {
		defer func() {
			if recovered := recover(); recovered != "formatted 7" {
				t.Fatalf("recovered %v, want formatted 7", recovered)
			}
		}()
		logger.Panicf("formatted %d", 7)
	}()
	if len(backend.entries) != 2 || backend.entries[1].msg != "formatted 7" {
		t.Fatalf("entries = %#v, want the formatted panic recorded", backend.entries)
	}
}

func TestEngineLoggerNilBackendIsNop(t *testing.T) {
	logger := EngineLogger(nil)
	logger.Info("discarded", "k", "v")
	logger.Warnf("discarded %d", 1)
}

func TestNewLoggerInstallsTheFacadeLevel(t *testing.T) {
	restoreLogger(t)

	cfg := xbclog.DefaultConfig()
	cfg.Level = "error"
	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger() = %v", err)
	}
	if logger == nil {
		t.Fatal("NewLogger() returned a nil engine.Logger")
	}
	if !xbclog.L().Enabled(xbclog.ErrorLevel) {
		t.Fatal("the installed facade does not enable error")
	}
	if xbclog.L().Enabled(xbclog.DebugLevel) {
		t.Fatal("the installed facade enables debug below its error level")
	}
}

// TestCloseLogFlushesTheConfiguredSink guards the composition roots' deferred
// close: a file sink must receive records written before exit, and a second
// close must stay harmless.
func TestCloseLogFlushesTheConfiguredSink(t *testing.T) {
	restoreLogger(t)

	path := filepath.Join(t.TempDir(), "xflow.jsonl")
	cfg := xbclog.DefaultConfig()
	cfg.Console.Enabled = false
	cfg.File.Enabled = true
	cfg.File.Path = path
	cfg.File.Format = xbclog.FormatJSON
	if err := xbclog.Init(cfg); err != nil {
		t.Fatalf("Init() = %v", err)
	}
	xbclog.L().Info("runner started", "component", "xbcinfra")

	if err := CloseLog(); err != nil {
		t.Fatalf("CloseLog() = %v", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(content), "runner started") {
		t.Fatalf("file sink did not receive the record before close: %q", content)
	}
	if err := CloseLog(); err != nil {
		t.Fatalf("second CloseLog() = %v, want nil", err)
	}
}

// restoreLogger puts the previous xbc facade back when the test finishes. The
// facade is process-wide by design, so a test that initializes it must not leak
// that state into another test.
func restoreLogger(t *testing.T) {
	t.Helper()
	previous := xbclog.L()
	t.Cleanup(func() { xbclog.SetLogger(previous) })
}
