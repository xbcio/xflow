package xbcinfra

import (
	"context"
	"log/slog"
	"reflect"
	"testing"
	"time"

	xbclog "github.com/xbcio/xbc/log"
)

// redactedValue proves the bridge resolves slog.LogValuer values before they
// reach the backend, so a type that hides its own contents is not flattened to
// its raw fields.
type redactedValue struct{}

func (redactedValue) LogValue() slog.Value { return slog.StringValue("[redacted]") }

func TestSlogBridgeMapsLevelsOntoTheFacade(t *testing.T) {
	backend := &recordingLogger{min: xbclog.DebugLevel}
	bridge := newSlogBridge(backend)
	ctx := context.Background()

	cases := []struct {
		level slog.Level
		want  xbclog.Level
	}{
		{level: slog.LevelDebug - 4, want: xbclog.DebugLevel},
		{level: slog.LevelInfo, want: xbclog.InfoLevel},
		{level: slog.LevelInfo + 1, want: xbclog.InfoLevel},
		{level: slog.LevelWarn, want: xbclog.WarnLevel},
		{level: slog.LevelError, want: xbclog.ErrorLevel},
		{level: slog.LevelError + 8, want: xbclog.ErrorLevel},
	}
	for _, tc := range cases {
		record := slog.NewRecord(time.Time{}, tc.level, "msg", 0)
		if err := bridge.Handle(ctx, record); err != nil {
			t.Fatalf("Handle(%v) = %v", tc.level, err)
		}
	}
	if len(backend.entries) != len(cases) {
		t.Fatalf("recorded %d entries, want %d: %#v", len(backend.entries), len(cases), backend.entries)
	}
	for i, tc := range cases {
		if got := backend.entries[i].level; got != tc.want {
			t.Fatalf("entry %d level = %v, want %v", i, got, tc.want)
		}
	}
}

func TestSlogBridgeFiltersAtTheBackendsLevel(t *testing.T) {
	backend := &recordingLogger{min: xbclog.WarnLevel}
	bridge := newSlogBridge(backend)
	ctx := context.Background()

	if bridge.Enabled(ctx, slog.LevelInfo) {
		t.Fatal("bridge enables info below the backend's warn minimum")
	}
	if !bridge.Enabled(ctx, slog.LevelWarn) || !bridge.Enabled(ctx, slog.LevelError) {
		t.Fatal("bridge disables levels the backend accepts")
	}

	for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn} {
		record := slog.NewRecord(time.Time{}, level, "msg", 0)
		if err := bridge.Handle(ctx, record); err != nil {
			t.Fatalf("Handle(%v) = %v", level, err)
		}
	}
	if len(backend.entries) != 1 || backend.entries[0].level != xbclog.WarnLevel {
		t.Fatalf("recorded %#v, want only the warn entry", backend.entries)
	}
}

func TestSlogBridgeFlattensAttrsGroupsAndLogValues(t *testing.T) {
	backend := &recordingLogger{min: xbclog.DebugLevel}
	// WithAttrs before WithGroup: the bound attribute keeps the prefix that was
	// active when it was attached, which is slog's own ordering rule.
	bridge := newSlogBridge(backend).
		WithAttrs([]slog.Attr{slog.String("service", "api")}).
		WithGroup("http")

	record := slog.NewRecord(time.Time{}, slog.LevelInfo, "request", 0)
	record.AddAttrs(
		slog.String("path", "/x"),
		slog.Group("request", slog.String("id", "r1")),
		slog.Any("secret", redactedValue{}),
	)
	if err := bridge.Handle(context.Background(), record); err != nil {
		t.Fatalf("Handle() = %v", err)
	}
	if len(backend.entries) != 1 {
		t.Fatalf("recorded %d entries, want 1", len(backend.entries))
	}
	want := []any{
		"service", "api",
		"http.path", "/x",
		"http.request.id", "r1",
		"http.secret", "[redacted]",
	}
	if got := backend.entries[0].kv; !reflect.DeepEqual(got, want) {
		t.Fatalf("kv = %#v, want %#v", got, want)
	}
}

func TestSlogBridgeUsesTheLoggerBoundToTheContext(t *testing.T) {
	handlerBackend := &recordingLogger{min: xbclog.DebugLevel}
	bound := &recordingLogger{min: xbclog.DebugLevel}
	bridge := newSlogBridge(handlerBackend)

	ctx := xbclog.NewContext(context.Background(), bound)
	record := slog.NewRecord(time.Time{}, slog.LevelInfo, "request", 0)
	if err := bridge.Handle(ctx, record); err != nil {
		t.Fatalf("Handle() = %v", err)
	}

	if len(handlerBackend.entries) != 0 {
		t.Fatalf("the handler's own logger recorded %#v, want nothing", handlerBackend.entries)
	}
	if len(bound.entries) != 1 || bound.entries[0].msg != "request" {
		t.Fatalf("context logger recorded %#v, want the request entry", bound.entries)
	}
}
