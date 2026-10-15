package distributed

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// warnRecordingLogger is an engine.Logger that records Warn messages only
// (recordingLogger in backend_shutdown_test.go records Error and drops Warn).
type warnRecordingLogger struct {
	mu    sync.Mutex
	warns []string
}

func (w *warnRecordingLogger) Debug(string, ...any)  {}
func (w *warnRecordingLogger) Debugf(string, ...any) {}
func (w *warnRecordingLogger) Info(string, ...any)   {}
func (w *warnRecordingLogger) Infof(string, ...any)  {}
func (w *warnRecordingLogger) Error(string, ...any)  {}
func (w *warnRecordingLogger) Errorf(string, ...any) {}
func (w *warnRecordingLogger) Panic(string, ...any)  {}
func (w *warnRecordingLogger) Panicf(string, ...any) {}
func (w *warnRecordingLogger) Warnf(string, ...any)  {}

func (w *warnRecordingLogger) Warn(msg string, _ ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.warns = append(w.warns, msg)
}

func (w *warnRecordingLogger) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.warns)
}

// Mutation note: dropping asynqtransport.WithQueueSuffix from New must turn
// the dedicated row red (the dedicated process would enqueue into the shared
// deployment's queue); applying the suffix to the default or unconfigured
// rows must turn those red (it would break rolling-upgrade queue sharing with
// v0.0.47 pods). The assertion is on the Redis keys the default transport
// really writes, not on an internal field.
func TestNewDerivesQueueNamesFromOwnedSet(t *testing.T) {
	cases := []struct {
		name      string
		owned     []namespace.Namespace
		wantQueue string
		absent    string
	}{
		{"unconfigured", nil, "xflow:default", "xflow:default:ns:local-jack"},
		{"default", []namespace.Namespace{namespace.Default}, "xflow:default", "xflow:default:ns:local-jack"},
		{"dedicated", []namespace.Namespace{"local-jack"}, "xflow:default:ns:local-jack", "xflow:default"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mr, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			defer mr.Close()

			b := newOwnedTestBackend(t, mr.Addr(), tc.owned)
			t.Cleanup(func() { _ = b.transport.Close() })

			if err := b.transport.Enqueue(context.Background(), &engine.Task{
				ExecutionID: types.ExecutionID("e1"), NodeName: "n", Type: engine.TaskTypeNodeExec,
			}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			if n, _ := mr.List("asynq:{" + tc.wantQueue + "}:pending"); len(n) != 1 {
				t.Fatalf("asynq:{%s}:pending holds %d tasks, want 1", tc.wantQueue, len(n))
			}
			if n, _ := mr.List("asynq:{" + tc.absent + "}:pending"); len(n) != 0 {
				t.Fatalf("asynq:{%s}:pending holds %d tasks, want 0", tc.absent, len(n))
			}
		})
	}
}

// Mutation note: moving the owned-set validation back below the Redis ping and
// transport construction must turn this red — against an unreachable address
// the error would become "redis ping" instead of the ownership error, and a
// reachable one would leave a client and asynq producer open on the error
// path. Validation runs first so a bad set costs no connection.
func TestNewValidatesOwnedSetBeforeConnecting(t *testing.T) {
	// Nothing listens here; any attempt to connect would surface as a ping error.
	const unreachable = "127.0.0.1:1"
	for _, owned := range [][]namespace.Namespace{
		{"a", "b"},
		{"a:b"},
	} {
		_, err := New(unreachable, nil, WithOwnedNamespaces(owned))
		if err == nil {
			t.Fatalf("New(owned=%v) = nil error, want rejection", owned)
		}
		if !strings.Contains(err.Error(), "owned namespaces") {
			t.Fatalf("New(owned=%v) error = %q, want the ownership error (validation must precede any connection)", owned, err)
		}
		if strings.Contains(err.Error(), "redis ping") {
			t.Fatalf("New(owned=%v) tried to connect before validating: %v", owned, err)
		}
	}
}

// Mutation note: dropping the warning must turn the dedicated row red; warning
// for default/unconfigured processes must turn the other rows red (an injected
// transport is the normal embedding setup there and must stay quiet).
func TestInjectedTransportWarnsOnlyWhenQueueSeparationIsBypassed(t *testing.T) {
	cases := []struct {
		name      string
		owned     []namespace.Namespace
		wantWarns int
	}{
		{"unconfigured", nil, 0},
		{"default", []namespace.Namespace{namespace.Default}, 0},
		{"dedicated", []namespace.Namespace{"local-jack"}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mr, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			defer mr.Close()

			log := &warnRecordingLogger{}
			opts := []Option{WithTransport(&closeErrTransport{}), WithStateLogger(log)}
			if tc.owned != nil {
				opts = append(opts, WithOwnedNamespaces(tc.owned))
			}
			b, err := New(mr.Addr(), nil, opts...)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = b.rdb.Close() })

			if got := log.count(); got != tc.wantWarns {
				t.Fatalf("warnings = %d (%v), want %d", got, log.warns, tc.wantWarns)
			}
		})
	}
}
