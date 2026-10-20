package rstate

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// scriptArgCapture records the full argument vector of every EVAL/EVALSHA the
// client issues. Route-checked mode is selected by the PRESENCE of ARGV[14] and
// its value, so the only way to pin it is to inspect the command stream: an
// outcome assertion cannot distinguish a script that received allowCycles from
// one that ignored it, and a regression that silently dropped the execution
// status key would make the route guard read "execution missing" and reject
// everything — visible, but not attributable without the argument vector.
type scriptArgCapture struct {
	mu    sync.Mutex
	calls [][]string
}

func (h *scriptArgCapture) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *scriptArgCapture) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		name := strings.ToLower(cmd.Name())
		if name == "eval" || name == "evalsha" {
			args := make([]string, 0, len(cmd.Args()))
			for _, arg := range cmd.Args() {
				args = append(args, argString(arg))
			}
			h.mu.Lock()
			h.calls = append(h.calls, args)
			h.mu.Unlock()
		}
		return next(ctx, cmd)
	}
}

func (h *scriptArgCapture) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *scriptArgCapture) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = nil
}

// scriptCalls returns a copy of the recorded vectors.
func (h *scriptArgCapture) scriptCalls() [][]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([][]string, len(h.calls))
	for i := range h.calls {
		out[i] = append([]string(nil), h.calls[i]...)
	}
	return out
}

func argString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case int:
		return strconv.Itoa(x)
	default:
		return ""
	}
}

// acquireScriptArgs finds the acquire script's argument vector among the
// recorded calls: it is the one declaring exactly four KEYS and starting at
// nodeStatusKey. EVAL and EVALSHA share the same prefix layout
// (name, script-or-sha, numkeys, keys..., args...).
func acquireScriptArgs(t *testing.T, calls [][]string, wantStatusKey string) []string {
	t.Helper()
	for _, args := range calls {
		if len(args) < 3 || args[2] != "4" {
			continue
		}
		if args[6] != wantStatusKey {
			continue
		}
		return args
	}
	t.Fatalf("no EVAL/EVALSHA carrying four KEYS and the execution status key %q was issued; captured calls: %v",
		wantStatusKey, calls)
	return nil
}

// TestAcquireTaskLeaseCheckedScriptCarriesRouteInputs pins the wire contract of
// the route-checked acquire Lua:
//
//   - KEYS[4] is the execution status key (same {id} hash tag, so no CROSSSLOT);
//   - ARGV[14] is allowCycles, '0' for an acyclic graph and '1' for a cyclic one;
//   - the legacy AcquireTaskLease omits ARGV[14] entirely, which is what keeps
//     its script behavior (and the call sites that rely on it) unchanged.
func TestAcquireTaskLeaseCheckedScriptCarriesRouteInputs(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	hook := &scriptArgCapture{}
	rdb.AddHook(hook)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	newLease := func(id types.ExecutionID) *engine.TaskLease {
		return &engine.TaskLease{
			LeaseID:    "L-args",
			LeaseToken: "T-args",
			IssuedAt:   time.Now().UTC(),
			TTL:        time.Minute,
			Task: engine.Task{
				ExecutionID: id,
				NodeName:    "start",
				NodeIdx:     0,
				Type:        engine.TaskTypeNodeExec,
			},
		}
	}

	for _, tc := range []struct {
		name        string
		allowCycles bool
		wantArg     string
	}{
		{name: "AcyclicGraph", allowCycles: false, wantArg: "0"},
		{name: "CyclicGraph", allowCycles: true, wantArg: "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := types.ExecutionID("exec-route-args-" + tc.name)
			if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
				ID: id, Graph: statestoretest.ContractGraph(), Status: types.ExecutionStatusRunning,
			}); err != nil {
				t.Fatalf("CreateExecution: %v", err)
			}
			hook.reset()
			if _, err := state.AcquireTaskLeaseChecked(ctx, newLease(id), engine.LeaseAcquireOptions{AllowCycles: tc.allowCycles}); err != nil {
				t.Fatalf("AcquireTaskLeaseChecked: %v", err)
			}
			statusKey := execKey(namespace.Default, id, "status")
			args := acquireScriptArgs(t, hook.scriptCalls(), statusKey)
			// name, script-or-sha, numkeys, 4 keys, 14 args
			if want := 3 + 4 + 14; len(args) != want {
				t.Fatalf("script argument vector length = %d, want %d (4 KEYS + 14 ARGV): %v", len(args), want, args)
			}
			if got := args[6]; got != statusKey {
				t.Fatalf("KEYS[4] = %q, want the execution status key %q", got, statusKey)
			}
			if got := args[len(args)-1]; got != tc.wantArg {
				t.Fatalf("ARGV[14] = %q, want %q (allowCycles for a %s graph)", got, tc.wantArg, tc.name)
			}
		})
	}

	t.Run("LegacyAcquireOmitsRouteMode", func(t *testing.T) {
		id := types.ExecutionID("exec-route-args-legacy")
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: statestoretest.ContractGraph(), Status: types.ExecutionStatusRunning,
		}); err != nil {
			t.Fatalf("CreateExecution: %v", err)
		}
		hook.reset()
		if _, _, err := state.AcquireTaskLease(ctx, newLease(id)); err != nil {
			t.Fatalf("AcquireTaskLease: %v", err)
		}
		statusKey := execKey(namespace.Default, id, "status")
		args := acquireScriptArgs(t, hook.scriptCalls(), statusKey)
		// name, script-or-sha, numkeys, 4 keys, 13 legacy args: no ARGV[14], so
		// the script takes the legacy branch and skips every route guard.
		if want := 3 + 4 + 13; len(args) != want {
			t.Fatalf("legacy script argument vector length = %d, want %d (no allowCycles argument): %v",
				len(args), want, args)
		}
	})
}
