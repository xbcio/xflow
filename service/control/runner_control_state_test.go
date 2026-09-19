package control

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/service/protocol"
)

// keyCommandHook counts a chosen Redis command against a fixed set of keys. It
// is how a test pins which command reaches a key rather than only how many reads
// happen, which matters for the fleet-wide handoff and deactivation ledgers:
// those hashes are keyed by claim/obligation across every runner, so reading one
// on a per-poll path costs O(fleet debt) for fields the poll caller does not use.
type keyCommandHook struct {
	mu      sync.Mutex
	command string
	keys    map[string]struct{}
	n       int
}

func newLedgerHGetAllHook(keys ...string) *keyCommandHook {
	return newKeyCommandHook("hgetall", keys...)
}

// newKeyCommandHook counts one named command against the given keys.
func newKeyCommandHook(command string, keys ...string) *keyCommandHook {
	set := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		set[key] = struct{}{}
	}
	return &keyCommandHook{command: command, keys: set}
}

func (h *keyCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *keyCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.observe(cmd)
		return next(ctx, cmd)
	}
}

func (h *keyCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.observe(cmd)
		}
		return next(ctx, cmds)
	}
}

func (h *keyCommandHook) observe(cmd redis.Cmder) {
	if strings.ToLower(cmd.Name()) != h.command {
		return
	}
	args := cmd.Args()
	if len(args) < 2 {
		return
	}
	key, ok := args[1].(string)
	if !ok {
		return
	}
	if _, tracked := h.keys[key]; !tracked {
		return
	}
	h.mu.Lock()
	h.n++
	h.mu.Unlock()
}

func (h *keyCommandHook) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

func (h *keyCommandHook) reset() {
	h.mu.Lock()
	h.n = 0
	h.mu.Unlock()
}

func ledgerKeys(directory *RedisRunnerDirectory) []string {
	return []string{
		directory.keys.handoffRunner,
		directory.keys.handoffState,
		directory.keys.deactivationObligationRunner,
		directory.keys.deactivationObligationState,
	}
}

func drainRunnerControlRequest(runnerID, requestID string) RunnerControlRequest {
	return RunnerControlRequest{
		RunnerID:     runnerID,
		DesiredState: RunnerDesiredStateDraining,
		Actor:        "operator-a",
		Action:       "drain",
		Reason:       "maintenance",
		RequestID:    requestID,
		RequestHash:  requestID + "-hash",
		Now:          time.Now().UTC(),
	}
}

// TestRunnerControlDirectiveSkipsLedgerAggregation pins the hot path: the poll
// and register directive consumes only desired state and generation, so it must
// not read the fleet-wide handoff/deactivation ledgers. The counter-check at the
// end proves the hook is live -- the full management projection still aggregates
// those same ledgers -- so a zero here means "not read by this path", not "hook
// never fires".
func TestRunnerControlDirectiveSkipsLedgerAggregation(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	hook := newLedgerHGetAllHook(ledgerKeys(directory)...)
	rdb.AddHook(hook)

	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-state", 4)
	core := &Core{runners: directory}

	directive := core.runnerControlDirective(ctx, "runner-state")
	if directive == nil || directive.DesiredState != string(RunnerDesiredStateActive) || directive.Generation != 0 {
		t.Fatalf("directive for a fresh runner = %+v, want ACTIVE generation 0", directive)
	}
	if n := hook.count(); n != 0 {
		t.Fatalf("ledger HGetAll reads for the poll directive = %d, want 0", n)
	}

	drain, err := directory.SetRunnerControl(ctx, drainRunnerControlRequest("runner-state", "req-drain-1"))
	if err != nil {
		t.Fatalf("SetRunnerControl() error = %v", err)
	}
	if err := directory.Heartbeat(ctx, HeartbeatRequest{
		RunnerID:  "runner-state",
		SessionID: session.SessionID,
		Capacity:  4,
		Now:       time.Unix(11, 0),
	}); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}

	hook.reset()
	directive = core.runnerControlDirective(ctx, "runner-state")
	if directive == nil || directive.DesiredState != string(RunnerDesiredStateDraining) {
		t.Fatalf("directive after drain = %+v, want DRAINING", directive)
	}
	if directive.Generation != drain.Generation {
		t.Fatalf("directive generation = %d, want %d", directive.Generation, drain.Generation)
	}
	if !directive.RecoveryOnly {
		t.Fatalf("directive RecoveryOnly = false for DRAINING, want true")
	}
	if n := hook.count(); n != 0 {
		t.Fatalf("ledger HGetAll reads for the draining poll directive = %d, want 0", n)
	}

	// Counter-check: the full projection must still aggregate the ledgers, so a
	// zero above is the lightweight path, not a dead hook.
	if _, found, err := directory.RunnerControl(ctx, "runner-state"); err != nil || !found {
		t.Fatalf("RunnerControl() found=%v err=%v, want projection", found, err)
	}
	if n := hook.count(); n == 0 {
		t.Fatal("ledger HGetAll reads for the full projection = 0, want > 0 (hook is not observing)")
	}
}

// TestRunnerControlStateMatchesFullProjection ensures the lightweight read does
// not diverge from the full projection on the two fields it reports, for a
// directory whose control state is defaulted, explicitly set, and unregistered.
func TestRunnerControlStateMatchesFullProjection(t *testing.T) {
	ctx := context.Background()

	t.Run("memory", func(t *testing.T) {
		directory := NewMemoryRunnerDirectory()
		session, err := directory.Register(ctx, RegisterRunnerRequest{
			RunnerID:     "runner-state",
			Capacity:     2,
			Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
			Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
			Now:          time.Unix(10, 0),
		})
		if err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		_ = session

		assertStateMatchesProjection(t, ctx, directory, "runner-state")

		if _, err := directory.SetRunnerControl(ctx, drainRunnerControlRequest("runner-state", "req-drain-mem")); err != nil {
			t.Fatalf("SetRunnerControl() error = %v", err)
		}
		assertStateMatchesProjection(t, ctx, directory, "runner-state")
	})

	t.Run("redis", func(t *testing.T) {
		_, rdb := newRedisRunnerDirectoryTestClient(t)
		directory := NewRedisRunnerDirectory(rdb)
		registerRedisDirectoryRunner(t, ctx, directory, "runner-state", 2)

		assertStateMatchesProjection(t, ctx, directory, "runner-state")

		if _, err := directory.SetRunnerControl(ctx, drainRunnerControlRequest("runner-state", "req-drain-redis")); err != nil {
			t.Fatalf("SetRunnerControl() error = %v", err)
		}
		assertStateMatchesProjection(t, ctx, directory, "runner-state")
	})

	t.Run("unknown runner", func(t *testing.T) {
		directory := NewMemoryRunnerDirectory()
		state, found, err := directory.RunnerControlState(ctx, "missing")
		if err != nil || found {
			t.Fatalf("RunnerControlState(missing) state=%+v found=%v err=%v, want not found", state, found, err)
		}
	})
}

func assertStateMatchesProjection(t *testing.T, ctx context.Context, directory RunnerControlStateDirectory, runnerID string) {
	t.Helper()

	full, found, err := directory.(RunnerControlDirectory).RunnerControl(ctx, runnerID)
	if err != nil || !found {
		t.Fatalf("RunnerControl() found=%v err=%v, want projection", found, err)
	}
	state, stateFound, err := directory.RunnerControlState(ctx, runnerID)
	if err != nil || !stateFound {
		t.Fatalf("RunnerControlState() found=%v err=%v, want projection", stateFound, err)
	}
	if state.DesiredState != full.DesiredState || state.Generation != full.Generation {
		t.Fatalf("RunnerControlState() = %+v, full projection desired=%q generation=%d",
			state, full.DesiredState, full.Generation)
	}
}
