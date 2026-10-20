package control

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

// scriptCallCounter counts the EVAL/EVALSHA commands one client issued. The
// script cache lives server-side, so watching the wire is the only way to
// observe redis.Script.Run's "EVALSHA first, EVAL on NOSCRIPT" contract from
// outside go-redis.
type scriptCallCounter struct {
	mu      sync.Mutex
	eval    int
	evalsha int
}

func (c *scriptCallCounter) record(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch name {
	case "eval":
		c.eval++
	case "evalsha":
		c.evalsha++
	}
}

func (c *scriptCallCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eval, c.evalsha = 0, 0
}

func (c *scriptCallCounter) counts() (eval, evalsha int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.eval, c.evalsha
}

type scriptCallCountingHook struct{ counter *scriptCallCounter }

func (h scriptCallCountingHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h scriptCallCountingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.counter.record(cmd.Name())
		return next(ctx, cmd)
	}
}

func (h scriptCallCountingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// newScriptCountingRedisRunnerDirectory builds a miniredis-backed directory
// whose client reports every EVAL/EVALSHA it sends. No real Redis is involved.
func newScriptCountingRedisRunnerDirectory(t *testing.T) (*miniredis.Miniredis, *redis.Client, *RedisRunnerDirectory, *scriptCallCounter) {
	t.Helper()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(server.Close)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	counter := &scriptCallCounter{}
	rdb.AddHook(scriptCallCountingHook{counter: counter})
	return server, rdb, NewRedisRunnerDirectory(rdb), counter
}

// TestRedisRunnerDirectoryScriptRunFallsBackOncePerColdCache pins the
// redis.Script.Run contract on a representative transition: the first
// execution after the server-side script cache is empty sends EVALSHA and then
// EVAL (the NOSCRIPT fallback), the next one is a bare EVALSHA, and a
// SCRIPT FLUSH puts it back on the fallback path without changing the result.
func TestRedisRunnerDirectoryScriptRunFallsBackOncePerColdCache(t *testing.T) {
	ctx := context.Background()
	server, rdb, directory, counter := newScriptCountingRedisRunnerDirectory(t)
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-script-heartbeat", 1)

	heartbeat := func(t *testing.T, at time.Time) {
		t.Helper()
		if err := directory.Heartbeat(ctx, HeartbeatRequest{
			RunnerID:  session.RunnerID,
			SessionID: session.SessionID,
			Capacity:  1,
			Now:       at,
		}); err != nil {
			t.Fatalf("Heartbeat() error = %v", err)
		}
	}

	counter.reset()
	heartbeat(t, time.Unix(20, 0))
	if eval, evalsha := counter.counts(); eval != 1 || evalsha != 1 {
		t.Fatalf("cold heartbeat sent eval=%d evalsha=%d, want 1 and 1 (NOSCRIPT fallback)", eval, evalsha)
	}

	counter.reset()
	heartbeat(t, time.Unix(21, 0))
	if eval, evalsha := counter.counts(); eval != 0 || evalsha != 1 {
		t.Fatalf("warm heartbeat sent eval=%d evalsha=%d, want 0 and 1 (script already cached)", eval, evalsha)
	}
	if got := server.HGet(directory.keys.runnerHeartbeat, session.RunnerID); got != "21000" {
		t.Fatalf("runner heartbeat after warm call = %q, want %q", got, "21000")
	}

	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("ScriptFlush() error = %v", err)
	}
	counter.reset()
	heartbeat(t, time.Unix(22, 0))
	if eval, evalsha := counter.counts(); eval != 1 || evalsha != 1 {
		t.Fatalf("post-flush heartbeat sent eval=%d evalsha=%d, want 1 and 1 (fresh NOSCRIPT fallback)", eval, evalsha)
	}
	if got := server.HGet(directory.keys.runnerHeartbeat, session.RunnerID); got != "22000" {
		t.Fatalf("runner heartbeat after flush = %q, want %q", got, "22000")
	}
}

// TestRedisRunnerDirectoryScriptTransitionsUseEvalSha is the table covering
// every non-pipelined transition family converted to redis.Script. Each case
// runs twice in a fresh directory: the first run starts on a flushed script
// cache and must issue one EVALSHA plus one EVAL per internal transition; the
// second run must issue EVALSHA only. The counts are exact because everything
// a case does before its measured operation (registration, enqueue, claim,
// finalize) is deliberately outside the counting window.
func TestRedisRunnerDirectoryScriptTransitionsUseEvalSha(t *testing.T) {
	type scriptPathCase struct {
		name string
		// scripts is how many Lua transitions one measured run executes.
		scripts int
		// setup prepares whatever state the operation needs in a fresh
		// directory (attempt distinguishes the two runs) and returns the
		// operation whose script calls are counted.
		setup func(t *testing.T, ctx context.Context, rdb *redis.Client, directory *RedisRunnerDirectory, attempt int) func() error
	}

	finalizeLease := func(attempt int, assignment Assignment) *engine.TaskLease {
		return &engine.TaskLease{
			LeaseID:    engine.LeaseID(fmt.Sprintf("lease-script-%d", attempt)),
			LeaseToken: engine.LeaseToken(fmt.Sprintf("token-script-%d", attempt)),
			Task:       assignment.Task,
			NodeType:   assignment.Routing.NodeType,
			IssuedAt:   time.Unix(31, 0),
			TTL:        time.Minute,
		}
	}

	cases := []scriptPathCase{
		{
			name: "register",
			// ReclaimExpiredClaims runs first in Register, then the register
			// transition itself.
			scripts: 2,
			setup: func(t *testing.T, ctx context.Context, _ *redis.Client, directory *RedisRunnerDirectory, attempt int) func() error {
				runnerID := fmt.Sprintf("runner-script-register-%d", attempt)
				return func() error {
					_, err := directory.Register(ctx, RegisterRunnerRequest{
						RunnerID:     runnerID,
						Capacity:     2,
						Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
						Policy:       RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
						Now:          time.Unix(30, 0),
					})
					return err
				}
			},
		},
		{
			name: "claim",
			// ReclaimExpiredClaims probes first, then the claim transition.
			scripts: 2,
			setup: func(t *testing.T, ctx context.Context, _ *redis.Client, directory *RedisRunnerDirectory, attempt int) func() error {
				session := registerRedisDirectoryRunner(t, ctx, directory, fmt.Sprintf("runner-script-claim-%d", attempt), 4)
				mustEnqueueRedisDirectoryAssignment(t, ctx, directory,
					redisDirectoryTestAssignment(AssignmentID(fmt.Sprintf("exec-script-claim-%d/node/activation-1", attempt))))
				return func() error {
					_, _, err := directory.ClaimForRunner(ctx, redisDirectoryClaimRequest(session, 4))
					return err
				}
			},
		},
		{
			name: "finalize",
			// ReclaimExpiredClaims then the claim -> leased transition.
			scripts: 2,
			setup: func(t *testing.T, ctx context.Context, _ *redis.Client, directory *RedisRunnerDirectory, attempt int) func() error {
				session := registerRedisDirectoryRunner(t, ctx, directory, fmt.Sprintf("runner-script-finalize-%d", attempt), 4)
				assignment := redisDirectoryTestAssignment(AssignmentID(fmt.Sprintf("exec-script-finalize-%d/node/activation-1", attempt)))
				mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
				claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 4)
				lease := finalizeLease(attempt, assignment)
				return func() error { return directory.FinalizeClaim(ctx, claim.ClaimID, lease) }
			},
		},
		{
			name: "reclaim",
			// The expired-claim recovery transition alone.
			scripts: 1,
			setup: func(t *testing.T, ctx context.Context, rdb *redis.Client, directory *RedisRunnerDirectory, attempt int) func() error {
				session := registerRedisDirectoryRunner(t, ctx, directory, fmt.Sprintf("runner-script-reclaim-%d", attempt), 2)
				mustEnqueueRedisDirectoryAssignment(t, ctx, directory,
					redisDirectoryTestAssignment(AssignmentID(fmt.Sprintf("exec-script-reclaim-%d/node/activation-1", attempt))))
				claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 2)
				// Backdate the claim's expiry instead of sleeping: the recovery
				// transition drives its scan from this sorted-set score.
				if err := rdb.ZAdd(ctx, directory.keys.claimsExpiry, redis.Z{
					Score:  float64(time.Now().Add(-time.Hour).UnixMilli()),
					Member: string(claim.ClaimID),
				}).Err(); err != nil {
					t.Fatalf("backdate claim expiry: %v", err)
				}
				return func() error { return directory.ReclaimExpiredClaims(ctx) }
			},
		},
		{
			name: "release_leased",
			// The leased -> released transition alone.
			scripts: 1,
			setup: func(t *testing.T, ctx context.Context, _ *redis.Client, directory *RedisRunnerDirectory, attempt int) func() error {
				session := registerRedisDirectoryRunner(t, ctx, directory, fmt.Sprintf("runner-script-release-%d", attempt), 4)
				assignment := redisDirectoryTestAssignment(AssignmentID(fmt.Sprintf("exec-script-release-%d/node/activation-1", attempt)))
				mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
				claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 4)
				lease := finalizeLease(attempt, assignment)
				if err := directory.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
					t.Fatalf("FinalizeClaim() error = %v", err)
				}
				return func() error {
					return directory.ReleaseLeased(ctx, ReleaseLeasedRequest{
						RunnerID:     session.RunnerID,
						SessionID:    session.SessionID,
						AssignmentID: assignment.AssignmentID,
						LeaseID:      lease.LeaseID,
						LeaseToken:   lease.LeaseToken,
					})
				}
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			_, rdb, directory, counter := newScriptCountingRedisRunnerDirectory(t)

			for attempt := 0; attempt < 2; attempt++ {
				run := testCase.setup(t, ctx, rdb, directory, attempt)
				if attempt == 0 {
					// Start cold so the NOSCRIPT fallback must happen.
					if err := rdb.ScriptFlush(ctx).Err(); err != nil {
						t.Fatalf("ScriptFlush() error = %v", err)
					}
				}
				counter.reset()
				if err := run(); err != nil {
					t.Fatalf("attempt %d: operation error = %v", attempt, err)
				}
				eval, evalsha := counter.counts()
				if attempt == 0 {
					if eval != testCase.scripts || evalsha != testCase.scripts {
						t.Fatalf("attempt 0 sent eval=%d evalsha=%d, want %d each (cold cache must fall back)",
							eval, evalsha, testCase.scripts)
					}
					continue
				}
				if eval != 0 || evalsha != testCase.scripts {
					t.Fatalf("attempt 1 sent eval=%d evalsha=%d, want 0 and %d (warm cache must use EVALSHA only)",
						eval, evalsha, testCase.scripts)
				}
			}
		})
	}
}
