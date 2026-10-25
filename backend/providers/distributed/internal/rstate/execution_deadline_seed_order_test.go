package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// isSeedScript matches the call that creates a seeded execution. The script is
// loaded up front by the test, so the call is a single EVALSHA that applies on
// the first attempt and "after" really means after the execution exists.
func isSeedScript(cmd redis.Cmder) bool {
	args := cmd.Args()
	return cmd.Name() == "evalsha" && len(args) > 1 && args[1] == seedExecutionLua.Hash()
}

// TestSeedExecutionWritesTheDeadlineBeforeTheStatus pins the creation order the
// deadline memo depends on: once the status key exists, a budgeted execution's
// deadline_ms exists too. Another replica reads "status present, deadline_ms
// absent" as an execution with no time budget and remembers it as one, so a seed
// that wrote the deadline after the script that creates the status would hand
// that replica a zero deadline for as long as the execution ran.
//
// Each case puts a reader on another replica at one edge of the seed script and
// requires it to find the real deadline: before the script (no status yet, the
// deadline already there) and after it (the status there, the deadline there
// already).
func TestSeedExecutionWritesTheDeadlineBeforeTheStatus(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		after bool
	}{
		{"before the seed script applies", false},
		{"right after the seed script applies", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, rdb, state := newMiniredisStore(t)
			if err := seedExecutionLua.Load(ctx, rdb).Err(); err != nil {
				t.Fatalf("load the seed script: %v", err)
			}

			id := types.ExecutionID("seed-order-" + types.ExecutionID(tc.name[:5]))
			deadline := time.Now().Add(replicaBudget)
			reader := newReplica(rdb)

			var read time.Time
			hook := &interleaveHook{
				matches: isSeedScript,
				after:   tc.after,
				run:     func() { read = reader.execDeadline(ctx, id) },
			}
			rdb.AddHook(hook)

			if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{Execution: &engine.ExecutionSnapshot{
				ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadline,
			}}); err != nil {
				t.Fatalf("SeedExecution: %v", err)
			}
			if !hook.fired.Load() {
				t.Fatal("the seed script was never observed: the interleaving did not run")
			}

			if got, want := read.UnixMilli(), deadline.UnixMilli(); got != want {
				t.Fatalf("another replica read deadline %v at that point of the seed, want %v: the "+
					"deadline has to be written before the status exists", read, deadline)
			}
			if memo := requireMemo(t, reader, id, true); memo.UnixMilli() != deadline.UnixMilli() {
				t.Fatalf("the replica remembered deadline %v, want %v", memo, deadline)
			}

			// What the memo is for: a write from that replica keeps the retention.
			if err := reader.PauseExecution(ctx, id); err != nil {
				t.Fatalf("PauseExecution on the replica that read mid-seed: %v", err)
			}
			requireStatusOutlivesDeadline(t, srv, id, deadline, "PauseExecution on the replica that read mid-seed")
		})
	}
}
