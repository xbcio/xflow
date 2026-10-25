package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// These tests are specific to this backend. The local backend answers
// (true, false, nil) for an execution that does not exist but has terminal node
// rows, and this one answers (false, false, nil); the difference is deliberate
// and explained at CheckCompletion: here a true means checkCompletionLua already
// wrote the terminal status, which it must not do for an execution nothing
// created. They must not move into the shared store contract.

func plantSuccessfulNodes(t *testing.T, srv interface{ Set(k, v string) error }, id types.ExecutionID, names ...string) []string {
	t.Helper()
	keys := make([]string, 0, len(names))
	for _, name := range names {
		key := nodeStatusKey(namespace.Default, id, name)
		if err := srv.Set(key, string(types.NodeStatusSuccess)); err != nil {
			t.Fatalf("plant %s: %v", key, err)
		}
		keys = append(keys, key)
	}
	return keys
}

// TestCheckCompletionRefusesAnAbsentExecution: the status key is gone (expired, or
// never written) while every node key is still there. Finalizing that would SET a
// bare success with a short TTL and no deadline_ms beside it, the state
// executionDeadline reads as "no time budget".
func TestCheckCompletionRefusesAnAbsentExecution(t *testing.T) {
	ctx := context.Background()
	srv, rdb, s := newMiniredisStore(t)
	g := twoNodeGraph(t)

	t.Run("node keys outlive the execution", func(t *testing.T) {
		id := types.ExecutionID("check-completion-absent")
		s.graphs.Put(id, g, 0)
		planted := plantSuccessfulNodes(t, srv, id, "a", "b")

		done, failed, err := s.CheckCompletion(ctx, id, 2)
		if err != nil || done || failed {
			t.Fatalf("CheckCompletion on an absent execution = (%v, %v, %v), want (false, false, nil)", done, failed, err)
		}
		if srv.Exists(execKey(namespace.Default, id, "status")) {
			got, _ := srv.Get(execKey(namespace.Default, id, "status"))
			t.Fatalf("CheckCompletion created a status key (%q) for an execution nothing created", got)
		}
		// Nothing but what the test planted: no status, no marker, no index entry.
		keys := execKeysOf(srv, id)
		if len(keys) != len(planted) {
			t.Fatalf("keys after CheckCompletion = %v, want only the planted node keys %v", keys, planted)
		}
		if deadline, settled := newReplica(rdb).executionDeadline(ctx, id); settled {
			t.Fatalf("executionDeadline = (%v, settled=true) after CheckCompletion, want unsettled", deadline)
		}
	})

	t.Run("no node keys either", func(t *testing.T) {
		id := types.ExecutionID("check-completion-absent-bare")
		s.graphs.Put(id, g, 0)

		done, failed, err := s.CheckCompletion(ctx, id, 2)
		if err != nil || done || failed {
			t.Fatalf("CheckCompletion = (%v, %v, %v), want (false, false, nil)", done, failed, err)
		}
		if keys := execKeysOf(srv, id); len(keys) != 0 {
			t.Fatalf("CheckCompletion left %v, want nothing", keys)
		}
	})
}

// TestCheckCompletionStillFinalizesAnExistingExecution is the control for the
// guard above: with the execution present and every node done, the script
// finalizes it as before and reports it.
func TestCheckCompletionStillFinalizesAnExistingExecution(t *testing.T) {
	ctx := context.Background()
	srv, _, s := newMiniredisStore(t)
	g := twoNodeGraph(t)
	id := types.ExecutionID("check-completion-existing")
	createWithDeadline(t, s, ctx, id, time.Now().Add(replicaBudget))
	s.graphs.Put(id, g, 0)
	plantSuccessfulNodes(t, srv, id, "a", "b")

	done, failed, err := s.CheckCompletion(ctx, id, 2)
	if err != nil || !done || failed {
		t.Fatalf("CheckCompletion on an existing execution = (%v, %v, %v), want (true, false, nil)", done, failed, err)
	}
	if got, _ := srv.Get(execKey(namespace.Default, id, "status")); got != string(types.ExecutionStatusSuccess) {
		t.Fatalf("status after the completion = %q, want %q", got, types.ExecutionStatusSuccess)
	}
}
