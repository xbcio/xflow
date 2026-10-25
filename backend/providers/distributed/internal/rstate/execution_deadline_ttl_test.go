package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// requireOutlivesDeadline fails unless the key's TTL reaches past the deadline
// by (almost) the grace. The allowance absorbs the seconds between the test
// computing the deadline and the store reading its own clock.
func requireOutlivesDeadline(t *testing.T, got time.Duration, deadline time.Time, what string) {
	t.Helper()
	want := time.Until(deadline) + deadlineGrace
	if got < want-time.Minute {
		t.Fatalf("%s TTL = %v, want about %v (the deadline plus the grace): the key would expire before its execution can time out", what, got, want)
	}
}

func TestRetentionPastDeadline(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name     string
		ttl      time.Duration
		deadline time.Time
		longer   bool
	}{
		{"no budget keeps the retention", time.Minute, time.Time{}, false},
		{"unbounded retention stays unbounded", 0, now.Add(time.Hour), false},
		{"a deadline inside the retention changes nothing", 48 * time.Hour, now.Add(time.Hour), false},
		{"a deadline past the retention lengthens it", time.Minute, now.Add(3 * time.Hour), true},
		{"a deadline already past is covered by the grace", time.Minute, now.Add(-time.Minute), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := retentionPastDeadline(tc.ttl, tc.deadline)
			if tc.longer && got <= tc.ttl {
				t.Fatalf("retentionPastDeadline(%v) = %v, want it lengthened", tc.ttl, got)
			}
			if !tc.longer && got != tc.ttl {
				t.Fatalf("retentionPastDeadline(%v) = %v, want it unchanged", tc.ttl, got)
			}
		})
	}
}

// TestCreateExecutionKeepsKeysPastTheDeadline: a store whose retention is
// shorter than the execution's time budget must still keep the execution's keys
// until the budget plus the grace has passed.
func TestCreateExecutionKeepsKeysPastTheDeadline(t *testing.T) {
	ctx := context.Background()
	srv, _, state := newMiniredisStore(t) // retention: one minute
	deadline := time.Now().Add(3 * time.Hour)
	createWithDeadline(t, state, ctx, "ttl-create", deadline)

	for _, suffix := range []string{"status", "deadline_ms", "graph"} {
		requireOutlivesDeadline(t, srv.TTL(execKey(namespace.Default, "ttl-create", suffix)), deadline, suffix)
	}
}

func TestCreateExecutionRetentionUnchangedWithoutDeadline(t *testing.T) {
	ctx := context.Background()
	srv, _, state := newMiniredisStore(t)
	createWithDeadline(t, state, ctx, "ttl-none", time.Time{})

	if got := srv.TTL(execKey(namespace.Default, "ttl-none", "status")); got != time.Minute {
		t.Fatalf("status TTL = %v, want the store's %v: an execution with no budget keeps its retention", got, time.Minute)
	}
}

func TestSeedExecutionKeepsKeysPastTheDeadline(t *testing.T) {
	ctx := context.Background()
	srv, _, state := newMiniredisStore(t)
	deadline := time.Now().Add(3 * time.Hour)
	if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{Execution: &engine.ExecutionSnapshot{
		ID: "ttl-seed", Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadline,
	}}); err != nil {
		t.Fatalf("SeedExecution: %v", err)
	}
	for _, suffix := range []string{"status", "deadline_ms"} {
		requireOutlivesDeadline(t, srv.TTL(execKey(namespace.Default, "ttl-seed", suffix)), deadline, suffix)
	}
}

func TestSeedExecutionFromEntryKeepsKeysPastTheDeadline(t *testing.T) {
	ctx := context.Background()
	srv, _, state := newMiniredisStore(t)
	req := timeoutSeedRequest(t, "ttl-entry", "ttl-entry-key", 3*time.Hour)
	resp, err := state.SeedExecutionFromEntry(ctx, req)
	if err != nil || resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("SeedExecutionFromEntry = (%+v, %v), want accepted", resp, err)
	}
	deadline := time.Now().Add(3 * time.Hour)
	for _, suffix := range []string{"status", "deadline_ms", "namespace"} {
		requireOutlivesDeadline(t, srv.TTL(execKey(namespace.Default, resp.ExecutionID, suffix)), deadline, suffix)
	}
}

// TestSuspendOnAnotherReplicaKeepsKeysPastTheDeadline is the cross-replica case
// the in-memory override cannot cover: replica B never created the execution,
// so its getExecTTL answers the default retention. The park renews the
// execution's keys, and it must renew them to the deadline, not back down to a
// retention shorter than the time budget still running. The spec's own timeout
// is deliberately short, so only the deadline can account for the lifetime.
func TestSuspendOnAnotherReplicaKeepsKeysPastTheDeadline(t *testing.T) {
	ctx := context.Background()
	srv, rdb, creator := newMiniredisStore(t)
	deadline := time.Now().Add(3 * time.Hour)
	id := types.ExecutionID("ttl-park")
	g := errorDetailsRedisGraph(t, "wait")
	if err := creator.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning, Deadline: deadline,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}

	other := New(rdb, nil, time.Minute) // a second replica: no in-memory override
	if got := other.getExecTTL(ctx, id); got != time.Minute {
		t.Fatalf("precondition: the other replica resolves a %v retention, want the %v default", got, time.Minute)
	}
	lease := privateRedisLease(id, "wait", 0, "lease-ttl", "token-ttl")
	lease.Task.ActivationID = 1
	if _, acquired, err := other.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1
	if _, claimed, err := other.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease claimed=%v err=%v", claimed, err)
	}
	if payload, _, err := other.SuspendTaskLease(ctx, lease, nil, false, false, &types.SuspendSpec{
		Mode: types.ModeSignal, Signals: []string{"go"}, Timeout: 30 * time.Second,
	}, ""); err != nil || payload != nil {
		t.Fatalf("SuspendTaskLease payload=%+v err=%v, want parked", payload, err)
	}

	for _, suffix := range []string{"status", "deadline_ms", "graph"} {
		requireOutlivesDeadline(t, srv.TTL(execKey(namespace.Default, id, suffix)), deadline, suffix)
	}
}
