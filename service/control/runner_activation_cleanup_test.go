package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
)

func cleanupActivation() engine.EntryActivation {
	return engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "cleanup-wf",
		WorkflowVersion: "v1",
		EntryUnitID:     "cleanup-entry",
		NodeType:        "kafka.source",
		Desired:         true,
	}
}

func registerCleanupRunner(t *testing.T, ctx context.Context, directory *MemoryRunnerDirectory, runnerID string, now time.Time, inventory []protocol.ActivationInventoryItem) RunnerSession {
	t.Helper()
	session, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID:    runnerID,
		Capacity:    2,
		Namespaces:  []namespace.Namespace{namespace.Default},
		Activations: inventory,
		Now:         now,
	})
	if err != nil {
		t.Fatalf("Register(%s): %v", runnerID, err)
	}
	return session
}

func drainCleanupRunner(t *testing.T, ctx context.Context, directory *MemoryRunnerDirectory, runnerID string, now time.Time) {
	t.Helper()
	if _, err := directory.SetRunnerControl(ctx, RunnerControlRequest{
		RunnerID:     runnerID,
		DesiredState: RunnerDesiredStateDraining,
		Actor:        "operator",
		Action:       "drain",
		Reason:       "cleanup receipt test",
		RequestID:    "cleanup-" + runnerID,
		RequestHash:  "cleanup-hash-" + runnerID,
		Now:          now,
	}); err != nil {
		t.Fatalf("SetRunnerControl(drain): %v", err)
	}
}

func cleanupAck(session RunnerSession, act engine.EntryActivation) protocol.ActivationAck {
	return protocol.ActivationAck{
		RunnerID:        session.RunnerID,
		SessionID:       session.SessionID,
		WorkflowID:      string(act.WorkflowID),
		WorkflowVersion: act.WorkflowVersion,
		GroupID:         act.EntryUnitID,
		ReplicaIndex:    act.ReplicaIndex,
		Generation:      1,
		Status:          protocol.ActivationStatusDeactivated,
	}
}

func TestDrainDeactivationObligationRedeliversUntilMatchingReceipt(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	directory := NewMemoryRunnerDirectory()
	owner := registerCleanupRunner(t, ctx, directory, "runner-owner", now, nil)
	_ = registerCleanupRunner(t, ctx, directory, "runner-replacement", now, nil)

	store := NewMemoryEntryActivationStore()
	act := cleanupActivation()
	key := keyOf(&act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Assign(ctx, key, owner.RunnerID, owner.SessionID, 1, now.Add(time.Minute)); err != nil || !ok {
		t.Fatalf("Assign: ok=%v err=%v", ok, err)
	}
	drainCleanupRunner(t, ctx, directory, owner.RunnerID, now)

	reconciler := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: directory, Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
	})
	if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	core := &Core{runners: directory, entryReconciler: reconciler}
	first, err := core.heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID: owner.RunnerID, SessionID: owner.SessionID, Capacity: 2, Timestamp: now.Add(2 * time.Second).Unix(),
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("first heartbeat: %v", err)
	}
	if first.Activations == nil || len(first.Activations.Deactivate) != 1 || first.Activations.Deactivate[0].Generation != 1 {
		t.Fatalf("first durable deactivate = %+v, want one generation-1 directive", first.Activations)
	}

	// Model a response lost after the server wrote it. The ledger is not
	// drain-once, so the next heartbeat gets the same cleanup instruction.
	second, err := core.heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID: owner.RunnerID, SessionID: owner.SessionID, Capacity: 2, Timestamp: now.Add(3 * time.Second).Unix(),
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("second heartbeat: %v", err)
	}
	if second.Activations == nil || len(second.Activations.Deactivate) != 1 || second.Activations.Deactivate[0] != first.Activations.Deactivate[0] {
		t.Fatalf("lost-response retry = %+v, want same durable directive %+v", second.Activations, first.Activations)
	}

	snapshot, found, err := directory.RunnerControl(ctx, owner.RunnerID)
	if err != nil || !found || snapshot.Drain == nil || snapshot.Drain.PendingActivationCleanup != 1 {
		t.Fatalf("drain snapshot = %+v found=%v err=%v, want one cleanup blocker", snapshot, found, err)
	}
	if snapshot.Drain.ServerQuiescent {
		t.Fatalf("drain snapshot = %+v, must not claim server quiescence before receipt", snapshot.Drain)
	}

	if err := core.activationAck(ctx, cleanupAck(owner, act), TransportInfo{}); err != nil {
		t.Fatalf("matching deactivation receipt: %v", err)
	}
	third, err := core.heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID: owner.RunnerID, SessionID: owner.SessionID, Capacity: 2, Timestamp: now.Add(4 * time.Second).Unix(),
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("third heartbeat: %v", err)
	}
	if third.Activations != nil && len(third.Activations.Deactivate) != 0 {
		t.Fatalf("post-receipt directives = %+v, want no durable deactivate", third.Activations)
	}
	snapshot, _, _ = directory.RunnerControl(ctx, owner.RunnerID)
	if snapshot.Drain == nil || snapshot.Drain.PendingActivationCleanup != 0 {
		t.Fatalf("post-receipt drain snapshot = %+v, want cleanup debt cleared", snapshot.Drain)
	}
}

func TestDrainDeactivationReceiptIsSessionFencedAcrossReregister(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 16, 13, 0, 0, 0, time.UTC)
	directory := NewMemoryRunnerDirectory()
	owner := registerCleanupRunner(t, ctx, directory, "runner-owner", now, nil)
	_ = registerCleanupRunner(t, ctx, directory, "runner-replacement", now, nil)
	store := NewMemoryEntryActivationStore()
	act := cleanupActivation()
	key := keyOf(&act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Assign(ctx, key, owner.RunnerID, owner.SessionID, 1, now.Add(time.Minute)); err != nil || !ok {
		t.Fatalf("Assign: ok=%v err=%v", ok, err)
	}
	drainCleanupRunner(t, ctx, directory, owner.RunnerID, now)
	reconciler := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: directory, Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
	})
	if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	// The replacement session explicitly proves it still hosts the old
	// generation. Register atomically moves delivery to it; the old session is
	// fenced and its delayed receipt cannot clear the new attempt.
	replacement := registerCleanupRunner(t, ctx, directory, owner.RunnerID, now.Add(2*time.Second), []protocol.ActivationInventoryItem{{
		WorkflowID: string(act.WorkflowID), WorkflowVersion: act.WorkflowVersion, EntryUnitID: act.EntryUnitID, Generation: 1,
	}})
	core := &Core{runners: directory, entryReconciler: reconciler}
	if err := core.activationAck(ctx, cleanupAck(owner, act), TransportInfo{}); err != nil {
		t.Fatalf("stale receipt transport result: %v", err)
	}
	snapshot, _, _ := directory.RunnerControl(ctx, owner.RunnerID)
	if snapshot.Drain == nil || snapshot.Drain.PendingActivationCleanup != 1 {
		t.Fatalf("stale receipt cleared cleanup: %+v", snapshot.Drain)
	}

	response, err := core.heartbeat(ctx, protocol.HeartbeatRequest{
		RunnerID: replacement.RunnerID, SessionID: replacement.SessionID, Capacity: 2, Timestamp: now.Add(3 * time.Second).Unix(),
	}, TransportInfo{})
	if err != nil || response.Activations == nil || len(response.Activations.Deactivate) != 1 {
		t.Fatalf("replacement directive = %+v err=%v, want one durable retry", response.Activations, err)
	}
	if err := core.activationAck(ctx, cleanupAck(replacement, act), TransportInfo{}); err != nil {
		t.Fatalf("replacement receipt: %v", err)
	}
	snapshot, _, _ = directory.RunnerControl(ctx, owner.RunnerID)
	if snapshot.Drain == nil || snapshot.Drain.PendingActivationCleanup != 0 {
		t.Fatalf("replacement receipt did not clear cleanup: %+v", snapshot.Drain)
	}
}

func TestDrainPendingDeactivationIntentRecoversAfterFenceWindowCrash(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 16, 14, 0, 0, 0, time.UTC)
	directory := NewMemoryRunnerDirectory()
	owner := registerCleanupRunner(t, ctx, directory, "runner-owner", now, nil)
	store := NewMemoryEntryActivationStore()
	act := cleanupActivation()
	key := keyOf(&act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Assign(ctx, key, owner.RunnerID, owner.SessionID, 1, now.Add(time.Minute)); err != nil || !ok {
		t.Fatalf("Assign: ok=%v err=%v", ok, err)
	}
	drainCleanupRunner(t, ctx, directory, owner.RunnerID, now)
	obligation := DeactivationObligation{
		RunnerID: owner.RunnerID, SessionID: owner.SessionID, Namespace: act.Namespace,
		WorkflowID: act.WorkflowID, WorkflowVersion: act.WorkflowVersion, EntryUnitID: act.EntryUnitID,
		Generation: 1, DrainGeneration: 1,
	}
	if applicable, err := directory.EnsureDeactivationObligation(ctx, obligation); err != nil || !applicable {
		t.Fatalf("EnsureDeactivationObligation: applicable=%v err=%v", applicable, err)
	}

	// Simulate a control-plane crash after persisting pending_fence but before
	// the activation Store.Fence call. A replacement reconciler must finish the
	// fence and publish the same durable obligation rather than forgetting it.
	reconciler := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: directory, Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
	})
	if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("recovery Reconcile: %v", err)
	}
	stored, ok, err := store.Get(ctx, key)
	if err != nil || !ok || stored.RunnerID != "" {
		t.Fatalf("recovered activation = %+v ok=%v err=%v, want old owner fenced", stored, ok, err)
	}
	directives, err := directory.DeactivationDirectives(ctx, owner.RunnerID, owner.SessionID)
	if err != nil || len(directives) != 1 || directives[0].Generation != 1 {
		t.Fatalf("recovered directives = %+v err=%v, want ready generation-1 obligation", directives, err)
	}
}

func TestRedisDrainDeactivationObligationPersistsAndSessionFencesReceipt(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 16, 15, 0, 0, 0, time.UTC)
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	owner := registerRedisDirectoryRunner(t, ctx, directory, "runner-owner", 2)
	_ = registerRedisDirectoryRunner(t, ctx, directory, "runner-replacement", 2)

	store := NewMemoryEntryActivationStore()
	act := cleanupActivation()
	key := keyOf(&act)
	if err := store.Upsert(ctx, act); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Assign(ctx, key, owner.RunnerID, owner.SessionID, 1, now.Add(time.Minute)); err != nil || !ok {
		t.Fatalf("Assign: ok=%v err=%v", ok, err)
	}
	if _, err := directory.SetRunnerControl(ctx, RunnerControlRequest{
		RunnerID: owner.RunnerID, DesiredState: RunnerDesiredStateDraining, Actor: "operator", Action: "drain",
		Reason: "cleanup receipt test", RequestID: "cleanup-redis", RequestHash: "cleanup-redis-hash", Now: now,
	}); err != nil {
		t.Fatalf("SetRunnerControl(drain): %v", err)
	}
	reconciler := NewEntryActivationReconciler(EntryActivationReconcilerConfig{
		Store: store, Lister: directory, Namespaces: []namespace.Namespace{namespace.Default}, LeaseTTL: time.Minute,
	})
	if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// A fresh directory instance models a control-plane restart. Durable Redis
	// state must still derive the directive, rather than relying on the old
	// reconciler's in-memory map.
	restarted := NewRedisRunnerDirectory(rdb)
	directives, err := restarted.DeactivationDirectives(ctx, owner.RunnerID, owner.SessionID)
	if err != nil || len(directives) != 1 || directives[0].Generation != 1 {
		t.Fatalf("post-restart directives = %+v err=%v, want one durable directive", directives, err)
	}
	projection, found, err := restarted.RunnerControl(ctx, owner.RunnerID)
	if err != nil || !found || projection.Drain == nil || projection.Drain.PendingActivationCleanup != 1 {
		t.Fatalf("post-restart drain projection = %+v found=%v err=%v, want one cleanup blocker", projection, found, err)
	}

	replacement, err := restarted.Register(ctx, RegisterRunnerRequest{
		RunnerID: owner.RunnerID, Capacity: 2, Namespaces: []namespace.Namespace{namespace.Default}, Now: now.Add(2 * time.Second),
		Activations: []protocol.ActivationInventoryItem{{
			WorkflowID: string(act.WorkflowID), WorkflowVersion: act.WorkflowVersion, EntryUnitID: act.EntryUnitID, Generation: 1,
		}},
	})
	if err != nil {
		t.Fatalf("replacement Register: %v", err)
	}
	if acknowledged, err := restarted.AcknowledgeDeactivation(namespace.WithNamespace(ctx, namespace.Default), cleanupAck(owner, act)); err != nil || acknowledged {
		t.Fatalf("old session receipt acknowledged=%v err=%v, want false/nil", acknowledged, err)
	}
	directives, err = restarted.DeactivationDirectives(ctx, replacement.RunnerID, replacement.SessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("replacement durable directives = %+v err=%v, want one", directives, err)
	}
	if acknowledged, err := restarted.AcknowledgeDeactivation(namespace.WithNamespace(ctx, namespace.Default), cleanupAck(replacement, act)); err != nil || !acknowledged {
		t.Fatalf("replacement receipt acknowledged=%v err=%v, want true/nil", acknowledged, err)
	}
	projection, _, _ = restarted.RunnerControl(ctx, owner.RunnerID)
	if projection.Drain == nil || projection.Drain.PendingActivationCleanup != 0 {
		t.Fatalf("post-receipt Redis projection = %+v, want cleanup cleared", projection.Drain)
	}
}
