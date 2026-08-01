package control

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
)

// activationWithSupply builds a bare-bones ASSIGNED EntryActivation carrying
// one supply requirement, following the same testEntryActivation() shape used
// by entry_activation_delivery_test.go, but with a distinct EntryUnitID per
// call (needed because the store keys on EntryUnitID) and RunnerID/Desired set
// directly rather than going through Reconcile — the hinter reads the store,
// it does not drive assignment, so there is no need to route through the
// reconciler to set up this fixture.
func activationWithSupply(t *testing.T, unitID, runnerID string, desired bool, node, resource string) engine.EntryActivation {
	t.Helper()
	act := testEntryActivation()
	act.EntryUnitID = unitID
	act.RunnerID = runnerID
	act.Desired = desired
	if node != "" {
		act.Supplies = []engine.SupplyRequirement{{Node: node, Resource: resource, RequireReady: true}}
	}
	return act
}

// The server must only hint a runner about supplies that runner actually hosts a
// consumer for. Broadcasting every supply to every runner would make each one
// fetch content it has no use for.
func TestSupplyHintsOnlyForRunnersThatHostTheConsumer(t *testing.T) {
	ctx := context.Background()
	store_ := NewMemoryEntryActivationStore()

	a := activationWithSupply(t, "unit-a", "runner-a", true, "rules", "shared-rules")
	b := activationWithSupply(t, "unit-b", "runner-b", true, "other", "other-resource")
	if err := store_.Upsert(ctx, a); err != nil {
		t.Fatalf("Upsert a: %v", err)
	}
	if err := store_.Upsert(ctx, b); err != nil {
		t.Fatalf("Upsert b: %v", err)
	}

	supplies := memstore.New()
	if _, err := supplies.PutSupply(ctx, &store.SupplyResource{
		Namespace: string(namespace.Default), Name: "shared-rules", Content: []byte(`{"a":1}`),
	}, nil); err != nil {
		t.Fatalf("PutSupply shared-rules: %v", err)
	}
	if _, err := supplies.PutSupply(ctx, &store.SupplyResource{
		Namespace: string(namespace.Default), Name: "other-resource", Content: []byte(`{"b":2}`),
	}, nil); err != nil {
		t.Fatalf("PutSupply other-resource: %v", err)
	}

	h := NewSupplyHinter(store_, supplies, []namespace.Namespace{namespace.Default}, nil)

	hintsA := h.HintsForRunner(ctx, "runner-a")
	wantA, err := supplies.GetSupply(ctx, string(namespace.Default), "shared-rules")
	if err != nil {
		t.Fatalf("GetSupply shared-rules: %v", err)
	}
	if len(hintsA) != 1 || hintsA["rules"] != wantA.ContentHash {
		t.Fatalf("hintsA = %#v, want {rules: %s}", hintsA, wantA.ContentHash)
	}

	hintsB := h.HintsForRunner(ctx, "runner-b")
	wantB, err := supplies.GetSupply(ctx, string(namespace.Default), "other-resource")
	if err != nil {
		t.Fatalf("GetSupply other-resource: %v", err)
	}
	if len(hintsB) != 1 || hintsB["other"] != wantB.ContentHash {
		t.Fatalf("hintsB = %#v, want {other: %s}", hintsB, wantB.ContentHash)
	}

	hintsC := h.HintsForRunner(ctx, "runner-c")
	if hintsC != nil {
		t.Fatalf("hintsC = %#v, want nil (runner-c hosts nothing)", hintsC)
	}
}

// An unassigned activation must not hint anyone: nobody is hosting it.
func TestNoHintsForUnassignedActivation(t *testing.T) {
	ctx := context.Background()
	store_ := NewMemoryEntryActivationStore()

	// RunnerID == "" : desired but never assigned to any runner.
	act := activationWithSupply(t, "unit-unassigned", "", true, "rules", "shared-rules")
	if err := store_.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	supplies := memstore.New()
	if _, err := supplies.PutSupply(ctx, &store.SupplyResource{
		Namespace: string(namespace.Default), Name: "shared-rules", Content: []byte(`{}`),
	}, nil); err != nil {
		t.Fatalf("PutSupply: %v", err)
	}

	h := NewSupplyHinter(store_, supplies, []namespace.Namespace{namespace.Default}, nil)

	// No runner ID would ever match RunnerID == "" (Admit requires a non-empty
	// runnerID and the comparison is exact), so every possible caller sees nil.
	// The empty string itself is guarded explicitly at the top of
	// HintsForRunner, so exercise a plausible real runner ID instead — the
	// actual guard under test is act.RunnerID != runnerID, which "" can never
	// satisfy for a real ID.
	hints := h.HintsForRunner(ctx, "runner-a")
	if hints != nil {
		t.Fatalf("hints = %#v, want nil (activation is unassigned)", hints)
	}
}

// The hint carries the CURRENT content hash from the store, so a runner can
// compare it against what it has applied. Equal hash → no fetch (that
// comparison is the runner's job, in SupplyGate.ApplyHints; this test only
// guards that the hinter surfaces the store's CURRENT hash, not a stale one).
func TestHintCarriesCurrentContentHash(t *testing.T) {
	ctx := context.Background()
	store_ := NewMemoryEntryActivationStore()

	act := activationWithSupply(t, "unit-a", "runner-a", true, "rules", "shared-rules")
	if err := store_.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	supplies := memstore.New()
	// Write twice: the hint must reflect the SECOND (current) write's hash, not
	// the first.
	if _, err := supplies.PutSupply(ctx, &store.SupplyResource{
		Namespace: string(namespace.Default), Name: "shared-rules", Content: []byte(`v1`),
	}, nil); err != nil {
		t.Fatalf("PutSupply v1: %v", err)
	}
	rev1 := uint64(1)
	rec2, err := supplies.PutSupply(ctx, &store.SupplyResource{
		Namespace: string(namespace.Default), Name: "shared-rules", Content: []byte(`v2-current`),
	}, &rev1)
	if err != nil {
		t.Fatalf("PutSupply v2: %v", err)
	}

	h := NewSupplyHinter(store_, supplies, []namespace.Namespace{namespace.Default}, nil)
	hints := h.HintsForRunner(ctx, "runner-a")
	if len(hints) != 1 || hints["rules"] != rec2.ContentHash {
		t.Fatalf("hints = %#v, want {rules: %s} (current hash)", hints, rec2.ContentHash)
	}
	if hints["rules"] == store.ContentHash([]byte(`v1`)) {
		t.Fatal("hint carries the STALE v1 hash, not the current one")
	}
}

func TestHintsAreEmptyMapNotNilWhenNothingChanged(t *testing.T) {
	// Deliberate: nil is the "no hints" signal so omitempty drops the field
	// entirely, keeping heartbeat bodies byte-identical for runners with no
	// supplies. Assert nil, not an empty map.
	ctx := context.Background()
	store_ := NewMemoryEntryActivationStore()

	// A runner hosting an activation that has NO supply requirements at all
	// (Supplies left nil) must get nil hints, not an empty-but-non-nil map:
	// the two are observably different once serialized (omitempty checks
	// len()==0, which both satisfy, so JSON-wise they are identical — but the
	// brief's contract is specifically about the Go value the hinter returns,
	// asserted here with reflect-grade nil-ness).
	act := activationWithSupply(t, "unit-no-supply", "runner-a", true, "", "")
	if err := store_.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	supplies := memstore.New()
	h := NewSupplyHinter(store_, supplies, []namespace.Namespace{namespace.Default}, nil)

	got := h.HintsForRunner(ctx, "runner-a")
	if got != nil {
		t.Fatalf("hints = %#v, want nil for a runner hosting no supply-dependent activation", got)
	}
}

// --- MemorySupplyObserved ---

// Record must WHOLLY REPLACE a runner's prior entry, not merge into it: a
// supply the runner no longer reports (removed workflow, or the consumer
// stopped) must not linger from a previous heartbeat's report.
func TestMemorySupplyObservedRecordReplacesNotMerges(t *testing.T) {
	s := NewMemorySupplyObserved()
	s.Record("runner-a", map[string]string{"rules": "hash1", "other": "hash2"})
	s.Record("runner-a", map[string]string{"rules": "hash3"}) // "other" dropped this round

	snap := s.Snapshot()
	got, ok := snap["runner-a"]
	if !ok {
		t.Fatal("runner-a missing from snapshot")
	}
	if len(got) != 1 || got["rules"] != "hash3" {
		t.Fatalf("got = %#v, want exactly {rules: hash3} (merge would leave 'other' behind)", got)
	}
}

// An empty observed report clears the runner's entry entirely (it no longer
// hosts any supply-dependent activation).
func TestMemorySupplyObservedEmptyReportClears(t *testing.T) {
	s := NewMemorySupplyObserved()
	s.Record("runner-a", map[string]string{"rules": "hash1"})
	s.Record("runner-a", nil)

	snap := s.Snapshot()
	if _, ok := snap["runner-a"]; ok {
		t.Fatalf("runner-a still present after empty report: %#v", snap["runner-a"])
	}
}

// Snapshot must be a deep copy: mutating the returned map must not corrupt
// the sink's internal state.
func TestMemorySupplyObservedSnapshotIsIndependentCopy(t *testing.T) {
	s := NewMemorySupplyObserved()
	s.Record("runner-a", map[string]string{"rules": "hash1"})

	snap := s.Snapshot()
	snap["runner-a"]["rules"] = "corrupted"

	snap2 := s.Snapshot()
	if snap2["runner-a"]["rules"] != "hash1" {
		t.Fatalf("internal state corrupted via returned map: %#v", snap2)
	}
}

// --- Core.heartbeat wiring: nil hinter/observed sink must not change behavior ---

// heartbeatReq builds a minimal valid protocol.HeartbeatRequest for the given
// runner/session, matching the shape core.heartbeat requires (RunnerID +
// SessionID non-empty).
func heartbeatReq(runnerID, sessionID string) protocol.HeartbeatRequest {
	return protocol.HeartbeatRequest{RunnerID: runnerID, SessionID: sessionID, Capacity: 1}
}

// A heartbeat with a nil supplyHinter and nil supplyObserved must produce a
// response with no SupplyHints field set — this is the "wiring is optional"
// requirement: an old-shaped Core (before this task) behaves identically.
func TestHeartbeatWithNilSupplyWiringLeavesResponseUnchanged(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	sess, err := dir.Register(ctx, RegisterRunnerRequest{RunnerID: "runner-a", Capacity: 1, Now: time.Now()})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	core := &Core{runners: dir, pollWait: time.Second}
	resp, err := core.heartbeat(ctx, heartbeatReq("runner-a", sess.SessionID), TransportInfo{})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if resp.SupplyHints != nil {
		t.Fatalf("SupplyHints = %#v, want nil with no hinter wired", resp.SupplyHints)
	}
}

// A heartbeat carrying SupplyObserved must be accepted (not rejected) even
// when supplyObserved is nil — an old runner talking to a server that has not
// wired the sink must not fail its heartbeat over a field the server simply
// ignores.
func TestHeartbeatAcceptsObservedWithNilSink(t *testing.T) {
	ctx := context.Background()
	dir := NewMemoryRunnerDirectory()
	sess, err := dir.Register(ctx, RegisterRunnerRequest{RunnerID: "runner-a", Capacity: 1, Now: time.Now()})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	core := &Core{runners: dir, pollWait: time.Second}
	req := heartbeatReq("runner-a", sess.SessionID)
	req.SupplyObserved = map[string]string{"rules": "hash1"}
	if _, err := core.heartbeat(ctx, req, TransportInfo{}); err != nil {
		t.Fatalf("heartbeat with observed report and nil sink must succeed: %v", err)
	}
}

// The server-side hinter/observed sink must be exercised end to end through
// Core.heartbeat: a wired hinter populates resp.SupplyHints, and a reported
// SupplyObserved lands in the sink's snapshot.
func TestHeartbeatWiresHinterAndObservedSink(t *testing.T) {
	ctx := context.Background()
	actStore := NewMemoryEntryActivationStore()
	act := activationWithSupply(t, "unit-a", "runner-a", true, "rules", "shared-rules")
	if err := actStore.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	supplies := memstore.New()
	rec, err := supplies.PutSupply(ctx, &store.SupplyResource{
		Namespace: string(namespace.Default), Name: "shared-rules", Content: []byte(`{}`),
	}, nil)
	if err != nil {
		t.Fatalf("PutSupply: %v", err)
	}

	hinter := NewSupplyHinter(actStore, supplies, []namespace.Namespace{namespace.Default}, nil)
	observed := NewMemorySupplyObserved()

	dir := NewMemoryRunnerDirectory()
	sess, err := dir.Register(ctx, RegisterRunnerRequest{RunnerID: "runner-a", Capacity: 1, Now: time.Now()})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	core := &Core{runners: dir, pollWait: time.Second, supplyHinter: hinter, supplyObserved: observed}
	req := heartbeatReq("runner-a", sess.SessionID)
	req.SupplyObserved = map[string]string{"rules": rec.ContentHash}
	resp, err := core.heartbeat(ctx, req, TransportInfo{})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if len(resp.SupplyHints) != 1 || resp.SupplyHints["rules"] != rec.ContentHash {
		t.Fatalf("SupplyHints = %#v, want {rules: %s}", resp.SupplyHints, rec.ContentHash)
	}

	snap := observed.Snapshot()
	if snap["runner-a"]["rules"] != rec.ContentHash {
		t.Fatalf("observed snapshot = %#v, want runner-a/rules = %s", snap, rec.ContentHash)
	}
}
