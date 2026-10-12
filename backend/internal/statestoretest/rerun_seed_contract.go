package statestoretest

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// RunExecutionSeedContract exercises engine.ExecutionSeeder against a concrete
// backend.
//
// A seed creates an execution around units whose outcome is already decided: the
// work they represent belongs to an execution this one derives from. The cases
// below are about the two halves of that sentence.
//
// RECORDED: every unit in the request is a resolution the store commits in the
// same transition as the creation — status, port, output — and a resolved unit
// is never scheduled. Not when the seed writes its own arrivals, and not later
// when another resolved unit's arrival reaches its counters. A backend that
// scheduled one anyway would re-run work whose result is already the new
// execution's input.
//
// SCHEDULED: everything the resolutions make ready is delivered. A successor
// whose in-degree the seeded arrivals satisfy is scheduled exactly as a commit
// would schedule it — execute when some arrival is active, skip when none is —
// and a unit no arrival reaches (a from-node that is a root) is delivered from
// the request's own entries. This is the half that makes the seeded execution
// actually continue instead of sitting at zero remaining with nothing queued.
//
// Admission is deliberately the same decision it is everywhere else: a seeded
// execution contends for its workflow's slot exactly as a submitted one does,
// and a refusal persists nothing at all.
//
// Units are node units. A group unit's boundary outputs come from a runtime
// activation, so a caller that had one to resolve refuses before reaching the
// store (see engine.ExecutionSeeder); no case here seeds one.
func RunExecutionSeedContract(t *testing.T, newStore ConcurrencyStoreFactory) {
	t.Helper()
	t.Run("RecordsTheResolvedUnits", func(t *testing.T) {
		seedRecordsTheResolvedUnits(t, newStore(t))
	})
	t.Run("DeliversWhatTheArrivalsSchedule", func(t *testing.T) {
		seedDeliversWhatTheArrivalsSchedule(t, newStore(t))
	})
	t.Run("SkipsWhatTheArrivalsSkip", func(t *testing.T) {
		seedSkipsWhatTheArrivalsSkip(t, newStore(t))
	})
	t.Run("DeliversTheEntries", func(t *testing.T) {
		seedDeliversTheEntries(t, newStore(t))
	})
	t.Run("NeverSchedulesAResolvedUnit", func(t *testing.T) {
		seedNeverSchedulesAResolvedUnit(t, newStore(t))
	})
	t.Run("FinalizesWhenEverythingIsResolved", func(t *testing.T) {
		seedFinalizesWhenEverythingIsResolved(t, newStore(t))
	})
	t.Run("RecordsTheRerunOrigin", func(t *testing.T) {
		seedRecordsTheRerunOrigin(t, newStore(t))
	})
	t.Run("RefusesWithoutPersistingOverTheLimit", func(t *testing.T) {
		seedRefusesWithoutPersistingOverTheLimit(t, newStore(t))
	})
	t.Run("ParksAQueuedAdmission", func(t *testing.T) {
		seedParksAQueuedAdmission(t, newStore(t))
	})
}

// seedContractGraph compiles start -> mid -> tail, the shape the seed cases
// resolve a prefix of. Three nodes rather than two so a case can tell "the
// resolved unit's own successor" from "everything else": only the first is
// scheduled, and a backend that scheduled the whole remainder fails on the
// second.
func seedContractGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    name,
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "mid", Type: "test.echo"},
			{Name: "tail", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "mid", Input: "main"}}}},
			"mid":   {"main": {Targets: []types.Connection{{Node: "tail", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// seedCapability resolves the seeder, refusing (not skipping) when the backend
// does not implement it: every backend under this suite is required to.
func seedCapability(t *testing.T, state engine.StateStore) engine.ExecutionSeeder {
	t.Helper()
	seeder, ok := state.(engine.ExecutionSeeder)
	if !ok {
		t.Fatalf("%T does not implement engine.ExecutionSeeder", state)
	}
	return seeder
}

// seedUnitResolving builds the resolution of one node unit as the engine builds
// it: the unit's identity, the status and port its stored state carries, and
// the arrivals its output port pushes to the named successors.
//
// The arrivals are constructed here rather than borrowed from the engine
// because the engine's own closure arithmetic is tested on its side of the
// boundary; what this suite pins is what the store does with an arrival it is
// handed.
func seedUnitResolving(t *testing.T, g *graph.Graph, name string, activePort string, output map[string]any, successors ...string) engine.SeedUnit {
	t.Helper()
	nodeIdx, ok := g.NodeIndex(name)
	if !ok {
		t.Fatalf("graph has no node %q", name)
	}
	unit := engine.SeedUnit{
		UnitIdx:  g.UnitIndexForNode(nodeIdx),
		NodeName: name,
		NodeIdx:  nodeIdx,
		Status:   types.NodeStatusSuccess,
		Port:     activePort,
		Output:   output,
	}
	for _, successor := range successors {
		unit.Downstream = append(unit.Downstream, seedArrival(t, g, activePort, successor))
	}
	return unit
}

// seedSkippedUnit builds the resolution of a unit the parent execution resolved
// without running: no output, and arrivals that carry no active port, which is
// how a skip propagates.
func seedSkippedUnit(t *testing.T, g *graph.Graph, name string, successors ...string) engine.SeedUnit {
	t.Helper()
	nodeIdx, ok := g.NodeIndex(name)
	if !ok {
		t.Fatalf("graph has no node %q", name)
	}
	unit := engine.SeedUnit{
		UnitIdx:  g.UnitIndexForNode(nodeIdx),
		NodeName: name,
		NodeIdx:  nodeIdx,
		Status:   types.NodeStatusSkipped,
	}
	for _, successor := range successors {
		unit.Downstream = append(unit.Downstream, seedArrival(t, g, "", successor))
	}
	return unit
}

// seedArrival is one arrival into a successor, as downstreamArrivals computes
// it for the source's active port: the edge count, whether it came from the
// active port, and the destination's merge mode and task type.
func seedArrival(t *testing.T, g *graph.Graph, activePort, successor string) engine.DownstreamArrival {
	t.Helper()
	nodeIdx, ok := g.NodeIndex(successor)
	if !ok {
		t.Fatalf("graph has no node %q", successor)
	}
	meta := g.NodeAt(nodeIdx)
	unitIdx := g.UnitIndexForNode(nodeIdx)
	taskType := engine.TaskTypeNodeExec
	if g.UnitKindAt(unitIdx) == graph.UnitGroup {
		taskType = engine.TaskTypeGroupExec
	}
	arrival := engine.DownstreamArrival{
		NodeName:     meta.Name,
		NodeIdx:      nodeIdx,
		UnitIdx:      unitIdx,
		ArrivalCount: 1,
		MergeMode:    meta.MergeMode,
		ExecTaskType: taskType,
	}
	if activePort != "" {
		arrival.ActiveCount = 1
	}
	return arrival
}

// seedEntryFor builds the execute intent for a unit the seed has to start
// without an arrival reaching it — a from-node that is a root.
func seedEntryFor(t *testing.T, id types.ExecutionID, g *graph.Graph, name string) engine.OutboxEntry {
	t.Helper()
	unit := seedUnitResolving(t, g, name, "main", nil)
	return engine.OutboxEntry{
		ID: "seed/" + string(id) + "/" + name,
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    name,
			NodeIdx:     unit.NodeIdx,
			UnitIdx:     unit.UnitIdx,
			Type:        engine.TaskTypeNodeExec,
		},
	}
}

// seedSnapshot is the execution a case creates, as the engine builds it: a
// fresh id, the compiled graph, and the status a caller without an admission
// asks for.
func seedSnapshot(t *testing.T, g *graph.Graph) *engine.ExecutionSnapshot {
	t.Helper()
	return &engine.ExecutionSnapshot{ID: engine.NewExecutionID(), Graph: g, Status: types.ExecutionStatusRunning}
}

// seedOutboxName reports whether the given intent list names the unit, and with
// which task type. Membership rather than position: the order intents are
// listed in is the backend's, and a case asserting an index would be pinning
// an implementation detail of whichever backend ran first.
func seedOutboxName(entries []engine.OutboxEntry, name string) (engine.TaskType, bool) {
	for _, entry := range entries {
		if entry.Task.NodeName == name {
			return entry.Task.Type, true
		}
	}
	return 0, false
}

// seedRecordsTheResolvedUnits pins the RECORDED half: the execution exists, the
// resolved unit reads back as its resolution — status, output and port — and
// nothing was queued for it.
func seedRecordsTheResolvedUnits(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := seedContractGraph(t, "seed-records")
	seeder := seedCapability(t, state)
	snap := seedSnapshot(t, g)

	output := map[string]any{"value": float64(41)}
	res, err := seeder.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: snap,
		Units:     []engine.SeedUnit{seedUnitResolving(t, g, "start", "main", output, "mid")},
	})
	if err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}
	if res.Queued {
		t.Fatalf("SeedExecution() Queued = true with no admission: %+v", res)
	}
	if got := executionStatus(t, state, ctx, snap.ID); got != types.ExecutionStatusRunning {
		t.Fatalf("status after the seed = %q, want %q", got, types.ExecutionStatusRunning)
	}

	node, err := state.GetNode(ctx, snap.ID, "start")
	if err != nil {
		t.Fatalf("GetNode(start) error = %v", err)
	}
	if node == nil {
		t.Fatal("GetNode(start) returned no node: the resolved unit was not recorded")
	}
	if node.Status != types.NodeStatusSuccess {
		t.Fatalf("node start status = %q, want %q", node.Status, types.NodeStatusSuccess)
	}
	if node.Port != "main" {
		t.Fatalf("node start port = %q, want %q", node.Port, "main")
	}
	// The output is read through Outputs rather than off the node snapshot: a
	// snapshot that carries Output as well is a backend's own convenience, not
	// part of the StateStore contract, and the assertion has to hold for the
	// backends that only serve it through GetOutput.
	got, err := state.GetOutput(ctx, snap.ID, "start")
	if err != nil {
		t.Fatalf("GetOutput(start) error = %v", err)
	}
	if got == nil || got["value"] != output["value"] {
		t.Fatalf("output of start = %#v, want %#v", got, output)
	}

	if typ, found := seedOutboxName(readyOutbox(t, state, ctx, snap.ID), "start"); found {
		t.Fatalf("a resolved unit was scheduled as %v; the seed records it, it does not run it", typ)
	}
}

// seedDeliversWhatTheArrivalsSchedule pins the SCHEDULED half for an execute: a
// successor whose in-degree the seeded arrivals satisfy is queued, and the unit
// beyond it is not touched.
func seedDeliversWhatTheArrivalsSchedule(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := seedContractGraph(t, "seed-delivers")
	seeder := seedCapability(t, state)
	snap := seedSnapshot(t, g)

	if _, err := seeder.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: snap,
		Units:     []engine.SeedUnit{seedUnitResolving(t, g, "start", "main", map[string]any{"value": 1}, "mid")},
	}); err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}

	entries := readyOutbox(t, state, ctx, snap.ID)
	if typ, found := seedOutboxName(entries, "mid"); !found || typ != engine.TaskTypeNodeExec {
		t.Fatalf("ready outbox = %+v, want an execute intent for mid: the seeded arrival satisfies its "+
			"in-degree", seedOutboxNames(entries))
	}
	if typ, found := seedOutboxName(entries, "tail"); found {
		t.Fatalf("tail was scheduled as %v by an arrival that never reached it; only the resolved unit's "+
			"own successors are touched", typ)
	}
}

// seedSkipsWhatTheArrivalsSkip pins the skip half of the arrival application: a
// resolved unit that produced no active port schedules its successor as a SKIP,
// not as an execute, and reports it.
func seedSkipsWhatTheArrivalsSkip(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := seedContractGraph(t, "seed-skips")
	seeder := seedCapability(t, state)
	snap := seedSnapshot(t, g)

	res, err := seeder.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: snap,
		Units:     []engine.SeedUnit{seedSkippedUnit(t, g, "start", "mid")},
	})
	if err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}
	entries := readyOutbox(t, state, ctx, snap.ID)
	if typ, found := seedOutboxName(entries, "mid"); !found || typ != engine.TaskTypeNodeSkip {
		t.Fatalf("ready outbox = %+v, want a skip intent for mid: an inactive arrival reaches it and must "+
			"not start it", seedOutboxNames(entries))
	}
	found := false
	for _, skip := range res.Skipped {
		if skip.NodeName == "mid" {
			found = true
		}
	}
	if !found {
		t.Fatalf("SeedExecution() Skipped = %+v, want mid: a seed is a scheduling transition and reports "+
			"the skips it decided", res.Skipped)
	}
}

// seedDeliversTheEntries pins the other way ready work reaches the store: an
// intent the caller carries, for a unit no arrival can reach.
func seedDeliversTheEntries(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := seedContractGraph(t, "seed-entries")
	seeder := seedCapability(t, state)
	snap := seedSnapshot(t, g)

	if _, err := seeder.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: snap,
		Entries:   []engine.OutboxEntry{seedEntryFor(t, snap.ID, g, "mid")},
	}); err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}
	entries := readyOutbox(t, state, ctx, snap.ID)
	if typ, found := seedOutboxName(entries, "mid"); !found || typ != engine.TaskTypeNodeExec {
		t.Fatalf("ready outbox = %+v, want the entry the request carried", seedOutboxNames(entries))
	}
}

// seedNeverSchedulesAResolvedUnit is the case a seed is most likely to get
// wrong: a resolved unit is still a unit of the graph, so arrivals from the
// other resolutions reach its counters. It must stay resolved — a backend that
// let the arrival schedule it would re-run a unit whose result the seeded
// execution was built on — while its own arrival still schedules the unit
// beyond it.
func seedNeverSchedulesAResolvedUnit(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := seedContractGraph(t, "seed-never-schedules")
	seeder := seedCapability(t, state)
	snap := seedSnapshot(t, g)

	if _, err := seeder.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: snap,
		Units: []engine.SeedUnit{
			seedUnitResolving(t, g, "start", "main", map[string]any{"value": 1}, "mid"),
			seedUnitResolving(t, g, "mid", "main", map[string]any{"value": 2}, "tail"),
		},
	}); err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}

	entries := readyOutbox(t, state, ctx, snap.ID)
	if typ, found := seedOutboxName(entries, "mid"); found {
		t.Fatalf("mid was scheduled as %v although the request resolved it", typ)
	}
	if typ, found := seedOutboxName(entries, "tail"); !found || typ != engine.TaskTypeNodeExec {
		t.Fatalf("ready outbox = %+v, want an execute intent for tail: the resolved mid propagates its "+
			"own arrival", seedOutboxNames(entries))
	}
}

// seedFinalizesWhenEverythingIsResolved pins the boundary the seeded execution
// must not fall off: with every unit resolved there is no work left, so the
// execution completes in the seeding transition instead of sitting at zero
// remaining forever with nothing queued.
func seedFinalizesWhenEverythingIsResolved(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := seedContractGraph(t, "seed-finalizes")
	seeder := seedCapability(t, state)
	snap := seedSnapshot(t, g)

	if _, err := seeder.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: snap,
		Units: []engine.SeedUnit{
			seedUnitResolving(t, g, "start", "main", map[string]any{"value": 1}, "mid"),
			seedUnitResolving(t, g, "mid", "main", map[string]any{"value": 2}, "tail"),
			seedUnitResolving(t, g, "tail", "main", map[string]any{"value": 3}),
		},
	}); err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}

	if got := executionStatus(t, state, ctx, snap.ID); got != types.ExecutionStatusSuccess {
		t.Fatalf("status after seeding every unit = %q, want %q: nothing is left to run", got,
			types.ExecutionStatusSuccess)
	}
	if entries := readyOutbox(t, state, ctx, snap.ID); len(entries) != 0 {
		t.Fatalf("ready outbox = %+v, want none: every unit was resolved", seedOutboxNames(entries))
	}
}

// seedRecordsTheRerunOrigin pins the link a seeded execution carries: the
// execution it derives from and the node it starts at. Both are what an
// operator reads to tell a rerun from an original run, and the seed is the only
// creation path that produces them.
func seedRecordsTheRerunOrigin(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := seedContractGraph(t, "seed-origin")
	seeder := seedCapability(t, state)
	snap := seedSnapshot(t, g)
	snap.ParentID = "exec-seed-parent"
	snap.RerunFrom = "mid"

	if _, err := seeder.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: snap,
		Units:     []engine.SeedUnit{seedUnitResolving(t, g, "start", "main", map[string]any{"value": 1}, "mid")},
	}); err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}

	persisted, err := state.GetExecution(ctx, snap.ID)
	if err != nil {
		t.Fatalf("GetExecution(%q) error = %v", snap.ID, err)
	}
	if persisted == nil {
		t.Fatalf("GetExecution(%q) = nil, want the seeded execution", snap.ID)
	}
	if persisted.ParentID != snap.ParentID {
		t.Fatalf("ParentID = %q, want %q", persisted.ParentID, snap.ParentID)
	}
	if persisted.RerunFrom != snap.RerunFrom {
		t.Fatalf("RerunFrom = %q, want %q", persisted.RerunFrom, snap.RerunFrom)
	}
}

// seedRefusesWithoutPersistingOverTheLimit pins the admission: a seeded
// execution contends for its workflow's slot like any other, and the refusal
// writes nothing — not the execution, not its resolved units.
func seedRefusesWithoutPersistingOverTheLimit(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := seedContractGraph(t, "seed-refuses")
	seeder := seedCapability(t, state)
	admission := concurrencyAdmissionFor(t, g, 1)

	holder, _ := concurrencySnapshot(t, g)
	limited := concurrencyCapability(t, state)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, holder, nil, admission); err != nil {
		t.Fatalf("CreateExecutionWithConcurrency(holder) error = %v", err)
	}

	snap := seedSnapshot(t, g)
	_, err := seeder.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: snap,
		Units:     []engine.SeedUnit{seedUnitResolving(t, g, "start", "main", map[string]any{"value": 1}, "mid")},
		Admission: &admission,
	})
	if !errors.Is(err, engine.ErrConcurrencyLimit) {
		t.Fatalf("SeedExecution() error = %v, want %v", err, engine.ErrConcurrencyLimit)
	}
	persisted, err := state.GetExecution(ctx, snap.ID)
	if err != nil {
		t.Fatalf("GetExecution(%q) error = %v", snap.ID, err)
	}
	if persisted != nil {
		t.Fatalf("GetExecution(%q) = %+v, want nothing: a refused admission persists nothing", snap.ID, persisted)
	}
}

// seedParksAQueuedAdmission pins the queue-mode admission: the seed takes a
// place in line, the resolved units are recorded either way, and the ready work
// waits for the promotion that gives the execution a slot.
func seedParksAQueuedAdmission(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := seedContractGraph(t, "seed-parks")
	seeder := seedCapability(t, state)
	admission := queueAdmissionFor(t, g, 1, 8, 0)

	holder, _ := concurrencySnapshot(t, g)
	limited := concurrencyCapability(t, state)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, holder, nil, admission); err != nil {
		t.Fatalf("CreateExecutionWithConcurrency(holder) error = %v", err)
	}

	snap := seedSnapshot(t, g)
	res, err := seeder.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: snap,
		Units:     []engine.SeedUnit{seedUnitResolving(t, g, "start", "main", map[string]any{"value": 1}, "mid")},
		Admission: &admission,
	})
	if err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}
	if !res.Queued || res.QueuePosition != 1 {
		t.Fatalf("SeedExecution() = %+v, want Queued with position 1 behind the holder", res)
	}
	if got := executionStatus(t, state, ctx, snap.ID); got != types.ExecutionStatusQueued {
		t.Fatalf("status after the parked seed = %q, want %q", got, types.ExecutionStatusQueued)
	}
	// The resolution is recorded even though the execution is parked: it is the
	// execution's starting state, not work waiting for a slot.
	node, err := state.GetNode(ctx, snap.ID, "start")
	if err != nil || node == nil || node.Status != types.NodeStatusSuccess {
		t.Fatalf("GetNode(start) = %+v, %v, want the resolved unit recorded on a queued execution", node, err)
	}
	if entries := readyOutbox(t, state, ctx, snap.ID); len(entries) != 0 {
		t.Fatalf("ready outbox = %+v, want nothing deliverable while queued", seedOutboxNames(entries))
	}
	if entries := parkedOutbox(t, state, ctx, snap.ID); len(entries) == 0 {
		t.Fatal("parked outbox is empty: the ready work must be held, not dropped")
	}
}

// seedOutboxNames renders an intent list for a failure message: names and task
// types only, since that is what the assertions above are about.
func seedOutboxNames(entries []engine.OutboxEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.Task.NodeName+"#"+taskTypeName(entry.Task.Type))
	}
	return out
}

func taskTypeName(taskType engine.TaskType) string {
	switch taskType {
	case engine.TaskTypeNodeExec:
		return "exec"
	case engine.TaskTypeNodeSkip:
		return "skip"
	case engine.TaskTypeNodeResume:
		return "resume"
	case engine.TaskTypeGroupExec:
		return "group"
	}
	return "unknown"
}
