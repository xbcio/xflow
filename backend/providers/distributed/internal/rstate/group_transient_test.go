package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// groupRetentionGraph compiles a durable workflow whose only scheduling unit is
// a co-location group {ingest -> analyze}, with the group in the given mode and
// an optional per-workflow completion TTL.
//
// The execution is deliberately left durable (no Options.Transient): the whole
// point of the group mode is that it applies inside a durable workflow, where
// isTransient answers false and the execution-level shortening never runs.
func groupRetentionGraph(t *testing.T, mode string, completionTTL time.Duration) *graph.Graph {
	t.Helper()
	opts := &types.WorkflowOptions{}
	if completionTTL > 0 {
		opts.TransientCompletionTTL = completionTTL
	}
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "group-retention",
		Nodes: []types.NodeDef{
			{Name: "ingest", Kind: types.NodeKindTrigger},
			{Name: "analyze", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"ingest": {"main": {Targets: []types.Connection{{Node: "analyze", Input: "main"}}}}},
		Groups:      []types.GroupDef{{Name: "edge", Members: []string{"ingest", "analyze"}, Mode: mode}},
		Options:     opts,
	})
	if err != nil {
		t.Fatalf("compile group graph: %v", err)
	}
	return g
}

func newGroupRetentionStore(t *testing.T, execTTL time.Duration) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, nil, execTTL), mr
}

// seedGroupExecution creates a durable execution for g and acquires the group's
// lease, leaving the group unit running — the state a terminal transition will
// find it in.
func seedGroupExecution(t *testing.T, state *Store, id types.ExecutionID, g *graph.Graph, leaseTTL time.Duration) *engine.GroupLease {
	t.Helper()
	ctx := context.Background()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning, Graph: g}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	gu := g.Groups()[0].UnitIdx
	lease := &engine.GroupLease{LeaseID: "L-1", LeaseToken: "tok-1", Attempt: 1,
		ExecutionID: id, GroupUnitIdx: gu, GroupID: "edge",
		IssuedAt: time.Now().UTC(), TTL: leaseTTL}
	ok, err := state.AcquireGroupLease(ctx, lease)
	if err != nil || !ok {
		t.Fatalf("AcquireGroupLease() = %v, %v; want true, nil", ok, err)
	}
	return lease
}

// commitGroupToTerminal runs the lease's commit (the group is the execution's
// only unit, so its commit finalizes the execution).
func commitGroupToTerminal(t *testing.T, state *Store, lease *engine.GroupLease) engine.GroupCommitResult {
	t.Helper()
	res, err := state.CommitGroup(context.Background(), engine.GroupCommitRequest{
		ExecutionID: lease.ExecutionID, GroupUnitIdx: lease.GroupUnitIdx,
		LeaseToken: lease.LeaseToken, Attempt: lease.Attempt,
		Outcome: engine.GroupOutcomeSuccess,
		Exits:   []engine.GroupExitResult{{NodeName: "analyze", Port: "main", Data: map[string]any{"k": "v"}}},
	})
	if err != nil {
		t.Fatalf("CommitGroup() error = %v", err)
	}
	return res
}

// assertKeyTTL pins a live key's remaining TTL to want. miniredis does not
// advance its clock on its own, so the tolerance only has to absorb the test's
// own elapsed wall-clock (a TTL of exactly `want` is set at write time and read
// back a few microseconds later).
func assertKeyTTL(t *testing.T, mr *miniredis.Miniredis, key string, want time.Duration) {
	t.Helper()
	if !mr.Exists(key) {
		t.Fatalf("%s does not exist; TTL assertion is vacuous without it", key)
	}
	if got := mr.TTL(key); got != want {
		t.Fatalf("%s TTL = %v, want %v", key, got, want)
	}
}

// TestTransientGroupRetention_ShortensAtExecutionTerminal is the core promise of
// groups[].mode: transient inside a durable workflow: while the execution runs
// the group's keys keep the full active TTL, and the moment the execution is
// terminal they drop to the completion TTL.
//
// The assertions are split deliberately. The exit output and the group's fence
// keys must be shortened; the EXECUTION's own status key must not be, because
// the group's mode is the group's own declaration and a durable workflow's
// audit trail does not become ephemeral because one group did.
func TestTransientGroupRetention_ShortensAtExecutionTerminal(t *testing.T) {
	state, mr := newGroupRetentionStore(t, time.Hour)
	ctx := context.Background()
	id := types.ExecutionID("exec-transient-group")
	g := groupRetentionGraph(t, types.GroupModeTransient, 20*time.Second)
	lease := seedGroupExecution(t, state, id, g, 30*time.Second)

	tns := namespace.Default
	// Before the execution is terminal nothing is shortened: the downstream and
	// Inspect reads the mode promises to keep safe happen in this window.
	assertKeyTTL(t, mr, groupUnitStatusKey(tns, id, 0), time.Hour)

	res := commitGroupToTerminal(t, state, lease)
	if !res.Applied || !res.ExecutionDone {
		t.Fatalf("commit = applied:%v done:%v, want true/true", res.Applied, res.ExecutionDone)
	}
	if state.isTransient(ctx, id) {
		t.Fatal("the execution resolved as transient; the group mode must not turn the whole execution ephemeral")
	}

	assertKeyTTL(t, mr, outputKey(tns, id, "analyze"), 20*time.Second)
	// Fence keys: max(completion TTL, one full lease window x the marker factor)
	// = max(20s, 2x30s). The floor is what keeps a late commit's duplicate
	// classification alive for as long as any lease that could present a token.
	assertKeyTTL(t, mr, groupUnitStatusKey(tns, id, 0), 60*time.Second)
	assertKeyTTL(t, mr, groupUnitMetaKey(tns, id, 0), 60*time.Second)
	// The durable execution's own state is untouched.
	assertKeyTTL(t, mr, execKey(tns, id, "status"), time.Hour)
}

// TestTransientGroupRetention_DefaultsWhenNothingConfigured pins the third
// resolution step: a durable deployment that configured no transient retention
// and a workflow that names none still gets the built-in completion TTL,
// because a mode that silently retains everything is the no-op the compile-time
// validation exists to prevent.
func TestTransientGroupRetention_DefaultsWhenNothingConfigured(t *testing.T) {
	state, mr := newGroupRetentionStore(t, time.Hour)
	id := types.ExecutionID("exec-transient-group-default")
	g := groupRetentionGraph(t, types.GroupModeTransient, 0)
	lease := seedGroupExecution(t, state, id, g, 30*time.Second)

	res := commitGroupToTerminal(t, state, lease)
	if !res.ExecutionDone {
		t.Fatalf("commit did not finalize the execution: %+v", res)
	}

	assertKeyTTL(t, mr, outputKey(namespace.Default, id, "analyze"), defaultGroupCompletionTTL)
}

// TestTransientGroupRetention_LeavesDurableGroupsAlone is the negative control:
// the same execution, same terminal transition, one group with the mode left
// empty. Nothing may be shortened.
func TestTransientGroupRetention_LeavesDurableGroupsAlone(t *testing.T) {
	state, mr := newGroupRetentionStore(t, time.Hour)
	id := types.ExecutionID("exec-durable-group")
	g := groupRetentionGraph(t, types.GroupModeDurable, 20*time.Second)
	lease := seedGroupExecution(t, state, id, g, 30*time.Second)

	res := commitGroupToTerminal(t, state, lease)
	if !res.ExecutionDone {
		t.Fatalf("commit did not finalize the execution: %+v", res)
	}

	assertKeyTTL(t, mr, outputKey(namespace.Default, id, "analyze"), time.Hour)
	assertKeyTTL(t, mr, groupUnitStatusKey(namespace.Default, id, 0), time.Hour)
}

// TestTransientGroupRetention_CancelPathShortens covers the second terminal
// transition: an execution canceled while its group never committed. The
// group's lease keys are exactly what is left behind in that shape, and they
// are what a cancellation would otherwise retain for the full active TTL.
func TestTransientGroupRetention_CancelPathShortens(t *testing.T) {
	state, mr := newGroupRetentionStore(t, time.Hour)
	ctx := context.Background()
	id := types.ExecutionID("exec-transient-group-cancel")
	g := groupRetentionGraph(t, types.GroupModeTransient, 20*time.Second)
	seedGroupExecution(t, state, id, g, 30*time.Second)

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceled) error = %v", err)
	}

	assertKeyTTL(t, mr, groupUnitStatusKey(namespace.Default, id, 0), 60*time.Second)
	assertKeyTTL(t, mr, groupUnitMetaKey(namespace.Default, id, 0), 60*time.Second)
}

// TestTransientGroupRetention_NeverExtends pins the cap: shortening must never
// lengthen a key's life, so a completion TTL larger than the execution's own
// active TTL resolves to the active TTL instead.
func TestTransientGroupRetention_NeverExtends(t *testing.T) {
	state, _ := newGroupRetentionStore(t, 10*time.Second)
	ctx := context.Background()
	id := types.ExecutionID("exec-transient-group-cap")
	g := groupRetentionGraph(t, types.GroupModeTransient, time.Hour)
	seedGroupExecution(t, state, id, g, 30*time.Second)

	if got := state.transientGroupCompletionTTL(ctx, id, g); got != 10*time.Second {
		t.Fatalf("transientGroupCompletionTTL() = %v, want the active TTL 10s: "+
			"a completion TTL longer than the key's own lifetime must not extend it", got)
	}
}

// recordingAuditStore records the SQL surface the group path is allowed to
// touch. store.Store is embedded so an unexpected call panics on the nil
// interface rather than being silently absorbed — the assertion here is "no
// node projection happens", and a fake that quietly no-ops unknown methods
// could not make it.
type recordingAuditStore struct {
	store.Store
	execCreates  int
	statusWrites int
	nodeUpserts  int
}

func (r *recordingAuditStore) CreateExecution(context.Context, *store.ExecutionRecord) error {
	r.execCreates++
	return nil
}

func (r *recordingAuditStore) UpdateExecutionStatus(context.Context, types.ExecutionID, types.ExecutionStatus, string) error {
	r.statusWrites++
	return nil
}

func (r *recordingAuditStore) UpsertNode(context.Context, *store.NodeRecord) error {
	r.nodeUpserts++
	return nil
}

// GetExecution answers "the row exists", the durable verdict. The terminal
// transition evicts the transient cache before projecting the status, so this
// read is what the re-resolution lands on; returning ErrNotFound would declare
// the execution transient and skip the status projection the test asserts.
func (r *recordingAuditStore) GetExecution(context.Context, types.ExecutionID) (*store.ExecutionRecord, error) {
	return &store.ExecutionRecord{}, nil
}

// TestTransientGroupRetention_WritesNoNodeProjection pins the design's "the
// group's exits do not reach the SQL node projection" against a regression: the
// commit path writes boundary exits, and a future projection added there would
// persist payloads the mode promised not to persist (transient is what keeps
// potentially-credential-bearing output out of the platform database).
//
// The execution row itself keeps being projected — only the exits are exempt.
func TestTransientGroupRetention_WritesNoNodeProjection(t *testing.T) {
	rec := &recordingAuditStore{}
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	state := New(rdb, rec, time.Hour)

	id := types.ExecutionID("exec-transient-group-sql")
	g := groupRetentionGraph(t, types.GroupModeTransient, 20*time.Second)
	lease := seedGroupExecution(t, state, id, g, 30*time.Second)
	res := commitGroupToTerminal(t, state, lease)
	if !res.ExecutionDone {
		t.Fatalf("commit did not finalize the execution: %+v", res)
	}

	if rec.nodeUpserts != 0 {
		t.Fatalf("UpsertNode called %d time(s) for a transient group's exits; "+
			"the exits must not be projected into SQL", rec.nodeUpserts)
	}
	if rec.execCreates != 1 || rec.statusWrites != 1 {
		t.Fatalf("execution projections = create:%d status:%d, want 1/1: the execution row "+
			"is projected normally; only the group's exits are exempt",
			rec.execCreates, rec.statusWrites)
	}
}
