package rstate

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// The status key is what makes an execution exist for this store: executionDeadline
// reads "status present, deadline_ms absent" as "no time budget" and a replica
// remembers that answer, so a status key written for an execution nothing created
// poisons the retention of the execution that is created under that ID later.
// updateExecutionStatusLua refuses to be the writer (TestStatusWriteOnAnAbsent
// ExecutionWritesNothing); this file asserts it of every other write method.
//
// TestAbsentExecutionGetsNoStatusKey calls each method in absentWriteCases once
// with an ID nothing ever created and requires that no status key exists after.
// TestAbsentWriteInventoryCoversEveryStoreMethod keeps the table honest: it
// enumerates the exported methods of *Store by reflection and fails on any method
// that is in neither the table nor exemptStoreMethods, and on any entry that names
// a method that no longer exists.
//
// To add a Store method: if it writes, add a case to absentWriteCases that calls it
// with well-formed arguments for the given ID (plant whatever dangling state a
// sweeper needs inside the case); if it only reads, or creates the execution as its
// purpose, add it to exemptStoreMethods with the reason. A method in neither fails
// the coverage test, which is the point.

// absentHarness is one store on one miniredis, with a graph the cases put in the
// cache so a method that loads it gets past that step and reaches its script.
type absentHarness struct {
	srv *miniredis.Miniredis
	rdb *redis.Client
	s   *Store
	g   *graph.Graph
}

type absentCase func(ctx context.Context, h *absentHarness, id types.ExecutionID)

func absentTask(id types.ExecutionID) engine.Task {
	return engine.Task{ExecutionID: id, NodeName: "a", NodeIdx: 0, Type: engine.TaskTypeNodeExec}
}

func absentLease(id types.ExecutionID) *engine.TaskLease {
	return &engine.TaskLease{
		LeaseID: "lease-1", LeaseToken: "token-1", Attempt: 1, Task: absentTask(id),
		NodeType: "test.echo", IssuedAt: time.Now(), TTL: time.Minute, Namespace: namespace.Default,
	}
}

func absentGroupLease(id types.ExecutionID) *engine.GroupLease {
	return &engine.GroupLease{
		LeaseID: "glease-1", LeaseToken: "gtoken-1", Attempt: 1, ExecutionID: id,
		GroupUnitIdx: 0, GroupID: "g", IssuedAt: time.Now(), TTL: time.Minute, Namespace: namespace.Default,
	}
}

func absentEntry(id types.ExecutionID) engine.OutboxEntry {
	return engine.OutboxEntry{ID: "root/" + string(id) + "/a/0", Task: absentTask(id)}
}

func absentSuspendSpec() *types.SuspendSpec {
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"sig"}}
}

// absentWriteCases is every method that can write and takes an execution, plus the
// sweepers that reach one through state they find in Redis. Errors are ignored: a
// refusal is the expected answer, and the assertion is on what is left behind.
var absentWriteCases = map[string]absentCase{
	// A conclusion is its own SET under the parent's key space; it must not
	// invent the parent's status key when the parent was never created.
	"RecordSubWorkflowConclusion": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_ = h.s.RecordSubWorkflowConclusion(ctx, id, "a", engine.SubWorkflowConclusion{
			ChildExecutionID: types.ExecutionID("child-" + string(id)),
			Status:           types.ExecutionStatusSuccess,
		})
	},
	"UpdateExecutionStatus": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_ = h.s.UpdateExecutionStatus(ctx, id, types.ExecutionStatusTimeout, "err")
	},
	"PauseExecution":  func(ctx context.Context, h *absentHarness, id types.ExecutionID) { _ = h.s.PauseExecution(ctx, id) },
	"ResumeExecution": func(ctx context.Context, h *absentHarness, id types.ExecutionID) { _ = h.s.ResumeExecution(ctx, id) },
	// CheckCompletion: the graph is cached and every node reads success, but the
	// execution's own status key is gone, which is what checkCompletionLua used to
	// finalize into a bare status. (This backend answers (false, false, nil) here;
	// see CheckCompletion for why that differs from the local backend.)
	"CheckCompletion": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		for _, name := range []string{"a", "b"} {
			_ = h.srv.Set(nodeStatusKey(namespace.Default, id, name), string(types.NodeStatusSuccess))
		}
		_, _, _ = h.s.CheckCompletion(ctx, id, 2)
	},
	"AcquireTaskLease": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _, _ = h.s.AcquireTaskLease(ctx, absentLease(id))
	},
	"AcquireTaskLeaseChecked": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.AcquireTaskLeaseChecked(ctx, absentLease(id), engine.LeaseAcquireOptions{})
	},
	"ClaimTaskLease": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _, _ = h.s.ClaimTaskLease(ctx, absentLease(id))
	},
	"UpsertNode": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_ = h.s.UpsertNode(ctx, &engine.NodeSnapshot{ExecutionID: id, Name: "a", Status: types.NodeStatusRunning})
	},
	"CommitNode": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.CommitNode(ctx, engine.CommitNodeRequest{
			ExecutionID: id, NodeName: "a", NodeIdx: 0, LeaseID: "lease-1", LeaseToken: "token-1", Attempt: 1,
			Status: types.NodeStatusSuccess,
		})
	},
	"CommitLeasedNode": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.CommitLeasedNode(ctx, engine.CommitNodeRequest{
			ExecutionID: id, NodeName: "a", NodeIdx: 0, LeaseID: "lease-1", LeaseToken: "token-1", Attempt: 1,
			Status: types.NodeStatusSuccess,
		})
	},
	"AdvanceNode": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.AdvanceNode(ctx, engine.AdvanceNodeRequest{
			ExecutionID: id, NodeName: "a", NodeIdx: 0,
			Arrivals: []engine.DownstreamArrival{{
				NodeName: "b", NodeIdx: 1, UnitIdx: 1, ArrivalCount: 1, ActiveCount: 1,
				ExecTaskType: engine.TaskTypeNodeExec,
			}},
		})
	},
	"CommitGroup": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.CommitGroup(ctx, engine.GroupCommitRequest{
			ExecutionID: id, GroupUnitIdx: 0, GroupID: "g", LeaseID: "glease-1", LeaseToken: "gtoken-1", Attempt: 1,
			Outcome: engine.GroupOutcomeSuccess,
		})
	},
	"AcquireGroupLease": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.AcquireGroupLease(ctx, absentGroupLease(id))
	},
	"RenewGroupLease": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.RenewGroupLease(ctx, id, 0, "gtoken-1", time.Now().Add(time.Minute))
	},
	"ExpireGroupLease": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.ExpireGroupLease(ctx, id, 0, "gtoken-1")
	},
	"RevokeGroupLeaseWithOutbox": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.RevokeGroupLeaseWithOutbox(ctx, id, 0, "gtoken-1", absentEntry(id))
	},
	"RevokeLease": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.RevokeLease(ctx, id, "a", "token-1")
	},
	"RevokeLeaseWithOutbox": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.RevokeLeaseWithOutbox(ctx, id, "a", "token-1", absentEntry(id))
	},
	"ResetNodeForRetryWithOutbox": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.ResetNodeForRetryWithOutbox(ctx, id, "a", "token-1", absentEntry(id))
	},
	"RenewTaskLease": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.RenewTaskLease(ctx, id, "a", "token-1", time.Now().Add(time.Minute))
	},
	"SuspendTaskLease": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _, _ = h.s.SuspendTaskLease(ctx, absentLease(id), nil, false, false, absentSuspendSpec(), "")
	},
	"SuspendTaskLeaseWithOutbox": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.SuspendTaskLeaseWithOutbox(ctx, absentLease(id), nil, false, false, absentSuspendSpec(), "")
	},
	"SuspendOrConsume": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.SuspendOrConsume(ctx, id, "a", absentSuspendSpec())
	},
	"ResuspendAtomic": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.ResuspendAtomic(ctx, id, "a", "old", "new", absentSuspendSpec())
	},
	"DeliverSignal": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _, _ = h.s.DeliverSignal(ctx, id, "sig", map[string]any{"k": 1})
	},
	"DeliverSignalWithOutbox": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _, _, _ = h.s.DeliverSignalWithOutbox(ctx, id, "sig", map[string]any{"k": 1}, engine.ResumeIntent{NodeName: "a"})
	},
	"RevokeSignal": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.RevokeSignal(ctx, id, "sig")
	},
	"AcquireResumeLock": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.AcquireResumeLock(ctx, id, "a")
	},
	"CancelSuspendedNode": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.CancelSuspendedNode(ctx, id, "a")
	},
	"BeginTaskExpansion": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.BeginTaskExpansion(ctx, absentLease(id))
	},
	"BeginTaskExpansionWithOutbox": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.BeginTaskExpansionWithOutbox(ctx, absentLease(id),
			[]engine.SubExecution{{ParentExecID: id, ParentNode: "a", ChildExecID: "child"}},
			[]engine.OutboxEntry{absentEntry(id)})
	},
	"BeginSubWorkflowWait": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.BeginSubWorkflowWait(ctx, absentLease(id), engine.SubWorkflowLink{
			ChildExecutionID: "child", ParentNode: "a", LeaseID: "lease-1", LeaseToken: "token-1", Attempt: 1,
		}, []engine.OutboxEntry{absentEntry(id)})
	},
	"CompleteSubExecution": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.CompleteSubExecution(ctx, id, "a", "child", types.ExecutionStatusSuccess, map[string]any{"k": 1})
	},
	"CompleteExpandedSubExecution": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _, _, _ = h.s.CompleteExpandedSubExecution(ctx, absentLease(id), "child", types.ExecutionStatusSuccess, map[string]any{"k": 1})
	},
	"CreateSubExecution": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_ = h.s.CreateSubExecution(ctx, &engine.SubExecution{ParentExecID: id, ParentNode: "a", ChildExecID: "child", Status: types.ExecutionStatusRunning})
	},
	"CreateExpandedSubExecution": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.CreateExpandedSubExecution(ctx, absentLease(id),
			&engine.SubExecution{ParentExecID: id, ParentNode: "a", ChildExecID: "child", Status: types.ExecutionStatusRunning})
	},
	"ReplayDeadLetter": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.ReplayDeadLetter(ctx, engine.ReplayDeadLetterRequest{
			ExecutionID: id, EntryID: "e", RequestID: "r", Operator: "op", Reason: "why",
		})
	},
	"ClaimIdempotency": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.ClaimIdempotency(ctx, engine.IdempotencyClaimRequest{
			ExecutionID: id, Fingerprint: "fp", TTL: time.Minute, Lease: time.Second, Token: "token-1",
		})
	},
	"ConfirmIdempotency": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_ = h.s.ConfirmIdempotency(ctx, id, "token-1")
	},
	"ReleaseIdempotency": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_ = h.s.ReleaseIdempotency(ctx, id, "token-1")
	},
	"PutOutput": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_ = h.s.PutOutput(ctx, id, "a", map[string]any{"k": 1})
	},
	"DecrementInDegree": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _, _ = h.s.DecrementInDegree(ctx, id, 0, true)
	},
	"LeaseOutbox": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.LeaseOutbox(ctx, id, time.Now(), 10)
	},
	"RenewOutbox": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.RenewOutbox(ctx, id, []engine.OutboxEntry{absentEntry(id)}, time.Now())
	},
	"ReleaseOutbox": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_ = h.s.ReleaseOutbox(ctx, id, absentEntry(id))
	},
	"AckOutbox": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_ = h.s.AckOutbox(ctx, id, absentEntry(id).ID)
	},
	"RecordOutboxFailure": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_, _ = h.s.RecordOutboxFailure(ctx, id, absentEntry(id), 3)
	},
	"AckDeadlineExpired": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_ = h.s.AckDeadlineExpired(ctx, id)
	},
	"PublishExecutionEvent": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		_ = h.s.PublishExecutionEvent(ctx, engine.ExecutionEvent{ExecutionID: id, Status: types.ExecutionStatusTimeout})
	},

	// The sweepers take no execution; they reach one through state they find. Each
	// case plants the dangling state for the ID and lets the sweep act on it.

	// ExpireQueuedExecutions: a line member whose queue deadline is due and whose
	// execution was never written (the window between admission and creation).
	"ExpireQueuedExecutions": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		tag := workflowDigestTag(namespace.Default, "absent-expiry")
		h.s.registerConcurrencyWorkflow(ctx, namespace.Default, tag, 0)
		h.rdb.ZAdd(ctx, concurrencyQueueKeyForTag(namespace.Default, tag), redis.Z{Score: 1, Member: concurrencySlotMember(id)})
		h.rdb.ZAdd(ctx, concurrencyDeadlineKeyForTag(namespace.Default, tag), redis.Z{Score: 1, Member: concurrencySlotMember(id)})
		_, _ = h.s.ExpireQueuedExecutions(ctx, 0)
	},
	// ReconcileConcurrencySlots: a slot older than the stale grace whose execution
	// never landed, with the same ID waiting at the head of the line, so the sweep
	// both clears the slot and promotes the head it frees room for.
	"ReconcileConcurrencySlots": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		tag := workflowDigestTag(namespace.Default, "absent-reconcile")
		h.s.registerConcurrencyWorkflow(ctx, namespace.Default, tag, 0)
		old := time.Now().Add(-time.Hour).Unix()
		h.rdb.ZAdd(ctx, concurrencySlotKeyForTag(namespace.Default, tag), redis.Z{Score: float64(old), Member: concurrencySlotMember(id)})
		h.rdb.ZAdd(ctx, concurrencyQueueKeyForTag(namespace.Default, tag), redis.Z{Score: 1, Member: concurrencySlotMember(id)})
		h.rdb.Set(ctx, concurrencyCapacityKeyForTag(namespace.Default, tag), "1", time.Hour)
		_, _ = h.s.ReconcileConcurrencySlots(ctx, 0)
	},
	// RepairLeaseIndex: a node status and meta left behind with the execution's own
	// keys gone.
	"RepairLeaseIndex": func(ctx context.Context, h *absentHarness, id types.ExecutionID) {
		h.rdb.Set(ctx, nodeStatusKey(namespace.Default, id, "a"), "running", time.Hour)
		h.rdb.HSet(ctx, nodeMetaKey(namespace.Default, id, "a"), "lease_token", "token-1", "lease_deadline_ms", time.Now().Add(time.Minute).UnixMilli())
		_, _ = h.s.RepairLeaseIndex(ctx, 0)
	},
}

// exemptStoreMethods are the exported methods that do not write an execution's
// status key on behalf of an execution a caller names, with the reason.
var exemptStoreMethods = map[string]string{
	// Creation: the purpose is to write the status. The order of what it writes
	// beside it is pinned by execution_deadline_seed_order_test.go and
	// TestCreationPathsRememberTheDeadline.
	"CreateExecution":                       "creates the execution",
	"CreateExecutionWithConcurrency":        "creates the execution",
	"CreateExecutionWithOutbox":             "creates the execution",
	"SeedExecution":                         "creates the execution",
	"SeedExecutionFromEntry":                "creates the execution",
	"SeedExecutionFromEntryWithConcurrency": "creates the execution",

	// Readers. They return what is there; none of them writes a key.
	"GetExecution":               "reader",
	"GetExecutionInputContext":   "reader",
	"GetExecutionRetention":      "reader",
	"GetExecutionStatus":         "reader",
	"GetExecutionStatuses":       "reader",
	"GetExecutionTerminalStatus": "reader",
	"GetExecutionTraceCarrier":   "reader",
	"GetGroupLease":              "reader",
	"GetGroupUnitPhase":          "reader",
	"GetNode":                    "reader",
	"GetOutput":                  "reader",
	"GetOutputs":                 "reader",
	"GetSubExecutionResults":     "reader",
	"GetSubWorkflowLink":         "reader",
	"GetSubWorkflowConclusion":   "reader",
	"ResolveItems":               "reader; resolves a blob reference",
	"ListDeadLetters":            "reader",
	"ListOutbox":                 "reader",
	"ListSuspendedNodes":         "reader",
	"LoadGraph":                  "reader",
	"PeekResumeTarget":           "reader",
	"ConcurrencyQueuePosition":   "reader",
	"WatchExecution":             "subscribes to the done channel",

	// Batch-item blobs. ExternalizeItems writes a blob and its link under the
	// execution's key space, never a status key, and does nothing at all (no
	// Redis command) unless ConfigureBlob enabled the layer, so no case can
	// reach it on this harness.
	"ExternalizeItems": "writes a blob and link, never a status; inert without ConfigureBlob",

	// Index and metric scans. They read the keyspace; the only writes they make are
	// to their own index (deadline, canceling, outbox-ready), never to a status.
	"ListExpiredLeases":              "scan; reads lease indexes",
	"ListDeadlineExceededExecutions": "scan; repairs the deadline index only",
	"ListCancelingExecutions":        "scan; repairs the canceling index only",
	"ListOutboxExecutions":           "scan; reads the outbox discovery index",
	"ListOutboxExecutionRefs":        "scan; reads the outbox discovery index across namespaces",
	"OutboxMetrics":                  "metrics read",
	"ScanReplayReceipts":             "scan; reads receipts",
	"AuditStats":                     "in-process counters",
	"FoldsArrivalsIntoCommit":        "capability flag",

	// Wiring. Called before the store serves anything; no Redis write.
	"ConfigureBlob":              "wiring",
	"ConfigureOutboxReadyIndex":  "wiring",
	"ConfigureOutputCompression": "wiring",
	"ConfigureOwnedNamespaces":   "wiring",
	"ConfigureTransient":         "wiring",
	"SetAuditObserver":           "wiring",
	"SetLeaseObserver":           "wiring",
	"SetLogger":                  "wiring",
}

// absentCommandCounter counts the commands sent, and how many of them ran a Lua
// script, so a case that never got past its argument checks cannot pass by
// accident.
type absentCommandCounter struct{ all, scripts atomic.Int64 }

func (c *absentCommandCounter) note(cmd redis.Cmder) {
	c.all.Add(1)
	if name := cmd.Name(); name == "eval" || name == "evalsha" {
		c.scripts.Add(1)
	}
}

func (c *absentCommandCounter) DialHook(next redis.DialHook) redis.DialHook { return next }

func (c *absentCommandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		c.note(cmd)
		return next(ctx, cmd)
	}
}

func (c *absentCommandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			c.note(cmd)
		}
		return next(ctx, cmds)
	}
}

// TestAbsentExecutionGetsNoStatusKey calls every write method once with an ID that
// was never created and requires that no status key exists afterwards.
func TestAbsentExecutionGetsNoStatusKey(t *testing.T) {
	ctx := context.Background()
	srv, rdb, s := newMiniredisStore(t)
	counter := &absentCommandCounter{}
	rdb.AddHook(counter)
	h := &absentHarness{srv: srv, rdb: rdb, s: s, g: twoNodeGraph(t)}

	names := make([]string, 0, len(absentWriteCases))
	for name := range absentWriteCases {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			id := types.ExecutionID("absent-" + name)
			h.s.graphs.Put(id, h.g, 0)
			allBefore, scriptsBefore := counter.all.Load(), counter.scripts.Load()

			absentWriteCases[name](ctx, h, id)

			sent, scripts := counter.all.Load()-allBefore, counter.scripts.Load()-scriptsBefore
			if sent == 0 {
				t.Errorf("%s sent no command to Redis: the case never reached the method's work, so it proves nothing", name)
			}
			t.Logf("%s: %d commands, %d scripts", name, sent, scripts)
			if h.srv.Exists(execKey(namespace.Default, id, "status")) {
				got, _ := h.srv.Get(execKey(namespace.Default, id, "status"))
				t.Errorf("%s left a status key (%q) for an execution nothing created: a replica would read it as \"no time budget\"", name, got)
			}
			if deadline, settled := newReplica(h.rdb).executionDeadline(ctx, id); settled {
				t.Errorf("%s left executionDeadline settled (%v) for an execution nothing created", name, deadline)
			}
		})
	}
}

// TestAbsentWriteInventoryCoversEveryStoreMethod fails on a method that is in
// neither absentWriteCases nor exemptStoreMethods, so a method added later cannot
// quietly skip the check above. It fails the other way too: an entry that names a
// method that no longer exists, and a method listed in both tables.
func TestAbsentWriteInventoryCoversEveryStoreMethod(t *testing.T) {
	storeType := reflect.TypeOf(&Store{})
	methods := make(map[string]bool, storeType.NumMethod())
	for i := 0; i < storeType.NumMethod(); i++ {
		methods[storeType.Method(i).Name] = true
	}

	var unclassified, stale, both []string
	for name := range methods {
		_, written := absentWriteCases[name]
		_, exempt := exemptStoreMethods[name]
		switch {
		case written && exempt:
			both = append(both, name)
		case !written && !exempt:
			unclassified = append(unclassified, name)
		}
	}
	for name := range absentWriteCases {
		if !methods[name] {
			stale = append(stale, "absentWriteCases:"+name)
		}
	}
	for name := range exemptStoreMethods {
		if !methods[name] {
			stale = append(stale, "exemptStoreMethods:"+name)
		}
	}
	sort.Strings(unclassified)
	sort.Strings(stale)
	sort.Strings(both)

	if len(unclassified) > 0 {
		t.Errorf("Store methods in neither absentWriteCases nor exemptStoreMethods: %s. "+
			"A method that writes needs a case that calls it with a never-created ID; one that only reads "+
			"or creates the execution needs an exemption with the reason (see the header of this file)",
			strings.Join(unclassified, ", "))
	}
	if len(stale) > 0 {
		t.Errorf("entries naming a method *Store no longer has: %s", strings.Join(stale, ", "))
	}
	if len(both) > 0 {
		t.Errorf("methods listed as both written and exempt: %s", strings.Join(both, ", "))
	}
}
