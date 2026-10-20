package rstate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestMiniredisCancelingExecutionContract runs the shared canceling-index
// contract against the Redis/Lua backend so its register-before-write behavior
// and age stability cannot drift from the in-memory reference.
func TestMiniredisCancelingExecutionContract(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	statestoretest.RunCancelingExecutionContract(t, func(t *testing.T) engine.StateStore {
		return New(rdb, nil, time.Minute)
	})
}

// TestRedisCancelingExecutionContract runs the same suite against a real Redis
// when XFLOW_TEST_REDIS_ADDR is set. The registration is an ordinary ZADD, but
// the status verification around it pairs a ZRANGEBYSCORE with pipelined GETs —
// worth pinning against the server the deployment actually runs.
func TestRedisCancelingExecutionContract(t *testing.T) {
	addr := realRedisAddr(t)
	statestoretest.RunCancelingExecutionContract(t, func(t *testing.T) engine.StateStore {
		return New(freshRealRedis(t, addr), nil, time.Minute)
	})
}

// TestCancelingIndexScopesByNamespace pins that the index is namespace-scoped
// in both directions: candidates carry the namespace their keys live under, and
// a store configured with an owned-namespace scope reports only that scope.
func TestCancelingIndexScopesByNamespace(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctxA := namespace.WithNamespace(context.Background(), "tenant-a")
	ctxB := namespace.WithNamespace(context.Background(), "tenant-b")
	for _, tc := range []struct {
		ctx context.Context
		id  types.ExecutionID
	}{{ctxA, "exec-ns-a"}, {ctxB, "exec-ns-b"}} {
		if err := state.CreateExecution(tc.ctx, &engine.ExecutionSnapshot{
			ID: tc.id, Status: types.ExecutionStatusRunning,
		}); err != nil {
			t.Fatalf("CreateExecution(%q) error = %v", tc.id, err)
		}
		if err := state.UpdateExecutionStatus(tc.ctx, tc.id, types.ExecutionStatusCanceling, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(canceling, %q) error = %v", tc.id, err)
		}
	}

	candidates, err := state.ListCancelingExecutions(context.Background(), time.Now().Add(time.Hour), 8)
	if err != nil {
		t.Fatalf("ListCancelingExecutions() error = %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("ListCancelingExecutions() = %+v, want both namespaces", candidates)
	}
	byID := make(map[types.ExecutionID]namespace.Namespace, len(candidates))
	for _, candidate := range candidates {
		byID[candidate.ExecutionID] = candidate.Namespace
	}
	if byID["exec-ns-a"] != "tenant-a" || byID["exec-ns-b"] != "tenant-b" {
		t.Fatalf("candidate namespaces = %+v, want tenant-a/tenant-b on their executions", byID)
	}

	state.ConfigureOwnedNamespaces([]namespace.Namespace{"tenant-b"})
	candidates, err = state.ListCancelingExecutions(context.Background(), time.Now().Add(time.Hour), 8)
	if err != nil {
		t.Fatalf("ListCancelingExecutions(owned) error = %v", err)
	}
	if len(candidates) != 1 || candidates[0].ExecutionID != "exec-ns-b" {
		t.Fatalf("ListCancelingExecutions(owned) = %+v, want only tenant-b's execution", candidates)
	}
}

// TestCancelingIndexRepairsStaleEntries covers the entry whose execution left
// canceling without its removal running (the ZREM is best-effort). The read
// must not hand it to the reaper and must delete it.
func TestCancelingIndexRepairsStaleEntries(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := namespace.WithNamespace(context.Background(), "tenant-stale")
	id := types.ExecutionID("exec-canceling-stale")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	// Simulate a lost terminal removal: the status moves on, the index entry
	// stays.
	if err := rdb.Set(ctx, execKey("tenant-stale", id, "status"), string(types.ExecutionStatusCanceled), time.Minute).Err(); err != nil {
		t.Fatalf("overwrite status: %v", err)
	}

	candidates, err := state.ListCancelingExecutions(ctx, time.Now().Add(time.Hour), 8)
	if err != nil {
		t.Fatalf("ListCancelingExecutions() error = %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("ListCancelingExecutions() = %+v, want the stale entry excluded", candidates)
	}
	if score, err := rdb.ZScore(ctx, cancelingIndexKey("tenant-stale"), string(id)).Result(); err != redis.Nil {
		t.Fatalf("stale index entry still present (score=%v, err=%v), want removed", score, err)
	}
}

// failCancelingIndexZAddHook refuses every canceling-index ZADD until the test
// disables it. Returning before next makes the registration fail without ever
// reaching Redis, which is the failure the register-before-write ordering must
// survive.
type failCancelingIndexZAddHook struct {
	key    string
	active atomic.Bool
}

func (h *failCancelingIndexZAddHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failCancelingIndexZAddHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if h.active.Load() && cmd.Name() == "zadd" && len(args) > 1 && fmt.Sprint(args[1]) == h.key {
			return errors.New("injected canceling-index registration failure")
		}
		return next(ctx, cmd)
	}
}

func (h *failCancelingIndexZAddHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestCancelingRegistrationPrecedesStatusWrite pins the ordering that makes the
// index complete: the registration is attempted, and must succeed, BEFORE the
// status write. A failed registration leaves the execution in its previous
// status so Cancel can be retried; an implementation that wrote the status
// first would return the same error with the status already canceling, which
// the first half asserts against, and would leave a canceling execution nothing
// can discover if the process died there.
func TestCancelingRegistrationPrecedesStatusWrite(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := namespace.WithNamespace(context.Background(), "tenant-order")
	id := types.ExecutionID("exec-canceling-order")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	hook := &failCancelingIndexZAddHook{key: cancelingIndexKey("tenant-order")}
	hook.active.Store(true)
	rdb.AddHook(hook)

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err == nil {
		t.Fatal("UpdateExecutionStatus(canceling) error = nil, want the failed registration to fail the transition")
	}
	status, err := rdb.Get(ctx, execKey("tenant-order", id, "status")).Result()
	if err != nil {
		t.Fatalf("read status after the failed transition: %v", err)
	}
	if types.ExecutionStatus(status) != types.ExecutionStatusRunning {
		t.Fatalf("status after a failed registration = %q, want running: the status write ran before the index registration", status)
	}
	if score, err := rdb.ZScore(ctx, cancelingIndexKey("tenant-order"), string(id)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("canceling index member present (score=%v, err=%v) after a failed registration", score, err)
	}

	hook.active.Store(false)
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) retry error = %v", err)
	}
	if _, err := rdb.ZScore(ctx, cancelingIndexKey("tenant-order"), string(id)).Result(); err != nil {
		t.Fatalf("canceling index has no entry after the retried write: %v", err)
	}
}

// TestCancelingIndexDropsEntriesWhoseStatusExpired covers the redis.Nil branch
// of the repair: the execution's status key expired (or the execution never
// existed), so the registration names nothing live. List must not return it and
// must clean it out of the index.
func TestCancelingIndexDropsEntriesWhoseStatusExpired(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := namespace.WithNamespace(context.Background(), "tenant-expired")
	id := types.ExecutionID("exec-canceling-expired")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	if err := rdb.Del(ctx, execKey("tenant-expired", id, "status")).Err(); err != nil {
		t.Fatalf("delete status key: %v", err)
	}

	candidates, err := state.ListCancelingExecutions(ctx, time.Now().Add(time.Hour), 8)
	if err != nil {
		t.Fatalf("ListCancelingExecutions() error = %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("ListCancelingExecutions() = %+v, want the expired execution excluded", candidates)
	}
	if score, err := rdb.ZScore(ctx, cancelingIndexKey("tenant-expired"), string(id)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("expired execution's index member still present (score=%v, err=%v), want removed", score, err)
	}
}

// pauseBeforeCancelingIndexZRemHook holds the canceling-index ZREM before it
// reaches Redis until the test releases it. Blocking before next (rather than
// after) is what makes the interleaving precise: List has already read the
// status and classified the member stale, but has not deleted it yet.
type pauseBeforeCancelingIndexZRemHook struct {
	key     string
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *pauseBeforeCancelingIndexZRemHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *pauseBeforeCancelingIndexZRemHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if cmd.Name() != "zrem" || len(args) < 2 || fmt.Sprint(args[1]) != h.key {
			return next(ctx, cmd)
		}
		blocked := false
		h.once.Do(func() { blocked = true })
		if !blocked {
			return next(ctx, cmd)
		}
		close(h.reached)
		select {
		case <-h.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return next(ctx, cmd)
	}
}

func (h *pauseBeforeCancelingIndexZRemHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestCancelingIndexListRestoresMemberMadeCancelingDuringRepair is the proof
// for the read-side half of the repair race. A retried Cancel finds its member
// already present — its first attempt registered it but failed to write the
// status — so the retry's ZADD NX is a no-op. If List has read the still
// running status and is about to ZREM the member, the retry's status write can
// land between the read and the removal, and the member is deleted after the
// execution became canceling. List must re-read and put it back with the
// ORIGINAL score; without the write-back the member is gone for good and the
// execution sits in canceling, invisible to the reaper, forever.
func TestCancelingIndexListRestoresMemberMadeCancelingDuringRepair(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := namespace.WithNamespace(context.Background(), "tenant-prune-race")
	id := types.ExecutionID("exec-prune-race")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	// The leftover of a Cancel whose registration succeeded but whose status
	// write did not: a member already past the threshold while the status is
	// still running.
	originalScore := float64(time.Now().Add(-time.Hour).UTC().UnixMilli())
	if err := rdb.ZAdd(ctx, cancelingIndexKey("tenant-prune-race"), redis.Z{
		Score: originalScore, Member: string(id),
	}).Err(); err != nil {
		t.Fatalf("seed canceling index member: %v", err)
	}

	hook := &pauseBeforeCancelingIndexZRemHook{
		key:     cancelingIndexKey("tenant-prune-race"),
		reached: make(chan struct{}),
		release: make(chan struct{}),
	}
	rdb.AddHook(hook)

	listDone := make(chan []engine.CancelingExecution, 1)
	listErr := make(chan error, 1)
	go func() {
		candidates, err := state.ListCancelingExecutions(context.Background(), time.Now().Add(time.Hour), 8)
		if err != nil {
			listErr <- err
			return
		}
		listDone <- candidates
	}()
	select {
	case <-hook.reached:
	case <-time.After(5 * time.Second):
		t.Fatal("List never reached the canceling-index ZREM; the stale branch was not exercised")
	}
	// The retried Cancel completes while the ZREM is held back: the member is
	// still present, so its ZADD NX is a no-op, and only the status write
	// lands.
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) retry error = %v", err)
	}
	close(hook.release)
	select {
	case err := <-listErr:
		t.Fatalf("ListCancelingExecutions() error = %v", err)
	case candidates := <-listDone:
		if len(candidates) != 0 {
			t.Fatalf("ListCancelingExecutions() = %+v, want the member excluded during the repair", candidates)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ListCancelingExecutions did not return after the ZREM was released")
	}

	status, err := rdb.Get(ctx, execKey("tenant-prune-race", id, "status")).Result()
	if err != nil {
		t.Fatalf("read status after the repair: %v", err)
	}
	if types.ExecutionStatus(status) != types.ExecutionStatusCanceling {
		t.Fatalf("status = %q, want canceling", status)
	}
	score, err := rdb.ZScore(ctx, cancelingIndexKey("tenant-prune-race"), string(id)).Result()
	if err != nil {
		t.Fatalf("canceling index member was lost by the repair: %v", err)
	}
	if score != originalScore {
		t.Fatalf("restored index score = %v, want the original %v (age must survive the repair)", score, originalScore)
	}
}

// deleteAfterCancelingIndexZAddHook removes the registered member with a second
// client immediately AFTER the first cancel-path ZADD NX returns, before the
// caller's status write. That is the crash window the write side must repair:
// the registration won, the member then vanished (a concurrent prune), and the
// status write is still going to land. The once guard keeps the
// re-registration issued after the status write from deleting itself.
type deleteAfterCancelingIndexZAddHook struct {
	rdb     *redis.Client
	key     string
	member  string
	once    sync.Once
	deleted atomic.Bool
}

func (h *deleteAfterCancelingIndexZAddHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *deleteAfterCancelingIndexZAddHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if cmd.Name() != "zadd" || len(args) < 2 || fmt.Sprint(args[1]) != h.key {
			return next(ctx, cmd)
		}
		first := false
		h.once.Do(func() { first = true })
		if !first {
			return next(ctx, cmd)
		}
		if err := next(ctx, cmd); err != nil {
			return err
		}
		if err := h.rdb.ZRem(ctx, h.key, h.member).Err(); err != nil {
			return fmt.Errorf("remove registered member with the second client: %w", err)
		}
		h.deleted.Store(true)
		return nil
	}
}

func (h *deleteAfterCancelingIndexZAddHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestCancelingStatusWriteReRegistersAPrunedMember is the proof for the
// write-side half. The pruning process is not driven here: the hook deletes the
// member right after the registration returns, which is exactly the state a
// List leaves behind when its ZREM wins the race against the retry's status
// write. UpdateExecutionStatus must re-assert the registration once the
// canceling status is durable, or the execution stays in canceling where no
// reaper can find it.
func TestCancelingStatusWriteReRegistersAPrunedMember(t *testing.T) {
	state, mr, rdb := newTestRedisState(t)
	ctx := namespace.WithNamespace(context.Background(), "tenant-rebuild")
	id := types.ExecutionID("exec-canceling-rebuild")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	// The second client is what keeps the hook's own deletion out of the hook.
	other := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = other.Close() })
	hook := &deleteAfterCancelingIndexZAddHook{
		rdb:    other,
		key:    cancelingIndexKey("tenant-rebuild"),
		member: string(id),
	}
	rdb.AddHook(hook)

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	if !hook.deleted.Load() {
		t.Fatal("the hook never deleted the registered member; the repair window was not exercised")
	}
	status, err := rdb.Get(ctx, execKey("tenant-rebuild", id, "status")).Result()
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	if types.ExecutionStatus(status) != types.ExecutionStatusCanceling {
		t.Fatalf("status = %q, want canceling", status)
	}
	if _, err := rdb.ZScore(ctx, cancelingIndexKey("tenant-rebuild"), string(id)).Result(); err != nil {
		t.Fatalf("canceling index member was not rebuilt after the status write: %v", err)
	}
}
