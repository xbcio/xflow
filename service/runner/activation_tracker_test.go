package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// mockActivationHandler records Activate/Deactivate calls for test assertions.
type mockActivationHandler struct {
	mu            sync.Mutex
	activations   []protocol.ActivateDirective
	deactivations []protocol.DeactivateDirective
	activateErr   error
	// lastCtx is the context of the most recent successfully established
	// subscription. It is only updated on a nil return, mirroring the real
	// ActivationHandler contract (Activate returns once the subscription is
	// established) so a failed upgrade attempt does not clobber it.
	lastCtx context.Context
}

func (m *mockActivationHandler) Activate(ctx context.Context, d protocol.ActivateDirective) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activations = append(m.activations, d)
	if m.activateErr == nil {
		m.lastCtx = ctx
	}
	return m.activateErr
}

func (m *mockActivationHandler) Deactivate(d protocol.DeactivateDirective) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deactivations = append(m.deactivations, d)
	return nil
}

func (m *mockActivationHandler) activateCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.activations)
}

func (m *mockActivationHandler) deactivateCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.deactivations)
}

func TestActivationTracker_Activate_StartsSubscription(t *testing.T) {
	handler := &mockActivationHandler{}
	tracker := NewActivationTracker(handler, slog.Default())

	ctx := context.Background()
	directives := &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{
			{
				WorkflowID:  "wf-1",
				EntryUnitID: "grp-a",
				Generation:  1,
			},
		},
	}

	if err := tracker.ProcessDirectives(ctx, directives); err != nil {
		t.Fatalf("ProcessDirectives failed: %v", err)
	}

	if got := handler.activateCount(); got != 1 {
		t.Fatalf("expected 1 activation call, got %d", got)
	}

	handler.mu.Lock()
	got := handler.activations[0]
	handler.mu.Unlock()

	if got.WorkflowID != "wf-1" || got.EntryUnitID != "grp-a" || got.Generation != 1 {
		t.Fatalf("unexpected directive passed to handler: %+v", got)
	}
}

func TestActivationTracker_Activate_Idempotent_SameGeneration(t *testing.T) {
	handler := &mockActivationHandler{}
	tracker := NewActivationTracker(handler, slog.Default())

	ctx := context.Background()
	directive := protocol.ActivateDirective{
		WorkflowID:  "wf-1",
		EntryUnitID: "grp-a",
		Generation:  1,
	}

	// Activate once.
	err := tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{directive},
	})
	if err != nil {
		t.Fatalf("first ProcessDirectives failed: %v", err)
	}

	// Activate again with same generation — should be idempotent.
	err = tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{directive},
	})
	if err != nil {
		t.Fatalf("second ProcessDirectives failed: %v", err)
	}

	if got := handler.activateCount(); got != 1 {
		t.Fatalf("expected 1 activation call (idempotent), got %d", got)
	}
}

func TestActivationTracker_Activate_UpgradesGeneration(t *testing.T) {
	handler := &mockActivationHandler{}
	tracker := NewActivationTracker(handler, slog.Default())

	ctx := context.Background()

	// Activate with generation 1.
	err := tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{
			{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 1},
		},
	})
	if err != nil {
		t.Fatalf("first ProcessDirectives failed: %v", err)
	}

	// Activate with generation 2 — should cancel old and start new.
	err = tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{
			{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 2},
		},
	})
	if err != nil {
		t.Fatalf("second ProcessDirectives failed: %v", err)
	}

	if got := handler.activateCount(); got != 2 {
		t.Fatalf("expected 2 activation calls (upgrade), got %d", got)
	}

	// Inventory should reflect generation 2.
	inv := tracker.Inventory()
	if len(inv) != 1 {
		t.Fatalf("expected 1 inventory item, got %d", len(inv))
	}
	if inv[0].Generation != 2 {
		t.Fatalf("expected generation 2 in inventory, got %d", inv[0].Generation)
	}
}

func TestActivationTracker_Deactivate_StopsSubscription(t *testing.T) {
	handler := &mockActivationHandler{}
	tracker := NewActivationTracker(handler, slog.Default())

	ctx := context.Background()

	// Activate first.
	err := tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{
			{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 1},
		},
	})
	if err != nil {
		t.Fatalf("activate failed: %v", err)
	}

	// Deactivate with matching generation.
	err = tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Deactivate: []protocol.DeactivateDirective{
			{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 1},
		},
	})
	if err != nil {
		t.Fatalf("deactivate failed: %v", err)
	}

	if got := handler.deactivateCount(); got != 1 {
		t.Fatalf("expected 1 deactivation call, got %d", got)
	}

	// Inventory should be empty.
	inv := tracker.Inventory()
	if len(inv) != 0 {
		t.Fatalf("expected empty inventory after deactivation, got %d items", len(inv))
	}
}

func TestActivationTracker_Inventory_ReportsActive(t *testing.T) {
	handler := &mockActivationHandler{}
	tracker := NewActivationTracker(handler, slog.Default())

	ctx := context.Background()

	// Activate two different workflow/group combos.
	err := tracker.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{
			{WorkflowID: "wf-1", EntryUnitID: "grp-a", Generation: 3},
			{WorkflowID: "wf-2", EntryUnitID: "grp-b", Generation: 7},
		},
	})
	if err != nil {
		t.Fatalf("ProcessDirectives failed: %v", err)
	}

	inv := tracker.Inventory()
	if len(inv) != 2 {
		t.Fatalf("expected 2 inventory items, got %d", len(inv))
	}

	// Build a lookup for assertions (order is map-iteration-dependent).
	lookup := make(map[string]protocol.ActivationInventoryItem)
	for _, item := range inv {
		lookup[item.WorkflowID+"/"+item.EntryUnitID] = item
	}

	item1, ok := lookup["wf-1/grp-a"]
	if !ok {
		t.Fatal("missing wf-1/grp-a in inventory")
	}
	if item1.Generation != 3 {
		t.Fatalf("expected generation 3 for wf-1/grp-a, got %d", item1.Generation)
	}

	item2, ok := lookup["wf-2/grp-b"]
	if !ok {
		t.Fatal("missing wf-2/grp-b in inventory")
	}
	if item2.Generation != 7 {
		t.Fatalf("expected generation 7 for wf-2/grp-b, got %d", item2.Generation)
	}
}

// 升级失败必须保留旧订阅：先 cancel 再 Activate 会在失败时留下一个指向已死
// context 的条目，使 runner 既不处理消息，又在 Inventory() 里声称自己在托管。
func TestActivateUpgradeFailureKeepsOldSubscriptionAlive(t *testing.T) {
	h := &mockActivationHandler{}
	tr := NewActivationTracker(h, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	d1 := protocol.ActivateDirective{WorkflowID: "w", EntryUnitID: "e", Generation: 1}
	if err := tr.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{d1},
	}); err != nil {
		t.Fatalf("first activate: %v", err)
	}

	// 第二次（升级到 gen 2）失败。
	h.activateErr = errors.New("supply not ready: rules")
	d2 := protocol.ActivateDirective{WorkflowID: "w", EntryUnitID: "e", Generation: 2}
	if err := tr.ProcessDirectives(ctx, &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{d2},
	}); err != nil {
		t.Fatalf("ProcessDirectives must not propagate per-directive errors: %v", err)
	}

	inv := tr.Inventory()
	if len(inv) != 1 {
		t.Fatalf("inventory = %#v, want exactly the surviving gen-1 entry", inv)
	}
	if inv[0].Generation != 1 {
		t.Fatalf("generation = %d, want 1 (the old subscription must survive)", inv[0].Generation)
	}
	// 旧订阅的 context 必须仍然存活。
	h.mu.Lock()
	lastCtx := h.lastCtx
	h.mu.Unlock()
	if lastCtx == nil {
		t.Fatal("handler never received a context")
	}
	if err := lastCtx.Err(); err != nil {
		t.Fatalf("old subscription context was cancelled (%v); a failed upgrade must not kill it", err)
	}
}

// 失败必须可被外部观测：没有这个回调，失败就止步于本地日志，server 永远不知道
// 该 activation 没被接住。
func TestProcessDirectivesReportsActivateFailure(t *testing.T) {
	h := &mockActivationHandler{activateErr: errors.New("supply not ready: rules")}
	tr := NewActivationTracker(h, slog.New(slog.NewTextHandler(io.Discard, nil)))

	var gotDirective protocol.ActivateDirective
	var gotErr error
	var calls int
	tr.SetOnActivateFailed(func(d protocol.ActivateDirective, err error) {
		calls++
		gotDirective = d
		gotErr = err
	})

	d := protocol.ActivateDirective{WorkflowID: "w", EntryUnitID: "e", Generation: 7}
	if err := tr.ProcessDirectives(context.Background(), &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{d},
	}); err != nil {
		t.Fatalf("ProcessDirectives: %v", err)
	}

	if calls != 1 {
		t.Fatalf("callback calls = %d, want 1", calls)
	}
	if gotDirective.Generation != 7 || gotDirective.WorkflowID != "w" {
		t.Fatalf("callback got %#v, want the failing directive verbatim", gotDirective)
	}
	if gotErr == nil {
		t.Fatal("callback must receive the activation error")
	}
}

// 成功不得触发回调——否则 server 会把正常激活误判为失败并重派。
func TestProcessDirectivesDoesNotReportOnSuccess(t *testing.T) {
	h := &mockActivationHandler{}
	tr := NewActivationTracker(h, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var calls int
	tr.SetOnActivateFailed(func(protocol.ActivateDirective, error) { calls++ })

	if err := tr.ProcessDirectives(context.Background(), &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{{WorkflowID: "w", EntryUnitID: "e", Generation: 1}},
	}); err != nil {
		t.Fatalf("ProcessDirectives: %v", err)
	}
	if calls != 0 {
		t.Fatalf("callback calls = %d on success, want 0", calls)
	}
}

// 钉住并发不变量本身，而不是只钉住可观测行为：回调内部反过来调用
// tr.Inventory()（需要重新 Lock t.mu）。如果 ProcessDirectives 在持锁期间调用
// 回调，这里会死锁——用超时把死锁转成可见的测试失败，而不是真的把测试进程
// 挂起。
func TestProcessDirectivesInvokesCallbackWithLockReleased(t *testing.T) {
	h := &mockActivationHandler{activateErr: errors.New("supply not ready: rules")}
	tr := NewActivationTracker(h, slog.New(slog.NewTextHandler(io.Discard, nil)))
	tr.SetOnActivateFailed(func(protocol.ActivateDirective, error) {
		// Reentrant call: only returns if t.mu was released before the callback ran.
		tr.Inventory()
	})

	d := protocol.ActivateDirective{WorkflowID: "w", EntryUnitID: "e", Generation: 1}
	done := make(chan error, 1)
	go func() {
		done <- tr.ProcessDirectives(context.Background(), &protocol.HeartbeatActivations{
			Activate: []protocol.ActivateDirective{d},
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ProcessDirectives: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ProcessDirectives deadlocked: the callback must run with t.mu released, " +
			"but Inventory() called from inside the callback never returned")
	}
}

// 回调 panic 不得让整批上报中断，也不得让驱动 ProcessDirectives 的 goroutine
// 崩溃（生产环境中这个 goroutine 是 heartbeatLoop，未恢复的 panic 会带走整个
// 进程）。
func TestProcessDirectivesRecoversFromCallbackPanicAndContinues(t *testing.T) {
	h := &mockActivationHandler{activateErr: errors.New("supply not ready: rules")}
	tr := NewActivationTracker(h, slog.New(slog.NewTextHandler(io.Discard, nil)))

	var calls int
	tr.SetOnActivateFailed(func(protocol.ActivateDirective, error) {
		calls++
		panic("simulated callback panic")
	})

	d1 := protocol.ActivateDirective{WorkflowID: "w1", EntryUnitID: "e1", Generation: 1}
	d2 := protocol.ActivateDirective{WorkflowID: "w2", EntryUnitID: "e2", Generation: 1}
	if err := tr.ProcessDirectives(context.Background(), &protocol.HeartbeatActivations{
		Activate: []protocol.ActivateDirective{d1, d2},
	}); err != nil {
		t.Fatalf("ProcessDirectives: %v", err)
	}
	if calls != 2 {
		t.Fatalf("callback calls = %d, want 2 (a panic in one callback must not stop reporting the rest)", calls)
	}
}
