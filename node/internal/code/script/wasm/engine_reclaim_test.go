package wasm

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// closeForTest tears down every engine a test host built. Production has no
// shutdown path — the runner process outlives its modules — so this exists only
// to keep a test's wazero runtimes and pooled instances from outliving the test.
func (h *reactorHost) closeForTest(ctx context.Context) {
	h.reclaimIdleEngines(ctx, time.Nanosecond)
	h.mu.Lock()
	rt := h.rt
	h.mu.Unlock()
	if rt != nil {
		_ = rt.Close(ctx)
	}
}

// newTestReactorHost builds a host isolated from sharedReactorHost. Reclamation
// mutates h.engines, so a test sharing the package-level host would strip
// modules out from under a concurrently running test.
//
// engineIdleTTL is forced to 0 (async sweep disabled) rather than inherited
// from engineIdleTTLFromEnv(): sweepEnginesAsync fires on every compile miss,
// and most reclaim tests fake an engine idle by rewinding lastUsed
// microseconds after seeding it, which a live background sweep can win the
// race on and reclaim out from under the test before it gets to assert
// anything. Zero also means the goroutine never self-re-arms, so a test host
// carries no timer past the test — see sweepEnginesAsync's ttl<=0 guard. A
// test that specifically exercises the async trigger (e.g.
// TestCompileMissTriggersSweep) opts in by setting h.engineIdleTTL itself
// after construction.
func newTestReactorHost(t *testing.T) *reactorHost {
	t.Helper()
	h := newReactorHost()
	h.engineIdleTTL = 0
	t.Cleanup(func() { h.closeForTest(context.Background()) })
	return h
}

// warmTestEngine compiles a module unique to this call and gives it a live pool,
// so the returned engine has instances to reclaim. Pool size 2 rather than
// defaultPoolSize(): the tests only need "more than one", and GOMAXPROCS
// instances per engine is a needless several seconds of compile and instantiate.
func warmTestEngine(t *testing.T, h *reactorHost, mod []byte) *reactorEngine {
	t.Helper()
	ctx := context.Background()
	e, err := h.engineForCode(ctx, b64(mod))
	if err != nil {
		t.Fatalf("warmTestEngine: compile: %v", err)
	}
	if err := e.swapConfig(ctx, emptyContent(), 2, 1); err != nil {
		t.Fatalf("warmTestEngine: swapConfig: %v", err)
	}
	return e
}

// 三条解析路径都必须把 lastUsed 推进。digest 路径与 bytes 路径是生产路径；
// codeCache 路径是本任务把它挪进 h.mu 的那一条，没有它回收就有 TOCTOU。
func TestEveryResolutionPathStampsLastUsed(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		resolve func(h *reactorHost, code string, raw []byte) error
	}{
		{"digest", func(h *reactorHost, code string, raw []byte) error {
			_, err := h.engineForSource(ctx, engine.Source{
				Digest: "sha256:" + moduleKeyOf(raw),
				Code:   code,
			})
			return err
		}},
		{"code", func(h *reactorHost, code string, raw []byte) error {
			_, err := h.engineForCode(ctx, code)
			return err
		}},
		{"bytes", func(h *reactorHost, code string, raw []byte) error {
			_, err := h.engineForBytes(ctx, raw)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestReactorHost(t)
			code := testReactorCode(t)
			raw := decodeForTest(t, code)
			e, err := h.engineForCode(ctx, code)
			if err != nil {
				t.Fatalf("seed engine: %v", err)
			}

			// 倒推一小时，再解析一次，断言戳被推到了「现在」。
			old := time.Now().Add(-time.Hour).UnixNano()
			e.lastUsed.Store(old)

			if err := tc.resolve(h, code, raw); err != nil {
				t.Fatalf("resolve: %v", err)
			}
			got := e.lastUsed.Load()
			if got == old {
				t.Fatalf("%s path did not stamp lastUsed (still %d) — the reclaim "+
					"decision would race this lookup", tc.name, old)
			}
			if age := time.Since(time.Unix(0, got)); age > time.Minute {
				t.Fatalf("%s path stamped %v ago, want ~now", tc.name, age)
			}
		})
	}
}

// 回收要真的把 engine 从三个容器里都摘掉：engines、engineList 快照、codeCache。
// 少摘一个都不算回收——留在 engineList 里等于永久持有引用，留在 codeCache 里
// 等于下次调用拿回一个已不在 engines 里的孤儿并在它上面重建池。
func TestReclaimRemovesEngineFromEveryContainer(t *testing.T) {
	ctx := context.Background()
	h := newTestReactorHost(t)
	code := testReactorCode(t)
	raw := decodeForTest(t, code)
	e, err := h.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	key := moduleKeyOf(raw)

	e.lastUsed.Store(time.Now().Add(-time.Hour).UnixNano())
	if n := h.reclaimIdleEngines(ctx, time.Minute); n != 1 {
		t.Fatalf("reclaimed %d engines, want exactly 1", n)
	}

	h.mu.Lock()
	_, inMap := h.engines[key]
	_, inCode := h.codeCache.Get(code)
	h.mu.Unlock()
	if inMap {
		t.Error("engine still in h.engines after reclaim")
	}
	if inCode {
		t.Error("codeCache still hands back the reclaimed engine — the next " +
			"inline-code Execute would rebuild a pool on an orphan")
	}
	for _, snap := range h.engineSnapshot() {
		if snap == e {
			t.Error("engineList snapshot still references the reclaimed engine")
		}
	}
	if !e.reclaimed.Load() {
		t.Error("engine not marked reclaimed; borrow cannot tell this apart from unconfigured")
	}
	if e.active.Load() != nil {
		t.Error("active pool not detached; its instances are still resident")
	}
}

// 意图必须活下来。这是与 spec §6.1 约束①②的两处故意偏离，两条都必须有牙：
// 清掉 sourceDriven 会让重建的 engine 走 legacy globals 路径对零规则放行。
func TestReclaimKeepsIntentSoRebuildFailsClosed(t *testing.T) {
	ctx := context.Background()
	h := newTestReactorHost(t)
	code := testReactorCode(t)
	raw := decodeForTest(t, code)
	key := moduleKeyOf(raw)

	if _, err := h.engineForCode(ctx, code); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h.seedSourceDrivenByKey(key)

	h.mu.Lock()
	h.engines[key].lastUsed.Store(time.Now().Add(-time.Hour).UnixNano())
	h.mu.Unlock()
	if n := h.reclaimIdleEngines(ctx, time.Minute); n != 1 {
		t.Fatalf("reclaimed %d, want 1", n)
	}

	h.mu.Lock()
	_, stillSourceDriven := h.sourceDriven[key]
	h.mu.Unlock()
	if !stillSourceDriven {
		t.Fatal("reclaim cleared sourceDriven: a rebuilt engine would run the " +
			"legacy globals path with an empty $config and pass every record through untagged")
	}

	rebuilt, err := h.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !rebuilt.configFromSource.Load() {
		t.Fatal("rebuilt engine is not source-driven; it would evaluate against no rules")
	}
	if got := rebuilt.availability(); got != AvailUnavailable {
		t.Fatalf("rebuilt engine availability = %v, want AvailUnavailable — that "+
			"verdict is what makes reactor.go refuse traffic until the supply lands", got)
	}
}

// 有实例在飞的 engine 不回收。这是 lastUsed 之外的第二道闸：解析返回后到
// borrow 之间那几微秒不在锁的保护内。
func TestReclaimSkipsEngineWithInFlightInstance(t *testing.T) {
	ctx := context.Background()
	h := newTestReactorHost(t)
	e := warmTestEngine(t, h, decodeForTest(t, testReactorCode(t)))

	inst, pool, err := e.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	e.lastUsed.Store(time.Now().Add(-time.Hour).UnixNano())

	if n := h.reclaimIdleEngines(ctx, time.Minute); n != 0 {
		t.Fatalf("reclaimed %d engines while one instance was borrowed, want 0", n)
	}
	e.giveBack(ctx, pool, inst)

	// 归还之后同一个 engine 必须变得可回收——否则上面那个 0 可能只是因为
	// 这个 engine 无论如何都回收不了，断言就没有牙。
	e.lastUsed.Store(time.Now().Add(-time.Hour).UnixNano())
	if n := h.reclaimIdleEngines(ctx, time.Minute); n != 1 {
		t.Fatalf("reclaimed %d after the instance came back, want 1", n)
	}
}

// TTL 未到不回收，且计数必须精确等于 1（不是 >=1）：一次调用回收两个
// 而只有一个该被回收，同样是缺陷。
func TestReclaimHonoursTTLPerEngine(t *testing.T) {
	ctx := context.Background()
	h := newTestReactorHost(t)
	modA := decodeForTest(t, testReactorCode(t))
	// A custom section changes the content hash without changing behaviour, so
	// this is a second, distinct module. Two calls to testReactorCode would work
	// too; deriving from modA keeps the two obviously related.
	modB := appendCustomSection(modA, "xflow-test-second", []byte{0x01})
	idle := warmTestEngine(t, h, modA)
	busy := warmTestEngine(t, h, modB)

	idle.lastUsed.Store(time.Now().Add(-time.Hour).UnixNano())
	busy.lastUsed.Store(time.Now().UnixNano())

	if n := h.reclaimIdleEngines(ctx, time.Minute); n != 1 {
		t.Fatalf("reclaimed %d, want exactly 1 (the idle one)", n)
	}
	h.mu.Lock()
	remaining := len(h.engines)
	h.mu.Unlock()
	if remaining != 1 {
		t.Fatalf("%d engines remain, want 1 (the busy one)", remaining)
	}
	if busy.reclaimed.Load() {
		t.Fatal("the engine used one nanosecond ago was reclaimed")
	}
}

// 解析拿到 engine 之后、borrow 之前被回收，必须重试一次并成功——而不是把
// 一条消息送进 error 端口。error 端口会抹平分类，Kafka 不会重投递。
func TestExecuteRetriesOnceAfterEngineReclaimedMidCall(t *testing.T) {
	ctx := context.Background()
	h := newTestReactorHost(t)
	modA := decodeForTest(t, testReactorCode(t))
	e := warmTestEngine(t, h, modA)

	// 模拟输掉竞态：engine 已被摘走并拆掉池，但调用方手里还攥着它。
	e.reclaimed.Store(true)
	if old := e.active.Swap(nil); old != nil {
		e.drainPool(ctx, old)
	}

	if _, _, err := e.borrow(ctx); !errors.Is(err, errEngineReclaimed) {
		t.Fatalf("borrow on a reclaimed engine returned %v, want errEngineReclaimed — "+
			"without a distinct sentinel this is indistinguishable from a genuine "+
			"misconfiguration, which retrying would only loop on", err)
	}

	// 未配置的 engine 必须仍然是另一个错误，否则上面那条哨兵没有区分力。
	fresh, err := h.engineForBytes(ctx, appendCustomSection(modA, "xflow-test-fresh", []byte{0x02}))
	if err != nil {
		t.Fatalf("compile fresh: %v", err)
	}
	if _, _, err := fresh.borrow(ctx); err == nil || errors.Is(err, errEngineReclaimed) {
		t.Fatalf("borrow on an unconfigured engine returned %v, want a non-reclaimed error", err)
	}
}

// TTL 从环境变量读，非法值不得静默变成「关闭」——那会让泄漏悄悄回来。
func TestEngineIdleTTLFromEnv(t *testing.T) {
	for _, tc := range []struct {
		set  string
		want time.Duration
	}{
		{"", defaultEngineIdleTTL},
		{"30m", 30 * time.Minute},
		{"0", 0},
		{"garbage", defaultEngineIdleTTL},
	} {
		t.Run("v="+tc.set, func(t *testing.T) {
			if tc.set == "" {
				t.Setenv(EngineIdleTTLEnv, "")
				os.Unsetenv(EngineIdleTTLEnv)
			} else {
				t.Setenv(EngineIdleTTLEnv, tc.set)
			}
			if got := engineIdleTTLFromEnv(); got != tc.want {
				t.Fatalf("ttl = %v, want %v", got, tc.want)
			}
		})
	}
}

// 插入触发一次扫，且扫是异步的：编译路径不能被回收拖住。
func TestCompileMissTriggersSweep(t *testing.T) {
	ctx := context.Background()
	h := newTestReactorHost(t)
	h.engineIdleTTL = 50 * time.Millisecond
	modA := decodeForTest(t, testReactorCode(t))

	first, err := h.engineForBytes(ctx, modA)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	first.lastUsed.Store(time.Now().Add(-time.Hour).UnixNano())

	// 编译第二个模块 = 一次 miss = 一次扫。
	if _, err := h.engineForBytes(ctx, appendCustomSection(modA, "xflow-test-trigger", []byte{0x03})); err != nil {
		t.Fatalf("second: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if first.reclaimed.Load() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle engine was never reclaimed after a compile miss fired a sweep")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
