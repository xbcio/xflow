package wasm

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
)

// closeForTest is a stub for this task: Task 1 only adds the idle timestamp,
// not reclamation itself, so there is nothing to tear down yet. Task 2 will
// replace this with the real close-and-drain implementation once the teardown
// code it needs to call exists.
func (h *reactorHost) closeForTest(ctx context.Context) {}

// newTestReactorHost builds a host isolated from sharedReactorHost. Reclamation
// mutates h.engines, so a test sharing the package-level host would strip
// modules out from under a concurrently running test.
func newTestReactorHost(t *testing.T) *reactorHost {
	t.Helper()
	h := newReactorHost()
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
