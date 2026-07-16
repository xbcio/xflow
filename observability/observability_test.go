package observability

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func TestHookChainContinuesAfterPanic(t *testing.T) {
	called := false
	chain := HookChain{
		panicHook{},
		nodeStartHook{fn: func() { called = true }},
	}

	chain.OnNodeStart(context.Background(), "exec-1", "node-1")
	if !called {
		t.Fatal("second hook was not called after first hook panicked")
	}
}

type panicHook struct {
	engine.BaseHooks
}

func (panicHook) OnNodeStart(context.Context, types.ExecutionID, string) {
	panic("boom")
}

type nodeStartHook struct {
	engine.BaseHooks
	fn func()
}

func (h nodeStartHook) OnNodeStart(context.Context, types.ExecutionID, string) {
	h.fn()
}
