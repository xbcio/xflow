package script

import (
	"context"
	"sync"
	"time"
)

// Observer receives script execution observations. Implementations must be
// non-blocking and must avoid high-cardinality labels such as execution IDs or
// raw script source.
type Observer interface {
	OnScriptExecute(ctx context.Context, language, runtime, outcome string, duration time.Duration)
	OnScriptOutputBytes(ctx context.Context, language, runtime string, size int)
}

type noopObserver struct{}

func (noopObserver) OnScriptExecute(context.Context, string, string, string, time.Duration) {}
func (noopObserver) OnScriptOutputBytes(context.Context, string, string, int)             {}

var (
	observerMu sync.RWMutex
	observer   Observer = noopObserver{}
)

// SetObserver installs the global script observer. Pass nil to restore the
// no-op default. Call once during process initialization.
func SetObserver(o Observer) {
	observerMu.Lock()
	defer observerMu.Unlock()
	if o == nil {
		observer = noopObserver{}
		return
	}
	observer = o
}

func observeExecute(ctx context.Context, language, runtime, outcome string, duration time.Duration) {
	observerMu.RLock()
	o := observer
	observerMu.RUnlock()
	o.OnScriptExecute(ctx, language, runtime, outcome, duration)
}

func observeOutputBytes(ctx context.Context, language, runtime string, size int) {
	observerMu.RLock()
	o := observer
	observerMu.RUnlock()
	o.OnScriptOutputBytes(ctx, language, runtime, size)
}
