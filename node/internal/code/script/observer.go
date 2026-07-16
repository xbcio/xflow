package script

import (
	"sync"
	"time"
)

// Observer receives script execution observations. Implementations must be
// non-blocking and must avoid high-cardinality labels such as execution IDs or
// raw script source.
type Observer interface {
	OnScriptExecute(language, runtime, outcome string, duration time.Duration)
	OnScriptOutputBytes(language, runtime string, size int)
}

type noopObserver struct{}

func (noopObserver) OnScriptExecute(string, string, string, time.Duration) {}
func (noopObserver) OnScriptOutputBytes(string, string, int)             {}

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

func observeExecute(language, runtime, outcome string, duration time.Duration) {
	observerMu.RLock()
	o := observer
	observerMu.RUnlock()
	o.OnScriptExecute(language, runtime, outcome, duration)
}

func observeOutputBytes(language, runtime string, size int) {
	observerMu.RLock()
	o := observer
	observerMu.RUnlock()
	o.OnScriptOutputBytes(language, runtime, size)
}
