// Package fireobs holds the observer a trigger reports a dropped firing
// through.
//
// The counter lives in observability/metrics, which imports the trigger packages
// and therefore cannot be imported by them, so the process that builds the
// runtime installs an observer here and the triggers call it. Both trigger
// packages that drop firings — cron and timer — share this counter rather than
// each carrying a copy of the install/read discipline: the observer is installed
// at process start and read by every firing goroutine, so the two must be
// ordered, and one implementation is how they stay ordered.
//
// Nothing here classifies a failure beyond what a trigger can prove on its own.
// The one sentinel worth splitting out — the engine's concurrency limit, which
// is the workflow throttling its own schedule rather than a fault — lives in
// package engine, which no production code under node/ may import: engine's own
// tests import node, so an import back into engine would close a cycle Go
// rejects at build time. The report therefore carries the error itself and the
// observer narrows it (see types.TriggerFireObserver).
package fireobs

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/xbcio/xflow/types"
)

// Counter is the install-once observer slot a trigger package exposes.
type Counter struct {
	mu        sync.Mutex
	installed bool
	observer  atomic.Pointer[types.TriggerFireObserver]
}

// Set installs the observer, or removes it when o is nil.
//
// A second non-nil install panics, deliberately: the observer is installed by
// whoever owns the process's metrics registry, and a second installer means two
// parts of the process believe they own it — silently keeping one of them would
// leave whichever metric the other one feeds missing, which is exactly the
// failure a double install causes in production. The release path (nil) is also
// what a test uses to put the slot back.
func (c *Counter) Set(o types.TriggerFireObserver) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if o == nil {
		var none types.TriggerFireObserver = noop{}
		c.observer.Store(&none)
		c.installed = false
		return
	}
	if c.installed {
		panic("trigger fire observer already installed; call SetObserver(nil) first")
	}
	c.observer.Store(&o)
	c.installed = true
}

// Observe reports one dropped firing. It is safe to call from any goroutine, and
// with no observer installed it does nothing — a process that collects no
// metrics loses the count, not the firing.
//
// reason is the trigger's own classification (the types.TriggerFireDropped*
// vocabulary); err is the failure the drop came from, or nil when the trigger
// decided (the overlap policy).
func (c *Counter) Observe(ctx context.Context, reason string, err error) {
	o := c.observer.Load()
	if o == nil || *o == nil {
		return
	}
	(*o).OnTriggerFireDropped(ctx, reason, err)
}

type noop struct{}

func (noop) OnTriggerFireDropped(context.Context, string, error) {}
