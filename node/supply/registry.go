package supply

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Default is the process-wide registry. The runner's supply activation handler
// writes into it; node handlers read from it.
var Default = NewRegistry()

// Registry caches supply snapshots and fans changes out to the consumers that
// registered against each name.
type Registry struct {
	mu        sync.RWMutex
	snapshots map[string]Snapshot
	// consumers maps supply name → consumer key → consumer. The key is the
	// caller's stable identity (e.g. "workflow/node"), so re-registering after a
	// re-activation replaces rather than duplicates.
	consumers map[string]map[string]Consumer
}

func NewRegistry() *Registry {
	return &Registry{
		snapshots: map[string]Snapshot{},
		consumers: map[string]map[string]Consumer{},
	}
}

// Apply records a snapshot and notifies this supply's consumers when the content
// hash changed. It never creates an execution or dispatches a task.
//
// A revision bump with an unchanged hash updates the cached revision but skips
// notification: rebuilding a consumer's derived state for identical bytes is
// pure waste.
//
// Consumer errors are collected and returned joined; every consumer is called
// regardless, and the cache is updated either way.
func (r *Registry) Apply(ctx context.Context, snap Snapshot) error {
	r.mu.Lock()
	prev, had := r.snapshots[snap.Name]
	changed := !had || prev.Hash != snap.Hash
	r.snapshots[snap.Name] = cloneSnapshot(snap)
	targets := make([]Consumer, 0, len(r.consumers[snap.Name]))
	if changed {
		for _, c := range r.consumers[snap.Name] {
			targets = append(targets, c)
		}
	}
	r.mu.Unlock()

	var errs []error
	for _, c := range targets {
		if err := safeNotify(ctx, c, snap); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Get returns the current snapshot for a name. The returned Content is a copy.
// This is the lazy read path used by $supplies expression evaluation: no supply
// content is ever placed into types.Input.
func (r *Registry) Get(name string) (Snapshot, bool) {
	r.mu.RLock()
	snap, ok := r.snapshots[name]
	r.mu.RUnlock()
	if !ok {
		return Snapshot{}, false
	}
	return cloneSnapshot(snap), true
}

// RegisterConsumer registers c under (name, key), replacing any consumer already
// at that key. If a snapshot for name is already cached, c is notified
// immediately — activation order between a supply and its consumer is not
// guaranteed, so a late-registering consumer must not wait for the next change.
func (r *Registry) RegisterConsumer(name, key string, c Consumer) {
	r.mu.Lock()
	if r.consumers[name] == nil {
		r.consumers[name] = map[string]Consumer{}
	}
	r.consumers[name][key] = c
	snap, has := r.snapshots[name]
	r.mu.Unlock()

	if has {
		// Best-effort: a rejection here leaves the consumer on its last-good
		// state, which is exactly the documented behaviour.
		_ = safeNotify(context.Background(), c, snap)
	}
}

// UnregisterConsumer removes the consumer at (name, key). It is idempotent.
func (r *Registry) UnregisterConsumer(name, key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m := r.consumers[name]; m != nil {
		delete(m, key)
		if len(m) == 0 {
			delete(r.consumers, name)
		}
	}
}

// Ready returns the sorted subset of names that have no cached snapshot, or nil
// when all are present. The runner uses it as the activation-time readiness gate:
// a non-empty result means "do not take over this entry activation".
func (r *Registry) Ready(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var missing []string
	for _, n := range names {
		if _, ok := r.snapshots[n]; !ok {
			missing = append(missing, n)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return missing
}

// safeNotify calls a consumer's OnSupplyChanged with panic recovery. A panicking
// consumer is treated identically to one that returns an error — the error is
// collected and other consumers are not affected. The panic value is not included
// in the error to avoid leaking content bytes indirectly.
func safeNotify(ctx context.Context, c Consumer, snap Snapshot) (err error) {
	defer func() {
		if r := recover(); r != nil {
			hashPrefix := snap.Hash
			if len(hashPrefix) > 12 {
				hashPrefix = hashPrefix[:12]
			}
			err = fmt.Errorf("supply %q [%s]: consumer panicked", snap.Name, hashPrefix)
		}
	}()
	return c.OnSupplyChanged(ctx, cloneSnapshot(snap))
}

func cloneSnapshot(s Snapshot) Snapshot {
	out := s
	out.Content = append([]byte(nil), s.Content...)
	return out
}
