package supply

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
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

	// accepted tracks whether the most recent Apply for each name completed
	// without any consumer error. The gate uses this to distinguish "cached but
	// unusable" from "ready": a snapshot can be in the cache (so $supplies still
	// sees the latest value) while its derived state was rejected by a consumer
	// (e.g. wasm configure returned -1). A supply with no registered consumers
	// is always accepted — nothing needs to build derived state from it.
	accepted map[string]bool

	// decoded is the published, READ-ONLY "name → decoded value" map handed to
	// expression evaluation as $supplies. It is replaced wholesale on every
	// content change (copy-on-write) and never mutated in place, because readers
	// hold the reference without a lock.
	//
	// Decoding happens here — once per content change — rather than per message.
	// expr's runtime.Fetch offers no lazy hook for a custom type (it only tries
	// MethodByName and struct fields), so a plain map is the only shape that
	// resolves a dynamic $supplies.<name>. Publishing a shared reference keeps
	// per-message cost at one map assignment regardless of content size.
	decoded atomic.Pointer[map[string]any]
}

func NewRegistry() *Registry {
	r := &Registry{
		snapshots: map[string]Snapshot{},
		consumers: map[string]map[string]Consumer{},
		accepted:  map[string]bool{},
	}
	empty := map[string]any{}
	r.decoded.Store(&empty)
	return r
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

	// Determine whether consumers need notification. Content changes always
	// trigger notification. Additionally, if the supply was previously rejected
	// (accepted=false) and has the same hash, re-notify: a consumer replacement
	// or fix may have occurred since the last attempt, and the gate needs to
	// know whether the supply is now usable.
	needNotify := changed
	if !changed && had && !r.accepted[snap.Name] {
		needNotify = true
	}

	targets := make([]Consumer, 0, len(r.consumers[snap.Name]))
	if needNotify {
		for _, c := range r.consumers[snap.Name] {
			targets = append(targets, c)
		}
	}
	if changed {
		r.republishDecodedLocked()
	} else if had && prev.Revision != snap.Revision {
		// Same bytes, newer server revision: republish so $supplies.<name>.$revision
		// reflects it, but skip consumer notification (nothing derived changed).
		r.republishDecodedLocked()
	}
	r.mu.Unlock()

	var errs []error
	for _, c := range targets {
		if err := safeNotify(ctx, c, snap); err != nil {
			errs = append(errs, err)
		}
	}

	// Record whether all consumers accepted this content. When no consumers are
	// registered (len(targets)==0 and needNotify was true), the supply is accepted
	// by default — nothing needs to build derived state. When needNotify is false,
	// we preserve the existing accepted state (already true).
	if needNotify {
		r.mu.Lock()
		r.accepted[snap.Name] = len(errs) == 0
		r.mu.Unlock()
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

// IsReady reports whether a supply has content that was accepted by all
// registered consumers (or has no consumers at all). The gate uses this instead
// of a bare Get to distinguish "cached but unusable" (a consumer rejected the
// content) from "ready to serve traffic".
//
// A supply with no snapshot is not ready. A supply with a snapshot but no
// consumers is always ready — nothing needs to build derived state. A supply
// whose last Apply produced a consumer error is NOT ready, even though its
// snapshot is cached (so $supplies still sees the value for expression reads).
func (r *Registry) IsReady(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, hasSnap := r.snapshots[name]
	if !hasSnap {
		return false
	}
	return r.accepted[name]
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
// If removing this consumer leaves no remaining consumers for the name and a
// snapshot exists, the supply is marked accepted — there is nothing left that
// needs to build derived state.
func (r *Registry) UnregisterConsumer(name, key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m := r.consumers[name]; m != nil {
		delete(m, key)
		if len(m) == 0 {
			delete(r.consumers, name)
			// No consumers left: if content is cached, it's accepted by default.
			if _, hasSnap := r.snapshots[name]; hasSnap {
				r.accepted[name] = true
			}
		}
	}
}

// republishDecodedLocked rebuilds the published decoded map from the current
// snapshots and swaps it in. Caller must hold r.mu.
//
// A whole-map rebuild is affordable because it runs only on a content change,
// and the number of supplies in one process is small. The alternative —
// mutating the live map — would race with lock-free readers.
func (r *Registry) republishDecodedLocked() {
	next := make(map[string]any, len(r.snapshots))
	for name, s := range r.snapshots {
		next[name] = decodeForExpr(s)
	}
	r.decoded.Store(&next)
}

// Decoded returns the published, read-only "name → decoded value" map. Callers
// must NOT mutate it or anything reachable from it.
func (r *Registry) Decoded() map[string]any {
	return *r.decoded.Load()
}

// decodeForExpr turns one snapshot into the value expressions see. A JSON object
// gains $revision/$hash metadata keys ($ prefixed so they cannot collide with a
// content key). Any other JSON value is returned as decoded. Content that is not
// JSON at all is returned as raw bytes so a non-JSON supply is still usable.
func decodeForExpr(s Snapshot) any {
	var decoded any
	if err := json.Unmarshal(s.Content, &decoded); err != nil {
		return append([]byte(nil), s.Content...)
	}
	obj, isObj := decoded.(map[string]any)
	if !isObj {
		return decoded
	}
	obj[SupplyRevisionKey] = s.Revision
	obj[SupplyHashKey] = s.Hash
	return obj
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
