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

// consumerEntry pairs a registered consumer with the outcome of the last
// OnSupplyChanged call it was given, and the content that call judged. This is
// the actual fact the registry needs to track: readiness is never assigned
// directly — it is the conjunction of these per-consumer outcomes over the
// CURRENTLY registered set, recomputed on every read (see IsReady).
type consumerEntry struct {
	consumer Consumer
	// regSeq identifies this registration. Consumer callbacks run outside the
	// lock, so a write-back must confirm the entry it is about to update is
	// still the same registration that was notified. A sequence number rather
	// than comparing the Consumer values directly: Consumer is an interface, and
	// == on an interface holding an uncomparable dynamic type (a struct with a
	// slice, map, or func field) panics at runtime — outside safeNotify's
	// recover, so it would take the caller down. Identity must not depend on
	// what an implementer happens to put in their struct.
	regSeq uint64
	// lastErr is the outcome, and lastHash is the content hash it judged. An
	// outcome is meaningless without the content it was about: a notification
	// for content that has since been superseded must not overwrite the outcome
	// for what is actually cached now (in either direction — a stale acceptance
	// masking a current rejection would let the gate admit traffic against
	// content the consumer cannot use).
	lastErr  error
	lastHash string
}

// Registry caches supply snapshots and fans changes out to the consumers that
// registered against each name.
type Registry struct {
	mu        sync.RWMutex
	snapshots map[string]Snapshot
	// consumers maps supply name → consumer key → entry (the consumer plus its
	// last notification outcome). The key is the caller's stable identity (e.g.
	// "workflow/node"), so re-registering after a re-activation replaces rather
	// than duplicates — and replacing a key replaces that key's outcome too.
	//
	// Readiness is deliberately NOT a separate assigned flag. An earlier
	// revision of this type tracked a single "accepted" bool per name, written
	// unconditionally from three different call sites (Apply's fan-out,
	// RegisterConsumer's immediate notify, UnregisterConsumer's last-consumer
	// case), each from its own outcome only. That is a lost-update bug by
	// construction: whichever write ran last won, regardless of what every
	// OTHER registered consumer had actually done. Keeping the outcome
	// per-consumer and deriving readiness as a live conjunction (see IsReady)
	// makes that class of bug structurally impossible — there is no aggregate
	// value left to race against itself.
	consumers map[string]map[string]consumerEntry

	// regSeq issues consumerEntry.regSeq values. Guarded by mu.
	regSeq uint64

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
		consumers: map[string]map[string]consumerEntry{},
	}
	empty := map[string]any{}
	r.decoded.Store(&empty)
	return r
}

// Apply records a snapshot and notifies this supply's consumers when the content
// hash changed. It never creates an execution or dispatches a task.
//
// A revision bump with an unchanged hash updates the cached revision but skips
// notification when every registered consumer has already accepted this exact
// content: rebuilding a consumer's derived state for identical bytes it already
// accepted is pure waste. It re-notifies when any consumer's readiness for this
// content is unresolved — it rejected it, or its last verdict was about content
// that has since been superseded. Readiness must always reflect the CURRENT
// consumer set's verdict on the CURRENT content, never a stale one.
//
// Consumer errors are collected and returned joined; every consumer is called
// regardless, and the cache is updated either way.
func (r *Registry) Apply(ctx context.Context, snap Snapshot) error {
	r.mu.Lock()
	prev, had := r.snapshots[snap.Name]
	changed := !had || prev.Hash != snap.Hash
	r.snapshots[snap.Name] = cloneSnapshot(snap)

	needNotify := changed
	if !needNotify {
		// Unchanged content, but re-notify if any registered consumer has not
		// accepted THIS content: it may have rejected it, or its last verdict
		// may be about superseded content (a callback that was still in flight
		// when this content was installed had its outcome dropped). Either way
		// its readiness is unresolved, and only a fresh notification resolves it.
		for _, entry := range r.consumers[snap.Name] {
			if entry.lastErr != nil || (entry.lastHash != "" && entry.lastHash != snap.Hash) {
				needNotify = true
				break
			}
		}
	}

	type target struct {
		key      string
		regSeq   uint64
		consumer Consumer
	}
	var targets []target
	if needNotify {
		for k, entry := range r.consumers[snap.Name] {
			targets = append(targets, target{key: k, regSeq: entry.regSeq, consumer: entry.consumer})
		}
	}

	if changed {
		r.republishDecodedLocked()
	} else if had && prev.Revision != snap.Revision {
		// Same bytes, newer server revision: republish so $supplies.<name>.$revision
		// reflects it, but this alone does not force notification above.
		r.republishDecodedLocked()
	}
	r.mu.Unlock()

	var errs []error
	results := make(map[string]error, len(targets))
	for _, t := range targets {
		err := safeNotify(ctx, t.consumer, snap)
		results[t.key] = err
		if err != nil {
			errs = append(errs, err)
		}
	}

	if needNotify {
		r.mu.Lock()
		for _, t := range targets {
			r.recordOutcomeLocked(snap.Name, t.key, t.regSeq, snap.Hash, results[t.key])
		}
		r.mu.Unlock()
	}

	return errors.Join(errs...)
}

// recordOutcomeLocked stores one notification's outcome against the entry that
// was notified. Three guards, all of them because callbacks run outside the
// lock:
//
//   - the entry still exists — it may have been unregistered mid-callback;
//   - regSeq matches — it may have been REPLACED mid-callback, and the newer
//     registration's own outcome must win;
//   - the notified content is still what is cached — content may have been
//     superseded mid-callback, and an outcome about content that is no longer
//     current says nothing about what IS current. Recording it anyway is the
//     stale-write-back bug: a stale acceptance would mask a current rejection
//     and let the gate admit traffic against unusable content.
//
// Dropping a superseded outcome loses nothing: the Apply that installed the
// newer content notifies every registered consumer itself.
//
// Caller must hold r.mu.
func (r *Registry) recordOutcomeLocked(name, key string, regSeq uint64, hash string, err error) {
	if cached, ok := r.snapshots[name]; !ok || cached.Hash != hash {
		return
	}
	cur, ok := r.consumers[name][key]
	if !ok || cur.regSeq != regSeq {
		return
	}
	cur.lastErr = err
	cur.lastHash = hash
	r.consumers[name][key] = cur
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

// IsReady reports whether a supply has content AND every CURRENTLY registered
// consumer has accepted THAT content. The gate uses this instead of a bare Get
// to distinguish "cached but unusable" (some consumer rejected the content)
// from "ready to serve traffic".
//
// This is computed live as a conjunction over the consumer set on every call —
// there is no separate stored flag to fall out of sync with that set. A supply
// with no snapshot is not ready. A supply with a snapshot but no registered
// consumers is always ready — nothing needs to build derived state from it.
//
// A consumer whose last outcome is for SUPERSEDED content counts as not ready,
// not as ready-by-default. It has said nothing about what is cached now, and
// the safe direction is the one that does not admit traffic: a runner that
// declines is visible as consumer-group lag, while one that admits on an
// unverified assumption mis-serves silently. This state is transient — the
// Apply that installed the newer content notifies every consumer, and its
// outcome lands here.
func (r *Registry) IsReady(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cached, ok := r.snapshots[name]
	if !ok {
		return false
	}
	for _, entry := range r.consumers[name] {
		if entry.lastErr != nil {
			return false
		}
		// Never notified (zero lastHash) is acceptance by the rule above: a
		// consumer registered before any content arrived has nothing to reject,
		// and Apply will notify it. A non-empty lastHash that does not match the
		// cached content is a verdict about something else.
		if entry.lastHash != "" && entry.lastHash != cached.Hash {
			return false
		}
	}
	return true
}

// RegisterConsumer registers c under (name, key), replacing any consumer already
// at that key — including that key's own last-outcome record, so a replacement
// consumer starts without inheriting its predecessor's rejection. If a snapshot
// for name is already cached, c is notified immediately — activation order
// between a supply and its consumer is not guaranteed, so a late-registering
// consumer must not wait for the next change — and that immediate notify's
// outcome is recorded under c's own key exactly like Apply's fan-out records
// each consumer's outcome under its own key. This is what makes IsReady's
// conjunction correct regardless of registration order: this is the path a
// gate reaches most often in practice, because content is usually fetched and
// cached before any consumer registers.
//
// The consumer callback itself keeps its documented best-effort semantics: a
// rejection here leaves the consumer on its last-good state, and does not
// prevent registration.
func (r *Registry) RegisterConsumer(name, key string, c Consumer) {
	r.mu.Lock()
	if r.consumers[name] == nil {
		r.consumers[name] = map[string]consumerEntry{}
	}
	r.regSeq++
	seq := r.regSeq
	r.consumers[name][key] = consumerEntry{consumer: c, regSeq: seq}
	snap, has := r.snapshots[name]
	r.mu.Unlock()

	if !has {
		return
	}
	err := safeNotify(context.Background(), c, snap)

	// Record under a fresh lock acquisition — never held across the callback
	// above — and only if this registration and this content are both still
	// current. See recordOutcomeLocked.
	r.mu.Lock()
	r.recordOutcomeLocked(name, key, seq, snap.Hash, err)
	r.mu.Unlock()
}

// UnregisterConsumer removes the consumer at (name, key). It is idempotent.
// Removing a rejecting consumer's entry is enough on its own to make IsReady's
// conjunction true again (once no other registered consumer is rejecting) —
// there is no separate aggregate state to update here.
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

// Ready returns the sorted subset of names that are NOT ready (see IsReady): no
// cached snapshot, or cached content that a currently-registered consumer
// rejected. Its semantics are defined entirely in terms of IsReady so this type
// never carries two divergent readiness definitions — Task 19's heartbeat
// readiness reporting and the activation gate must agree on what "ready" means
// for the same name.
func (r *Registry) Ready(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	var missing []string
	for _, n := range names {
		if !r.IsReady(n) {
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
