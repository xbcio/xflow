package blob

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/xbcio/xflow/namespace"
)

// memObject is one row of MemoryIndex's object table.
type memObject struct {
	size       int64
	stored     int64
	codec      string
	generation int64
	state      string
	lastRefAt  time.Time
}

// ref returns the Ref the index records for the object at key.
func (o *memObject) ref(key memKey) Ref {
	return Ref{Namespace: string(key.ns), Digest: key.digest, Size: o.size, Stored: o.stored, Codec: o.codec}
}

// touch records a reference at now. The last-reference time only moves forward:
// with several nodes sharing a SQL index their clocks differ, and a reference
// stamped by a slow clock must not make an object look idle again.
func (o *memObject) touch(now time.Time) {
	if now.After(o.lastRefAt) {
		o.lastRefAt = now
	}
}

// MemoryIndex is an in-process Index for tests and local use. It keeps every
// object and link in RAM and is safe for concurrent use.
//
// A single mutex guards all state. Purge holds it while it runs the delete
// callback, which is what makes the delete atomic with respect to Register and
// Link; a SQL index gets the same effect from a row lock.
type MemoryIndex struct {
	now func() time.Time

	mu      sync.Mutex
	objects map[memKey]*memObject
	// links maps an object to the expiry of each execution's link to it.
	links map[memKey]map[string]time.Time
}

var _ Index = (*MemoryIndex)(nil)

// NewMemoryIndex returns an empty MemoryIndex that reads the time from now. A
// nil now means time.Now. Tests pass a controllable clock so that grace periods
// and link expiries can be crossed without waiting.
func NewMemoryIndex(now func() time.Time) *MemoryIndex {
	if now == nil {
		now = time.Now
	}
	return &MemoryIndex{
		now:     func() time.Time { return IndexTime(now()) },
		objects: make(map[memKey]*memObject),
		links:   make(map[memKey]map[string]time.Time),
	}
}

// Register implements Index.
func (m *MemoryIndex) Register(ctx context.Context, ref Ref) (Registration, error) {
	if err := ctx.Err(); err != nil {
		return Registration{}, err
	}
	if err := ref.Validate(); err != nil {
		return Registration{}, err
	}
	key := memKey{ns: namespace.Namespace(ref.Namespace), digest: ref.Digest}
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	obj, ok := m.objects[key]
	if !ok {
		obj = &memObject{size: ref.Size, stored: ref.Stored, codec: ref.Codec, generation: 1, state: ObjectLive, lastRefAt: now}
		m.objects[key] = obj
		return Registration{Ref: obj.ref(key)}, nil
	}
	if obj.size != ref.Size {
		return Registration{}, fmt.Errorf("%w: %s/%s is recorded with size %d, ref has %d", ErrMetadataConflict, key.ns, key.digest, obj.size, ref.Size)
	}
	revived := obj.state == ObjectDeleting
	if revived {
		obj.state = ObjectLive
		obj.generation++
		obj.stored, obj.codec = ref.Stored, ref.Codec
	}
	obj.touch(now)
	return Registration{Ref: obj.ref(key), Revived: revived}, nil
}

// Link implements Index.
func (m *MemoryIndex) Link(ctx context.Context, execID string, ref Ref, expireAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateExecID(execID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRef, err)
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	key := memKey{ns: namespace.Namespace(ref.Namespace), digest: ref.Digest}
	expireAt = IndexTime(expireAt)
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	if !expireAt.After(now) {
		return fmt.Errorf("blob: link expiry %s is not after the current time %s", expireAt.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	}
	obj, ok := m.objects[key]
	if !ok {
		return fmt.Errorf("%w: %s/%s is not registered", ErrNotFound, key.ns, key.digest)
	}
	if obj.state != ObjectLive {
		return fmt.Errorf("%w: %s/%s is claimed for deletion", ErrFenced, key.ns, key.digest)
	}
	byExec := m.links[key]
	if byExec == nil {
		byExec = make(map[string]time.Time)
		m.links[key] = byExec
	}
	if cur, ok := byExec[execID]; !ok || expireAt.After(cur) {
		byExec[execID] = expireAt
	}
	obj.touch(now)
	return nil
}

// PruneLinks implements Index.
func (m *MemoryIndex) PruneLinks(ctx context.Context, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if limit <= 0 {
		return 0, fmt.Errorf("blob: prune limit %d is not positive", limit)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	removed := 0
	for key, byExec := range m.links {
		for execID, expireAt := range byExec {
			if removed == limit {
				return removed, nil
			}
			if !expireAt.After(now) {
				delete(byExec, execID)
				removed++
			}
		}
		if len(byExec) == 0 {
			delete(m.links, key)
		}
	}
	return removed, nil
}

// Candidates implements Index.
func (m *MemoryIndex) Candidates(ctx context.Context, grace time.Duration, limit int) ([]Ref, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateGCArgs(grace, limit); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	var keys []memKey
	for key := range m.objects {
		if m.isCandidateLocked(key, now, grace) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := m.objects[keys[i]], m.objects[keys[j]]
		if !a.lastRefAt.Equal(b.lastRefAt) {
			return a.lastRefAt.Before(b.lastRefAt)
		}
		if keys[i].ns != keys[j].ns {
			return keys[i].ns < keys[j].ns
		}
		return keys[i].digest < keys[j].digest
	})
	if len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]Ref, len(keys))
	for i, key := range keys {
		out[i] = m.objects[key].ref(key)
	}
	return out, nil
}

// MarkDeleting implements Index.
func (m *MemoryIndex) MarkDeleting(ctx context.Context, ref Ref, grace time.Duration) (Fence, error) {
	if err := ctx.Err(); err != nil {
		return Fence{}, err
	}
	if err := ref.Validate(); err != nil {
		return Fence{}, err
	}
	if err := ValidateGCArgs(grace, 1); err != nil {
		return Fence{}, err
	}
	key := memKey{ns: namespace.Namespace(ref.Namespace), digest: ref.Digest}
	m.mu.Lock()
	defer m.mu.Unlock()

	obj, ok := m.objects[key]
	if !ok {
		return Fence{}, fmt.Errorf("%w: %s/%s is not registered", ErrNotFound, key.ns, key.digest)
	}
	// An object that is already claimed is still garbage: Link refuses it and a
	// writer's Register would have revived it, so nothing can have changed.
	if obj.state == ObjectLive {
		if !m.isCandidateLocked(key, m.now(), grace) {
			return Fence{}, fmt.Errorf("%w: %s/%s", ErrNotCandidate, key.ns, key.digest)
		}
		obj.state = ObjectDeleting
	}
	return Fence{Ref: obj.ref(key), Generation: obj.generation}, nil
}

// Purge implements Index.
func (m *MemoryIndex) Purge(ctx context.Context, fence Fence, del func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fence.Validate(); err != nil {
		return err
	}
	if del == nil {
		return fmt.Errorf("blob: purge needs a delete callback")
	}
	key := memKey{ns: namespace.Namespace(fence.Ref.Namespace), digest: fence.Ref.Digest}
	m.mu.Lock()
	defer m.mu.Unlock()

	obj, ok := m.objects[key]
	if !ok {
		return fmt.Errorf("%w: %s/%s no longer exists", ErrFenced, key.ns, key.digest)
	}
	if obj.state != ObjectDeleting || obj.generation != fence.Generation {
		return fmt.Errorf("%w: %s/%s is %s at generation %d, the claim was generation %d",
			ErrFenced, key.ns, key.digest, obj.state, obj.generation, fence.Generation)
	}
	if err := del(ctx); err != nil {
		return err
	}
	delete(m.objects, key)
	delete(m.links, key)
	return nil
}

// isCandidateLocked reports whether the object at key is garbage as of now:
// last referenced before the grace period and with no unexpired link. The
// caller holds m.mu.
func (m *MemoryIndex) isCandidateLocked(key memKey, now time.Time, grace time.Duration) bool {
	obj := m.objects[key]
	if obj == nil || !obj.lastRefAt.Before(IndexTime(now.Add(-grace))) {
		return false
	}
	for _, expireAt := range m.links[key] {
		if expireAt.After(now) {
			return false
		}
	}
	return true
}
