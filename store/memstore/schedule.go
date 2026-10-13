package memstore

import (
	"context"
	"sort"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

var _ store.ScheduleOverrideStore = (*Store)(nil)

// scheduleOverrideKey is (namespace, workflow, node) flattened. The separator is
// "/" like the other composite keys in this package; the store refuses a
// namespace containing it (store.ValidateNamespaceScope), so the three parts
// cannot be confused for one another.
func scheduleOverrideKey(ns namespace.Namespace, workflowID types.WorkflowID, node string) string {
	return string(ns) + "/" + string(workflowID) + "/" + node
}

// cloneScheduleOverride copies a row including the pointer fields, so neither a
// caller's write nor a reader's mutation can reach the stored row through an
// aliased pointer.
func cloneScheduleOverride(rec *store.ScheduleOverride) *store.ScheduleOverride {
	cp := *rec
	cp.Expression = cloneStringPtr(rec.Expression)
	cp.Timezone = cloneStringPtr(rec.Timezone)
	cp.Misfire = cloneStringPtr(rec.Misfire)
	cp.Overlap = cloneStringPtr(rec.Overlap)
	cp.Paused = cloneBoolPtr(rec.Paused)
	return &cp
}

func cloneStringPtr(v *string) *string {
	if v == nil {
		return nil
	}
	cp := *v
	return &cp
}

func cloneBoolPtr(v *bool) *bool {
	if v == nil {
		return nil
	}
	cp := *v
	return &cp
}

func cloneScheduleOverrides(m map[string]*store.ScheduleOverride) map[string]*store.ScheduleOverride {
	out := make(map[string]*store.ScheduleOverride, len(m))
	for k, v := range m {
		out[k] = cloneScheduleOverride(v)
	}
	return out
}

func (s *Store) GetScheduleOverride(_ context.Context, ns namespace.Namespace, workflowID types.WorkflowID, node string) (*store.ScheduleOverride, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.scheduleOverrides[scheduleOverrideKey(ns, workflowID, node)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return cloneScheduleOverride(rec), nil
}

func (s *Store) ListScheduleOverrides(_ context.Context, ns namespace.Namespace, workflowID types.WorkflowID) ([]*store.ScheduleOverride, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*store.ScheduleOverride, 0, len(s.scheduleOverrides))
	for _, rec := range s.scheduleOverrides {
		if rec.Namespace == ns && rec.WorkflowID == workflowID {
			out = append(out, cloneScheduleOverride(rec))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node < out[j].Node })
	return out, nil
}

func (s *Store) PutScheduleOverride(_ context.Context, rec *store.ScheduleOverride, ifMatch *uint64) (*store.ScheduleOverride, error) {
	if err := store.ValidateNamespaceScope(rec.Namespace); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scheduleOverrideKey(rec.Namespace, rec.WorkflowID, rec.Node)
	cur, exists := s.scheduleOverrides[key]
	var curRev uint64
	if exists {
		curRev = cur.Revision
	}
	if ifMatch != nil && *ifMatch != curRev {
		return nil, store.ErrRevisionConflict
	}
	next := cloneScheduleOverride(rec)
	next.Revision = curRev + 1
	next.UpdatedAt = time.Now().UTC()
	s.scheduleOverrides[key] = next
	return cloneScheduleOverride(next), nil
}

func (s *Store) DeleteScheduleOverride(_ context.Context, ns namespace.Namespace, workflowID types.WorkflowID, node string, ifMatch *uint64) error {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scheduleOverrideKey(ns, workflowID, node)
	cur, exists := s.scheduleOverrides[key]
	// An absent row is not a conflict even under If-Match: the caller asked for
	// the overlay to be gone, and it is. It is only a conflict when the row
	// exists at a revision the caller did not expect — that is the case where
	// someone else's change would be silently discarded.
	if !exists {
		return nil
	}
	if ifMatch != nil && *ifMatch != cur.Revision {
		return store.ErrRevisionConflict
	}
	delete(s.scheduleOverrides, key)
	return nil
}

func (s *Store) DeleteScheduleOverridesForWorkflow(_ context.Context, ns namespace.Namespace, workflowID types.WorkflowID) (int, error) {
	if err := store.ValidateNamespaceScope(ns); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for key, rec := range s.scheduleOverrides {
		if rec.Namespace == ns && rec.WorkflowID == workflowID {
			delete(s.scheduleOverrides, key)
			removed++
		}
	}
	return removed, nil
}
