package memstore

import (
	"context"
	"time"

	"github.com/xbcio/xflow/store"
)

func supplyKey(namespace, name string) string { return namespace + "/" + name }

func (s *Store) GetSupply(_ context.Context, namespace, name string) (*store.SupplyResource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.supplies[supplyKey(namespace, name)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *rec
	cp.Content = append([]byte(nil), rec.Content...)
	return &cp, nil
}

func (s *Store) PutSupply(_ context.Context, rec *store.SupplyResource, ifMatch *uint64) (*store.SupplyResource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := supplyKey(rec.Namespace, rec.Name)
	cur, exists := s.supplies[key]
	var curRev uint64
	if exists {
		curRev = cur.Revision
	}
	if ifMatch != nil && *ifMatch != curRev {
		return nil, store.ErrRevisionConflict
	}
	next := *rec
	next.Content = append([]byte(nil), rec.Content...)
	next.Revision = curRev + 1
	next.ContentHash = store.ContentHash(next.Content)
	next.UpdatedAt = time.Now().UTC()
	s.supplies[key] = &next
	out := next
	out.Content = append([]byte(nil), next.Content...)
	return &out, nil
}

func cloneSupplies(m map[string]*store.SupplyResource) map[string]*store.SupplyResource {
	out := make(map[string]*store.SupplyResource, len(m))
	for k, v := range m {
		cp := *v
		cp.Content = append([]byte(nil), v.Content...)
		out[k] = &cp
	}
	return out
}
