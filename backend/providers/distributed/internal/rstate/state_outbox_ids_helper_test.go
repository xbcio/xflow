package rstate

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// readOutboxReadyIndex is readOutboxReadyIndexRefs for one namespace with the
// result collected as bare IDs, which is the shape the orphan-repair tests assert
// on. Production code reads the index through the (namespace, ID) form; this
// projection exists so those tests keep their single-namespace vocabulary.
//
// ids is both input and output, as before: IDs already in it count as seen and
// are not added again.
func (s *Store) readOutboxReadyIndex(ctx context.Context, t namespace.Namespace, now time.Time, limit int, ids map[types.ExecutionID]struct{}) (int, error) {
	refs := make(map[engine.OutboxExecutionRef]struct{}, len(ids))
	for id := range ids {
		refs[engine.OutboxExecutionRef{Namespace: t, ID: id}] = struct{}{}
	}
	added, err := s.readOutboxReadyIndexRefs(ctx, t, now, limit, refs)
	for ref := range refs {
		ids[ref.ID] = struct{}{}
	}
	return added, err
}
