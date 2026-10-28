package engine

import (
	"context"
	"sort"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// OutboxExecutionRef names an execution that has ready outbox work together with
// the namespace that execution lives in.
//
// The pair is the identity, not the ID: two namespaces may hold executions with
// the same ID, and each has its own outbox.
type OutboxExecutionRef struct {
	Namespace namespace.Namespace
	ID        types.ExecutionID
}

// NamespacedOutboxLister is an optional StateStore capability that reports which
// namespace each execution with ready outbox work belongs to.
//
// It exists because ListOutboxExecutions answers with bare execution IDs, and a
// store that keys its outbox by namespace cannot be flushed from a bare ID: the
// dispatcher runs on a context that carries no namespace, so every key the flush
// builds resolves to the default namespace. Discovery then finds the execution
// and the flush looks for its outbox in the wrong place and finds nothing. The
// work is never delivered -- a timer resume, or the backoff of a failed node,
// leaves the execution running forever -- and nothing reports it, because an
// empty claim is a normal result. Returning the namespace lets the dispatcher
// flush each execution under the namespace it was found in.
//
// It is optional, not part of AtomicStateStore, for the same reason as every
// other capability here: a store whose outbox is not partitioned by namespace
// (the in-process store) has nothing to add, and widening the required interface
// would break every implementation and fake for no gain. A store that does not
// implement it is discovered with ListOutboxExecutions and flushed on the
// dispatcher's own context, exactly as before.
//
// A store that implements it must keep ListOutboxExecutions answering the same
// executions with the namespaces dropped, so the two views cannot disagree about
// what is pending.
type NamespacedOutboxLister interface {
	// ListOutboxExecutionRefs reports up to roughly limit executions with ready
	// outbox work, each with its namespace. It has the same paging and budget
	// contract as ListOutboxExecutions.
	ListOutboxExecutionRefs(ctx context.Context, limit int) ([]OutboxExecutionRef, error)
}

// discoverOutboxRefs reports one discovery page for the dispatcher, ordered so a
// drain is deterministic.
//
// A NamespacedOutboxLister is asked first. The refs are ordered and de-duplicated
// by (namespace, ID) rather than by ID: sorting by ID alone would interleave
// namespaces arbitrarily, and de-duplicating by ID alone would drop one of two
// same-named executions in different namespaces.
//
// Any other store is read through ListOutboxExecutions, ordered by ID and not
// de-duplicated, as the dispatcher always did. Its refs carry no namespace, which
// tells the flush to leave the dispatcher's context untouched.
func discoverOutboxRefs(ctx context.Context, state AtomicStateStore, limit int) ([]OutboxExecutionRef, error) {
	if lister, ok := state.(NamespacedOutboxLister); ok {
		refs, err := lister.ListOutboxExecutionRefs(ctx, limit)
		if err != nil {
			return nil, err
		}
		return sortDedupeOutboxRefs(refs), nil
	}
	ids, err := state.ListOutboxExecutions(ctx, limit)
	if err != nil {
		return nil, err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	refs := make([]OutboxExecutionRef, len(ids))
	for i, id := range ids {
		refs[i] = OutboxExecutionRef{ID: id}
	}
	return refs, nil
}

// sortDedupeOutboxRefs orders refs by (namespace, ID) and drops exact repeats,
// reusing the slice's storage.
func sortDedupeOutboxRefs(refs []OutboxExecutionRef) []OutboxExecutionRef {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Namespace != refs[j].Namespace {
			return refs[i].Namespace < refs[j].Namespace
		}
		return refs[i].ID < refs[j].ID
	})
	out := refs[:0]
	for i, ref := range refs {
		if i > 0 && ref == refs[i-1] {
			continue
		}
		out = append(out, ref)
	}
	return out
}

// flushContext returns the context FlushOutbox must run on for this ref. A ref
// that names a namespace carries it on the context, which is where every store
// method reads it from; a ref without one leaves ctx as it was.
//
// FlushOutbox and everything it starts -- the lease keeper's renewals, the ack,
// the release, the failure record and the dead-letter -- inherit this context,
// so anchoring it once here is what puts the whole delivery under the namespace.
func (r OutboxExecutionRef) flushContext(ctx context.Context) context.Context {
	if r.Namespace == "" {
		return ctx
	}
	return namespace.WithNamespace(ctx, r.Namespace)
}
