package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// BatchItemsStore is an optional StateStore capability that keeps a map node's
// items array out of the batch tasks. Without it every batch carries its own
// copy of the whole array (items plus all_items), so the bytes the queue and
// the outbox hold grow with batches x items; with it each batch carries a small
// reference plus its [start, end) window and the array is stored once.
//
// The engine finds the capability by type assertion on its StateStore, exactly
// like DurableLeaseExpander. A store that does not implement it, or that
// answers externalized=false, leaves every batch inline.
type BatchItemsStore interface {
	// ExternalizeItems stores items for the given execution and returns an
	// opaque reference to them. externalized=false means nothing was stored (the
	// capability is off, or the array is too small to be worth it) and the caller
	// keeps the items inline. A non-nil error is a failure of the store itself:
	// the engine fails the expansion with it rather than falling back to inline,
	// because only the store can tell a degradation it chose from an outage.
	ExternalizeItems(ctx context.Context, execID string, items []any) (ref json.RawMessage, externalized bool, err error)
	// ResolveItems returns the array a reference was made from. The engine
	// caches the result per reference, so an implementation sees one call per
	// reference per cache miss, not one per batch.
	ResolveItems(ctx context.Context, ref json.RawMessage) ([]any, error)
}

// Payload keys of a batch task whose items live in a BatchItemsStore. They
// replace "items" and "all_items"; a task carries either the pair or the trio,
// never both, and readers accept both.
const (
	batchItemsRefKey   = "items_ref"
	batchItemsStartKey = "items_start"
	batchItemsEndKey   = "items_end"
)

// externalizeBatchPayload rewrites an inline batch task so it points at the
// stored items array instead of carrying it: items and all_items go away, and
// the reference plus this batch's half-open window [start, end) into the array
// take their place. Everything else in the payload is left alone.
func externalizeBatchPayload(task *Task, ref json.RawMessage, start, end int) {
	data := task.Payload.Data
	delete(data, "items")
	delete(data, "all_items")
	data[batchItemsRefKey] = ref
	data[batchItemsStartKey] = start
	data[batchItemsEndKey] = end
}

// externalizeExpansionBatches stores the map node's items array once and points
// every batch entry at it. entries[i] is batch i's outbox entry as
// expansionBatchTask built it; they are rewritten in place only when all of the
// following hold, and are otherwise left exactly as built (inline):
//
//   - the state store implements BatchItemsStore;
//   - data["items"] is the whole array, and the batches add up to exactly that
//     many elements, so that each batch is a [start, end) window of it;
//   - the store reports externalized=true.
//
// A store error is returned, not absorbed. It is returned before the expansion
// is started, like any other failure of the expansion, and degrading to inline
// is the store's decision (externalized=false), never the engine's.
func (e *Engine) externalizeExpansionBatches(ctx context.Context, execID types.ExecutionID, data map[string]any, batches [][]any, entries []OutboxEntry) error {
	store, ok := e.state.(BatchItemsStore)
	if !ok {
		return nil
	}
	items, ok := data["items"].([]any)
	if !ok {
		return nil
	}
	total := 0
	for _, batch := range batches {
		total += len(batch)
	}
	if total != len(items) || len(entries) != len(batches) {
		return nil
	}
	ref, externalized, err := store.ExternalizeItems(ctx, string(execID), items)
	if err != nil {
		return err
	}
	if !externalized {
		return nil
	}
	start := 0
	for i, batch := range batches {
		end := start + len(batch)
		externalizeBatchPayload(&entries[i].Task, ref, start, end)
		start = end
	}
	return nil
}

// batchItemsCacheEntries is how many resolved items arrays one process keeps.
//
// Worst case memory is this many arrays at the store's per-blob limit (64 MiB of
// encoded JSON by default), and a decoded []any is several times larger than its
// encoding, so 4 maximal arrays are a ceiling to size a pod against, not the
// usual footprint. It is small on purpose: batches of one map node arrive
// back to back, so a handful of entries absorbs every batch after the first.
const batchItemsCacheEntries = 4

// batchItemsCache holds resolved arrays. The key is the context namespace plus
// the canonical bytes of the reference (see batchItemsCacheKey), so an entry is
// only ever served to a caller that would have been allowed to resolve the same
// reference itself.
//
// The arrays in it are the cache's own and are never lent out: a body can write
// through the items it is given (a script engine binds Go maps and slices into
// its runtime without copying), and a lent array would let one batch change what
// the next batch of the map node, or another execution storing the same content,
// reads. resolveBatchView hands every batch a private deep copy instead. That
// costs one O(N) copy per batch; what the cache still saves is the store read,
// the decompression and the JSON decode.
var batchItemsCache = newBatchItemsCache(batchItemsCacheEntries)

func newBatchItemsCache(size int) *lru.Cache[string, []any] {
	c, err := lru.New[string, []any](size)
	if err != nil {
		// Only reachable with a non-positive size, which is a programming error
		// in this package rather than a runtime condition.
		panic("engine: invalid batch items cache size: " + err.Error())
	}
	return c
}

// errBatchItemsUnsupported is returned when a batch task points at stored items
// but this engine's state store cannot resolve them (a backend without the
// capability, or one downgraded after tasks were written).
var errBatchItemsUnsupported = errors.New("batch task references stored items but the state store cannot resolve them")

// canonicalBatchItemsRef turns the items_ref payload value into the bytes handed
// to the store and used as the cache key. In process the value is the
// json.RawMessage the store returned; after a queue round trip it is a decoded
// map. Both must produce the same bytes, so the value is re-encoded through a
// decode that keeps numbers verbatim and writes object keys in sorted order.
func canonicalBatchItemsRef(value any) (json.RawMessage, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("batch task has invalid %s: %w", batchItemsRefKey, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var decoded any
	if err := dec.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("batch task has invalid %s: %w", batchItemsRefKey, err)
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return nil, fmt.Errorf("batch task has invalid %s: %w", batchItemsRefKey, err)
	}
	return canonical, nil
}

// resolveBatchItemsArray returns the stored array for ref, from the process
// cache when it is there and from the store otherwise. A hit never reaches the
// store.
func (e *Engine) resolveBatchItemsArray(ctx context.Context, ref json.RawMessage) ([]any, error) {
	key := string(namespace.FromContext(ctx)) + "\x00" + string(ref)
	if all, ok := batchItemsCache.Get(key); ok {
		return all, nil
	}
	store, ok := e.state.(BatchItemsStore)
	if !ok {
		return nil, errBatchItemsUnsupported
	}
	all, err := store.ResolveItems(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("resolve batch items: %w", err)
	}
	batchItemsCache.Add(key, all)
	return all, nil
}

// resolveBatchView returns the task the batch readers (expansionBatchLease,
// mapBatchingContext) should look at. A task that carries its items inline is
// returned as is. A task with items_ref comes back as a shallow copy whose
// payload is a new map: the small keys are copied, items is this batch's
// [items_start, items_end) window and all_items is the whole array. The original
// task and its payload are never touched, because the task may be the very
// object a queue or a runner directory persists, and writing the array into it
// would put back the bytes the reference exists to keep out.
//
// The array in the view is a private deep copy made for this call, and items is
// a window of that copy, so a body that sorts $items or edits the maps in it
// changes nothing another batch, another execution or the cache can see. It is
// the isolation an inline batch has by construction (every task decodes its own
// all_items), bought here with one O(N) copy per batch rather than per item.
func (e *Engine) resolveBatchView(ctx context.Context, t *Task) (*Task, error) {
	if t == nil || t.Payload == nil || t.Payload.Data == nil {
		return t, nil
	}
	rawRef := t.Payload.Data[batchItemsRefKey]
	if rawRef == nil {
		return t, nil
	}
	ref, err := canonicalBatchItemsRef(rawRef)
	if err != nil {
		return nil, err
	}
	start, err := batchPayloadInt(t.Payload.Data, batchItemsStartKey)
	if err != nil {
		return nil, err
	}
	end, err := batchPayloadInt(t.Payload.Data, batchItemsEndKey)
	if err != nil {
		return nil, err
	}
	all, err := e.resolveBatchItemsArray(ctx, ref)
	if err != nil {
		return nil, err
	}
	if start < 0 || start > end || end > len(all) {
		return nil, fmt.Errorf("batch task window [%d, %d) is outside its %d stored items", start, end, len(all))
	}
	data := make(map[string]any, len(t.Payload.Data)+2)
	for key, value := range t.Payload.Data {
		switch key {
		case batchItemsRefKey, batchItemsStartKey, batchItemsEndKey:
		default:
			data[key] = value
		}
	}
	// The three-index slice keeps an append by the reader from writing into the
	// neighbouring batch's window of the copy.
	own := cloneBatchItems(all)
	data["items"] = own[start:end:end]
	data["all_items"] = own

	view := *t
	payload := *t.Payload
	payload.Data = data
	view.Payload = &payload
	return &view, nil
}

// cloneBatchItems returns a deep copy of a stored items array. The arrays come
// from BatchItemsStore.ResolveItems, which decodes JSON, so the only containers
// in them are map[string]any and []any; every other value (numbers, strings,
// bools, nil) is immutable and is shared. A store that returned another
// container type would have it shared too.
func cloneBatchItems(items []any) []any {
	if items == nil {
		return nil
	}
	out := make([]any, len(items))
	for i, item := range items {
		out[i] = cloneBatchValue(item)
	}
	return out
}

func cloneBatchValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		if v == nil {
			return v
		}
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = cloneBatchValue(item)
		}
		return out
	case []any:
		return cloneBatchItems(v)
	default:
		return value
	}
}

// FailBatchTask settles batch t as failed without running it. It is for a batch
// whose work cannot be done at all, as opposed to one that ran and failed: today
// that is a batch whose stored items cannot be read back and never will be.
//
// The settlement is the one a runner's failed report gets: the batch is
// committed as Failed through the parent lease fence, its result carries the
// reason under batchErrorKey, and if it is the last batch to arrive the map node
// is finalized. The map node then fails according to its own OnError, because a
// failed batch is a hole in the results that no setting of continue_on_error
// fills in (that setting only decides when a batch whose items failed counts as
// failed, see BatchResultForCommit).
//
// A batch that no longer counts is not an error: a parent reclaimed into a newer
// generation, or an execution that is no longer active, returns nil. A non-nil
// error is a failure of the commit itself and means the batch is still
// unsettled, so the caller may call again.
//
// cause is recorded in the node's error text, so it must already be safe to
// show; the blob layer's errors are built that way.
func (e *Engine) FailBatchTask(ctx context.Context, t *Task, cause error) error {
	lease, childExecID, _, err := expansionBatchLease(t)
	if err != nil {
		return err
	}
	_, active, err := e.loadActiveGraph(ctx, lease.Task.ExecutionID)
	if err != nil {
		return fmt.Errorf("load graph for loop/split parent %q: %w", lease.Task.ExecutionID, err)
	}
	if !active {
		return nil
	}
	return e.settleUnreadableBatch(ctx, lease, childExecID, t, cause)
}

// settleUnreadableBatch commits the failed verdict FailBatchTask describes.
// lease is the parent lease expansionBatchLease read off the task. Fenced-off and
// inactive outcomes all end the same way for the caller, the batch no longer
// counts, so only a transient error is reported.
func (e *Engine) settleUnreadableBatch(ctx context.Context, lease *TaskLease, childExecID types.ExecutionID, batch *Task, cause error) error {
	reason := "batch items unavailable"
	if cause != nil {
		reason += ": " + cause.Error()
	}
	_, err := e.settleExpandedBatch(ctx, lease, childExecID, batch, types.ExecutionStatusFailed, map[string]any{batchErrorKey: reason})
	return err
}
