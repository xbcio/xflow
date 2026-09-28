package engine

import "github.com/xbcio/xflow/types"

// NodeStateKey is the slot a node's private state occupies inside its own stored
// output.
//
// The engine persists a node's state by folding it into the map it stores as
// that node's output, because that map is already the one thing a resumption
// reads back, and a second storage slot would mean a second write to fail
// halfway, a second TTL to renew, and a second thing for every backend to
// implement. The fold is invisible from outside: every read that hands an output
// to anybody -- a downstream node's Data, a $nodes reference, a body's snapshot
// of the outer graph, the inspection surface -- goes through StripNodeState,
// which removes this key. A node sees its own state only through Input.State,
// which is read from this slot and nowhere else.
//
// The key is "$"-prefixed because that prefix is the engine's, not the DSL's: a
// node cannot reach it through an expression, and no node author is invited to
// write it. It is not a security boundary on its own — a node CAN publish a
// "$state" key, since output maps are untyped — which is exactly why the engine
// strips rather than trusts: a forged key is dropped everywhere it could be
// read, so publishing one is inert.
const NodeStateKey = "$state"

// withNodeState folds a node's private state into the map stored as its output.
//
// A nil state returns data unchanged rather than a copy with an empty slot, so a
// node that does not use state stores exactly the bytes it did before this
// mechanism existed.
func withNodeState(data, state map[string]any) map[string]any {
	if state == nil {
		return data
	}
	out := make(map[string]any, len(data)+1)
	for k, v := range data {
		out[k] = v
	}
	out[NodeStateKey] = state
	return out
}

// splitNodeState separates a stored output into the node's private state and the
// public remainder, both as maps the caller owns.
//
// This is the only reader that KEEPS the state slot, and it is reachable only
// from a resumption of the node that owns the output. Callers that are not the
// owning node use StripNodeState.
func splitNodeState(raw map[string]any) (state map[string]any, data map[string]any) {
	if raw == nil {
		return nil, nil
	}
	if inner, ok := raw[NodeStateKey].(map[string]any); ok && inner != nil {
		// Copied, not aliased: the store must not be reachable for mutation
		// through the input a handler holds.
		state = make(map[string]any, len(inner))
		for k, v := range inner {
			state[k] = v
		}
	}
	// Always a fresh map, so that the scope merge and the handler itself write
	// into a copy rather than into the store's map.
	data = make(map[string]any, len(raw))
	for k, v := range raw {
		if k == NodeStateKey {
			continue
		}
		data[k] = v
	}
	return state, data
}

// StripNodeState returns m without the engine's node-state slot.
//
// Every projection of a stored output goes through it -- the engine's own reads
// and, for the local backend's wait result, callers outside the engine -- so that
// there is exactly one definition of what a reader of a node output may see. It
// is exported for that second set of callers; a node's handler must never need
// it, because the input it receives has already been stripped.
//
// When the slot is absent -- every node that uses no state, which is nearly all
// of them -- m is returned as-is so no copy is paid. When it is present the
// result is a fresh map, which is why callers that need an isolated map must
// still clone first; this function removes the slot, it does not take ownership.
// Crucially it never removes it IN PLACE: m may be the map the store holds.
func StripNodeState(m map[string]any) map[string]any {
	if _, ok := m[NodeStateKey]; !ok {
		return m
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		if k == NodeStateKey {
			continue
		}
		out[k] = v
	}
	return out
}

// stripNodeStateFromInput removes the engine's node-state slot from every channel
// buildInput assembled: the main data, each multi-port input, and each $nodes
// entry.
//
// It runs once, after the whole input is assembled, because the slot can arrive
// from a source buildInput does not otherwise filter: applyExecutionScope merges
// the execution scope into Data, and a scope is not engine-owned -- submission
// params become one. Stripping at the exit makes "Input.Data never contains the
// node-state key" hold regardless of which source filled it, which is what lets
// Input.State be the only way a node reads its own state back.
func stripNodeStateFromInput(input *types.Input) {
	input.Data = StripNodeState(input.Data)
	for name, raw := range input.Inputs {
		if m, ok := raw.(map[string]any); ok {
			input.Inputs[name] = StripNodeState(m)
		}
	}
	for name, raw := range input.Nodes {
		if m, ok := raw.(map[string]any); ok {
			input.Nodes[name] = StripNodeState(m)
		}
	}
}
