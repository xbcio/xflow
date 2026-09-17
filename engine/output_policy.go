package engine

import "github.com/xbcio/xflow/engine/graph"

// nodeOutputIsPrivate reports the compiled node policy. This is intentionally
// separate from persisted snapshot state: a current graph is authoritative for
// a normal execution commit, while snapshots only provide a fail-closed
// fallback for public reads when that graph is unavailable.
func nodeOutputIsPrivate(meta graph.NodeMeta) bool {
	return meta.Output != nil && meta.Output.Private
}

// privateOutputForTask derives output privacy from the authoritative graph for
// a leased task. A malformed task must not turn a potentially private output
// into a public one, so unresolved graph/index/name combinations fail closed.
func privateOutputForTask(g *graph.Graph, task *Task) bool {
	if g == nil || task == nil || task.NodeIdx < 0 || task.NodeIdx >= g.NodeCount() {
		return true
	}
	meta := g.NodeAt(task.NodeIdx)
	if meta.Name != task.NodeName {
		return true
	}
	return nodeOutputIsPrivate(meta)
}
