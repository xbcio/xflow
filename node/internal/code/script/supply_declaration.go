package script

import (
	"sort"
	"sync"
)

// supplyDeclarationTable records which supply nodes each wasm script node
// consumes, as declared by the control plane at activation time and consumed by
// Execute once boundary evaluation has resolved the real artifact digest.
//
// It is a process-global for the same reason sharedReactorHost and
// sharedArtifactCode are: the producer (the runner's activation handler, in
// service/runner) and the consumer (ScriptNode.Execute, here) have no object
// graph in common, and threading a handle from one to the other would mean a new
// field on types.Input for something that is per-process, not per-message.
//
// Entries are REFERENCE COUNTED, not overwritten. One activation may be hosted
// at several replicas on one runner, each declaring the same pair; a plain
// delete on the first Deactivate would strand the others with no declaration,
// and §4.3.1's execution-time guard would then fail every one of their messages.
type supplyDeclarationTable struct {
	mu sync.RWMutex
	// entries maps a node's runtime identity to supply name -> refcount.
	entries map[supplyDeclarationKey]map[string]int
}

type supplyDeclarationKey struct {
	workflow string
	node     string
}

func newSupplyDeclarationTable() *supplyDeclarationTable {
	return &supplyDeclarationTable{entries: make(map[supplyDeclarationKey]map[string]int)}
}

// supplyDeclarations is the process-wide table. Production writes to it through
// the node-package forwarders; Execute reads it directly.
var supplyDeclarations = newSupplyDeclarationTable()

func (t *supplyDeclarationTable) declare(workflow, node string, supplies []string) {
	if workflow == "" || node == "" || len(supplies) == 0 {
		return
	}
	k := supplyDeclarationKey{workflow: workflow, node: node}
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.entries[k]
	if m == nil {
		m = make(map[string]int, len(supplies))
		t.entries[k] = m
	}
	for _, s := range supplies {
		if s == "" {
			continue
		}
		m[s]++
	}
}

func (t *supplyDeclarationTable) undeclare(workflow, node string, supplies []string) {
	if workflow == "" || node == "" || len(supplies) == 0 {
		return
	}
	k := supplyDeclarationKey{workflow: workflow, node: node}
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.entries[k]
	if m == nil {
		return
	}
	for _, s := range supplies {
		if m[s] <= 1 {
			delete(m, s)
			continue
		}
		m[s]--
	}
	if len(m) == 0 {
		delete(t.entries, k)
	}
}

// lookup returns the supply names declared for this node, sorted, as a fresh
// slice. nil means "nothing declared" -- which Execute must distinguish from
// "declared but empty", because the fail-closed guard only engages on the former
// being absent.
func (t *supplyDeclarationTable) lookup(workflow, node string) []string {
	if workflow == "" || node == "" {
		return nil
	}
	t.mu.RLock()
	m := t.entries[supplyDeclarationKey{workflow: workflow, node: node}]
	if len(m) == 0 {
		t.mu.RUnlock()
		return nil
	}
	out := make([]string, 0, len(m))
	for s := range m {
		out = append(out, s)
	}
	t.mu.RUnlock()
	sort.Strings(out)
	return out
}
