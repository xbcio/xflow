package control

import (
	"context"
	"sync"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// SupplyHintSource reads current supply content metadata for hint computation.
// It is the read half of store.Supplies; narrowing it here keeps the control
// plane from depending on the write path.
type SupplyHintSource interface {
	GetSupply(ctx context.Context, ns string, name string) (*store.SupplyResource, error)
}

// SupplyHinter computes per-runner supply hints from activation records.
//
// There is deliberately NO separate reverse index. An EntryActivation already
// carries both RunnerID (who hosts it) and Supplies (what content it needs), so
// "which runner cares about which supply" is a read over records that already
// exist. A second index would be one more thing to keep consistent with
// assignment changes, for no new information.
type SupplyHinter struct {
	activations engine.EntryActivationStore
	supplies    SupplyHintSource
	namespaces  []namespace.Namespace
	logger      engine.Logger
}

// NewSupplyHinter constructs a SupplyHinter. ns mirrors the reconciler's own
// namespace list (EntryActivationReconcilerConfig.Namespaces) — the hinter must
// enumerate the SAME namespaces the reconciler owns, not a separately
// configured set, so the two never silently diverge. Empty defaults to
// {namespace.Default}.
func NewSupplyHinter(acts engine.EntryActivationStore, sup SupplyHintSource, ns []namespace.Namespace, logger engine.Logger) *SupplyHinter {
	if len(ns) == 0 {
		ns = []namespace.Namespace{namespace.Default}
	}
	return &SupplyHinter{activations: acts, supplies: sup, namespaces: ns, logger: logger}
}

// HintsForRunner returns "supply node name → current content hash" for every
// supply the runner hosts a consumer for. It returns nil — not an empty map — when
// there is nothing, so omitempty drops the field and heartbeat bodies stay
// byte-identical for runners with no supplies.
//
// Hints are an optimization, never a correctness guarantee: they can be lost to a
// partition, a runner restart, or a leader change. The activation-time fetch and
// the TTL watcher are what actually guarantee convergence. Making a hint the only
// notification channel is how a stale value survives indefinitely.
//
// NOTE on scope: this only affects WHEN an already-hosted runner refreshes its
// content. It has nothing to do with whether an activation ever GETS hosted in
// the first place — a gate-declined activation is a separate, currently-open
// gap (see the Task 18 addendum) that this method neither causes nor fixes.
func (h *SupplyHinter) HintsForRunner(ctx context.Context, runnerID string) map[string]string {
	if h == nil || h.activations == nil || h.supplies == nil || runnerID == "" {
		return nil
	}
	var out map[string]string
	for _, ns := range h.namespaces {
		acts, err := h.activations.List(ctx, ns)
		if err != nil {
			// A hint is best-effort by construction; log and move on rather than
			// failing the heartbeat, which would take the runner offline over an
			// optimization.
			if h.logger != nil {
				h.logger.Warn("supply hints: list activations failed", "namespace", string(ns), "error", err)
			}
			continue
		}
		for i := range acts {
			act := &acts[i]
			if act.RunnerID != runnerID || !act.Desired {
				continue
			}
			for _, req := range act.Supplies {
				if _, seen := out[req.Node]; seen {
					continue
				}
				rec, err := h.supplies.GetSupply(ctx, string(ns), req.Resource)
				if err != nil || rec == nil {
					// No content yet: nothing to hint. The gate already declined
					// or is serving degraded; a hint cannot help either way.
					continue
				}
				if out == nil {
					out = map[string]string{}
				}
				out[req.Node] = rec.ContentHash
			}
		}
	}
	return out
}

// SupplyObservedSink records what each runner reports as its applied content.
// A simple in-memory map is enough: the question it answers ("is everyone on
// revision N") is about the present moment, and a leader change legitimately
// resets it — the next heartbeat round refills it.
type SupplyObservedSink interface {
	Record(runnerID string, observed map[string]string)
	// Snapshot returns runner ID → (supply name → applied hash).
	Snapshot() map[string]map[string]string
}

// MemorySupplyObserved is the in-memory SupplyObservedSink. A leader failover
// legitimately loses this state — it is diagnostic ("has everyone converged"),
// never a gate on anything, and the next heartbeat round from every runner
// refills it within one heartbeat interval.
type MemorySupplyObserved struct {
	mu   sync.Mutex
	byRn map[string]map[string]string
}

// NewMemorySupplyObserved returns a ready-to-use MemorySupplyObserved.
func NewMemorySupplyObserved() *MemorySupplyObserved {
	return &MemorySupplyObserved{byRn: map[string]map[string]string{}}
}

// Record replaces runnerID's entire reported set with observed. It is a whole
// replacement, never a merge: if a supply is no longer in observed (the runner
// stopped hosting its consumer, or the workflow was removed), its old hash must
// not linger — a merge would leave a permanently stale entry that nothing ever
// clears.
func (s *MemorySupplyObserved) Record(runnerID string, observed map[string]string) {
	if s == nil || runnerID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(observed) == 0 {
		delete(s.byRn, runnerID)
		return
	}
	cp := make(map[string]string, len(observed))
	for k, v := range observed {
		cp[k] = v
	}
	s.byRn[runnerID] = cp
}

// Snapshot returns a deep copy of the current runner → (supply → hash) view.
func (s *MemorySupplyObserved) Snapshot() map[string]map[string]string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]map[string]string, len(s.byRn))
	for runnerID, m := range s.byRn {
		cp := make(map[string]string, len(m))
		for k, v := range m {
			cp[k] = v
		}
		out[runnerID] = cp
	}
	return out
}

var _ SupplyObservedSink = (*MemorySupplyObserved)(nil)
