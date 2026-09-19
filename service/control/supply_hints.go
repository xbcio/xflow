package control

import (
	"context"
	"errors"
	"sync"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/store"
)

// SupplyHintSource reads current supply content metadata for hint computation.
// It is the read half of store.Supplies; narrowing it here keeps the control
// plane from depending on the write path.
type SupplyHintSource interface {
	GetSupply(ctx context.Context, ns string, name string) (*store.SupplyResource, error)
}

// SupplyHintObserver observes supply reads that fail while reporting hints.
//
// Every read error other than "not written yet" is a real fault, and this is the
// seam that lets the control plane report one without importing the metrics
// package (the same local-mirror-interface pattern observability/metrics uses to
// avoid a cycle with this package).
type SupplyHintObserver interface {
	// OnSupplyHintReadError records one failed read. namespace is the namespace
	// the read was actually issued against — NOT the one on ctx, which carries
	// the heartbeat's scope and is typically empty here. cause is a bounded
	// label, currently "decrypt" or "other".
	OnSupplyHintReadError(ctx context.Context, namespace, cause string)
}

// supplyHintReadCause buckets a read error for the cause label.
//
// It is deliberately coarse. The decrypt family is worth splitting out because
// it is the shape an at-rest KEK change produces, and it is the one an operator
// can act on ("a key is missing from this replica's keyring"). Everything else
// shares "other" because there is no sentinel to tell the cases apart: the
// content-hash mismatch from store/sqlstore/supply.go is a plain fmt.Errorf
// with no %w, so it is indistinguishable from a database error, and inventing a
// distinct label would mean asserting a split the code cannot make.
//
// A high-cardinality cause (the error text itself) is not an option: it would
// put every distinct error message into a label and blow up the series count.
func supplyHintReadCause(err error) string {
	switch {
	case errors.Is(err, supplyenc.ErrUnknownKey),
		errors.Is(err, supplyenc.ErrDecryptFailed),
		errors.Is(err, supplyenc.ErrUnsupportedVersion),
		errors.Is(err, supplyenc.ErrNotEncrypted):
		return "decrypt"
	default:
		return "other"
	}
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

	// The observer is guarded rather than a plain field because HintsForRunner
	// runs concurrently, once per heartbeat per runner: an install after the
	// first heartbeat would otherwise be a data race on the field. Only the
	// error path takes the lock.
	observerMu sync.RWMutex
	observer   SupplyHintObserver
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

// SetSupplyHintObserver installs the observer; nil removes it. It is optional —
// every test that predates it, and any deployment without metrics, leaves it
// unset, and HintsForRunner must behave identically either way.
func (h *SupplyHinter) SetSupplyHintObserver(o SupplyHintObserver) {
	if h == nil {
		return
	}
	h.observerMu.Lock()
	defer h.observerMu.Unlock()
	h.observer = o
}

// notifyReadError reports one failed supply read. It is nil-safe in both the
// hinter and the observer, so an unwired control plane pays only a read lock.
func (h *SupplyHinter) notifyReadError(ctx context.Context, ns, cause string) {
	if h == nil {
		return
	}
	h.observerMu.RLock()
	o := h.observer
	h.observerMu.RUnlock()
	if o != nil {
		o.OnSupplyHintReadError(ctx, ns, cause)
	}
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
				if err != nil {
					// ErrNotFound is the expected "no content yet" case: the
					// resource was never written, so there is nothing to hint
					// and nothing to report.
					//
					// Every OTHER error is a real fault — most importantly a
					// decryption failure (decrypt: supplyenc: no key matches
					// kid, after the KEK changed) or a content-hash mismatch —
					// and it must stay distinguishable from that expected case.
					// It used to be indistinguishable: both landed in one silent
					// `continue`, so an unreadable supply produced no log and no
					// metric on this path, on every heartbeat, while the runner
					// kept serving last-good content and /readyz stayed green.
					// The HTTP GET path answers 500 for that same row, so the
					// two disagreed about whether anything was wrong at all.
					//
					// It stays best-effort — we do not fail the heartbeat over a
					// hint (see the method comment) — but we stop pretending the
					// fault did not happen. Per-occurrence Warn matches how the
					// runner side already reports its own "supply hint: fetch
					// failed" (service/runner/supply_gate.go), which is likewise
					// unthrottled, so this needs no new machinery.
					//
					// The metric is the half that can page someone who is not
					// watching logs, and it shares this exact condition — a log
					// line and a counter that disagreed about what counts as a
					// fault would be worse than either alone.
					if !errors.Is(err, store.ErrNotFound) {
						if h.logger != nil {
							h.logger.Warn("supply hints: get supply failed",
								"namespace", string(ns), "resource", req.Resource,
								"node", req.Node, "error", err)
						}
						h.notifyReadError(ctx, string(ns), supplyHintReadCause(err))
					}
					continue
				}
				if rec == nil {
					// A nil record with a nil error breaks the store contract,
					// and a hint is not worth failing a heartbeat over. There is
					// no error value to log here, so this stays silent rather
					// than inventing one.
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
