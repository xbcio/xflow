package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node/supply"
)

// SupplyFetcher retrieves one supply's current content. The runner uses it at
// activation time; a second implementation will serve pull mode.
type SupplyFetcher interface {
	// Fetch returns the content, its content hash, and the server-side revision.
	// A missing resource must return ErrSupplyNotFound so the gate can tell
	// "never written" from a transport failure in its logs.
	Fetch(ctx context.Context, name string) (content []byte, hash string, revision uint64, err error)
}

// NotReadyError reports that this runner declined an activation because one or
// more required supplies have no content here. Missing holds the supply NODE
// names, sorted.
//
// Declining is the whole point: the alternative is to accept the activation, let
// the Kafka consumer commit offsets, and fail per-message — which silently
// evaporates traffic (a node with no retry policy does not retry, does not go to
// a dead-letter queue, and the failed execution expires without a trace).
type NotReadyError struct {
	Missing []string
}

func (e *NotReadyError) Error() string {
	return fmt.Sprintf("supply not ready: %s", strings.Join(e.Missing, ", "))
}

// SupplyGateObserver receives activation-time readiness-gate observations.
// Implementations must be non-blocking and must never use content, hashes, or
// execution IDs as labels — see the package-level metrics safety note in
// observability/metrics/supply.go.
type SupplyGateObserver interface {
	// OnSupplyFetch records one content fetch attempt. result is "ok" or
	// "error".
	OnSupplyFetch(ctx context.Context, name, result string)
	// OnSupplyNotReady reports whether Admit is currently declining for a
	// missing required supply. Must be reported for every requirement on
	// every Admit call — both true and false — so the gauge always reflects
	// the current state rather than getting stuck at 1 after the condition
	// clears.
	OnSupplyNotReady(ctx context.Context, workflow, supplyName string, notReady bool)
	// OnSupplyServingUnavailable reports that a require_ready:false supply is
	// currently serving with no content ever fetched. Must also be reported
	// in both directions.
	OnSupplyServingUnavailable(ctx context.Context, name string, serving bool)
}

// SupplyGate is the activation-time readiness gate. It fetches any required
// supply this process does not have yet, publishes it into the registry, and
// reports whether the activation may proceed.
//
// It is also the first-fetch path for a supply this runner has never seen, which
// is why the gate — not a change notification — is what resolves every cold-start
// race: content that was never written, a freshly started runner, and a consumer
// activated before its supply all reduce to "fetch it now, synchronously".
type SupplyGate struct {
	fetcher  SupplyFetcher
	registry *supply.Registry
	logger   *slog.Logger

	// observer, when set, receives metrics-facing notifications. nil means no
	// observation (existing callers/tests that never call SetObserver).
	observer SupplyGateObserver
}

func NewSupplyGate(f SupplyFetcher, reg *supply.Registry, logger *slog.Logger) *SupplyGate {
	if reg == nil {
		reg = supply.Default
	}
	return &SupplyGate{fetcher: f, registry: reg, logger: logger}
}

// SetObserver installs the gate's observer. nil disables observation.
func (g *SupplyGate) SetObserver(o SupplyGateObserver) { g.observer = o }

// Admit reports whether the activation may proceed. It returns *NotReadyError
// listing every required-but-absent supply — all of them, so an operator does not
// need one reconcile round per missing supply to discover the next.
//
// Content is cached under the supply NODE name, not the resource name: the node
// name is the identity that $supplies expressions and the consumer registry use.
//
// workflowID identifies the activation for the xflow_supply_not_ready label —
// it is a workflow ID, not an execution ID, so it stays low-cardinality.
func (g *SupplyGate) Admit(ctx context.Context, workflowID string, reqs []engine.SupplyRequirement) error {
	if len(reqs) == 0 {
		return nil
	}
	var missing []string
	for _, req := range reqs {
		if g.registry.IsReady(req.Node) {
			g.notifyNotReady(ctx, workflowID, req.Node, false)
			continue
		}
		content, hash, revision, err := g.fetcher.Fetch(ctx, req.Resource)
		if err != nil {
			g.notifyFetch(ctx, req.Resource, "error")
			// Both "never written" and "cannot reach the server" mean the same
			// thing for the decision: this runner has no content. They differ only
			// in what an operator should look at, hence the log level split.
			if errors.Is(err, ErrSupplyNotFound) {
				g.log(ctx, slog.LevelInfo, "supply has no content yet", req, "")
			} else {
				g.log(ctx, slog.LevelWarn, "supply fetch failed", req, "")
			}
			if req.RequireReady {
				missing = append(missing, req.Node)
				g.notifyNotReady(ctx, workflowID, req.Node, true)
			} else {
				g.notifyNotReady(ctx, workflowID, req.Node, false)
				g.notifyServingUnavailable(ctx, req.Node, true)
			}
			continue
		}
		g.notifyFetch(ctx, req.Resource, "ok")
		if err := g.registry.Apply(ctx, supply.Snapshot{
			Name:      req.Node,
			Content:   content,
			Hash:      hash,
			Revision:  revision,
			FetchedAt: time.Now(),
		}); err != nil {
			// A consumer rejected the content (e.g. wasm configure returned -1).
			// The snapshot is cached regardless, but a consumer that could not
			// apply it has no usable derived state, so a required supply is not
			// ready.
			g.log(ctx, slog.LevelWarn, "supply content rejected by a consumer", req, hash)
			if req.RequireReady {
				missing = append(missing, req.Node)
				g.notifyNotReady(ctx, workflowID, req.Node, true)
			} else {
				g.notifyNotReady(ctx, workflowID, req.Node, false)
			}
			continue
		}
		// Apply returned nil, but that can mean either "consumers accepted" or
		// "content unchanged, no notification happened". Re-check IsReady to catch
		// the case where the content is cached-but-rejected from a prior Apply
		// with the same hash (a re-fetch that returned identical bytes).
		if !g.registry.IsReady(req.Node) {
			g.log(ctx, slog.LevelWarn, "supply content previously rejected, still not ready", req, hash)
			if req.RequireReady {
				missing = append(missing, req.Node)
				g.notifyNotReady(ctx, workflowID, req.Node, true)
			} else {
				g.notifyNotReady(ctx, workflowID, req.Node, false)
			}
			continue
		}
		g.notifyNotReady(ctx, workflowID, req.Node, false)
		g.notifyServingUnavailable(ctx, req.Node, false)
		g.log(ctx, slog.LevelInfo, "supply ready", req, hash)
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return &NotReadyError{Missing: missing}
}

// notifyFetch, notifyNotReady, notifyServingUnavailable are all nil-safe: most
// callers in this codebase (every pre-existing supply_gate_test.go case) never
// install an observer.
func (g *SupplyGate) notifyFetch(ctx context.Context, name, result string) {
	if g.observer != nil {
		g.observer.OnSupplyFetch(ctx, name, result)
	}
}

func (g *SupplyGate) notifyNotReady(ctx context.Context, workflowID, supplyName string, notReady bool) {
	if g.observer != nil {
		g.observer.OnSupplyNotReady(ctx, workflowID, supplyName, notReady)
	}
}

func (g *SupplyGate) notifyServingUnavailable(ctx context.Context, name string, serving bool) {
	if g.observer != nil {
		g.observer.OnSupplyServingUnavailable(ctx, name, serving)
	}
}

// log emits one gate event. It records the supply name and at most a hash
// PREFIX — never the content, which is not a secret but is also never worth
// putting in a log line, and never an operator's to read from there.
func (g *SupplyGate) log(_ context.Context, level slog.Level, msg string, req engine.SupplyRequirement, hash string) {
	if g.logger == nil {
		return
	}
	attrs := []any{"supply_node", req.Node, "resource", req.Resource, "require_ready", req.RequireReady}
	if hash != "" {
		attrs = append(attrs, "hash", hashPrefix(hash))
	}
	g.logger.Log(context.Background(), level, msg, attrs...)
}

// hashPrefix shortens a content hash for logging. 12 hex chars after the
// algorithm prefix is enough to correlate two log lines and far too little to
// reveal anything about the content.
func hashPrefix(hash string) string {
	const keep = 12
	body := hash
	if i := strings.IndexByte(hash, ':'); i >= 0 {
		body = hash[i+1:]
	}
	if len(body) > keep {
		body = body[:keep]
	}
	return body
}
