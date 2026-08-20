package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
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

	// observerMu guards observer and observerInstalled.
	observerMu sync.RWMutex
	// observer, when set, receives metrics-facing notifications. nil means no
	// observation (existing callers/tests that never call SetObserver).
	observer SupplyGateObserver
	// observerInstalled tracks whether a non-nil observer is currently set so
	// a second non-nil SetObserver call can panic rather than silently overwrite.
	observerInstalled bool

	// resources maps supply NODE name → resource name, recorded by Admit for
	// every requirement it processes. A heartbeat hint carries only the node
	// name (that is the identity $supplies and the consumer registry use), but
	// SupplyFetcher.Fetch needs the resource name — they differ whenever a node
	// overrides `resource`. This is populated at activation time, which is the
	// only place the mapping is known; ApplyHints has no other way to recover
	// it later.
	resources sync.Map // string(node name) -> string(resource name)

	// hintsInFlight marks supply names currently being fetched by ApplyHints.
	// The caller (Runner.processSupplyHints) runs ApplyHints in its own
	// goroutine per heartbeat so a slow fetch cannot delay the next heartbeat;
	// this map is what stops two overlapping heartbeat rounds from firing a
	// second concurrent fetch for the same name while the first is still in
	// flight (which would waste a fetch and could apply results out of order).
	hintsInFlight sync.Map // string(node name) -> struct{}
}

func NewSupplyGate(f SupplyFetcher, reg *supply.Registry, logger *slog.Logger) *SupplyGate {
	if reg == nil {
		reg = supply.Default
	}
	return &SupplyGate{fetcher: f, registry: reg, logger: logger}
}

// SetObserver installs the gate's observer. nil disables observation.
//
// A second non-nil call panics: two callers racing to set the observer means
// one of them would silently lose all its observations, which is harder to
// diagnose than a startup panic. Pass nil first to remove the current observer
// before installing a new one (tests use this as their teardown path).
func (g *SupplyGate) SetObserver(o SupplyGateObserver) {
	g.observerMu.Lock()
	defer g.observerMu.Unlock()
	if o == nil {
		g.observer = nil
		g.observerInstalled = false
		return
	}
	if g.observerInstalled {
		panic("runner.SupplyGate.SetObserver: observer already installed; call SetObserver(nil) first")
	}
	g.observer = o
	g.observerInstalled = true
}

// Fetcher returns the underlying SupplyFetcher. Used by the runner to install
// supply encryption keys on the HTTP fetcher after registration.
func (g *SupplyGate) Fetcher() SupplyFetcher { return g.fetcher }

// resourceFor returns the resource name recorded for a supply NODE name at a
// prior Admit call, or name itself when never recorded (the common case: node
// name and resource name are equal unless a workflow overrides `resource`).
func (g *SupplyGate) resourceFor(name string) string {
	if v, ok := g.resources.Load(name); ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return name
}

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
		// Record the node→resource mapping regardless of outcome below: a later
		// heartbeat hint carries only the node name, and this is the only place
		// the resource name is known. Recording it even when a fetch fails or a
		// consumer rejects keeps ApplyHints able to retry with the right name.
		g.resources.Store(req.Node, req.Resource)
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
// install an observer. Each snaps the observer pointer under a read lock so the
// call is data-race-free relative to a concurrent SetObserver.
func (g *SupplyGate) notifyFetch(ctx context.Context, name, result string) {
	g.observerMu.RLock()
	o := g.observer
	g.observerMu.RUnlock()
	if o != nil {
		o.OnSupplyFetch(ctx, name, result)
	}
}

func (g *SupplyGate) notifyNotReady(ctx context.Context, workflowID, supplyName string, notReady bool) {
	g.observerMu.RLock()
	o := g.observer
	g.observerMu.RUnlock()
	if o != nil {
		o.OnSupplyNotReady(ctx, workflowID, supplyName, notReady)
	}
}

func (g *SupplyGate) notifyServingUnavailable(ctx context.Context, name string, serving bool) {
	g.observerMu.RLock()
	o := g.observer
	g.observerMu.RUnlock()
	if o != nil {
		o.OnSupplyServingUnavailable(ctx, name, serving)
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

// ApplyHints reacts to the server's supply hints: for each name whose hinted
// hash differs from what this process has, fetch once and publish.
//
// A hint is advisory. Losing one is not an error and is not retried here — the
// activation-time fetch (Admit) and the TTL watcher are the convergence
// guarantees. That separation is deliberate: a design where the piggybacked
// notification is the only channel keeps a stale value forever the first time
// a message is dropped (network partition, runner restart, server failover).
//
// Scope note: this affects only WHEN an already-hosted runner refreshes
// content it already consumes. It has nothing to do with whether an
// activation gets hosted in the first place — a gate-declined activation
// staying declined is a separate, currently-open gap (see the Task 18
// addendum) that ApplyHints neither causes nor fixes; there is no hint for a
// supply this process has no requirement for at all.
func (g *SupplyGate) ApplyHints(ctx context.Context, hints map[string]string) {
	if g == nil || len(hints) == 0 {
		return
	}
	for name, hash := range hints {
		if cur, ok := g.registry.Get(name); ok && cur.Hash == hash {
			continue // already on this content: zero cost
		}
		// De-dupe: if a fetch for this name is already in flight (from an
		// overlapping ApplyHints call, e.g. two heartbeat rounds whose fetches
		// both outlived the heartbeat interval), skip rather than fire a second
		// concurrent fetch. The in-flight one will land the same content this
		// hint is asking for.
		if _, already := g.hintsInFlight.LoadOrStore(name, struct{}{}); already {
			continue
		}
		g.applyOneHint(ctx, name)
	}
}

// applyOneHint fetches and applies a single hinted supply, always clearing its
// in-flight marker on return (including on every early-return path, which is
// why this is its own function rather than inlined into ApplyHints' loop).
func (g *SupplyGate) applyOneHint(ctx context.Context, name string) {
	defer g.hintsInFlight.Delete(name)
	// The hint keys on the supply NODE name; the fetch needs the RESOURCE
	// name. They are equal unless the node overrode `resource`, so the gate
	// remembers the mapping when it admits an activation (see resources).
	resource := g.resourceFor(name)
	content, fetchedHash, revision, err := g.fetcher.Fetch(ctx, resource)
	if err != nil {
		g.notifyFetch(ctx, resource, "error")
		g.logHintOutcome(slog.LevelWarn, "supply hint: fetch failed", name, resource, "")
		return
	}
	g.notifyFetch(ctx, resource, "ok")
	if err := g.registry.Apply(ctx, supply.Snapshot{
		Name: name, Content: content, Hash: fetchedHash,
		Revision: revision, FetchedAt: time.Now(),
	}); err != nil {
		// A consumer rejected it (e.g. the wasm canary said no). last-good is
		// preserved by construction; the next hint or TTL poll retries.
		g.logHintOutcome(slog.LevelWarn, "supply hint: content rejected by a consumer", name, resource, fetchedHash)
		return
	}
	g.logHintOutcome(slog.LevelInfo, "supply hint: applied", name, resource, fetchedHash)
}

// logHintOutcome logs one ApplyHints outcome via the shared g.log formatter,
// carrying only the supply name/resource and a hash PREFIX — never content.
func (g *SupplyGate) logHintOutcome(level slog.Level, msg, node, resource, hash string) {
	g.log(context.Background(), level, msg, engine.SupplyRequirement{Node: node, Resource: resource}, hash)
}
