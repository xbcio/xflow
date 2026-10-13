package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// SupplyFetcher retrieves one supply's current content from the control
// plane's store-backed supply API (GET /v1/supplies/{name}). It is the fetcher
// for every requirement that does NOT declare a pull source; a requirement that
// does is routed to PullSupplyFetcher by the gate, because only the gate knows
// the recorded declaration for a supply key (see SupplyGate.fetchSupply).
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
	// "error"; source is what asked for it — "admission" (an activation-time
	// Admit), "hint" (a heartbeat-piggybacked hash), or "poll" (the TTL poll).
	// The three are separate series on purpose: a fleet where most fetches are
	// "poll" is one whose hints are not arriving, and a "poll" rate that tracks
	// the hosted supply count is the poll working as designed rather than a
	// retry storm.
	OnSupplyFetch(ctx context.Context, name, result, source string)
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

// The closed set of supply fetch sources, reported as the source label of
// xflow_supply_fetch_total. A new caller of notifyFetch takes one of these
// rather than inventing a value: each distinct string becomes its own series,
// so an unlisted one silently splits the metric instead of failing.
const (
	// supplySourceAdmission is the activation-time fetch (Admit).
	supplySourceAdmission = "admission"
	// supplySourceHint is a fetch triggered by a heartbeat-piggybacked hash.
	supplySourceHint = "hint"
	// supplySourcePoll is a fetch the TTL poll made.
	supplySourcePoll = "poll"
)

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

	// resources maps a supply KEY (namespace + node name) → resource name,
	// recorded by Admit for every requirement it processes. A heartbeat hint
	// carries the same key (that is the identity $supplies and the consumer
	// registry use), but SupplyFetcher.Fetch needs the resource name — they
	// differ whenever a node overrides `resource`. This is populated at
	// activation time, which is the only place the mapping is known; the hint
	// path has no other way to recover it later.
	//
	// Its key set is also what the TTL poll iterates: the supplies this process
	// "hosts" are exactly the ones some activation required here (see
	// hostedKeys).
	resources sync.Map // supply.Key -> string(resource name)

	// declarations records, per supply key, the last declaration this runner
	// admitted for it: which workflow declared it, and — for a pull node — the
	// parsed fetch source. The registry cache is keyed by (namespace, node name)
	// and shared by every consumer of that key, so two declarations that
	// disagree about where the content comes from are a conflict rather than a
	// last-writer-wins race; see recordDeclaration.
	declarations sync.Map // supply.Key -> supplyDeclaration
	// declMu serializes declaration recording. Admit runs concurrently for
	// different activations, and a load-then-store on declarations must not
	// interleave.
	declMu sync.Mutex

	// pullFetcher fetches supplies whose declaration carries a pull source. nil
	// (a runner that hosts no triggers, so no wiring ran) makes such an
	// activation fail with ErrSupplyPullUnavailable rather than fall back to the
	// store-backed path. Set once before Run; never swapped at runtime.
	pullFetcher *PullSupplyFetcher

	// newPollTicker builds the tick source RunPoller waits on. nil (the
	// production value) means time.NewTicker. Tests replace it to drive ticks
	// by hand: *time.Ticker's channel is not settable from outside, so a fake
	// needs this seam rather than a fake clock.
	newPollTicker func(time.Duration) pollTicker

	// hintsInFlight marks supply keys currently being fetched. The caller
	// (Runner.processSupplyHints) runs ApplyHints in its own goroutine per
	// heartbeat so a slow fetch cannot delay the next heartbeat; this map is
	// what stops two overlapping heartbeat rounds — or a heartbeat round and a
	// TTL poll — from firing a second concurrent fetch for the same key while
	// the first is still in flight (which would waste a fetch and could apply
	// results out of order).
	hintsInFlight sync.Map // supply.Key -> struct{}
}

func NewSupplyGate(f SupplyFetcher, reg *supply.Registry, logger *slog.Logger) *SupplyGate {
	if reg == nil {
		reg = supply.Default
	}
	return &SupplyGate{fetcher: f, registry: reg, logger: logger}
}

// DefaultSupplyPollInterval is the supply TTL poll period used when the
// configured interval is zero (unset).
const DefaultSupplyPollInterval = 60 * time.Second

// MinSupplyPollInterval is the lowest supply poll interval this process will
// run, and the value a configured interval below it is raised to. Polling is
// deliberately not disableable and not tunable to a hammering cadence: it is
// the only convergence path for a supply whose heartbeat hint was lost (a
// dropped message, a runner restart, a server failover), and each tick costs
// one control-plane round trip per hosted supply.
const MinSupplyPollInterval = 10 * time.Second

// ValidateSupplyPollInterval reports whether a configured supply poll interval
// is usable. Zero means unset and is accepted — the gate then applies
// DefaultSupplyPollInterval.
//
// A non-zero value below MinSupplyPollInterval is refused rather than silently
// raised or silently ignored: polling cannot be switched off, so an operator
// who set 5s (or a negative value, which is what "disable this" looks like on
// a duration knob) needs to hear about it at config time rather than discover a
// different cadence running.
func ValidateSupplyPollInterval(ttl time.Duration) error {
	if ttl == 0 || ttl >= MinSupplyPollInterval {
		return nil
	}
	return fmt.Errorf("supply poll interval %s is below the %s minimum (polling cannot be disabled)",
		ttl, MinSupplyPollInterval)
}

// resolveSupplyPollInterval turns a configured interval into the one RunPoller
// uses: unset (zero) and nonsensical (negative) both mean the default, and a
// positive value below the floor is raised to it.
//
// The config surfaces (sdk/xflow.RunnerConfig, the standalone runner's flag/env
// /YAML) already reject those cases through ValidateSupplyPollInterval, so this
// is the backstop for a caller that builds Config directly: it resolves to a
// running poller rather than to no poller, because "polling is off" is the one
// outcome that must not be reachable.
func resolveSupplyPollInterval(ttl time.Duration, logger *slog.Logger) time.Duration {
	switch {
	case ttl <= 0:
		return DefaultSupplyPollInterval
	case ttl < MinSupplyPollInterval:
		if logger != nil {
			logger.Warn("supply poll interval raised to the floor",
				"configured", ttl, "using", MinSupplyPollInterval)
		}
		return MinSupplyPollInterval
	default:
		return ttl
	}
}

// pollTicker is the tick source RunPoller waits on. It exists as an interface
// only so a test can drive ticks by hand.
type pollTicker interface {
	C() <-chan time.Time
	Stop()
}

// realPollTicker adapts *time.Ticker to pollTicker. *time.Ticker cannot be used
// directly: its channel is a struct field, so a fake cannot stand in for it.
type realPollTicker struct{ t *time.Ticker }

func (r realPollTicker) C() <-chan time.Time { return r.t.C }
func (r realPollTicker) Stop()               { r.t.Stop() }

func newRealPollTicker(d time.Duration) pollTicker { return realPollTicker{t: time.NewTicker(d)} }

// supplyPollJitter spreads a poll period by ±10% — the same shape as the
// control plane's own retry jitter, and for the same reason: every runner in a
// fleet starts its poller on its own clock, and without the spread a fleet
// rolled out together fires its ticks together and turns a per-minute trickle
// into a periodic spike.
func supplyPollJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	spread := d / 10 // 10%
	if spread <= 0 {
		return d
	}
	// rand.Int63n panics on n <= 0; spread is > 0 here. The offset is in
	// [-spread, +spread].
	offset := rand.Int63n(int64(spread)*2+1) - int64(spread)
	return d + time.Duration(offset)
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

// SetPullFetcher installs the fetcher used for requirements that declare a pull
// source (xflow.supply.http). Call it before Run: it is read by the activation
// and poll paths and never swapped at runtime. nil (the default) leaves pull
// requirements failing closed with ErrSupplyPullUnavailable.
func (g *SupplyGate) SetPullFetcher(f *PullSupplyFetcher) { g.pullFetcher = f }

// PullFetcher returns the installed pull fetcher, or nil when pull mode is not
// wired. Nothing in the runtime reads it: it exists so the assembly that sets
// the fetcher can be tested against the gate it built (sdk/xflow's wiring
// assertions), the same way Fetcher serves the encryption install path.
func (g *SupplyGate) PullFetcher() *PullSupplyFetcher { return g.pullFetcher }

// supplyDeclaration is what the gate remembers about one supply key's
// declaration.
type supplyDeclaration struct {
	// WorkflowID is the workflow that declared the key. A re-declaration by the
	// SAME workflow is an upgrade — a new version may legitimately move the URL —
	// and replaces the record; refusing it would make a source immutable until
	// this runner restarted. A different workflow disagreeing is a conflict.
	WorkflowID string
	// PullSource is non-nil when the declaring node is xflow.supply.http.
	PullSource *types.SupplyPullSource
}

// recordDeclaration notes how the declaring workflow says k's content is
// obtained, and refuses a second, DIFFERENT origin for the same key.
//
// The refusal is the P9 fail-closed rule, and it is the whole reason this
// record exists: content is cached per (namespace, node name) and read by every
// consumer of that key, so if workflow B could re-point a key workflow A
// declared, A's nodes would start reading B's source — the same key serving two
// different definitions with no error anywhere. Nothing is switched silently;
// the second activation is declined with an error naming both origins.
//
// Two declarations agree when they have the same origin: both store-backed, or
// both pull with the same URL. Anything else about a pull source (credential
// name, poll interval) is not part of the origin: the first record's values
// stay in force, since the content they describe is the same.
func (g *SupplyGate) recordDeclaration(k supply.Key, workflowID string, req engine.SupplyRequirement) error {
	next := supplyDeclaration{WorkflowID: workflowID, PullSource: req.PullSource}

	g.declMu.Lock()
	defer g.declMu.Unlock()
	existing, ok := g.declarations.Load(k)
	if !ok {
		g.declarations.Store(k, next)
		return nil
	}
	prev, _ := existing.(supplyDeclaration)
	if prev.WorkflowID == workflowID || sameSupplyOrigin(prev.PullSource, next.PullSource) {
		// Same workflow (an upgrade: replace) or same origin (one cached
		// snapshot serves both by design: keep the first record).
		if prev.WorkflowID == workflowID {
			g.declarations.Store(k, next)
		}
		return nil
	}
	return fmt.Errorf("%w: supply node %q in namespace %q is %s for workflow %s and %s for workflow %s",
		ErrSupplyPullSourceConflict, k.Name, k.Namespace,
		describeSupplyOrigin(prev.PullSource), prev.WorkflowID,
		describeSupplyOrigin(next.PullSource), next.WorkflowID)
}

// sameSupplyOrigin reports whether two declarations name the same content
// origin: both store-backed, or both pull from the same URL.
func sameSupplyOrigin(a, b *types.SupplyPullSource) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}
	return a.URL == b.URL
}

// describeSupplyOrigin names a declaration's origin for the conflict error.
// A pull source is named by HOST: this message reaches logs, and a URL may
// carry a query string the operator did not intend as log content.
func describeSupplyOrigin(src *types.SupplyPullSource) string {
	if src == nil {
		return "store-backed"
	}
	u, err := url.Parse(src.URL)
	if err != nil || u.Host == "" {
		return "a pull source with an unparseable URL"
	}
	return "pulled from " + u.Host
}

// pullSourceFor returns the recorded pull source for k, if k's declaration is a
// pull node.
func (g *SupplyGate) pullSourceFor(k supply.Key) (types.SupplyPullSource, bool) {
	v, ok := g.declarations.Load(k)
	if !ok {
		return types.SupplyPullSource{}, false
	}
	d, ok := v.(supplyDeclaration)
	if !ok || d.PullSource == nil {
		return types.SupplyPullSource{}, false
	}
	return *d.PullSource, true
}

// fetchSupply routes one fetch by the key's recorded declaration: a pull source
// goes to the pull fetcher under the key's identity, everything else to the
// store-backed fetcher under the resource name.
//
// Routing lives here rather than inside a composite fetcher because the
// declaration — not the caller — decides, and the gate is what records it.
func (g *SupplyGate) fetchSupply(ctx context.Context, k supply.Key, resource string) ([]byte, string, uint64, error) {
	if src, ok := g.pullSourceFor(k); ok {
		if g.pullFetcher == nil {
			return nil, "", 0, fmt.Errorf("%w (supply %q in namespace %q)", ErrSupplyPullUnavailable, k.Name, k.Namespace)
		}
		return g.pullFetcher.Fetch(ctx, k, src)
	}
	return g.fetcher.Fetch(ctx, resource)
}

// resourceFor returns the resource name recorded for a supply key at a prior
// Admit call, or the key's own name when never recorded (the common case: node
// name and resource name are equal unless a workflow overrides `resource`).
func (g *SupplyGate) resourceFor(k supply.Key) string {
	if v, ok := g.resources.Load(k); ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return k.Name
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
	return g.AdmitWithDeclarations(ctx, workflowID, reqs, nil)
}

// AdmitWithDeclarations is Admit plus the activation's wasm supply declarations.
//
// It exists because Admit alone cannot answer the question for a pointer supply
// on the FIRST activation. Activate calls Admit BEFORE it calls
// registerSupplyConsumers (see trigger_activation_handler.go: the order is
// load-bearing — registering before Admit delivers nothing, because
// RegisterConsumer notifies only against content already cached). At that moment
// the declaration's consumers do not exist yet, and isReadyLocked is a
// conjunction over the consumer set, so ZERO consumers means ready for whatever
// content is cached. A pointer supply publishing a digest for one wasm node and
// nothing usable for another therefore passes the gate on the first evaluation,
// the subscription starts, and Kafka offsets advance — for a workflow that
// cannot run. The declared half of the work is fixed only per message, which is
// too late: once a subscription is live, a message whose artifact is missing has
// nowhere safe to go.
//
// Passing the declarations in lets the gate resolve each declared node's
// artifact_digest against the content it just fetched, so readiness is decided
// the same way on the first Admit as on every later one, independent of when
// consumers register.
//
// The check is deliberately narrow, mirroring wasmWarmupConsumer: an expression
// that cannot be rendered against $supplies alone is skipped rather than
// rejected, and a digest that renders to a well-formed sha256 the artifact store
// cannot serve is NOT detected here — that costs a fetch, so it stays with the
// consumer verdict and the execution-time guard.
func (g *SupplyGate) AdmitWithDeclarations(ctx context.Context, workflowID string, reqs []engine.SupplyRequirement, decls []engine.SupplyConsumerBinding) error {
	if len(reqs) == 0 {
		return nil
	}
	// The activation's namespace is the authority for every supply key below.
	// It comes off ctx, which the activation path fills from the directive's own
	// namespace — never from the requirement records, which carry names only.
	ns := namespace.FromContext(ctx)
	// Every per-supply call below runs under this namespace, re-stated onto the
	// context rather than inherited from whatever the caller passed. That is not
	// bookkeeping: the HTTP fetcher sets its request's namespace header from the
	// context (supply_client.go), so a fetch that only happened to carry the
	// right namespace would silently read the default namespace's content the
	// moment a caller built its context differently — and it would then be
	// published under this supply's real (namespace, name) key. The design's
	// rule is that a fetch's namespace comes from the recorded identity, never
	// from the call chain's ambient context.
	supplyCtx := namespace.WithNamespace(ctx, ns)
	var missing []string
	for _, req := range reqs {
		k := supply.KeyOf(ns, req.Node)
		// Record the key→resource mapping regardless of outcome below: a later
		// heartbeat hint carries the same key, and this is the only place the
		// resource name is known. Recording it even when a fetch fails or a
		// consumer rejects keeps the hint path able to retry with the right name.
		g.resources.Store(k, req.Resource)
		// Record how this key's content is obtained before fetching it: the
		// fetch below is routed by that record, and a conflicting declaration
		// (another workflow pointing this key at a different source) declines
		// the whole activation rather than letting one workflow's consumers read
		// the other's source.
		if err := g.recordDeclaration(k, workflowID, req); err != nil {
			if g.logger != nil {
				g.logger.Error("supply declaration conflict: refusing the activation",
					"supply_node", req.Node, "namespace", string(ns), "error", err.Error())
			}
			return err
		}
		declared := declaresFor(decls, req.Node)
		// The ready fast path is only sound when nothing declares a node on this
		// supply. With a declaration present the cached content must still be
		// resolved below, because "cached" is not "complete".
		if len(declared) == 0 && g.registry.IsReady(k) {
			g.notifyNotReady(supplyCtx, workflowID, req.Node, false)
			continue
		}
		content, hash, revision, err := g.fetchSupply(supplyCtx, k, req.Resource)
		if err != nil {
			g.notifyFetch(supplyCtx, req.Resource, "error", supplySourceAdmission)
			// Both "never written" and "cannot reach the server" mean the same
			// thing for the decision: this runner has no content. They differ only
			// in what an operator should look at, hence the log level split.
			if errors.Is(err, ErrSupplyNotFound) {
				g.log(supplyCtx, slog.LevelInfo, "supply has no content yet", req, "")
			} else {
				g.log(supplyCtx, slog.LevelWarn, "supply fetch failed", req, "")
				// The cause of a generic fetch failure stays out of the log (the
				// gate's log lines are deliberately content-free), so a pull
				// requirement on a runner with no pull wiring would otherwise be
				// indistinguishable from an unreachable source. This one case is a
				// wiring/config error an operator must be able to see.
				if errors.Is(err, ErrSupplyPullUnavailable) && g.logger != nil {
					g.logger.Warn("supply pull is not wired on this runner",
						"supply_node", req.Node, "error", err.Error())
				}
			}
			if req.RequireReady {
				missing = append(missing, req.Node)
				g.notifyNotReady(supplyCtx, workflowID, req.Node, true)
			} else {
				g.notifyNotReady(supplyCtx, workflowID, req.Node, false)
				g.notifyServingUnavailable(supplyCtx, req.Node, true)
			}
			continue
		}
		g.notifyFetch(supplyCtx, req.Resource, "ok", supplySourceAdmission)
		if err := g.registry.Apply(supplyCtx, supply.Snapshot{
			Namespace: k.Namespace,
			Name:      k.Name,
			Content:   content,
			Hash:      hash,
			Revision:  revision,
			FetchedAt: time.Now(),
		}); err != nil {
			// A consumer rejected the content (e.g. wasm configure returned -1).
			// The snapshot is cached regardless, but a consumer that could not
			// apply it has no usable derived state, so a required supply is not
			// ready.
			g.log(supplyCtx, slog.LevelWarn, "supply content rejected by a consumer", req, hash)
			if req.RequireReady {
				missing = append(missing, req.Node)
				g.notifyNotReady(supplyCtx, workflowID, req.Node, true)
			} else {
				g.notifyNotReady(supplyCtx, workflowID, req.Node, false)
			}
			continue
		}
		// Resolve every declared node against the content just cached. This runs
		// after Apply, not before: the render reads the registry's decoded view,
		// and before Apply that view still holds the PREVIOUS content (or nothing
		// at all on a first activation), so an expression would be validated
		// against the wrong pointer — reporting a complete set as incomplete for
		// one heartbeat, or accepting an incomplete one.
		if nodeName, ok := g.unresolvableDeclaration(k, declared); ok {
			g.logDeclaredNodeUnusable(supplyCtx, req, nodeName, hash)
			if req.RequireReady {
				missing = append(missing, req.Node)
				g.notifyNotReady(supplyCtx, workflowID, req.Node, true)
			} else {
				g.notifyNotReady(supplyCtx, workflowID, req.Node, false)
			}
			continue
		}
		// Apply returned nil, but that can mean either "consumers accepted" or
		// "content unchanged, no notification happened". Re-check IsReady to catch
		// the case where the content is cached-but-rejected from a prior Apply
		// with the same hash (a re-fetch that returned identical bytes).
		if !g.registry.IsReady(k) {
			g.log(supplyCtx, slog.LevelWarn, "supply content previously rejected, still not ready", req, hash)
			if req.RequireReady {
				missing = append(missing, req.Node)
				g.notifyNotReady(supplyCtx, workflowID, req.Node, true)
			} else {
				g.notifyNotReady(supplyCtx, workflowID, req.Node, false)
			}
			continue
		}
		g.notifyNotReady(supplyCtx, workflowID, req.Node, false)
		g.notifyServingUnavailable(supplyCtx, req.Node, false)
		g.log(supplyCtx, slog.LevelInfo, "supply ready", req, hash)
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return &NotReadyError{Missing: missing}
}

// declaresFor returns the DECLARATION-shaped bindings naming supply, in order.
//
// Legacy bindings (ModuleDigest set, names empty) are excluded deliberately: a
// legacy binding names a concrete module, so its readiness is already decided by
// compiling and registering it, and registerSupplyConsumers propagates that
// failure out of Activate. Only a declaration defers resolution to a digest
// expression, and only a deferred resolution can be unresolved at gate time.
func declaresFor(decls []engine.SupplyConsumerBinding, supply string) []engine.SupplyConsumerBinding {
	var out []engine.SupplyConsumerBinding
	for _, b := range decls {
		if b.IsDeclaration() && b.SupplyNode == supply {
			out = append(out, b)
		}
	}
	return out
}

// unresolvableDeclaration reports the first declared node on this supply whose
// artifact_digest does not resolve to a usable digest, and whether there was one.
//
// "Usable" is store.ValidateDigest: a sha256:<64 hex> string. An empty or
// malformed value is what an unpublished or half-published pointer looks like,
// and it is exactly the state that must not admit an activation — the node has
// no module, so there is nothing to run.
//
// An expression that fails to RENDER is skipped, not rejected, for the reason
// wasmWarmupConsumer documents: DigestExpr may legitimately reference $input,
// $env or $credentials, none of which exist in this environment, and rejecting
// those would leave a correctly-configured node permanently not-ready. The
// cost is that such a node keeps its consumer verdict as its only gate, which
// is what it had before this check existed.
//
// k identifies the supply the declarations belong to: the digest expressions
// are rendered against that supply's own namespace's $supplies view, so a
// pointer in namespace A can never resolve a digest published by namespace B.
func (g *SupplyGate) unresolvableDeclaration(k supply.Key, decls []engine.SupplyConsumerBinding) (string, bool) {
	if len(decls) == 0 {
		return "", false
	}
	env := exprx.SuppliesEnv(g.registry, k.Namespace)
	for _, b := range decls {
		rendered, err := exprx.RenderTemplate(b.DigestExpr, env)
		if err != nil {
			continue
		}
		digest, _ := rendered.(string)
		if store.ValidateDigest(digest) != nil {
			return b.NodeName, true
		}
	}
	return "", false
}

// logDeclaredNodeUnusable records that a supply publishes no usable artifact for
// one of the nodes declared on it.
//
// The node name appears; the digest expression and the rendered value do not.
// The rendered value is supply CONTENT, and supply content is not log material
// (same rule as Admit's log, which keeps a hash prefix and nothing else). The
// expression is operator-authored node configuration that may interpolate
// credentials, which is why wasmWarmupConsumer refuses to log it either.
func (g *SupplyGate) logDeclaredNodeUnusable(_ context.Context, req engine.SupplyRequirement, nodeName, hash string) {
	if g.logger == nil {
		return
	}
	attrs := []any{
		"supply_node", req.Node,
		"resource", req.Resource,
		"require_ready", req.RequireReady,
		"declared_node", nodeName,
	}
	if hash != "" {
		attrs = append(attrs, "hash", hashPrefix(hash))
	}
	g.logger.Log(context.Background(), slog.LevelWarn,
		"supply publishes no usable artifact digest for a declared node", attrs...)
}

// notifyFetch, notifyNotReady, notifyServingUnavailable are all nil-safe: most
// callers in this codebase (every pre-existing supply_gate_test.go case) never
// install an observer. Each snaps the observer pointer under a read lock so the
// call is data-race-free relative to a concurrent SetObserver.
func (g *SupplyGate) notifyFetch(ctx context.Context, name, result, source string) {
	g.observerMu.RLock()
	o := g.observer
	g.observerMu.RUnlock()
	if o != nil {
		o.OnSupplyFetch(ctx, name, result, source)
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

// ApplyHints reacts to the server's supply hints: for each supply whose hinted
// hash differs from what this process has, fetch once and publish.
//
// A hint is advisory. Losing one is not an error and is not retried here — the
// activation-time fetch (Admit) and the TTL poll are the other convergence
// paths. Keeping the notification advisory is deliberate: a design where the
// piggybacked notification is the only channel keeps a stale value forever the
// first time a message is dropped (network partition, runner restart, server
// failover).
//
// Each hint carries its supply's own (namespace, node) identity, and the fetch
// below runs under a context carrying THAT namespace. This is not bookkeeping:
// the HTTP fetcher sets its namespace header from the context
// (supply_client.go), so a hint applied against the heartbeat's context — which
// carries no namespace at all — would fetch from the default namespace and
// publish one tenant's content under another tenant's supply.
//
// Scope note: this affects only WHEN an already-hosted runner refreshes
// content it already consumes. Whether an activation gets hosted at all is a
// separate path, and a declined one is not stranded here: AdmitWithDeclarations
// records every required key in g.resources BEFORE it decides (see the
// resources.Store above its fetch), so the TTL poll re-reads a declined
// supply's content too — the next delivery of the directive then passes the
// gate. What no hint can reach is a supply this process has no requirement for
// at all.
func (g *SupplyGate) ApplyHints(ctx context.Context, hints []protocol.SupplyHint) {
	if g == nil || len(hints) == 0 {
		return
	}
	// A repeated entry is tolerated rather than trusted away: the loop below is
	// the only place that decides how many fetches a round of hints costs, and
	// the server's producer is not the only possible source of a duplicate.
	seen := make(map[supply.Key]struct{}, len(hints))
	for _, h := range hints {
		if h.Node == "" {
			continue
		}
		k := supply.KeyOf(namespace.Namespace(h.Namespace), h.Node)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		// De-dupe against an in-flight fetch for the same key: overlapping
		// heartbeat rounds whose fetches outlived the heartbeat interval would
		// otherwise fire a second concurrent fetch. The in-flight one lands the
		// same content this hint is asking for.
		if _, already := g.hintsInFlight.LoadOrStore(k, struct{}{}); already {
			continue
		}
		g.refreshOne(namespace.WithNamespace(ctx, k.Namespace), k, h.Hash, supplySourceHint)
	}
}

// refreshOne fetches and applies a single supply, always clearing its in-flight
// marker on return (including on every early-return path, which is why this is
// its own function rather than inlined into its callers' loops).
//
// Its two callers are the hint path (ApplyHints) and the TTL poll
// (pollHosted), so everything here has to be true for both: the log lines name
// the event, not the channel that asked for it.
//
// ctx MUST already carry k's namespace: the fetch reads it from there (see
// ApplyHints and pollHosted), and both callers build their context from the key
// rather than from whatever context they happened to be handed.
//
// wantHash, when non-empty, is the content hash the caller was told is current:
// the fetch is skipped when the cached content already matches it, which is the
// "already on this content: zero cost" case for a hint. The poll passes "" —
// discovering the server's current hash is the whole point of a tick.
//
// source names the caller for the fetch metric (supplySourceHint or
// supplySourcePoll). It is passed rather than inferred from wantHash, because
// the label's whole point is to tell an operator which channel is actually
// keeping this runner current.
func (g *SupplyGate) refreshOne(ctx context.Context, k supply.Key, wantHash, source string) {
	defer g.hintsInFlight.Delete(k)
	if wantHash != "" {
		if cur, ok := g.registry.Get(k); ok && cur.Hash == wantHash {
			return
		}
	}
	// The hint keys on the supply's own identity; the fetch needs the RESOURCE
	// name. They are equal unless the node overrode `resource`, so the gate
	// remembers the mapping when it admits an activation (see resources). A pull
	// key fetches from its recorded source instead, which is why the fetch goes
	// through fetchSupply rather than straight to g.fetcher.
	resource := g.resourceFor(k)
	content, fetchedHash, revision, err := g.fetchSupply(ctx, k, resource)
	if err != nil {
		g.notifyFetch(ctx, resource, "error", source)
		g.logRefreshOutcome(slog.LevelWarn, "supply refresh: fetch failed", k, resource, "")
		return
	}
	g.notifyFetch(ctx, resource, "ok", source)
	if err := g.registry.Apply(ctx, supply.Snapshot{
		Namespace: k.Namespace, Name: k.Name, Content: content, Hash: fetchedHash,
		Revision: revision, FetchedAt: time.Now(),
	}); err != nil {
		// A consumer rejected it (e.g. the wasm canary said no). last-good is
		// preserved by construction; the next hint or tick retries.
		g.logRefreshOutcome(slog.LevelWarn, "supply refresh: content rejected by a consumer", k, resource, fetchedHash)
		return
	}
	g.logRefreshOutcome(slog.LevelInfo, "supply refresh: applied", k, resource, fetchedHash)
}

// logRefreshOutcome logs one refresh outcome via the shared g.log formatter,
// carrying only the supply name/resource and a hash PREFIX — never content.
func (g *SupplyGate) logRefreshOutcome(level slog.Level, msg string, k supply.Key, resource, hash string) {
	g.log(context.Background(), level, msg, engine.SupplyRequirement{Node: k.Name, Resource: resource}, hash)
}

// RunPoller refreshes every hosted supply once per interval until ctx is done.
//
// The heartbeat hint is the fast path, but it is notification-only: it is
// piggybacked on a response the runner may never receive, and a runner that
// restarts, a dropped response, or a server failover between two writes all
// lose it. A supply then keeps whatever content it had — forever, because
// nothing else in the system re-reads a supply that already has a snapshot. The
// poll is what makes convergence independent of the notification channel: every
// hosted key is re-read unconditionally, and the hash that comes back decides
// whether anything changed.
//
// It shares refreshOne and hintsInFlight with ApplyHints, so a hint and a tick
// that land on the same key cost one fetch, not two, and apply in one order.
//
// The caller runs this in its own goroutine. ttl goes through
// resolveSupplyPollInterval first, so neither an unset nor a nonsensical
// interval can leave this runner without a poller.
//
// A pull node's declared poll_interval can shorten the period (see pollPeriod):
// the period is re-derived before every round, so a supply declared after this
// call started polling still speeds the cadence up. Each round waits on its own
// ticker and stops it before polling, so a slow poll round cannot queue up
// ticks.
func (g *SupplyGate) RunPoller(ctx context.Context, ttl time.Duration) {
	if g == nil {
		return
	}
	newTicker := g.newPollTicker
	if newTicker == nil {
		newTicker = newRealPollTicker
	}
	base := resolveSupplyPollInterval(ttl, g.logger)
	for {
		ticker := newTicker(supplyPollJitter(g.pollPeriod(base)))
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C():
			ticker.Stop()
		}
		g.pollHosted(ctx)
	}
}

// pollPeriod is the tick period for the next round: the runner's resolved
// interval, or the fastest per-node poll_interval declared so far, whichever is
// shorter.
//
// A pull node's poll_interval can only make the runner's poll FASTER — its
// declared value is bounded below by MinSupplyPollInterval, the same floor the
// runner's own interval obeys, so it can neither disable the poll nor turn it
// into a hammering cadence.
//
// The loop runs at one cadence, so a fast declaration also re-reads this
// runner's store-backed supplies early. That is the cost of keeping the poll a
// single serial loop, it is bounded by the floor, and the alternative —
// per-key schedules inside the poller — is more machinery on a path whose job
// is to be the boring backstop. The declared cadence affects only this runner;
// nothing is written back to the declaration.
func (g *SupplyGate) pollPeriod(base time.Duration) time.Duration {
	period := base
	g.declarations.Range(func(_, v any) bool {
		d, ok := v.(supplyDeclaration)
		if !ok || d.PullSource == nil {
			return true
		}
		p := d.PullSource.PollInterval
		if p <= 0 {
			return true
		}
		// Clamp rather than drop: a requirement written by another version (the
		// wire is not re-validated on decode) must not be able to poll below the
		// floor, and honoring the clamped value keeps the author's intent.
		if p < MinSupplyPollInterval {
			p = MinSupplyPollInterval
		}
		if p < period {
			period = p
		}
		return true
	})
	return period
}

// pollHosted refreshes each hosted supply once, serially.
//
// Serial is deliberate and cheap: a supply fetch is bounded by the fetcher's own
// timeout, and the hosted set is small (one entry per supply node name some
// activation of this process required). Fanning out would only add a second
// concurrency bound to reason about on a path whose whole job is to be the
// boring backstop.
func (g *SupplyGate) pollHosted(ctx context.Context) {
	for _, k := range g.hostedKeys() {
		if ctx.Err() != nil {
			return
		}
		// Same in-flight de-dupe as ApplyHints: a hint-triggered fetch for this
		// key already in flight is asking the server for the same current
		// content, so a second one would only waste a round trip and could
		// apply two results out of order.
		if _, already := g.hintsInFlight.LoadOrStore(k, struct{}{}); already {
			continue
		}
		// An empty wantHash means "fetch it whatever we have": the poll's job is
		// to discover the server's current hash, so it cannot skip the fetch on
		// a hash it already believes. The fetch result decides what changes.
		g.refreshOne(namespace.WithNamespace(ctx, k.Namespace), k, "", supplySourcePoll)
	}
}

// hostedKeys returns the supplies this process hosts, sorted by namespace then
// name so a tick's fetch order is deterministic.
//
// "Hosted" is the key set Admit recorded — every (namespace, supply node) pair
// some activation of this process required — and NOT the set of keys with a
// registered consumer. A supply consumed only by expressions ($supplies.rules
// read inside an xflow.if condition, with no wasm consumer ever registering)
// has a consumer count of zero by construction: pruning it here would strand
// exactly the supplies whose only reader is a condition, which is the case a
// lost hint is least visible in. The set is bounded by what Admit has seen and
// matches the registry's own snapshot lifetime, so the poll can never keep a
// supply alive that the registry has already forgotten.
//
// Nothing is pruned when an activation goes away, for the same reason: the gate
// is not told when the last activation requiring a key deactivates, and the
// cost of a stale entry is one GET per interval against a supply count the
// design keeps small (D3).
func (g *SupplyGate) hostedKeys() []supply.Key {
	var keys []supply.Key
	g.resources.Range(func(k, _ any) bool {
		key, ok := k.(supply.Key)
		if !ok {
			return true
		}
		keys = append(keys, key)
		return true
	})
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Namespace != keys[j].Namespace {
			return keys[i].Namespace < keys[j].Namespace
		}
		return keys[i].Name < keys[j].Name
	})
	return keys
}
