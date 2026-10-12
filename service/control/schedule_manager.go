package control

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// Overridable field names. They are the keys a PATCH may name, the names the
// API's error messages use, and the names the audit row records — one
// vocabulary, spelled once.
const (
	ScheduleFieldExpression = "expression"
	ScheduleFieldTimezone   = "timezone"
	ScheduleFieldMisfire    = "misfire"
	ScheduleFieldOverlap    = "overlap"
)

// ErrScheduleNotFound reports that no schedule exists at the requested address:
// the node is not a live trigger node of this workflow, or no overlay has been
// written for it.
//
// It is deliberately ONE sentinel for every "there is nothing here" case,
// including an overlay whose node no longer exists. The API maps it to 404, and
// a caller that could tell those cases apart could use the difference to probe
// for nodes of workflows it cannot see.
var ErrScheduleNotFound = errors.New("control: no such schedule")

// ScheduleFieldError is a rejected field of a PATCH, with the field name the
// caller used. It is typed rather than a bare error because the API must answer
// with 400 AND name the field: "invalid request" tells an operator nothing about
// which of four optional fields their cron tooling got wrong.
type ScheduleFieldError struct {
	Field   string
	Message string
}

func (e *ScheduleFieldError) Error() string {
	if e.Field == "" {
		return "control: invalid schedule field: " + e.Message
	}
	return fmt.Sprintf("control: invalid schedule field %q: %s", e.Field, e.Message)
}

// ScheduleWorkflow is the workflow a schedule request operates on: its identity
// and its compiled graph.
//
// The manager does not resolve a workflow ID itself, and that is the point. The
// registry read that turns an id into a graph is also where the namespace check
// belongs — a cross-namespace id must resolve to not-found rather than to
// another tenant's graph — and that check needs the authenticated principal,
// which exists only at the HTTP boundary. Handing the manager a resolved graph
// keeps one authority for the check instead of two that could disagree.
type ScheduleWorkflow struct {
	Namespace namespace.Namespace
	ID        types.WorkflowID
	Graph     *graph.Graph
}

// SchedulePatch is a partial update of one overlay.
//
// Presence is carried by the map key, not by the value, because the three cases
// are genuinely distinct and a struct of pointers can only express two of them:
//
//	field absent           -> leave the overlay's field alone
//	field: "0 3 * * *"     -> override with that value
//	field: null            -> REVERT that field to the definition's value
//
// The third case is the reason DELETE .../override (drop the whole row) is not
// the only way back: an operator who pinned an expression and a timezone and
// then wants the expression to follow the definition again would otherwise have
// to drop both and re-apply one.
type SchedulePatch struct {
	Fields map[string]*string
}

// Names returns the field names the patch mentions, sorted, for error messages
// and audit records.
func (p SchedulePatch) Names() []string {
	if len(p.Fields) == 0 {
		return nil
	}
	out := make([]string, 0, len(p.Fields))
	for name := range p.Fields {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// ScheduleView is one schedule as an operator sees it: the trigger node's own
// timing with the overlay applied, plus the two facts that live in neither —
// when it last fired and when it fires next.
//
// It is a snapshot, not a handle. It carries Revision so a client can do a
// conditional write with If-Match without a second read.
type ScheduleView struct {
	WorkflowID types.WorkflowID
	Node       string
	// Kind is ScheduleKindCron or ScheduleKindTimer, and EMPTY when the overlay
	// is orphaned: the kind is read from the live node, and an orphaned overlay
	// has no node to read it from. The overlay does not store a kind on purpose
	// — a stored kind would be a second copy of a fact the graph owns, and the
	// copy is exactly what goes stale when a node changes type.
	Kind string
	// Expression, Timezone, Misfire and Overlap are the EFFECTIVE values: the
	// overlay where it speaks, the definition where it does not. Misfire and
	// Overlap are reported resolved (the node's documented defaults are "skip"
	// and "allow"), because "what will happen" is the question an operator is
	// asking; the definition's own silence is visible through Overridden.
	Expression string
	Timezone   string
	Misfire    string
	Overlap    string
	// Paused is the overlay's pause flag. While paused the schedule does not
	// fire, but its watermark keeps advancing, so resuming never replays the
	// paused window (see Resume).
	Paused bool
	// Overridden is true when an overlay row exists for this node, whether or
	// not it changes anything.
	Overridden bool
	// Orphaned is true when an overlay exists for a node that is no longer a
	// trigger node of this workflow (deleted, renamed, or changed to another
	// type). Orphaned overlays are LISTED rather than deleted: the node may come
	// back under the same name in the next version, and an overlay that
	// silently vanished would be indistinguishable from one that was never
	// written.
	Orphaned bool
	// LastScheduled and NextFire are nil when unknown — no watermark has been
	// recorded yet, or no trigger host can be asked (cluster mode without
	// #17 host ownership), or the effective expression cannot be parsed (a
	// definition may hold one the trigger refuses at activation; the view
	// reports that as a missing next instant rather than failing the listing
	// for the whole workflow).
	LastScheduled *time.Time
	NextFire      *time.Time

	// Revision, UpdatedAt and UpdatedBy describe the overlay row. They are zero
	// when Overridden is false.
	Revision  uint64
	UpdatedAt time.Time
	UpdatedBy string
}

// ScheduleReloader is the trigger host's entry point for "re-read your
// overlays": after a mutation the manager calls it so the host can merge the
// new overlay and re-activate the node. It is the design's ScheduleReloader.
//
// A nil reloader is a supported configuration, not a bug: in cluster mode the
// subscription is held by a runner (spec 2026-10-12 §9 defers that host
// ownership), and a control plane that knows of no host still stores the
// overlay. The change then takes effect at the host's next activation rather
// than immediately — which the API must say, not hide (see ErrScheduleReload).
type ScheduleReloader interface {
	ReloadSchedule(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, node string) error
}

// ErrScheduleReload reports that a mutation was STORED but the trigger host
// could not be told about it. The write is durable and the caller's If-Match
// chain is intact; what is missing is the immediate effect.
//
// It is a distinct sentinel rather than a generic failure because the two
// require different operator actions: a rejected write should be corrected and
// retried, while a stored-but-unannounced one is correct and only needs the
// host told again (any later write, or the host's next activation, does that).
var ErrScheduleReload = errors.New("control: the schedule override was stored but the trigger host was not notified")

// ScheduleStateReader answers "when did this schedule last fire" from the
// trigger's own recorded state, which is where the watermark lives.
//
// The watermark is not in the store the control plane reads for overlays: it is
// written by whichever process hosted the trigger (a TriggerRuntime's State),
// and in cluster mode that is a runner. An implementation is therefore only
// available where the same process both hosts the trigger and serves this API —
// sdk/xflow's local trigger host. nil means the answer is unknown, and the API
// reports last_scheduled as null rather than guessing.
type ScheduleStateReader interface {
	LastScheduled(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, node string) (time.Time, bool, error)
}

// ScheduleManagerConfig configures a ScheduleManager.
type ScheduleManagerConfig struct {
	// Overrides is the durable overlay store. Required.
	Overrides store.ScheduleOverrideStore
	// Reloader notifies the trigger host of a change. May be nil.
	Reloader ScheduleReloader
	// State reads the scheduling watermark. May be nil.
	State ScheduleStateReader
	// Emitter fires one instant of a schedule's own timeline, for a backfill.
	// May be nil, which is the deployment whose triggers are hosted elsewhere:
	// it stores and displays schedules, and refuses to backfill them.
	Emitter ScheduleEmitter
	// Now supplies the instant next_fire is computed from. May be nil (time.Now);
	// tests set it so a view is a function of the schedule rather than of the
	// clock the test happens to run on.
	Now func() time.Time
}

// ScheduleManager is the control plane's schedule service: it derives the views
// an operator reads, validates and stores overlays, and notifies the trigger
// host.
//
// It writes no audit rows itself. The HTTP layer's authorization wrapper writes
// the admission and outcome rows for every mutating route, taken from the
// authenticated principal; a second, manager-level audit would duplicate those
// rows with a subject the manager cannot authenticate. What the manager DOES
// own is the actor recorded on the row (UpdatedBy), taken from the same
// principal.
type ScheduleManager struct {
	overrides store.ScheduleOverrideStore
	reloader  ScheduleReloader
	state     ScheduleStateReader
	emitter   ScheduleEmitter
	now       func() time.Time
}

// NewScheduleManager constructs a manager. It returns nil when no overlay store
// is configured: without durable storage the API has nothing to serve, and a nil
// manager makes that a refusal at every call site rather than a nil-dereference
// deep inside a request.
func NewScheduleManager(cfg ScheduleManagerConfig) *ScheduleManager {
	if cfg.Overrides == nil {
		return nil
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &ScheduleManager{
		overrides: cfg.Overrides,
		reloader:  cfg.Reloader,
		state:     cfg.State,
		emitter:   cfg.Emitter,
		now:       now,
	}
}

// List returns every schedule of a workflow: the trigger nodes it declares, each
// with its overlay applied, followed by overlays whose node is gone.
func (m *ScheduleManager) List(ctx context.Context, wf ScheduleWorkflow) ([]ScheduleView, error) {
	if m == nil || m.overrides == nil {
		return nil, errors.New("control: no schedule store configured")
	}
	overrides, err := m.overrides.ListScheduleOverrides(ctx, wf.Namespace, wf.ID)
	if err != nil {
		return nil, err
	}
	byNode := make(map[string]*store.ScheduleOverride, len(overrides))
	for _, ov := range overrides {
		byNode[ov.Node] = ov
	}
	nodes, err := scheduleNodes(wf.Graph)
	if err != nil {
		return nil, err
	}
	views := make([]ScheduleView, 0, len(nodes)+len(overrides))
	for _, nm := range nodes {
		// Per-iteration variable (go >= 1.22), so the address is this node's.
		view, err := m.view(ctx, wf, nm.Name, &nm, byNode[nm.Name])
		if err != nil {
			return nil, err
		}
		delete(byNode, nm.Name)
		views = append(views, view)
	}
	// Whatever is left is an orphan: an overlay for a node this version does not
	// declare as a schedule. It is listed (see ScheduleView.Orphaned) so an
	// operator can see the leftover and delete it explicitly.
	for _, ov := range byNode {
		view, err := m.view(ctx, wf, ov.Node, nil, ov)
		if err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Node < views[j].Node })
	return views, nil
}

// Get returns one schedule: a trigger node with its overlay applied, or an
// orphaned overlay.
func (m *ScheduleManager) Get(ctx context.Context, wf ScheduleWorkflow, node string) (ScheduleView, error) {
	if m == nil || m.overrides == nil {
		return ScheduleView{}, errors.New("control: no schedule store configured")
	}
	nm, err := m.node(wf.Graph, node)
	if err != nil {
		return ScheduleView{}, err
	}
	ov, err := m.overlay(ctx, wf, node)
	if err != nil {
		return ScheduleView{}, err
	}
	if nm == nil && ov == nil {
		return ScheduleView{}, fmt.Errorf("%w: node %q", ErrScheduleNotFound, node)
	}
	return m.view(ctx, wf, node, nm, ov)
}

// Patch applies a partial update to one schedule's overlay, creating the
// overlay when there is none, and returns the resulting view.
//
// The write is validated against the live node's KIND before it is stored: a
// timezone on a timer is refused, because the timer handler has no such
// parameter and accepting it would report an override as in effect while the
// trigger ran unchanged.
//
// A patch that names at least one field always writes, bumping the overlay's
// revision even when the value is what it already was — the store's rule, and
// the one a conditional writer expects. A patch that names nothing is a read:
// it returns the current view and writes nothing.
func (m *ScheduleManager) Patch(ctx context.Context, wf ScheduleWorkflow, node, actor string, patch SchedulePatch, ifMatch *uint64) (ScheduleView, error) {
	if m == nil || m.overrides == nil {
		return ScheduleView{}, errors.New("control: no schedule store configured")
	}
	nm, err := m.node(wf.Graph, node)
	if err != nil {
		return ScheduleView{}, err
	}
	if nm == nil {
		// A patch is validated against the node it targets, so it cannot be
		// applied to an orphan: there is no kind left to refuse a timer's
		// timezone with, and an override nothing reads is not what the operator
		// asked for. Deleting the orphan is, and that is DeleteOverride.
		return ScheduleView{}, fmt.Errorf("%w: node %q is not a trigger node of this workflow", ErrScheduleNotFound, node)
	}
	kind, _ := ScheduleKindOf(nm.Type)
	ov, err := m.overlay(ctx, wf, node)
	if err != nil {
		return ScheduleView{}, err
	}
	if err := m.checkIfMatch(ov, ifMatch); err != nil {
		return ScheduleView{}, err
	}
	if len(patch.Fields) == 0 {
		// A read with a precondition: the revision is checked first, because
		// the caller asserted something about the resource whether or not the
		// patch would have changed it.
		return m.view(ctx, wf, node, nm, ov)
	}
	next := newScheduleOverride(wf, node, actor, ov)
	caps := ScheduleCapabilitiesFor(kind)
	for name, value := range patch.Fields {
		field := strings.ToLower(strings.TrimSpace(name))
		switch field {
		case ScheduleFieldExpression:
			if value != nil {
				// Parsed with the node's own parser. The timezone is not needed
				// to judge the expression (both kinds' spellings are
				// zone-independent), and passing none keeps a bad timezone from
				// being reported as a bad expression.
				if _, err := ParseScheduleExpression(kind, *value, ""); err != nil {
					return ScheduleView{}, &ScheduleFieldError{Field: field, Message: err.Error()}
				}
			}
			next.Expression = value
		case ScheduleFieldTimezone:
			if !caps.Timezone {
				return ScheduleView{}, &ScheduleFieldError{Field: field, Message: fmt.Sprintf("a %s schedule has no timezone", kind)}
			}
			if value != nil {
				if _, err := time.LoadLocation(*value); err != nil {
					return ScheduleView{}, &ScheduleFieldError{Field: field, Message: fmt.Sprintf("unknown IANA time zone %q", *value)}
				}
			}
			next.Timezone = value
		case ScheduleFieldMisfire:
			if !caps.Misfire {
				return ScheduleView{}, &ScheduleFieldError{Field: field, Message: fmt.Sprintf("a %s schedule has no misfire policy", kind)}
			}
			if err := checkSchedulePolicy(field, value, ScheduleMisfirePolicies); err != nil {
				return ScheduleView{}, err
			}
			next.Misfire = value
		case ScheduleFieldOverlap:
			if !caps.Overlap {
				return ScheduleView{}, &ScheduleFieldError{Field: field, Message: fmt.Sprintf("a %s schedule has no overlap policy", kind)}
			}
			if err := checkSchedulePolicy(field, value, ScheduleOverlapPolicies); err != nil {
				return ScheduleView{}, err
			}
			next.Overlap = value
		default:
			return ScheduleView{}, &ScheduleFieldError{Field: name, Message: "not an overridable schedule field"}
		}
	}
	stored, err := m.put(ctx, next, ov)
	if err != nil {
		return ScheduleView{}, err
	}
	view, err := m.view(ctx, wf, node, nm, stored)
	if err != nil {
		return ScheduleView{}, err
	}
	return view, m.reload(ctx, wf, node)
}

// DeleteOverride drops a schedule's overlay, returning it to the definition's
// own timing, and returns the resulting view. It is the only operation that
// works on an orphaned overlay — a leftover row nobody can see is a leftover
// row nobody can clean up.
func (m *ScheduleManager) DeleteOverride(ctx context.Context, wf ScheduleWorkflow, node, actor string, ifMatch *uint64) (ScheduleView, error) {
	if m == nil || m.overrides == nil {
		return ScheduleView{}, errors.New("control: no schedule store configured")
	}
	nm, err := m.node(wf.Graph, node)
	if err != nil {
		return ScheduleView{}, err
	}
	ov, err := m.overlay(ctx, wf, node)
	if err != nil {
		return ScheduleView{}, err
	}
	if ov == nil {
		return ScheduleView{}, fmt.Errorf("%w: node %q has no override", ErrScheduleNotFound, node)
	}
	if err := m.checkIfMatch(ov, ifMatch); err != nil {
		return ScheduleView{}, err
	}
	if err := m.overrides.DeleteScheduleOverride(ctx, wf.Namespace, wf.ID, node, &ov.Revision); err != nil {
		return ScheduleView{}, err
	}
	view, err := m.view(ctx, wf, node, nm, nil)
	if err != nil {
		return ScheduleView{}, err
	}
	return view, m.reload(ctx, wf, node)
}

// Pause stops a schedule from firing without changing what it says.
//
// It is idempotent: pausing a schedule that is already paused writes nothing
// and bumps no revision, so a retried or concurrent pause cannot invalidate
// another operator's If-Match. Nothing is validated about the expression —
// pause is the action an operator reaches for when a schedule is misbehaving,
// and refusing it because the definition holds an expression the trigger
// rejects would be exactly backwards.
//
// Pausing is not a catch-up's mirror image: the watermark keeps advancing while
// the schedule is paused, so Resume does NOT replay the paused window. The
// skipped instants are skipped, and asking for them is what backfill is for.
func (m *ScheduleManager) Pause(ctx context.Context, wf ScheduleWorkflow, node, actor string, ifMatch *uint64) (ScheduleView, error) {
	return m.setPaused(ctx, wf, node, actor, true, ifMatch)
}

// Resume lets a paused schedule fire again, from the next instant onward. See
// Pause for why the paused window is not replayed.
func (m *ScheduleManager) Resume(ctx context.Context, wf ScheduleWorkflow, node, actor string, ifMatch *uint64) (ScheduleView, error) {
	return m.setPaused(ctx, wf, node, actor, false, ifMatch)
}

func (m *ScheduleManager) setPaused(ctx context.Context, wf ScheduleWorkflow, node, actor string, paused bool, ifMatch *uint64) (ScheduleView, error) {
	if m == nil || m.overrides == nil {
		return ScheduleView{}, errors.New("control: no schedule store configured")
	}
	nm, err := m.node(wf.Graph, node)
	if err != nil {
		return ScheduleView{}, err
	}
	if nm == nil {
		return ScheduleView{}, fmt.Errorf("%w: node %q is not a trigger node of this workflow", ErrScheduleNotFound, node)
	}
	ov, err := m.overlay(ctx, wf, node)
	if err != nil {
		return ScheduleView{}, err
	}
	if err := m.checkIfMatch(ov, ifMatch); err != nil {
		return ScheduleView{}, err
	}
	if schedulePaused(ov) == paused {
		// Already there: no write, no revision bump, and no reload — there is
		// nothing the host would read differently.
		return m.view(ctx, wf, node, nm, ov)
	}
	next := newScheduleOverride(wf, node, actor, ov)
	next.Paused = &paused
	stored, err := m.put(ctx, next, ov)
	if err != nil {
		return ScheduleView{}, err
	}
	view, err := m.view(ctx, wf, node, nm, stored)
	if err != nil {
		return ScheduleView{}, err
	}
	return view, m.reload(ctx, wf, node)
}

// put writes a row, passing the read row's revision as the store-level CAS.
//
// The CAS is what makes Patch a read-modify-write rather than a lost-update
// race: a concurrent writer that lands between the read above and this write
// bumps the revision, and this write then fails with ErrRevisionConflict
// instead of overwriting. When there is no row yet, the CAS is 0 — "create only
// if still absent" — so two callers creating the same overlay cannot both win.
func (m *ScheduleManager) put(ctx context.Context, next *store.ScheduleOverride, current *store.ScheduleOverride) (*store.ScheduleOverride, error) {
	cas := uint64(0)
	if current != nil {
		cas = current.Revision
	}
	return m.overrides.PutScheduleOverride(ctx, next, &cas)
}

// checkIfMatch applies the caller's conditional write. nil means unconditional.
//
// 0 means "only if this overlay does not exist yet" and any other value means
// "only if its revision is exactly this" — the same If-Match semantics supplies
// use, so an operator learns one rule for the API's conditional writes.
func (m *ScheduleManager) checkIfMatch(current *store.ScheduleOverride, ifMatch *uint64) error {
	if ifMatch == nil {
		return nil
	}
	if current == nil {
		if *ifMatch == 0 {
			return nil
		}
		return fmt.Errorf("%w: no override exists to match revision %d", store.ErrRevisionConflict, *ifMatch)
	}
	if *ifMatch != current.Revision {
		return fmt.Errorf("%w: override revision is %d, not %d", store.ErrRevisionConflict, current.Revision, *ifMatch)
	}
	return nil
}

// reload notifies the trigger host, if there is one and it is configured.
func (m *ScheduleManager) reload(ctx context.Context, wf ScheduleWorkflow, node string) error {
	if m.reloader == nil {
		return nil
	}
	if err := m.reloader.ReloadSchedule(ctx, wf.Namespace, wf.ID, node); err != nil {
		return fmt.Errorf("%w: %s", ErrScheduleReload, err)
	}
	return nil
}

// overlay reads one overlay, mapping "absent" to nil so callers can tell the
// three states (no overlay, overlay, store failure) apart.
func (m *ScheduleManager) overlay(ctx context.Context, wf ScheduleWorkflow, node string) (*store.ScheduleOverride, error) {
	ov, err := m.overrides.GetScheduleOverride(ctx, wf.Namespace, wf.ID, node)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return ov, nil
}

// node returns the schedule node `name` names, or nil when the graph has no
// such schedule. The error return is for the template-rendering failure below,
// which is a defect in the definition rather than a missing node.
func (m *ScheduleManager) node(g *graph.Graph, name string) (*graph.NodeMeta, error) {
	nodes, err := scheduleNodes(g)
	if err != nil {
		return nil, err
	}
	for i := range nodes {
		if nodes[i].Name == name {
			return &nodes[i], nil
		}
	}
	return nil, nil
}

// view assembles one view. nm may be nil (an orphaned overlay, or the
// just-deleted overlay of a node that no longer exists), and ov may be nil
// (a node with no overlay, or one whose overlay was just deleted); node is the
// address the caller asked about, so those cases still have a name to report.
func (m *ScheduleManager) view(ctx context.Context, wf ScheduleWorkflow, node string, nm *graph.NodeMeta, ov *store.ScheduleOverride) (ScheduleView, error) {
	view := ScheduleView{
		WorkflowID: wf.ID,
		Node:       node,
		Orphaned:   nm == nil,
		Overridden: ov != nil,
		Paused:     schedulePaused(ov),
		UpdatedBy:  overrideUpdatedBy(ov),
	}
	if ov != nil {
		view.Revision = ov.Revision
		view.UpdatedAt = ov.UpdatedAt
	}
	if nm == nil {
		// The overlay's own recorded values are all that is left to report: the
		// node that would have supplied kind and definition values is gone. With
		// no overlay either — the row was just deleted, and its node is gone —
		// there is nothing to report but the address, which is the honest answer.
		view.Expression = derefString(overrideField(ov, ScheduleFieldExpression))
		view.Timezone = derefString(overrideField(ov, ScheduleFieldTimezone))
		view.Misfire = derefString(overrideField(ov, ScheduleFieldMisfire))
		view.Overlap = derefString(overrideField(ov, ScheduleFieldOverlap))
		return view, nil
	}
	kind, _ := ScheduleKindOf(nm.Type)
	view.Kind = kind

	params, err := m.params(wf, nm)
	if err != nil {
		return ScheduleView{}, err
	}
	caps := ScheduleCapabilitiesFor(kind)
	view.Expression = effectiveParam(params, ov, ScheduleExpressionParam(kind), ScheduleFieldExpression)
	if caps.Timezone {
		// Resolution mirrors the cron handler's: the node's own timezone, then
		// the workflow's settings.timezone, then UTC. Reporting the raw (empty)
		// parameter would answer "what does the definition say" when the
		// operator asked "when does this fire".
		view.Timezone = effectiveParam(params, ov, ScheduleFieldTimezone, ScheduleFieldTimezone)
		if view.Timezone == "" {
			view.Timezone = wf.Graph.Timezone()
		}
		if view.Timezone == "" {
			view.Timezone = "UTC"
		}
	}
	if caps.Misfire {
		view.Misfire = effectiveParam(params, ov, ScheduleFieldMisfire, ScheduleFieldMisfire)
		if view.Misfire == "" {
			view.Misfire = ScheduleMisfirePolicies[0] // skip
		}
	}
	if caps.Overlap {
		view.Overlap = effectiveParam(params, ov, ScheduleFieldOverlap, ScheduleFieldOverlap)
		if view.Overlap == "" {
			view.Overlap = ScheduleOverlapPolicies[0] // allow
		}
	}
	// next_fire is derived from the EFFECTIVE schedule, so an operator who
	// patched the expression sees the new instant without a second call. An
	// expression the parser refuses (a definition the trigger will refuse at
	// activation) leaves it null: the listing is still the useful part.
	if expr, err := ParseScheduleExpression(kind, view.Expression, view.Timezone); err == nil {
		if next := expr.Next(m.now()); !next.IsZero() {
			view.NextFire = &next
		}
	}
	if m.state != nil {
		last, ok, err := m.state.LastScheduled(ctx, wf.Namespace, wf.ID, view.Node)
		if err != nil {
			return ScheduleView{}, err
		}
		if ok {
			view.LastScheduled = &last
		}
	}
	return view, nil
}

// effectiveParam resolves one overridable value: the overlay where it speaks,
// the node's rendered parameter where it does not.
//
// The overlay field and the parameter are named separately because they are not
// always the same word — a timer's expression is its `interval` — and conflating
// them would make an override land on a parameter the handler never reads.
func effectiveParam(params map[string]any, ov *store.ScheduleOverride, param, field string) string {
	if value := overrideField(ov, field); value != nil {
		return *value
	}
	return scheduleParamString(params, param)
}

// overrideField reads one field of an overlay, nil-safely: a nil overlay has no
// fields, which is what the caller's fallback and the view both need.
func overrideField(ov *store.ScheduleOverride, field string) *string {
	if ov == nil {
		return nil
	}
	switch field {
	case ScheduleFieldExpression:
		return ov.Expression
	case ScheduleFieldTimezone:
		return ov.Timezone
	case ScheduleFieldMisfire:
		return ov.Misfire
	case ScheduleFieldOverlap:
		return ov.Overlap
	}
	return nil
}

// scheduleNodes returns the workflow's independent cron and timer trigger nodes,
// in the graph's own order.
//
// "Independent" is the whole of the selection rule: a trigger that is a member
// of a co-location group is not an entry unit of its own (the group is), so its
// activation does not exist to be retimed here, and neither is a trigger kind
// without a timing rule (a webhook has a route, not a schedule). Both are
// skipped rather than reported with an empty kind, because a schedule list that
// names things that cannot be scheduled is noise an operator has to filter.
func scheduleNodes(g *graph.Graph) ([]graph.NodeMeta, error) {
	if g == nil {
		return nil, nil
	}
	var out []graph.NodeMeta
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) != graph.UnitNode {
			continue
		}
		nm := g.NodeAt(g.UnitNodeIndex(i))
		if nm.Kind != types.NodeKindTrigger {
			continue
		}
		if _, ok := ScheduleKindOf(nm.Type); !ok {
			continue
		}
		out = append(out, nm)
	}
	return out, nil
}

// IsIndependentSchedule reports whether node is one of the graph's independent
// schedule nodes — the same rule the schedule API's listing and its writes use.
//
// It is exported for the trigger HOST, which must apply an overlay under exactly
// the rule the API writes one under: a host that applied an overlay the API
// reports as orphaned would run a schedule the operator was told is not in
// effect, and a host that ignored one the API reports as in effect would run the
// wrong one. A nil graph reports false — with no compiled definition there is
// nothing to decide from, and the definition's own parameters are the safe
// answer.
func IsIndependentSchedule(g *graph.Graph, node string) bool {
	nodes, err := scheduleNodes(g)
	if err != nil {
		return false
	}
	for i := range nodes {
		if nodes[i].Name == node {
			return true
		}
	}
	return false
}

// scheduleParamString reads a string parameter the way the trigger handlers do:
// a value that is not a string but has a textual form is reported as that form,
// and a missing one as empty.
func scheduleParamString(params map[string]any, name string) string {
	if name == "" || params == nil {
		return ""
	}
	switch v := params[name].(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}

// checkSchedulePolicy validates one policy field against its accepted values.
func checkSchedulePolicy(field string, value *string, allowed []string) error {
	if value == nil {
		// An explicit null reverts the field to the definition's value; there is
		// nothing of the caller's to validate.
		return nil
	}
	for _, a := range allowed {
		if *value == a {
			return nil
		}
	}
	return &ScheduleFieldError{Field: field, Message: fmt.Sprintf("must be one of %s", strings.Join(allowed, ", "))}
}

// newScheduleOverride starts the row a write will store: the existing row's
// fields when there is one (a patch is partial), the target identity and actor
// always.
func newScheduleOverride(wf ScheduleWorkflow, node, actor string, current *store.ScheduleOverride) *store.ScheduleOverride {
	next := &store.ScheduleOverride{
		Namespace:  wf.Namespace,
		WorkflowID: wf.ID,
		Node:       node,
		UpdatedBy:  actor,
	}
	if current != nil {
		next.Expression = current.Expression
		next.Timezone = current.Timezone
		next.Misfire = current.Misfire
		next.Overlap = current.Overlap
		next.Paused = current.Paused
		next.Revision = current.Revision
	}
	return next
}

// schedulePaused reports whether an overlay pauses its schedule. A row that
// does not say (nil) does not pause: pausing is the state that has to be asked
// for.
func schedulePaused(ov *store.ScheduleOverride) bool {
	return ov != nil && ov.Paused != nil && *ov.Paused
}

func overrideUpdatedBy(ov *store.ScheduleOverride) string {
	if ov == nil {
		return ""
	}
	return ov.UpdatedBy
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
