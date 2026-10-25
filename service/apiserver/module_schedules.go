package apiserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// The schedule API: the runtime overlay over a workflow's cron and timer trigger
// nodes.
//
// A schedule is not an entity of its own. It is a trigger node of a registered
// workflow, plus whatever an operator has changed about it without registering a
// new version — the design's answer to "schedules need to change faster than
// definitions do". Every route here therefore resolves the WORKFLOW first, and
// everything it reports is derived from the compiled graph plus the overlay
// (see control.ScheduleManager).
//
// Namespace: the principal's, resolved server-side by the authz wrapper and read
// here via namespace.FromContext. A workflow id that resolves to another
// namespace's record is reported as not-found — never as forbidden, which would
// confirm the id exists — and never reaches the manager, so no store read is
// attempted under the wrong tenant.
//
// The manager writes no audit rows of its own: every mutating route below is
// registered through the authz wrapper, which appends the admission and outcome
// rows from the authenticated principal. What the manager records is the actor
// inside the overlay itself (updated_by), taken from the same principal.

// scheduleViewResponse is one schedule on the wire: the design's ScheduleView.
type scheduleViewResponse struct {
	WorkflowID string `json:"workflow_id"`
	Node       string `json:"node"`
	// Kind is "cron" or "timer", and EMPTY for an orphaned overlay — the kind
	// belongs to the live node, and an orphaned overlay has none to read it from.
	Kind string `json:"kind"`
	// Expression, Timezone, Misfire and Overlap are the EFFECTIVE values: the
	// overlay where it speaks, the definition where it does not. Misfire and
	// Overlap are resolved to the node's documented defaults ("skip" and
	// "allow") because "what will happen" is what an operator is asking;
	// Overridden says whether the definition's own silence is still in play.
	Expression string `json:"expression"`
	Timezone   string `json:"timezone,omitempty"`
	Misfire    string `json:"misfire,omitempty"`
	Overlap    string `json:"overlap,omitempty"`
	// Paused means the schedule does not fire. Its watermark keeps advancing, so
	// resuming never replays the paused window — asking for those instants is
	// what the backfill route is for.
	Paused bool `json:"paused"`
	// Overridden is true when an overlay row exists, whether or not it changes
	// anything; Orphaned is true when that row's node is no longer a trigger node
	// of the current version (renamed, deleted, or changed type). Orphaned rows
	// are listed rather than deleted: the node may return under the same name in
	// the next version.
	Overridden bool `json:"overridden"`
	Orphaned   bool `json:"orphaned"`
	// LastScheduled and NextFire are null when unknown: no fire has been recorded
	// yet, this server cannot read the trigger's watermark (see
	// control.ScheduleStateReader), or the effective expression cannot be parsed.
	LastScheduled *time.Time `json:"last_scheduled"`
	NextFire      *time.Time `json:"next_fire"`
	// Revision is the write-side token a conditional write echoes back in
	// If-Match. It is bumped by every write, even one that changes nothing.
	Revision  uint64 `json:"revision"`
	UpdatedAt string `json:"updated_at,omitempty"`
	UpdatedBy string `json:"updated_by,omitempty"`
}

// scheduleViewBody projects one manager view onto the wire.
func scheduleViewBody(v control.ScheduleView) scheduleViewResponse {
	body := scheduleViewResponse{
		WorkflowID:    string(v.WorkflowID),
		Node:          v.Node,
		Kind:          v.Kind,
		Expression:    v.Expression,
		Timezone:      v.Timezone,
		Misfire:       v.Misfire,
		Overlap:       v.Overlap,
		Paused:        v.Paused,
		Overridden:    v.Overridden,
		Orphaned:      v.Orphaned,
		LastScheduled: utcOrNil(v.LastScheduled),
		NextFire:      utcOrNil(v.NextFire),
		Revision:      v.Revision,
		UpdatedBy:     v.UpdatedBy,
	}
	if !v.UpdatedAt.IsZero() {
		body.UpdatedAt = v.UpdatedAt.UTC().Format(time.RFC3339)
	}
	return body
}

// utcOrNil normalizes an instant to UTC so the wire form does not vary with the
// location the server's clock happened to carry. The instant is the same moment
// either way; a client comparing two instants as strings is not.
func utcOrNil(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	utc := t.UTC()
	return &utc
}

// scheduleBackfillResponse reports a backfill: what the range holds, and what
// happened to each instant. Every planned instant ends up in exactly one of
// emitted or skipped, so the two lists are the whole answer — and a repeated
// request is safe, because an instant that already ran is reported as a
// duplicate skip rather than run a second time.
type scheduleBackfillResponse struct {
	From    time.Time                   `json:"from"`
	To      time.Time                   `json:"to"`
	DryRun  bool                        `json:"dry_run"`
	Planned []time.Time                 `json:"planned"`
	Emitted []scheduleBackfillEmission  `json:"emitted"`
	Skipped []scheduleBackfillSkipEntry `json:"skipped"`
}

type scheduleBackfillEmission struct {
	Instant     time.Time `json:"instant"`
	ExecutionID string    `json:"execution_id"`
}

// scheduleBackfillSkipEntry is one instant that did not run, and why. Reason is
// stable vocabulary (duplicate / refused / failed); detail is the underlying
// error as the server saw it, and is empty for a duplicate, which is not a
// failure.
type scheduleBackfillSkipEntry struct {
	Instant time.Time `json:"instant"`
	Reason  string    `json:"reason"`
	Detail  string    `json:"detail,omitempty"`
}

// scheduleBackfillRequest is the backfill body. from and to are RFC3339
// timestamps; the range is (from, to] — an instant exactly at from is the one
// the schedule last acknowledged, and asking for its re-run is a different
// request than asking for a range.
type scheduleBackfillRequest struct {
	From   string `json:"from"`
	To     string `json:"to"`
	DryRun bool   `json:"dry_run"`
}

// registerScheduleRoutes mounts the schedule routes. They are always registered,
// even when no overlay store is configured: a deployment without one answers
// them with 501 "not configured on this server", which tells an operator why the
// route is not working. A 404 would instead read as "this server is older than
// the route", which is a different and false statement (the same reasoning the
// executions collection follows when its store is absent).
//
// No 404 catch is registered for method mismatches under this subtree. The
// /v1/executions/{id}/ subtree has one, but the /v1/workflows subtree never has,
// and the two policies are not interchangeable: the executions catch exists to
// keep a method mismatch from confirming that an execution id exists, and here
// the id is a workflow the caller has already addressed by path.
func (m *workflowControlModule) registerScheduleRoutes(mux *http.ServeMux) {
	authz := m.authzWrap
	mux.HandleFunc("GET "+PathWorkflowSchedules, authz(OpScheduleRead, false, m.handleListSchedules, workflowIDResolver()))
	mux.HandleFunc("GET "+PathWorkflowScheduleByNode, authz(OpScheduleRead, false, m.handleGetSchedule, scheduleNodeResolver()))
	mux.HandleFunc("PATCH "+PathWorkflowScheduleByNode, authz(OpScheduleWrite, true, m.handlePatchSchedule, scheduleNodeResolver()))
	mux.HandleFunc("DELETE "+PathWorkflowScheduleOverride, authz(OpScheduleWrite, true, m.handleDeleteScheduleOverride, scheduleNodeResolver()))
	mux.HandleFunc("POST "+PathWorkflowSchedulePause, authz(OpScheduleWrite, true, m.handlePauseSchedule, scheduleNodeResolver()))
	mux.HandleFunc("POST "+PathWorkflowScheduleResume, authz(OpScheduleWrite, true, m.handleResumeSchedule, scheduleNodeResolver()))
	mux.HandleFunc("POST "+PathWorkflowScheduleBackfill, authz(OpScheduleBackfill, true, m.handleBackfillSchedule, scheduleNodeResolver()))
}

// registerScheduleRoutesBare is the legacy (no PrincipalAuthenticator) twin of
// registerScheduleRoutes. It exists for the same reason every other route in
// this module has one: a deployment that never configured principal auth still
// serves the API, authenticated by whatever WorkflowAuthenticator it declared.
// Without it the routes would vanish from such a deployment, and the schedules
// API would be the only part of the control plane that disappears with an
// unrelated configuration choice.
func (m *workflowControlModule) registerScheduleRoutesBare(mux *http.ServeMux, wrap func(op string, h http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("GET "+PathWorkflowSchedules, wrap("schedule.read", m.handleListSchedules))
	mux.HandleFunc("GET "+PathWorkflowScheduleByNode, wrap("schedule.read", m.handleGetSchedule))
	mux.HandleFunc("PATCH "+PathWorkflowScheduleByNode, wrap("schedule.write", m.handlePatchSchedule))
	mux.HandleFunc("DELETE "+PathWorkflowScheduleOverride, wrap("schedule.write", m.handleDeleteScheduleOverride))
	mux.HandleFunc("POST "+PathWorkflowSchedulePause, wrap("schedule.write", m.handlePauseSchedule))
	mux.HandleFunc("POST "+PathWorkflowScheduleResume, wrap("schedule.write", m.handleResumeSchedule))
	mux.HandleFunc("POST "+PathWorkflowScheduleBackfill, wrap("schedule.backfill", m.handleBackfillSchedule))
}

// scheduleNodeResolver is the audit-target resolver for the routes that address
// one schedule: the row names the workflow AND the node, because that pair is
// the resource the caller actually acted on. ResourceNamespace is left empty —
// the authoritative IDOR defense is the namespace comparison in
// resolveScheduleWorkflow plus the namespace-scoped store read behind it.
func scheduleNodeResolver() func(*http.Request) (string, string, string, string) {
	return func(r *http.Request) (string, string, string, string) {
		id := r.PathValue("id")
		node := r.PathValue("node")
		return "workflow/" + id + "/schedule/" + node, "", id, ""
	}
}

// resolveScheduleWorkflow resolves the workflow a schedule request addresses:
// its compiled graph, and the namespace the caller may see it in.
//
// The registry read is by id and is therefore NOT the access control; the
// namespace comparison below is, and it answers not-found (never forbidden) so a
// probe cannot use the difference to learn that another tenant's workflow
// exists. This is the same check handleGetWorkflow makes, deliberately repeated
// rather than factored out: it is the line that has to be auditable in every
// handler that resolves a workflow by an id from the path.
func (m *workflowControlModule) resolveScheduleWorkflow(w http.ResponseWriter, r *http.Request) (control.ScheduleWorkflow, bool) {
	if m.schedules == nil {
		writeFail(w, r, http.StatusNotImplemented, "schedules_unavailable",
			"the schedules API is not configured on this server")
		return control.ScheduleWorkflow{}, false
	}
	id := types.WorkflowID(r.PathValue("id"))
	registry := m.registry()
	if registry == nil || id == "" {
		writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
		return control.ScheduleWorkflow{}, false
	}
	rec, err := registry.GetWorkflow(r.Context(), id)
	if err != nil {
		if errors.Is(err, backend.ErrWorkflowNotFound) {
			writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
			return control.ScheduleWorkflow{}, false
		}
		if m.log != nil {
			m.log.Error("schedule_workflow_lookup_failed", "err", err)
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return control.ScheduleWorkflow{}, false
	}
	ns := namespace.FromContext(r.Context())
	if rec.Namespace != "" && ns != "" && rec.Namespace != string(ns) {
		writeFail(w, r, http.StatusNotFound, "workflow_not_found", "workflow not found")
		return control.ScheduleWorkflow{}, false
	}
	return control.ScheduleWorkflow{Namespace: ns, ID: id, Graph: rec.Graph}, true
}

// scheduleActor is the principal whose change this is. The authz wrapper has
// already authenticated it; a deployment on the bare (non-principal) path has no
// subject to report, and an empty updated_by is the honest answer there.
func scheduleActor(r *http.Request) string {
	if p, ok := principalFromRequest(r); ok {
		return p.Subject
	}
	return ""
}

// nodeParam is the {node} path value, trimmed. The name is used verbatim as the
// overlay's key, so whitespace is a mistake worth refusing rather than storing.
func nodeParam(r *http.Request) string {
	return strings.TrimSpace(r.PathValue("node"))
}

// parseIfMatch reads the conditional-write precondition.
//
// The header carries the revision a previous read reported, or 0 for "only if
// this schedule has no override yet" — one vocabulary with the store's own
// PutScheduleOverride(ifMatch), so an operator learns one rule. HTTP's symbolic
// `If-Match: *` is refused rather than guessed at: its meaning ("the resource
// exists") has no counterpart in that vocabulary, and quietly mapping it onto
// one of the three real cases would give the caller a guarantee they did not
// ask for.
func parseIfMatch(w http.ResponseWriter, r *http.Request) (*uint64, bool) {
	raw := strings.TrimSpace(r.Header.Get("If-Match"))
	if raw == "" {
		return nil, true
	}
	revision, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		// The value is not echoed: it is unvalidated caller input, and there is
		// nothing to learn from seeing it back.
		writeFail(w, r, http.StatusBadRequest, "invalid_if_match",
			"If-Match must be the revision number a previous read reported, or 0 for create-only-if-absent")
		return nil, false
	}
	return &revision, true
}

// decodeSchedulePatch reads a PATCH body into the three-state form
// SchedulePatch expresses: a missing key leaves the overlay's field alone, a
// value overrides it, and an explicit null reverts it to the definition's.
//
// An empty body is an empty patch — a read with a precondition — rather than a
// malformed request: "does revision N still hold" is a question the If-Match
// header can ask on its own, and answering it with 400 would make the
// precondition unusable without inventing a body.
//
// The read carries the same MaxRequestBodyBytes cap as decodeJSON: a patch is
// small, and an unbounded body here would let a caller buffer arbitrarily much
// before any field validation runs. Overflow answers decodeJSON's exact 413.
func decodeSchedulePatch(w http.ResponseWriter, r *http.Request) (control.SchedulePatch, bool) {
	defer func() { _ = r.Body.Close() }()
	var fields map[string]*string
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxRequestBodyBytes)).Decode(&fields); err != nil && !errors.Is(err, io.EOF) {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeFail(w, r, http.StatusRequestEntityTooLarge, "request_too_large", "request body too large")
			return control.SchedulePatch{}, false
		}
		writeFail(w, r, http.StatusBadRequest, "bad_request", "invalid JSON")
		return control.SchedulePatch{}, false
	}
	return control.SchedulePatch{Fields: fields}, true
}

// writeScheduleFail maps the manager's refusals onto the wire.
//
// Every case is a distinct status AND a distinct code, because the operator
// action differs: a missing schedule is a path mistake, a revision conflict is a
// lost race to re-read, a field error names what to fix, and the two
// availability answers say which part of the deployment is missing.
func (m *workflowControlModule) writeScheduleFail(w http.ResponseWriter, r *http.Request, err error) {
	var fieldErr *control.ScheduleFieldError
	switch {
	case errors.Is(err, control.ErrScheduleNotFound):
		writeFail(w, r, http.StatusNotFound, "schedule_not_found", "schedule not found")
	case errors.Is(err, store.ErrRevisionConflict):
		writeFail(w, r, http.StatusConflict, "schedule_revision_conflict",
			"the schedule changed since it was read; re-read it and retry with the current revision")
	case errors.As(err, &fieldErr):
		// The message names the field and why it was refused. It carries nothing
		// but the caller's own field name and a fixed explanation -- no internals.
		writeFail(w, r, http.StatusBadRequest, "invalid_schedule_field", fieldErr.Error())
	case errors.Is(err, control.ErrScheduleBackfillRange):
		writeFail(w, r, http.StatusBadRequest, "invalid_backfill_range", err.Error())
	case errors.Is(err, control.ErrScheduleNoEmitter):
		writeFail(w, r, http.StatusNotImplemented, "backfill_unavailable",
			"this server does not host the workflow's triggers, so it cannot fire a backfilled instant")
	default:
		if m.log != nil {
			m.log.Error("schedule_request_failed", "err", err)
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

// writeScheduleView answers with a view, distinguishing "stored and the trigger
// host was told" from "stored but the host was not notified".
//
// The second is 202: the write is durable and the caller's If-Match chain is
// intact, but the effect is pending, and saying 200 would claim an immediacy
// that did not happen. The body is the same view either way, because the state
// the caller asked for IS the state that was stored.
func (m *workflowControlModule) writeScheduleView(w http.ResponseWriter, r *http.Request, view control.ScheduleView, reloadErr error) {
	status := http.StatusOK
	if reloadErr != nil {
		if !errors.Is(reloadErr, control.ErrScheduleReload) {
			m.writeScheduleFail(w, r, reloadErr)
			return
		}
		status = http.StatusAccepted
	}
	writeData(w, r, status, scheduleViewBody(view))
}

// handleListSchedules serves GET /v1/workflows/{id}/schedules: every schedule
// the current version declares, each with its overlay applied, followed by
// overlays whose node is gone.
//
// The list is not paginated, deliberately. It is bounded by the number of
// trigger nodes in ONE workflow — a count the definition itself carries — so
// there is no unbounded scan for a page size to protect, and the shape stays the
// collection envelope every other list endpoint uses.
func (m *workflowControlModule) handleListSchedules(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	ref, ok := m.resolveScheduleWorkflow(w, r)
	if !ok {
		return
	}
	views, err := m.schedules.List(r.Context(), ref)
	if err != nil {
		m.writeScheduleFail(w, r, err)
		return
	}
	items := make([]scheduleViewResponse, 0, len(views))
	for _, v := range views {
		items = append(items, scheduleViewBody(v))
	}
	writeList(w, r, items, len(items))
}

// handleGetSchedule serves GET /v1/workflows/{id}/schedules/{node}: one
// schedule, or the orphaned overlay of a node this version no longer declares.
func (m *workflowControlModule) handleGetSchedule(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	ref, ok := m.resolveScheduleWorkflow(w, r)
	if !ok {
		return
	}
	view, err := m.schedules.Get(r.Context(), ref, nodeParam(r))
	if err != nil {
		m.writeScheduleFail(w, r, err)
		return
	}
	writeData(w, r, http.StatusOK, scheduleViewBody(view))
}

// handlePatchSchedule serves PATCH /v1/workflows/{id}/schedules/{node}: a
// partial update of the overlay, creating it when there is none.
//
// Pause and resume are NOT fields of this patch. They have their own routes and
// their own audit rows, and a schedule's paused state is a state transition an
// operator takes deliberately — not something that should be reachable by
// including a key in a request that was about a cron expression.
func (m *workflowControlModule) handlePatchSchedule(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPatch) {
		return
	}
	ref, ok := m.resolveScheduleWorkflow(w, r)
	if !ok {
		return
	}
	ifMatch, ok := parseIfMatch(w, r)
	if !ok {
		return
	}
	patch, ok := decodeSchedulePatch(w, r)
	if !ok {
		return
	}
	view, err := m.schedules.Patch(r.Context(), ref, nodeParam(r), scheduleActor(r), patch, ifMatch)
	if err != nil && !errors.Is(err, control.ErrScheduleReload) {
		m.writeScheduleFail(w, r, err)
		return
	}
	m.writeScheduleView(w, r, view, err)
}

// handleDeleteScheduleOverride serves DELETE
// /v1/workflows/{id}/schedules/{node}/override: drop the overlay, returning the
// schedule to what its definition says.
//
// It is the only operation that works on an orphaned overlay — a leftover row
// nobody can see is a leftover row nobody can clean up — and it is idempotent in
// the sense that it reports what the state now is; deleting an overlay that was
// never written is a 404, because the caller addressed something that does not
// exist rather than asking for a state that already holds.
func (m *workflowControlModule) handleDeleteScheduleOverride(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodDelete) {
		return
	}
	ref, ok := m.resolveScheduleWorkflow(w, r)
	if !ok {
		return
	}
	ifMatch, ok := parseIfMatch(w, r)
	if !ok {
		return
	}
	view, err := m.schedules.DeleteOverride(r.Context(), ref, nodeParam(r), scheduleActor(r), ifMatch)
	if err != nil && !errors.Is(err, control.ErrScheduleReload) {
		m.writeScheduleFail(w, r, err)
		return
	}
	m.writeScheduleView(w, r, view, err)
}

// handlePauseSchedule serves POST /v1/workflows/{id}/schedules/{node}/pause.
//
// Pausing is idempotent: pausing an already-paused schedule writes nothing and
// bumps no revision, so a retried pause cannot invalidate another operator's
// If-Match. Pausing does NOT take the schedule out of the trigger host's hands —
// the host keeps the subscription and drops each firing, which is what keeps the
// watermark moving and makes resume a continuation rather than a replay.
func (m *workflowControlModule) handlePauseSchedule(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	ref, ok := m.resolveScheduleWorkflow(w, r)
	if !ok {
		return
	}
	ifMatch, ok := parseIfMatch(w, r)
	if !ok {
		return
	}
	view, err := m.schedules.Pause(r.Context(), ref, nodeParam(r), scheduleActor(r), ifMatch)
	if err != nil && !errors.Is(err, control.ErrScheduleReload) {
		m.writeScheduleFail(w, r, err)
		return
	}
	m.writeScheduleView(w, r, view, err)
}

// handleResumeSchedule serves POST /v1/workflows/{id}/schedules/{node}/resume.
// Resuming is not a catch-up: what the pause skipped stays skipped, and asking
// for those instants is what the backfill route is for.
func (m *workflowControlModule) handleResumeSchedule(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	ref, ok := m.resolveScheduleWorkflow(w, r)
	if !ok {
		return
	}
	ifMatch, ok := parseIfMatch(w, r)
	if !ok {
		return
	}
	view, err := m.schedules.Resume(r.Context(), ref, nodeParam(r), scheduleActor(r), ifMatch)
	if err != nil && !errors.Is(err, control.ErrScheduleReload) {
		m.writeScheduleFail(w, r, err)
		return
	}
	m.writeScheduleView(w, r, view, err)
}

// handleBackfillSchedule serves POST
// /v1/workflows/{id}/schedules/{node}/backfill: run the instants of a past
// range, one at a time, reporting each.
//
// Dry run is not a convenience: it is the only way to see what a range holds
// before it runs. It is not an escape from the instant cap — a range over the
// cap is refused outright rather than truncated, and that refusal applies to a
// dry run too, so a plan can never silently cover less than was asked for.
func (m *workflowControlModule) handleBackfillSchedule(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	ref, ok := m.resolveScheduleWorkflow(w, r)
	if !ok {
		return
	}
	var req scheduleBackfillRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	from, ok := parseScheduleInstant(w, r, "from", req.From)
	if !ok {
		return
	}
	to, ok := parseScheduleInstant(w, r, "to", req.To)
	if !ok {
		return
	}
	result, err := m.schedules.Backfill(r.Context(), ref, nodeParam(r), control.ScheduleBackfillRequest{
		From:   from,
		To:     to,
		DryRun: req.DryRun,
	})
	// Every refusal is answered with its own status and code, and NO body: a
	// plan returned beside a 400 would be a plan for a request that was not
	// honoured, and a client that read it as "these ran" would be wrong. The
	// range a refused backfill would have covered is discoverable by dry-running
	// the narrower range the error asks for.
	if err != nil {
		m.writeScheduleFail(w, r, err)
		return
	}
	body := scheduleBackfillResponse{
		From:    result.From.UTC(),
		To:      result.To.UTC(),
		DryRun:  result.DryRun,
		Planned: instantsUTC(result.Planned),
		Emitted: make([]scheduleBackfillEmission, 0, len(result.Emitted)),
		Skipped: make([]scheduleBackfillSkipEntry, 0, len(result.Skipped)),
	}
	for _, e := range result.Emitted {
		body.Emitted = append(body.Emitted, scheduleBackfillEmission{
			Instant:     e.Instant.UTC(),
			ExecutionID: string(e.ExecutionID),
		})
	}
	for _, s := range result.Skipped {
		body.Skipped = append(body.Skipped, scheduleBackfillSkipEntry{
			Instant: s.Instant.UTC(),
			Reason:  s.Reason,
			Detail:  s.Detail,
		})
	}
	writeData(w, r, http.StatusOK, body)
}

// parseScheduleInstant reads one RFC3339 timestamp out of the backfill body,
// naming the field in the refusal so a client with two of them knows which.
func parseScheduleInstant(w http.ResponseWriter, r *http.Request, field, raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		writeFail(w, r, http.StatusBadRequest, "invalid_backfill_range",
			fmt.Sprintf("%s is required and must be an RFC3339 timestamp", field))
		return time.Time{}, false
	}
	instant, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeFail(w, r, http.StatusBadRequest, "invalid_backfill_range",
			fmt.Sprintf("%s must be an RFC3339 timestamp", field))
		return time.Time{}, false
	}
	return instant, true
}

// instantsUTC normalizes a plan to UTC, and never to null: an empty plan is an
// empty list, which is what the caller's .map() needs.
func instantsUTC(instants []time.Time) []time.Time {
	out := make([]time.Time, 0, len(instants))
	for _, i := range instants {
		out = append(out, i.UTC())
	}
	return out
}
