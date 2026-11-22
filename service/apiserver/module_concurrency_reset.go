package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// concurrencyResetter is the optional engine capability behind
// POST /v1/workflows/{name}/concurrency/reset. It is asserted from m.eng at
// request time rather than added to control.EngineFacade: the reset is an
// operator capability a facade without it simply does not have, and widening
// the facade would force every test fake to grow a method none of them use.
//
// The absence of the capability is one operator-facing fact shared with the
// store-level absence (a backend whose state store does not implement
// engine.ConcurrencyOrphanReclaimer): both answer 501 concurrency_reset_unsupported.
type concurrencyResetter interface {
	ResetWorkflowConcurrency(ctx context.Context, ns namespace.Namespace, workflowName string, opts engine.ResetConcurrencyOptions) (engine.OrphanReclaimResult, error)
}

// resetWorkflowConcurrencyRequest is the optional body of the reset route.
//
// The zero value is a DRY RUN. Apply is a positive opt-in so a caller that
// sends "{}" (or no body at all) previews the judgement instead of terminating
// work. IncludeLive widens the judgement to executions that still have a graph;
// it is honored for previews too, so the dry run a console shows first names
// exactly what an apply would terminate.
type resetWorkflowConcurrencyRequest struct {
	Apply       bool `json:"apply"`
	IncludeLive bool `json:"include_live"`
}

// concurrencyResetCandidate is one execution the reset judged. It carries the
// evidence the judgement was made on — status, age and the evidence string —
// because an operator about to terminate live work must be able to see why each
// execution is on the list.
type concurrencyResetCandidate struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
	// Workflow is the persisted workflow name when the SQL mirror can supply
	// it, otherwise the workflow tag the concurrency state is keyed by.
	Workflow string `json:"workflow,omitempty"`
	// Status is empty for an execution whose status key is already gone.
	Status types.ExecutionStatus `json:"status,omitempty"`
	// AgeMS is the execution's age in milliseconds, measured from its status
	// clock or, when that key is gone, from the concurrency member's own
	// admission time.
	AgeMS int64 `json:"age_ms"`
	// Evidence is engine.OrphanEvidenceGraphMissing or
	// engine.OrphanEvidenceExecutionMissing.
	Evidence string `json:"evidence"`
}

// concurrencyResetResponse reports one reset pass. Inspected counts everything
// the pass examined; Candidates lists what would be (dry run) or was reclaimed;
// Reclaimed is always 0 for a dry run.
type concurrencyResetResponse struct {
	Applied     bool                        `json:"applied"`
	IncludeLive bool                        `json:"include_live"`
	Inspected   int                         `json:"inspected"`
	Reclaimed   int                         `json:"reclaimed"`
	Candidates  []concurrencyResetCandidate `json:"candidates"`
}

// handleResetWorkflowConcurrency serves
// POST /v1/workflows/{name}/concurrency/reset.
//
// The namespace comes from the authenticated principal's context — never the
// path, query or body — and the workflow name comes from the path. The
// concurrency key is namespace plus name, so a caller can only ever reach its
// own namespace's slot holders: a cross-namespace reset inspects nothing and
// answers an empty candidate list rather than another tenant's work.
//
// The judgement itself lives in the engine (engine.ResetWorkflowConcurrency):
// the default reclaims only executions the engine can no longer route, and only
// Apply + IncludeLive converges executions whose graph still exists. Every such
// convergence is recorded with reason operator_reset in the structured log and
// the xflow_orphan_reclaimed_total metric; this handler adds the request-level
// audit line naming the caller, the scope, and what the pass did.
func (m *workflowControlModule) handleResetWorkflowConcurrency(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.PathValue("name"))

	var req resetWorkflowConcurrencyRequest
	if !decodeOptionalJSON(w, r, &req) {
		return
	}

	resetter, ok := m.eng.(concurrencyResetter)
	if !ok {
		writeFail(w, r, http.StatusNotImplemented, "concurrency_reset_unsupported",
			"concurrency reset is not supported by this deployment")
		return
	}

	ns := namespace.FromContext(r.Context())
	res, err := resetter.ResetWorkflowConcurrency(r.Context(), ns, name, engine.ResetConcurrencyOptions{
		Apply:       req.Apply,
		IncludeLive: req.IncludeLive,
	})
	if err != nil {
		writeConcurrencyResetFail(w, r, err)
		return
	}

	if m.log != nil {
		// The request-level audit line. reason is the same operator_reset value
		// each converged execution's own log and metric carry, so a tenant's
		// whole reset — the request and the executions it ended — can be
		// correlated by one reason without reading the response body.
		m.log.Info("workflow_concurrency_reset",
			"namespace", string(ns),
			"workflow", name,
			"apply", req.Apply,
			"include_live", req.IncludeLive,
			"inspected", res.Inspected,
			"reclaimed", res.Reclaimed,
			"reason", engine.OrphanTriggerReset,
		)
	}

	resp := concurrencyResetResponse{
		Applied:     req.Apply,
		IncludeLive: req.IncludeLive,
		Inspected:   res.Inspected,
		Reclaimed:   res.Reclaimed,
		Candidates:  make([]concurrencyResetCandidate, 0, len(res.Candidates)),
	}
	for _, c := range res.Candidates {
		resp.Candidates = append(resp.Candidates, concurrencyResetCandidate{
			ExecutionID: c.ExecutionID,
			Workflow:    c.Workflow,
			Status:      c.Status,
			AgeMS:       c.Age.Milliseconds(),
			Evidence:    c.Evidence,
		})
	}
	writeData(w, r, http.StatusOK, resp)
}

// writeConcurrencyResetFail maps reset refusals onto stable failure shapes.
//
// A name that cannot address a workflow is a 400 (the caller's input, not the
// server's state). A deployment whose store cannot reclaim is a 501 — the
// operator-facing fact is "this deployment has no reset", never "nothing to
// reset". Everything else is a generic 500: the message never echoes the
// store's error, so an internal detail cannot leak through the operator
// surface.
func writeConcurrencyResetFail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, engine.ErrInvalidWorkflowName):
		writeFail(w, r, http.StatusBadRequest, "invalid_workflow_name",
			"workflow name must be between 1 and 128 characters")
	case errors.Is(err, engine.ErrConcurrencyOrphanReclaimUnsupported):
		writeFail(w, r, http.StatusNotImplemented, "concurrency_reset_unsupported",
			"concurrency reset is not supported by this deployment")
	case errors.Is(err, namespace.ErrNameEmpty), errors.Is(err, namespace.ErrNameTooLong), errors.Is(err, namespace.ErrNameChars):
		// The namespace is server-issued, so this is only reachable through a
		// misconfigured authenticator. It is still the caller's request on the
		// wire, but the detail is not theirs to see: answer 400 generically.
		writeFail(w, r, http.StatusBadRequest, "invalid_namespace", "invalid namespace")
	default:
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

// workflowNameResolver builds the resource resolver for a workflow route keyed
// by NAME rather than registry id: it reads the {name} the mux matched and
// returns the workflow-scoped audit resource string. ResourceNamespace is left
// empty — the authoritative IDOR defense is the namespace-scoped concurrency
// key the handler builds from the principal's context, so a cross-namespace
// name inspects nothing.
func workflowNameResolver(suffix string) func(*http.Request) (string, string, string, string) {
	return func(r *http.Request) (string, string, string, string) {
		resource := "workflow/" + r.PathValue("name")
		if suffix != "" {
			resource += "/" + suffix
		}
		return resource, "", "", ""
	}
}

// decodeOptionalJSON decodes an optional request body: an absent or empty body
// leaves dst at its zero value and succeeds, because the reset route's default
// is a dry run and a preview POST may carry no body at all. Malformed and
// over-limit bodies are refused with decodeJSON's exact shapes (400 bad_request
// / 413 request_too_large) so the route cannot diverge from the rest of the
// user face.
func decodeOptionalJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Body == nil {
		return true
	}
	if r.ContentLength == 0 {
		return true
	}
	err := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxRequestBodyBytes)).Decode(dst)
	if err == nil || errors.Is(err, io.EOF) {
		_ = r.Body.Close()
		return true
	}
	_ = r.Body.Close()
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeFail(w, r, http.StatusRequestEntityTooLarge, "request_too_large", "request body too large")
		return false
	}
	writeFail(w, r, http.StatusBadRequest, "bad_request", "invalid JSON")
	return false
}
