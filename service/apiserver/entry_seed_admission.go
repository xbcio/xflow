package apiserver

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
)

// This file holds the one implementation of "run an entry-seed admission and
// answer the caller", shared by the two server-side callers of that admission:
// POST /v1/executions (the runner-protocol-face route, spec §0.1) and the
// server-hosted webhook route POST|<method> /v1/hooks/{namespace}/{workflow_id}/
// {entry_unit} (spec §6).
//
// They are one admission, so they must answer with one set of statuses and body
// shapes. A webhook handler that mapped the result itself would drift from the
// runner contract the first time either side changed — and the drift would be
// silent, because both callers only look at the status.
//
// The bodies are deliberately BARE (errorResponse / protocol DTOs), not the §3
// envelope: this is the runner-protocol face's load-bearing shape (spec §8.2),
// and the 409/429 discriminations a caller must make are defined on it.

// seedAdmission admits one entry-unit seed through the ControlPlane's Core so
// the server-side namespace resolution and the generation fence (spec §11.6)
// always apply. cp is the production path; eng is the fallback for unit tests
// that build a module over a fake facade, and is only reached when cp is nil.
func seedAdmission(ctx context.Context, cp *control.ControlPlane, eng control.EngineFacade, req engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error) {
	if cp != nil {
		return cp.SeedExecutionFromEntry(ctx, req)
	}
	if eng == nil {
		return engine.SeedExecutionFromEntryResponse{}, errors.New("apiserver: no entry-seed admission available")
	}
	return eng.SeedExecutionFromEntry(ctx, req)
}

// writeSeedAdmissionResult maps one admission result onto the wire.
//
// Every branch is a decision the caller must distinguish:
//   - stale_generation 409: the caller lost its activation; it is fenced, not
//     broken, and must stop rather than retry.
//   - workflow_unknown 404: the seed names a workflow/entry unit the control
//     plane cannot resolve — fail closed, never admit without a topology.
//   - concurrency unavailable 503: the deployment cannot enforce the workflow's
//     declared limit; the answer changes when the deployment does.
//   - conflict 409 with the execution id: another caller already produced this
//     result, and the id is what correlates the two.
//   - refused 429 with Retry-After: nothing was persisted and the caller still
//     owns the entry (its offset, its HTTP request), so it retries after the
//     stated floor rather than treating it as a failure or as an admission.
//
// log may be nil. The handler-level error is logged without the request body,
// which on this face carries workflow input and, for the webhook, a signature
// and a secret-bearing payload.
func writeSeedAdmissionResult(w http.ResponseWriter, log engine.Logger, resp engine.SeedExecutionFromEntryResponse, err error) {
	if err != nil {
		if errors.Is(err, control.ErrStaleGeneration) {
			writeJSON(w, http.StatusConflict, errorResponse{Error: "stale_generation"})
			return
		}
		if errors.Is(err, control.ErrEntrySeedWorkflowUnknown) {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "workflow_unknown"})
			return
		}
		if errors.Is(err, engine.ErrConcurrencyUnsupported) || errors.Is(err, engine.ErrConcurrencyQueueUnsupported) {
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "workflow_concurrency_unavailable"})
			return
		}
		if log != nil {
			log.Error("seed_execution_failed", "err", err)
		}
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "internal server error"})
		return
	}
	if resp.State == engine.AdmissionStateConflict {
		writeJSON(w, http.StatusConflict, protocol.SeedExecutionResponse{
			State:       "conflict",
			ExecutionID: string(resp.ExecutionID),
		})
		return
	}
	if resp.State == engine.AdmissionStateOverLimit || resp.State == engine.AdmissionStateQueueFull {
		w.Header().Set("Retry-After", strconv.Itoa(concurrencyRetryAfterSeconds))
		code := "workflow_concurrency_limit"
		if resp.State == engine.AdmissionStateQueueFull {
			code = "workflow_concurrency_queue_full"
		}
		writeJSON(w, http.StatusTooManyRequests, protocol.SeedExecutionRefusal{
			State: string(resp.State),
			Error: code,
			// The body carries the same floor as the header. The runner-protocol
			// callers already have a cadence and read neither; the field is for
			// the in-process webhook admission, which has no header of its own to
			// copy and would otherwise invent a backoff.
			RetryAfterSeconds: concurrencyRetryAfterSeconds,
		})
		return
	}
	writeJSON(w, http.StatusOK, protocol.SeedExecutionResponse{
		State:         string(resp.State),
		ExecutionID:   string(resp.ExecutionID),
		Duplicate:     resp.Duplicate,
		Queued:        resp.Queued,
		QueuePosition: resp.QueuePosition,
	})
}
