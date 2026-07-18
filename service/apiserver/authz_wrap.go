package apiserver

import (
	"context"
	"net/http"
	"time"
)

// authzHolder carries the B3 resource/operation authz dependencies shared by
// modules that mount privileged routes (workflow-control, management). Embed
// it in a module to gain the authzWrap / auditDeny / auditReconcile methods.
// When principalAuth is nil the module must fall back to a legacy auth path
// and not call authzWrap.
type authzHolder struct {
	principalAuth PrincipalAuthenticator
	authorizer    Authorizer
	audit         AuditSink
}

// authzWrap returns a handler that enforces authenticate → authorize → audit
// admission → handler → audit reconcile outcome. Extracted so any module that
// embeds authzHolder can wrap its routes, and tests can exercise the wrapper
// against stub handlers without mounting real routes.
func (h *authzHolder) authzWrap(op string, isMutation bool, fn http.HandlerFunc, resourceResolver func(*http.Request) (resource, workflowID, executionID string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal, err := h.principalAuth.Authenticate(r)
		if err != nil {
			h.auditDeny(r, principal, op, "", "", "", "unauthenticated")
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		resource, wfID, execID := "", "", ""
		if resourceResolver != nil {
			resource, wfID, execID = resourceResolver(r)
		}
		decision, derr := h.authorizer.Authorize(r.Context(), AuthorizationRequest{
			Principal: principal, Operation: op, Resource: resource,
			WorkflowID: wfID, ExecutionID: execID,
		})
		if derr != nil || decision != DecisionAllow {
			reason := "denied"
			if derr != nil {
				reason = "error"
			}
			h.auditDeny(r, principal, op, resource, wfID, execID, reason)
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		// Mutation fail-closed: persist the admission audit BEFORE the
		// operation. If the audit sink is unavailable, deny rather than
		// execute an unaudited mutation.
		reqID := newRequestID(r)
		if isMutation {
			if err := h.audit.Append(r.Context(), AuditEvent{
				RequestID: reqID, Principal: principal.Subject, TenantID: principal.TenantID,
				Operation: op, Resource: resource, WorkflowID: wfID, ExecutionID: execID,
				Decision: DecisionAllow, Reason: "admitted", Outcome: "admitted",
				Timestamp: time.Now().UTC(),
			}); err != nil {
				h.auditDeny(r, principal, op, resource, wfID, execID, "audit_unavailable")
				writeError(w, http.StatusServiceUnavailable, "audit unavailable")
				return
			}
		} else {
			// Read paths record the decision without fail-closed; an audit
			// gap is observable but does not block reads.
			_ = h.audit.Append(r.Context(), AuditEvent{
				RequestID: reqID, Principal: principal.Subject, TenantID: principal.TenantID,
				Operation: op, Resource: resource, WorkflowID: wfID, ExecutionID: execID,
				Decision: DecisionAllow, Reason: "admitted", Outcome: "admitted",
				Timestamp: time.Now().UTC(),
			})
		}
		r = r.WithContext(context.WithValue(r.Context(), authzContextKey{}, principal))
		if isMutation {
			// Wrap the response writer so we can observe the handler's status
			// code and append a reconcile outcome once it settles. A 2xx flips
			// admitted→reconciled (the authoritative effect landed); a non-2xx
			// records failed. The reconcile append is best-effort: a gap here
			// is observable in audit but must not fail the already-admitted
			// mutation.
			rw := &statusRecorder{ResponseWriter: w}
			defer h.auditReconcile(r, principal, op, resource, wfID, execID, reqID, rw)
			fn(rw, r)
			return
		}
		fn(w, r)
	}
}

// wrapForTest exposes the authz wrapper for tests with a stub handler.
func (h *authzHolder) wrapForTest(op string, isMutation bool, fn http.HandlerFunc, resolver func(*http.Request) (string, string, string)) http.HandlerFunc {
	return h.authzWrap(op, isMutation, fn, resolver)
}

// auditDeny records a deny decision before returning the rejection. Deny
// audits are written best-effort (a sink gap must not mask the denial).
func (h *authzHolder) auditDeny(r *http.Request, principal Principal, op, resource, wfID, execID, reason string) {
	if h.audit == nil {
		return
	}
	_ = h.audit.Append(r.Context(), AuditEvent{
		RequestID:   newRequestID(r),
		Principal:   principal.Subject,
		TenantID:    principal.TenantID,
		Operation:   op,
		Resource:    resource,
		WorkflowID:  wfID,
		ExecutionID: execID,
		Decision:    DecisionDeny,
		Reason:      reason,
		Outcome:     "denied",
		Timestamp:   time.Now().UTC(),
	})
}

// auditReconcile appends the post-handler outcome for a mutation: reconciled
// on a 2xx (the authoritative effect landed), or records failed on non-2xx.
// Best-effort — an audit gap must not fail an already-admitted mutation. The
// reconcile record reuses the admission RequestID so the two rows can be
// joined during audit reconciliation.
func (h *authzHolder) auditReconcile(r *http.Request, principal Principal, op, resource, wfID, execID, reqID string, rw *statusRecorder) {
	if h.audit == nil {
		return
	}
	outcome := "reconciled"
	reason := "ok"
	if !rw.succeeded() {
		outcome = "failed"
		reason = "handler_" + http.StatusText(rw.status)
	}
	_ = h.audit.Append(r.Context(), AuditEvent{
		RequestID:   reqID,
		Principal:   principal.Subject,
		TenantID:    principal.TenantID,
		Operation:   op,
		Resource:    resource,
		WorkflowID:  wfID,
		ExecutionID: execID,
		Decision:    DecisionAllow,
		Reason:      reason,
		Outcome:     outcome,
		Timestamp:   time.Now().UTC(),
	})
}

// statusRecorder wraps http.ResponseWriter to capture the status code of the
// handler's response so the authz wrapper can append a reconcile outcome. It
// delegates every WriteHeader/Write to the underlying writer.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if !s.wroteHeader {
		s.status = http.StatusOK
		s.wroteHeader = true
	}
	return s.ResponseWriter.Write(b)
}

// succeeded reports whether the handler returned a 2xx response.
func (s *statusRecorder) succeeded() bool {
	return s.status >= 200 && s.status < 300
}
