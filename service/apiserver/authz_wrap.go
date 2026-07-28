package apiserver

import (
	"context"
	"net/http"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/types"
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
//
// Namespace boundary (Task 7.3): the verified principal's Namespace is injected
// into the request context here (namespace.WithNamespace) so every downstream store
// read is scoped to the principal's namespace — the authoritative IDOR defense.
// The client request body is never consulted for namespace.
func (h *authzHolder) authzWrap(op string, isMutation bool, fn http.HandlerFunc, resourceResolver func(*http.Request) (resource, workflowID, executionID, resourceNamespace string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal, err := h.principalAuth.Authenticate(r)
		if err != nil {
			h.auditDeny(r, principal, op, "", "", "", "unauthenticated")
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		resource, wfID, execID, resNamespace := "", "", "", ""
		if resourceResolver != nil {
			resource, wfID, execID, resNamespace = resourceResolver(r)
		}
		decision, derr := h.authorizer.Authorize(r.Context(), AuthorizationRequest{
			Principal: principal, Operation: op, Resource: resource,
			WorkflowID: wfID, ExecutionID: execID, ResourceNamespace: resNamespace,
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
		// Namespace boundary (Task 7.3/7.4): inject the verified principal's namespace
		// into the request context before audit and handler execution so every
		// downstream read and the audit sink draw namespace from the same source:
		// namespace.FromContext(ctx).
		r = r.WithContext(namespace.WithNamespace(context.WithValue(r.Context(), authzContextKey{}, principal), namespace.Namespace(principal.Namespace)))

		// R3.1: when the resolver carried an execution id (pre-allocated for
		// workflow create/invoke, or the path param for execution-scoped
		// mutations), propagate it into the submission context so engine
		// Submit/Invoke persist the SAME id the admission audit row already
		// recorded. This closes the audit↔execution correlation gap that left
		// reconcile Probe reading an empty ExecutionID. signal/revoke/cancel
		// do not call Submit/Invoke, so injecting their path-param id is inert.
		if execID != "" {
			r = r.WithContext(engine.WithExecutionID(r.Context(), types.ExecutionID(execID)))
		}

		// Mutation fail-closed: persist the admission audit BEFORE the
		// operation. If the audit sink is unavailable, deny rather than
		// execute an unaudited mutation.
		reqID := newRequestID(r)
		traceID := tracing.TraceIDFromContext(r.Context())
		if isMutation {
			if err := h.audit.Append(r.Context(), AuditEvent{
				RequestID: reqID, Principal: principal.Subject, Namespace: principal.Namespace,
				Operation: op, Resource: resource, WorkflowID: wfID, ExecutionID: execID,
				Decision: DecisionAllow, Reason: "admitted", Outcome: "admitted",
				Phase:     "admission",
				TraceID:   traceID,
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
				RequestID: reqID, Principal: principal.Subject, Namespace: principal.Namespace,
				Operation: op, Resource: resource, WorkflowID: wfID, ExecutionID: execID,
				Decision: DecisionAllow, Reason: "admitted", Outcome: "admitted",
				Phase:     "admission",
				TraceID:   traceID,
				Timestamp: time.Now().UTC(),
			})
		}
		if isMutation {
			// Wrap the response writer so we can observe the handler's status
			// code and append a reconcile outcome inline after the handler
			// returns. A 2xx flips admitted→reconciled (the authoritative effect
			// landed); a non-2xx records failed. The reconcile append is
			// best-effort: a gap here is observable in audit but must not fail
			// the already-admitted mutation. Crash-safety for a panic between
			// the mutation success and this append is explicitly T9's scope.
			rw := &statusRecorder{ResponseWriter: w}
			fn(rw, r)
			h.auditReconcile(r, principal, op, resource, wfID, execID, reqID, rw)
			return
		}
		fn(w, r)
	}
}

// wrapForTest exposes the authz wrapper for tests with a stub handler.
func (h *authzHolder) wrapForTest(op string, isMutation bool, fn http.HandlerFunc, resolver func(*http.Request) (string, string, string, string)) http.HandlerFunc {
	return h.authzWrap(op, isMutation, fn, resolver)
}

// resolvedRoute is the per-request authz decision resolved from the request path
// and method BEFORE the authz wrapper runs. It lets a single mounted path
// (e.g. /v1/executions/) serve multiple operations with different mutation
// semantics: inspect/wait → execution.read (non-mutation), signal →
// execution.signal (mutation), revoke-signal → execution.revoke (mutation),
// cancel → execution.cancel (mutation). ok=false means the verb is unknown → the
// wrapper denies (default-deny) and returns 404 without invoking the handler.
type resolvedRoute struct {
	operation         string
	resource          string
	workflowID        string
	executionID       string
	resourceNamespace string
	isMutation        bool
}

// authzWrapResolved is like authzWrap but resolves the operation, mutation
// flag, and resource ids from the request inside the wrapper (per-verb). Used by
// the /v1/executions/ subtree whose sub-paths map to distinct operations. An
// unknown verb (ok=false) is denied and answered 404 "route not found" — no
// existence leak, no authz audit row for an operation that does not exist
// (unknown op default-deny). It delegates to authzWrap with the resolved op so
// the admission+outcome audit + fail-closed path is identical to static-op
// routes.
func (h *authzHolder) authzWrapResolved(fn http.HandlerFunc, resolver func(*http.Request) (resolvedRoute, bool)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rt, ok := resolver(r)
		if !ok {
			// Unknown verb: default-deny. 404 (not 403) so a probe learns nothing
			// about which verbs exist; the authz audit sink records nothing for a
			// non-existent operation (there is no principal yet for an unmounted
			// route, and an unknown op would be denied anyway).
			writeError(w, http.StatusNotFound, "route not found")
			return
		}
		h.authzWrap(rt.operation, rt.isMutation, fn, func(*http.Request) (string, string, string, string) {
			return rt.resource, rt.workflowID, rt.executionID, rt.resourceNamespace
		})(w, r)
	}
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
		Namespace:   principal.Namespace,
		Operation:   op,
		Resource:    resource,
		WorkflowID:  wfID,
		ExecutionID: execID,
		Decision:    DecisionDeny,
		Reason:      reason,
		Outcome:     "denied",
		Phase:       "admission",
		TraceID:     tracing.TraceIDFromContext(r.Context()),
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
		Namespace:   principal.Namespace,
		Operation:   op,
		Resource:    resource,
		WorkflowID:  wfID,
		ExecutionID: execID,
		Decision:    DecisionAllow,
		Reason:      reason,
		Outcome:     outcome,
		Phase:       "outcome",
		TraceID:     tracing.TraceIDFromContext(r.Context()),
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
