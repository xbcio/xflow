package apiserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// These tests pin the per-request audit exemption for OpExecutionSeed (see
// auditedPerRequest in authz_wrap.go for the measurement behind it). The
// exemption is narrow by construction, so the negative cases carry the weight:
// what must NOT change is that authentication still rejects, authorization still
// rejects, a denial is still recorded, and every other operation still writes
// both of its rows.

// seedAuthzModule builds a module whose authorizer and audit sink are the ones
// under test. The principal carries the "execution" scope OpExecutionSeed maps
// to, so an allowed seed is the baseline rather than a denial.
func seedAuthzModule(t *testing.T, audit AuditSink) *workflowControlModule {
	t.Helper()
	return authzModule(t,
		staticPrincipalAuth{principal: Principal{Subject: "runner-1", Scopes: []string{"execution"}}},
		ScopeAuthorizer{}, audit)
}

// runWrapped drives one wrapped request and reports the status plus whether the
// handler ran, so a test can tell "allowed and dispatched" from "allowed and
// silently dropped".
func runWrapped(t *testing.T, m *workflowControlModule, op string, isMutation bool, method string, audit AuditSink) (status int, handlerRan bool, events []AuditEvent) {
	t.Helper()
	ran := false
	h := m.wrapForTest(op, isMutation, func(w http.ResponseWriter, _ *http.Request) {
		ran = true
		w.WriteHeader(http.StatusOK)
	}, nil)

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(method, "/v1/executions", nil))

	if sink, ok := audit.(*InMemoryAuditSink); ok {
		events = sink.Events()
	}
	return rec.Code, ran, events
}

// TestSeedExemptFromPerRequestAudit is the positive case: the seed mutation is
// authorized and dispatched normally, and writes no ledger rows.
func TestSeedExemptFromPerRequestAudit(t *testing.T) {
	sink := NewInMemoryAuditSink()
	m := seedAuthzModule(t, sink)

	status, ran, events := runWrapped(t, m, OpExecutionSeed, true, http.MethodPost, sink)

	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200: the exemption must not break dispatch", status)
	}
	if !ran {
		t.Fatal("handler did not run, so the exemption changed behaviour rather than " +
			"only what is recorded")
	}
	if len(events) != 0 {
		t.Errorf("seed wrote %d audit row(s), want 0; the whole point of the "+
			"exemption is that a per-batch data-plane operation stops writing two "+
			"synchronous rows — rows: %+v", len(events), events)
	}
}

// TestSeedDenialIsStillAudited is the most important negative case. The
// exemption covers the admitted path ONLY: a refusal is exactly the evidence an
// audit ledger exists to keep, and hiding it would be a security regression
// dressed up as a throughput fix.
func TestSeedDenialIsStillAudited(t *testing.T) {
	sink := NewInMemoryAuditSink()
	// No "execution" scope → the authorizer denies.
	m := authzModule(t,
		staticPrincipalAuth{principal: Principal{Subject: "nobody", Scopes: []string{"workflow"}}},
		ScopeAuthorizer{}, sink)

	status, ran, events := runWrapped(t, m, OpExecutionSeed, true, http.MethodPost, sink)

	if status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: the exemption must not weaken authorization", status)
	}
	if ran {
		t.Fatal("handler ran despite a denied authorization")
	}
	if len(events) != 1 {
		t.Fatalf("denied seed wrote %d audit row(s), want exactly 1; a refusal must "+
			"remain recorded", len(events))
	}
	if events[0].Decision != DecisionDeny {
		t.Errorf("recorded decision = %q, want %q", events[0].Decision, DecisionDeny)
	}
}

// TestSeedUnauthenticatedIsStillAudited covers the other refusal path, which is
// evaluated before authorization and so must be equally unaffected.
func TestSeedUnauthenticatedIsStillAudited(t *testing.T) {
	sink := NewInMemoryAuditSink()
	m := authzModule(t,
		staticPrincipalAuth{err: ErrWorkflowUnauthenticated},
		ScopeAuthorizer{}, sink)

	status, ran, events := runWrapped(t, m, OpExecutionSeed, true, http.MethodPost, sink)

	if status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", status)
	}
	if ran {
		t.Fatal("handler ran for an unauthenticated request")
	}
	if len(events) != 1 {
		t.Fatalf("unauthenticated seed wrote %d audit row(s), want exactly 1", len(events))
	}
}

// TestOtherExecutionMutationsKeepBothRows is the guard against over-exempting.
// The exemption is keyed on the operation, and the sibling execution mutations
// are operator-initiated and bounded by human action — they keep the fail-closed
// admission row and the outcome row.
func TestOtherExecutionMutationsKeepBothRows(t *testing.T) {
	for _, op := range []string{OpExecutionSignal, OpExecutionRevoke, OpExecutionCancel} {
		t.Run(op, func(t *testing.T) {
			sink := NewInMemoryAuditSink()
			m := seedAuthzModule(t, sink)

			status, ran, events := runWrapped(t, m, op, true, http.MethodPost, sink)

			if status != http.StatusOK || !ran {
				t.Fatalf("status = %d ran = %v, want 200/true", status, ran)
			}
			if len(events) != 2 {
				t.Fatalf("%s wrote %d audit row(s), want 2 (admission + outcome); the "+
					"exemption must apply to execution.seed alone", op, len(events))
			}
			phases := map[string]bool{}
			for _, ev := range events {
				phases[ev.Phase] = true
			}
			if !phases["admission"] || !phases["outcome"] {
				t.Errorf("%s phases = %v, want both admission and outcome", op, phases)
			}
		})
	}
}

// TestSeedSurvivesAnUnavailableAuditSink pins the second-order effect of the
// exemption: the seed path can no longer be denied because the ledger is down.
// That is deliberate — the ledger is not a dependency of the data plane any more
// — and it is worth pinning because the previous behaviour (503 on a dead sink)
// is what turned a slow ledger into a stopped pipeline.
func TestSeedSurvivesAnUnavailableAuditSink(t *testing.T) {
	m := authzModule(t,
		staticPrincipalAuth{principal: Principal{Subject: "runner-1", Scopes: []string{"execution"}}},
		ScopeAuthorizer{}, failingAuditSink{})

	status, ran, _ := runWrapped(t, m, OpExecutionSeed, true, http.MethodPost, nil)

	if status != http.StatusOK || !ran {
		t.Fatalf("status = %d ran = %v, want 200/true: an exempted operation must not "+
			"fail closed on an audit sink it no longer uses", status, ran)
	}
}

// TestAuditedPerRequest covers the predicate directly, so the exemption's scope
// is readable without going through an HTTP round trip.
func TestAuditedPerRequest(t *testing.T) {
	if auditedPerRequest(OpExecutionSeed) {
		t.Error("OpExecutionSeed is audited per request; the measured ledger volume " +
			"makes that the bottleneck this exemption exists to remove")
	}
	for _, op := range []string{
		OpExecutionRead, OpExecutionSignal, OpExecutionRevoke, OpExecutionCancel,
		OpDeadLetterList, OpDeadLetterReplay, OpManagementRead, OpWorkflowRegister,
	} {
		if !auditedPerRequest(op) {
			t.Errorf("%s is exempt from per-request audit but is not the seed data "+
				"plane; the exemption must be the narrowest thing that fixes the "+
				"measured problem", op)
		}
	}
}
