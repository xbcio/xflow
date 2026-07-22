package control

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// tenantAssignment builds an assignment whose authoritative TenantID is the
// given value (set server-side at submit time, never trusted from a client).
func tenantAssignment(tenantID tenant.TenantID, execID, nodeName string) Assignment {
	task := engine.Task{
		ExecutionID: types.ExecutionID(execID),
		NodeName:    nodeName,
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	return Assignment{
		AssignmentID: BuildAssignmentID(&task),
		Task:         task,
		Routing:      engine.TaskRouting{NodeType: "xflow.function"},
		TenantID:     tenantID,
	}
}

// finalizeTenantLease registers a runner scoped to tenantID, enqueues+claims a
// tenant-scoped assignment, and finalizes a lease carrying the authoritative
// TenantID. It returns the lease and the live runner session so a test can echo
// (or tamper with) the lease JSON in a ReportResult RPC.
func finalizeTenantLease(t *testing.T, dir *MemoryRunnerDirectory, tenantID tenant.TenantID) (*engine.TaskLease, RunnerSession) {
	t.Helper()
	ctx := context.Background()
	session := mustRegisterHTTPRunnerForTenant(t, dir, tenantID)
	assignment := tenantAssignment(tenantID, "exec-tenant", "node-a")
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)
	lease := &engine.TaskLease{
		LeaseID:    "lease-tenant",
		LeaseToken: "token-tenant",
		Task:       claim.Assignment.Task,
		NodeType:   claim.Assignment.Routing.NodeType,
		TenantID:   tenantID,
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	return lease, session
}

// mustRegisterHTTPRunnerForTenant registers a runner scoped to a single tenant
// (the default mustRegisterHTTPRunner only serves the default tenant).
func mustRegisterHTTPRunnerForTenant(t *testing.T, dir *MemoryRunnerDirectory, tenantID tenant.TenantID) RunnerSession {
	t.Helper()
	session, err := dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     4,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
		Tenants:      []tenant.TenantID{tenantID},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	return session
}

// mustRegisterHTTPRunnerForTenantNamed registers a runner with an explicit
// runnerID scoped to a single tenant, so a test can stage multiple distinct
// runner sessions in one directory (e.g. the cross-runner lease-swap probe).
func mustRegisterHTTPRunnerForTenantNamed(t *testing.T, dir *MemoryRunnerDirectory, runnerID string, tenantID tenant.TenantID) RunnerSession {
	t.Helper()
	session, err := dir.Register(context.Background(), RegisterRunnerRequest{
		RunnerID:     runnerID,
		Capacity:     4,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
		Tenants:      []tenant.TenantID{tenantID},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	return session
}

// finalizeTenantLeaseFor is the named-runner variant of finalizeTenantLease:
// register runnerID under tenantID, enqueue+claim a tenant-scoped assignment,
// and finalize a lease carrying the authoritative TenantID.
func finalizeTenantLeaseFor(t *testing.T, dir *MemoryRunnerDirectory, runnerID string, tenantID tenant.TenantID) (*engine.TaskLease, RunnerSession) {
	t.Helper()
	ctx := context.Background()
	session := mustRegisterHTTPRunnerForTenantNamed(t, dir, runnerID, tenantID)
	assignment := tenantAssignment(tenantID, "exec-"+string(tenantID), "node-"+string(tenantID))
	mustEnqueueAssignment(t, ctx, dir, assignment)
	claim := mustClaimAssignment(t, ctx, dir, session)
	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("lease-" + string(tenantID)),
		LeaseToken: engine.LeaseToken("token-" + string(tenantID)),
		Task:       claim.Assignment.Task,
		NodeType:   claim.Assignment.Routing.NodeType,
		TenantID:   tenantID,
	}
	if err := dir.FinalizeClaim(ctx, claim.ClaimID, lease); err != nil {
		t.Fatalf("FinalizeClaim() error = %v", err)
	}
	return lease, session
}

// TestReportResultRejectsCrossRunnerLeaseSwap is the 2026-07-21 P0 probe for B1:
// runner-a/session-a reports runner-b's fully-valid tenant-b lease. The
// directory's LookupLease fences by (runner, session) and returns ok=false
// (runner-a never finalized tenant-b's lease). reportResult MUST reject fail
// closed — not fall back to req.Lease and commit cross-tenant.
func TestReportResultRejectsCrossRunnerLeaseSwap(t *testing.T) {
	fake := &fakeControlEngine{}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	// runner-b/tenant-b finalizes a valid tenant-b lease.
	leaseB, _ := finalizeTenantLeaseFor(t, dir, "runner-b", tenant.TenantID("tenant-b"))

	// runner-a/tenant-a is a separate live session.
	sessionA := mustRegisterHTTPRunnerForTenantNamed(t, dir, "runner-a", tenant.TenantID("tenant-a"))

	// runner-a reports runner-b's lease verbatim. req.Lease is a complete,
	// valid tenant-b lease token — the only thing wrong is the reporter.
	var resp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID:  sessionA.RunnerID,
		SessionID: sessionA.SessionID,
		Lease:     leaseB,
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, http.StatusConflict, &resp)
	if resp.Accepted {
		t.Fatal("cross-runner report must not be accepted")
	}
	if fake.committedLease != nil {
		t.Fatalf("commit must not run for a cross-runner lease swap, got %+v", fake.committedLease)
	}
	if fake.committedTenant != "" {
		t.Fatalf("commit tenant must be empty, got %q", fake.committedTenant)
	}
}

// TestReportResultTenantTampering proves the report path uses the
// server-authoritative lease TenantID, not the runner-echoed lease JSON. A
// runner registered for tenant-a echoes back the lease with TenantID rewritten
// to tenant-b; the commit must still receive the tenant-a namespace context.
// Regression for the 2026-07-21 trust-boundary probe.
func TestReportResultTenantTampering(t *testing.T) {
	fake := &fakeControlEngine{}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	lease, session := finalizeTenantLease(t, dir, tenant.TenantID("tenant-a"))
	// Tamper: the runner echoes back tenant-b instead of the authoritative
	// tenant-a. Without LookupLease this would redirect the commit into the
	// tenant-b Redis namespace.
	tampered := *lease
	tampered.TenantID = tenant.TenantID("tenant-b")

	var resp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     &tampered,
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, http.StatusOK, &resp)
	if !resp.Accepted {
		t.Fatalf("report not accepted: %+v", resp)
	}
	// The authoritative tenant (tenant-a) must win over the tampered echo.
	if fake.committedTenant != tenant.TenantID("tenant-a") {
		t.Fatalf("commit tenant = %q, want tenant-a (authoritative), not the tampered tenant-b", fake.committedTenant)
	}
}

// TestReportResultTenantFieldMissing proves that an old runner that drops the
// TenantID field from the echoed lease still commits under the authoritative
// namespace (server-side completion), so cross-namespace commits never silently
// fall back to default.
func TestReportResultTenantFieldMissing(t *testing.T) {
	fake := &fakeControlEngine{}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	lease, session := finalizeTenantLease(t, dir, tenant.TenantID("tenant-a"))
	echoed := *lease
	echoed.TenantID = "" // old runner omits the field

	var resp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     &echoed,
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, http.StatusOK, &resp)
	if !resp.Accepted {
		t.Fatalf("report not accepted: %+v", resp)
	}
	if fake.committedTenant != tenant.TenantID("tenant-a") {
		t.Fatalf("commit tenant = %q, want tenant-a (authoritative, server-completed)", fake.committedTenant)
	}
}

// TestReportResultImmutableFieldMismatch proves the report path rejects a lease
// whose immutable identity (ExecutionID) was rewritten by the runner. The
// server-authoritative lease is looked up by token, so a mismatch on
// ExecutionID/NodeName/NodeIdx/Attempt/LeaseID/LeaseToken is a fencing
// violation (HTTP 409, ErrInvalidLeaseToken), not a silent commit under the
// wrong execution.
func TestReportResultImmutableFieldMismatch(t *testing.T) {
	fake := &fakeControlEngine{}
	dir := NewMemoryRunnerDirectory()
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	lease, session := finalizeTenantLease(t, dir, tenant.TenantID("tenant-a"))
	tampered := *lease
	tampered.Task.ExecutionID = "exec-different" // rewrite immutable identity

	var resp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     &tampered,
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, http.StatusConflict, &resp)
	if resp.Accepted {
		t.Fatal("report with rewritten ExecutionID must not be accepted")
	}
	if fake.committedLease != nil {
		t.Fatalf("commit must not run for an immutable-mismatch lease, got %+v", fake.committedLease)
	}
}

// lookuplessDirectory wraps a MemoryRunnerDirectory via a NAMED (non-embedded)
// field so Go method promotion does NOT surface LookupLease. It forwards the
// RunnerDirectory methods but deliberately does not implement LeaseLookup,
// simulating a production directory that cannot resolve authoritative leases.
// reportResult must reject (fail closed) rather than fall back to req.Lease.
type lookuplessDirectory struct {
	inner *MemoryRunnerDirectory
}

func (d *lookuplessDirectory) Register(ctx context.Context, req RegisterRunnerRequest) (RunnerSession, error) {
	return d.inner.Register(ctx, req)
}
func (d *lookuplessDirectory) ValidateSession(ctx context.Context, runnerID, sessionID string) error {
	return d.inner.ValidateSession(ctx, runnerID, sessionID)
}
func (d *lookuplessDirectory) Heartbeat(ctx context.Context, req HeartbeatRequest) error {
	return d.inner.Heartbeat(ctx, req)
}
func (d *lookuplessDirectory) EnqueueAssignment(ctx context.Context, assignment Assignment) (bool, error) {
	return d.inner.EnqueueAssignment(ctx, assignment)
}
func (d *lookuplessDirectory) ClaimForRunner(ctx context.Context, req ClaimRequest) (Claim, bool, error) {
	return d.inner.ClaimForRunner(ctx, req)
}
func (d *lookuplessDirectory) FinalizeClaim(ctx context.Context, claimID ClaimID, lease *engine.TaskLease) error {
	return d.inner.FinalizeClaim(ctx, claimID, lease)
}
func (d *lookuplessDirectory) ReleaseClaim(ctx context.Context, claimID ClaimID, reason ReleaseClaimReason) error {
	return d.inner.ReleaseClaim(ctx, claimID, reason)
}
func (d *lookuplessDirectory) ReleaseLeased(ctx context.Context, req ReleaseLeasedRequest) error {
	return d.inner.ReleaseLeased(ctx, req)
}
func (d *lookuplessDirectory) ClearAssignment(ctx context.Context, assignmentID AssignmentID) error {
	return d.inner.ClearAssignment(ctx, assignmentID)
}
func (d *lookuplessDirectory) Runner(ctx context.Context, runnerID string) (RunnerSnapshot, bool) {
	return d.inner.Runner(ctx, runnerID)
}
func (d *lookuplessDirectory) ReleaseExpiredLease(ctx context.Context, req ExpiredDirectoryLeaseRequest) (ExpiredDirectoryLeaseOutcome, error) {
	return d.inner.ReleaseExpiredLease(ctx, req)
}

// Compile-time guard: lookuplessDirectory satisfies RunnerDirectory but does
// NOT satisfy LeaseLookup (no promoted LookupLease via the named field).
var _ RunnerDirectory = (*lookuplessDirectory)(nil)

// TestReportResultRejectsDirectoryWithoutLeaseLookup proves a directory that
// does not implement LeaseLookup is rejected fail-closed on the report path,
// rather than silently falling back to the echoed (client-mutable) lease.
func TestReportResultRejectsDirectoryWithoutLeaseLookup(t *testing.T) {
	fake := &fakeControlEngine{}
	inner := NewMemoryRunnerDirectory()
	dir := &lookuplessDirectory{inner: inner}
	server := httptest.NewServer(NewServer(fake, dir).Handler())
	defer server.Close()

	lease, session := finalizeTenantLease(t, inner, tenant.TenantID("tenant-a"))

	var resp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID:  session.RunnerID,
		SessionID: session.SessionID,
		Lease:     lease,
		Result:    engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, http.StatusConflict, &resp)
	if resp.Accepted {
		t.Fatal("report on a directory without LeaseLookup must not be accepted")
	}
	if fake.committedLease != nil {
		t.Fatalf("commit must not run without LeaseLookup, got %+v", fake.committedLease)
	}
}
