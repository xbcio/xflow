package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

type hookedAtomicWorkflowRegistry struct {
	backend.WorkflowRegistry
	durable backend.DurableWorkflowReplaceCapability

	mu                      sync.Mutex
	calls                   int
	beforeCall              func(int, backend.WorkflowReplaceRequest)
	afterCall               func(int, backend.WorkflowReplaceRequest, backend.WorkflowReplaceResult, error) error
	seenCanceled            []bool
	seenNamespace           []namespace.Namespace
	seenHasDeadline         []bool
	projectionClaimIDs      []string
	projectionClaimStates   []backend.WorkflowActivationProjectionClaimState
	projectionAckIDs        []string
	projectionAckSuccessful []bool
}

var _ backend.DurableWorkflowReplaceCapability = (*hookedAtomicWorkflowRegistry)(nil)

func newHookedAtomicWorkflowRegistry(reg backend.WorkflowRegistry) *hookedAtomicWorkflowRegistry {
	return &hookedAtomicWorkflowRegistry{
		WorkflowRegistry: reg,
		durable:          reg.(backend.DurableWorkflowReplaceCapability),
	}
}

func (r *hookedAtomicWorkflowRegistry) CompareAndReplaceWorkflow(ctx context.Context, req backend.WorkflowReplaceRequest) (backend.WorkflowReplaceResult, error) {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.seenCanceled = append(r.seenCanceled, ctx.Err() != nil)
	r.seenNamespace = append(r.seenNamespace, namespace.FromContext(ctx))
	_, hasDeadline := ctx.Deadline()
	r.seenHasDeadline = append(r.seenHasDeadline, hasDeadline)
	before := r.beforeCall
	after := r.afterCall
	r.mu.Unlock()

	if before != nil {
		before(call, req)
	}
	result, err := r.durable.CompareAndReplaceWorkflow(ctx, req)
	if after != nil {
		err = after(call, req, result, err)
	}
	return result, err
}

func (r *hookedAtomicWorkflowRegistry) ListWorkflowActivationProjectionNamespaces(ctx context.Context) ([]namespace.Namespace, error) {
	return r.durable.ListWorkflowActivationProjectionNamespaces(ctx)
}

func (r *hookedAtomicWorkflowRegistry) ListPendingWorkflowActivationProjections(ctx context.Context, ns namespace.Namespace, cursor uint64, limit int) ([]backend.WorkflowActivationProjectionRef, uint64, error) {
	return r.durable.ListPendingWorkflowActivationProjections(ctx, ns, cursor, limit)
}

func (r *hookedAtomicWorkflowRegistry) ClaimWorkflowActivationProjection(ctx context.Context, ns namespace.Namespace, mutationID, token string, ttl time.Duration) (backend.WorkflowActivationProjectionClaim, error) {
	claim, err := r.durable.ClaimWorkflowActivationProjection(ctx, ns, mutationID, token, ttl)
	r.mu.Lock()
	r.projectionClaimIDs = append(r.projectionClaimIDs, mutationID)
	r.projectionClaimStates = append(r.projectionClaimStates, claim.State)
	r.mu.Unlock()
	return claim, err
}

func (r *hookedAtomicWorkflowRegistry) AckWorkflowActivationProjection(ctx context.Context, ns namespace.Namespace, mutationID, token string) (bool, error) {
	acked, err := r.durable.AckWorkflowActivationProjection(ctx, ns, mutationID, token)
	r.mu.Lock()
	r.projectionAckIDs = append(r.projectionAckIDs, mutationID)
	r.projectionAckSuccessful = append(r.projectionAckSuccessful, acked)
	r.mu.Unlock()
	return acked, err
}

func (r *hookedAtomicWorkflowRegistry) observations() (int, []bool, []namespace.Namespace, []bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls,
		append([]bool(nil), r.seenCanceled...),
		append([]namespace.Namespace(nil), r.seenNamespace...),
		append([]bool(nil), r.seenHasDeadline...)
}

func (r *hookedAtomicWorkflowRegistry) projectionObservations() ([]string, []backend.WorkflowActivationProjectionClaimState, []string, []bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.projectionClaimIDs...),
		append([]backend.WorkflowActivationProjectionClaimState(nil), r.projectionClaimStates...),
		append([]string(nil), r.projectionAckIDs...),
		append([]bool(nil), r.projectionAckSuccessful...)
}

type atomicOnlyWorkflowRegistry struct {
	backend.WorkflowRegistry
	atomic backend.WorkflowReplaceCapability

	mu    sync.Mutex
	calls int
}

func newAtomicOnlyWorkflowRegistry(reg backend.WorkflowRegistry) *atomicOnlyWorkflowRegistry {
	return &atomicOnlyWorkflowRegistry{
		WorkflowRegistry: reg,
		atomic:           reg.(backend.WorkflowReplaceCapability),
	}
}

func (r *atomicOnlyWorkflowRegistry) CompareAndReplaceWorkflow(ctx context.Context, req backend.WorkflowReplaceRequest) (backend.WorkflowReplaceResult, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	return r.atomic.CompareAndReplaceWorkflow(ctx, req)
}

func (r *atomicOnlyWorkflowRegistry) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

type workflowPutResult struct {
	status int
	err    error
}

func putWorkflowStatus(base, token, id string, body any) (int, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequest(http.MethodPut, base+"/v1/workflows/"+id, bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode, nil
}

func assertProjectionApplied(t *testing.T, outbox backend.WorkflowActivationProjectionOutbox, ns namespace.Namespace, mutationID string) {
	t.Helper()
	refs, _, err := outbox.ListPendingWorkflowActivationProjections(context.Background(), ns, 0, 10)
	if err != nil {
		t.Fatalf("ListPendingWorkflowActivationProjections: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("pending projections = %#v, want empty after inline ack", refs)
	}
	claim, err := outbox.ClaimWorkflowActivationProjection(context.Background(), ns, mutationID, "inspect-applied", time.Minute)
	if err != nil {
		t.Fatalf("ClaimWorkflowActivationProjection(%q): %v", mutationID, err)
	}
	if claim.State != backend.WorkflowProjectionClaimApplied {
		t.Fatalf("projection %q claim state = %q, want applied", mutationID, claim.State)
	}
	if claim.Intent.MutationID != mutationID {
		t.Fatalf("applied intent mutation ID = %q, want %q", claim.Intent.MutationID, mutationID)
	}
}

func assertSinglePendingProjection(t *testing.T, outbox backend.WorkflowActivationProjectionOutbox, ns namespace.Namespace, mutationID string) {
	t.Helper()
	refs, _, err := outbox.ListPendingWorkflowActivationProjections(context.Background(), ns, 0, 10)
	if err != nil {
		t.Fatalf("ListPendingWorkflowActivationProjections: %v", err)
	}
	if len(refs) != 1 || refs[0].MutationID != mutationID {
		t.Fatalf("pending projections = %#v, want only %q", refs, mutationID)
	}
}

func TestReplaceWorkflowDurableProjectionFastPathClaimsProjectsAndAcks(t *testing.T) {
	base := local.New().WorkflowRegistry()
	hooked := newHookedAtomicWorkflowRegistry(base)
	activationStore := control.NewMemoryEntryActivationStore()
	srv, cp := newWorkflowReplaceDependencyTestServer(t, hooked, activationStore, nil)

	resp := postWorkflows(t, srv.URL, "tok-full", configuredTriggerWorkflow("topic-original"))
	defer func() { _ = resp.Body.Close() }()
	var created registerWorkflowResponse
	decodeEnvelope(t, resp, &created)

	const mutationID = "http:durable-fast-path"
	putResp := putWorkflowWithRequestID(t, srv.URL, "tok-full", string(created.WorkflowID), configuredTriggerWorkflow("topic-committed"), "durable-fast-path")
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", putResp.StatusCode)
	}
	assertWorkflowTopic(t, cp.WorkflowRegistry(), created.WorkflowID, "topic-committed")
	assertWorkflowActivationDesired(t, cp.EntryActivationStore(), created.WorkflowID, "topic-committed")

	claimIDs, claimStates, ackIDs, acked := hooked.projectionObservations()
	if len(claimIDs) != 1 || claimIDs[0] != mutationID || claimStates[0] != backend.WorkflowProjectionClaimAcquired {
		t.Fatalf("projection claims = ids:%v states:%v, want one acquired claim for %q", claimIDs, claimStates, mutationID)
	}
	if len(ackIDs) != 1 || ackIDs[0] != mutationID || !acked[0] {
		t.Fatalf("projection acks = ids:%v acked:%v, want one successful ack for %q", ackIDs, acked, mutationID)
	}
	assertProjectionApplied(t, base.(backend.WorkflowActivationProjectionOutbox), "namespaceA", mutationID)
}

func TestReplaceWorkflowProjectionFailureCommitsAndLeavesIntentPending(t *testing.T) {
	base := local.New().WorkflowRegistry()
	hooked := newHookedAtomicWorkflowRegistry(base)
	activationStore := &failingDesiredUpsertStore{EntryActivationStore: control.NewMemoryEntryActivationStore()}
	srv, cp := newWorkflowReplaceDependencyTestServer(t, hooked, activationStore, nil)

	resp := postWorkflows(t, srv.URL, "tok-full", configuredTriggerWorkflow("topic-original"))
	defer func() { _ = resp.Body.Close() }()
	var created registerWorkflowResponse
	decodeEnvelope(t, resp, &created)

	for range 3 {
		activationStore.failNextDesiredUpsert(errors.New("injected durable projection failure"))
	}
	const mutationID = "http:durable-projection-failure"
	putResp := putWorkflowWithRequestID(t, srv.URL, "tok-full", string(created.WorkflowID), configuredTriggerWorkflow("topic-committed"), "durable-projection-failure")
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", putResp.StatusCode)
	}
	assertWorkflowTopic(t, cp.WorkflowRegistry(), created.WorkflowID, "topic-committed")
	assertWorkflowActivationNotDesired(t, cp.EntryActivationStore(), created.WorkflowID)
	assertSinglePendingProjection(t, base.(backend.WorkflowActivationProjectionOutbox), "namespaceA", mutationID)

	claimIDs, claimStates, ackIDs, _ := hooked.projectionObservations()
	if len(claimIDs) != 1 || claimIDs[0] != mutationID || claimStates[0] != backend.WorkflowProjectionClaimAcquired {
		t.Fatalf("projection claims = ids:%v states:%v, want one acquired claim for %q", claimIDs, claimStates, mutationID)
	}
	if len(ackIDs) != 0 {
		t.Fatalf("projection ack IDs = %v, want none after projection failure", ackIDs)
	}
}

func TestPutWorkflowLedgerReplayWithBusyProjectionReturnsInternalServerError(t *testing.T) {
	base := local.New().WorkflowRegistry()
	durable := base.(backend.DurableWorkflowReplaceCapability)
	hooked := newHookedAtomicWorkflowRegistry(base)
	type replaceAttempt struct {
		request backend.WorkflowReplaceRequest
		result  backend.WorkflowReplaceResult
		err     error
	}
	var attemptsMu sync.Mutex
	var attempts []replaceAttempt
	hooked.afterCall = func(_ int, req backend.WorkflowReplaceRequest, result backend.WorkflowReplaceResult, err error) error {
		attemptsMu.Lock()
		attempts = append(attempts, replaceAttempt{request: req, result: result, err: err})
		attemptsMu.Unlock()
		return err
	}

	activationStore := &failingDesiredUpsertStore{EntryActivationStore: control.NewMemoryEntryActivationStore()}
	srv, cp := newWorkflowReplaceDependencyTestServer(t, hooked, activationStore, nil)
	resp := postWorkflows(t, srv.URL, "tok-full", configuredTriggerWorkflow("topic-original"))
	defer func() { _ = resp.Body.Close() }()
	var created registerWorkflowResponse
	decodeEnvelope(t, resp, &created)
	original, err := base.GetWorkflow(context.Background(), created.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow(%q) before replace: %v", created.WorkflowID, err)
	}

	for range 3 {
		activationStore.failNextDesiredUpsert(errors.New("injected durable projection failure"))
	}
	const (
		requestID  = "busy-projection-replay"
		mutationID = "http:" + requestID
	)
	replacement := configuredTriggerWorkflow("topic-committed")
	firstPut := putWorkflowWithRequestID(t, srv.URL, "tok-full", string(created.WorkflowID), replacement, requestID)
	defer func() { _ = firstPut.Body.Close() }()
	if firstPut.StatusCode != http.StatusInternalServerError {
		t.Fatalf("first PUT status = %d, want 500 after projection failure", firstPut.StatusCode)
	}
	assertRequestIDEcho(t, firstPut, requestID)
	assertWorkflowTopic(t, cp.WorkflowRegistry(), created.WorkflowID, "topic-committed")
	assertWorkflowActivationNotDesired(t, cp.EntryActivationStore(), created.WorkflowID)
	assertSinglePendingProjection(t, durable, "namespaceA", mutationID)
	committed, err := base.GetWorkflow(context.Background(), created.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow(%q) after first PUT: %v", created.WorkflowID, err)
	}
	if committed.RegistryRevision == original.RegistryRevision {
		t.Fatalf("registry revision after committed CAS = %d, want different from original %d", committed.RegistryRevision, original.RegistryRevision)
	}

	secondPut := putWorkflowWithRequestID(t, srv.URL, "tok-full", string(created.WorkflowID), replacement, requestID)
	defer func() { _ = secondPut.Body.Close() }()
	if secondPut.StatusCode != http.StatusInternalServerError {
		t.Fatalf("second PUT status = %d, want 500 while replayed projection claim is busy", secondPut.StatusCode)
	}
	assertRequestIDEcho(t, secondPut, requestID)
	assertWorkflowTopic(t, cp.WorkflowRegistry(), created.WorkflowID, "topic-committed")
	assertWorkflowActivationNotDesired(t, cp.EntryActivationStore(), created.WorkflowID)
	assertSinglePendingProjection(t, durable, "namespaceA", mutationID)
	replayed, err := base.GetWorkflow(context.Background(), created.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow(%q) after replay: %v", created.WorkflowID, err)
	}
	if replayed.RegistryRevision != committed.RegistryRevision {
		t.Fatalf("registry revision after ledger replay = %d, want committed revision %d", replayed.RegistryRevision, committed.RegistryRevision)
	}

	calls, _, _, _ := hooked.observations()
	if calls != 2 {
		t.Fatalf("CompareAndReplace calls = %d, want one per complete PUT", calls)
	}
	attemptsMu.Lock()
	gotAttempts := append([]replaceAttempt(nil), attempts...)
	attemptsMu.Unlock()
	if len(gotAttempts) != 2 {
		t.Fatalf("captured replace attempts = %d, want 2", len(gotAttempts))
	}
	wantExpected := []backend.WorkflowRevision{
		backend.RevisionOfWorkflow(original),
		backend.RevisionOfWorkflow(committed),
	}
	for i, attempt := range gotAttempts {
		if attempt.err != nil {
			t.Fatalf("replace attempt %d error = %v, want successful CAS or ledger replay", i+1, attempt.err)
		}
		if attempt.request.MutationID != mutationID {
			t.Fatalf("replace attempt %d mutation ID = %q, want %q", i+1, attempt.request.MutationID, mutationID)
		}
		if attempt.request.Expected != wantExpected[i] {
			t.Fatalf("replace attempt %d expected revision = %#v, want %#v", i+1, attempt.request.Expected, wantExpected[i])
		}
		if attempt.result.Status != backend.WorkflowReplaceReplaced {
			t.Fatalf("replace attempt %d status = %q, want ledger result %q", i+1, attempt.result.Status, backend.WorkflowReplaceReplaced)
		}
		if attempt.result.Current.RegistryRevision != committed.RegistryRevision {
			t.Fatalf("replace attempt %d current revision = %d, want %d", i+1, attempt.result.Current.RegistryRevision, committed.RegistryRevision)
		}
	}

	claimIDs, claimStates, ackIDs, _ := hooked.projectionObservations()
	if len(claimIDs) != 2 || claimIDs[0] != mutationID || claimIDs[1] != mutationID {
		t.Fatalf("projection claim IDs = %v, want two attempts for %q", claimIDs, mutationID)
	}
	if len(claimStates) != 2 || claimStates[0] != backend.WorkflowProjectionClaimAcquired || claimStates[1] != backend.WorkflowProjectionClaimBusy {
		t.Fatalf("projection claim states = %v, want [%q %q]", claimStates, backend.WorkflowProjectionClaimAcquired, backend.WorkflowProjectionClaimBusy)
	}
	if len(ackIDs) != 0 {
		t.Fatalf("projection ack IDs = %v, want none while intent remains pending", ackIDs)
	}
}

func TestPutWorkflowByIDConcurrentStaleFailurePreservesCommittedRevision(t *testing.T) {
	base := local.New().WorkflowRegistry()
	hooked := newHookedAtomicWorkflowRegistry(base)
	loserEntered := make(chan struct{})
	releaseLoser := make(chan struct{})
	defer func() {
		select {
		case <-releaseLoser:
		default:
			close(releaseLoser)
		}
	}()
	hooked.beforeCall = func(call int, _ backend.WorkflowReplaceRequest) {
		if call == 1 {
			close(loserEntered)
			<-releaseLoser
		}
	}

	activationStore := control.NewMemoryEntryActivationStore()
	srv, cp := newWorkflowReplaceDependencyTestServer(t, hooked, activationStore, nil)
	resp := postWorkflows(t, srv.URL, "tok-full", configuredTriggerWorkflow("topic-original"))
	defer func() { _ = resp.Body.Close() }()
	var created registerWorkflowResponse
	decodeEnvelope(t, resp, &created)

	loserResult := make(chan workflowPutResult, 1)
	go func() {
		status, err := putWorkflowStatus(srv.URL, "tok-full", string(created.WorkflowID), configuredTriggerWorkflow("topic-loser"))
		loserResult <- workflowPutResult{status: status, err: err}
	}()
	<-loserEntered

	winner := putWorkflow(t, srv.URL, "tok-full", string(created.WorkflowID), configuredTriggerWorkflow("topic-winner"))
	defer func() { _ = winner.Body.Close() }()
	if winner.StatusCode != 200 {
		t.Fatalf("winner status = %d, want 200", winner.StatusCode)
	}
	close(releaseLoser)
	loser := <-loserResult
	if loser.err != nil {
		t.Fatalf("stale loser request: %v", loser.err)
	}
	if loser.status != 409 {
		t.Fatalf("stale loser status = %d, want 409", loser.status)
	}
	assertWorkflowTopic(t, cp.WorkflowRegistry(), created.WorkflowID, "topic-winner")
	assertWorkflowActivationDesired(t, cp.EntryActivationStore(), created.WorkflowID, "topic-winner")
}

func TestReplaceWorkflowIndeterminatePostCommitReplaysWithoutRollback(t *testing.T) {
	base := local.New().WorkflowRegistry()
	durable := base.(backend.DurableWorkflowReplaceCapability)
	hooked := newHookedAtomicWorkflowRegistry(base)
	injected := errors.New("injected response loss after commit")
	type pendingSnapshot struct {
		refs []backend.WorkflowActivationProjectionRef
		err  error
	}
	var snapshotMu sync.Mutex
	var snapshots []pendingSnapshot
	hooked.afterCall = func(call int, _ backend.WorkflowReplaceRequest, _ backend.WorkflowReplaceResult, err error) error {
		if err == nil {
			refs, _, listErr := durable.ListPendingWorkflowActivationProjections(context.Background(), "namespaceA", 0, 10)
			snapshotMu.Lock()
			snapshots = append(snapshots, pendingSnapshot{refs: refs, err: listErr})
			snapshotMu.Unlock()
		}
		if call == 1 && err == nil {
			return &backend.WorkflowMutationIndeterminateError{Err: injected}
		}
		return err
	}

	activationStore := control.NewMemoryEntryActivationStore()
	srv, cp := newWorkflowReplaceDependencyTestServer(t, hooked, activationStore, nil)
	resp := postWorkflows(t, srv.URL, "tok-full", configuredTriggerWorkflow("topic-original"))
	defer func() { _ = resp.Body.Close() }()
	var created registerWorkflowResponse
	decodeEnvelope(t, resp, &created)

	putResp := putWorkflowWithRequestID(t, srv.URL, "tok-full", string(created.WorkflowID), configuredTriggerWorkflow("topic-committed"), "response-loss")
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 after operation-ledger replay", putResp.StatusCode)
	}
	assertWorkflowTopic(t, cp.WorkflowRegistry(), created.WorkflowID, "topic-committed")
	assertWorkflowActivationDesired(t, cp.EntryActivationStore(), created.WorkflowID, "topic-committed")
	calls, canceled, namespaces, deadlines := hooked.observations()
	if calls != 2 {
		t.Fatalf("CompareAndReplace calls = %d, want 2", calls)
	}
	if canceled[1] || namespaces[1] != namespace.Namespace("namespaceA") || !deadlines[1] {
		t.Fatalf("replay context canceled=%v namespace=%q deadline=%v; want false,namespaceA,true", canceled[1], namespaces[1], deadlines[1])
	}
	snapshotMu.Lock()
	gotSnapshots := append([]pendingSnapshot(nil), snapshots...)
	snapshotMu.Unlock()
	if len(gotSnapshots) != 2 {
		t.Fatalf("pending snapshots = %d, want one after each CAS call", len(gotSnapshots))
	}
	const mutationID = "http:response-loss"
	for i, snapshot := range gotSnapshots {
		if snapshot.err != nil {
			t.Fatalf("pending snapshot %d: %v", i, snapshot.err)
		}
		if len(snapshot.refs) != 1 || snapshot.refs[0].MutationID != mutationID {
			t.Fatalf("pending snapshot %d = %#v, want only %q", i, snapshot.refs, mutationID)
		}
	}
	claimIDs, claimStates, ackIDs, acked := hooked.projectionObservations()
	if len(claimIDs) != 1 || claimIDs[0] != mutationID || claimStates[0] != backend.WorkflowProjectionClaimAcquired {
		t.Fatalf("projection claims = ids:%v states:%v, want one acquired claim for %q", claimIDs, claimStates, mutationID)
	}
	if len(ackIDs) != 1 || ackIDs[0] != mutationID || !acked[0] {
		t.Fatalf("projection acks = ids:%v acked:%v, want one successful ack for %q", ackIDs, acked, mutationID)
	}
	assertProjectionApplied(t, durable, "namespaceA", mutationID)
}

func TestPutWorkflowWholeHTTPRequestReplaysAfterPostCommitResponseLoss(t *testing.T) {
	base := local.New().WorkflowRegistry()
	durable := base.(backend.DurableWorkflowReplaceCapability)
	hooked := newHookedAtomicWorkflowRegistry(base)
	injected := errors.New("injected post-commit response loss")

	var requestsMu sync.Mutex
	var requests []backend.WorkflowReplaceRequest
	hooked.afterCall = func(call int, req backend.WorkflowReplaceRequest, _ backend.WorkflowReplaceResult, err error) error {
		requestsMu.Lock()
		requests = append(requests, req)
		requestsMu.Unlock()
		if call <= 4 && err == nil {
			return &backend.WorkflowMutationIndeterminateError{Err: injected}
		}
		return err
	}

	activationStore := control.NewMemoryEntryActivationStore()
	srv, cp := newWorkflowReplaceDependencyTestServer(t, hooked, activationStore, nil)
	resp := postWorkflows(t, srv.URL, "tok-full", configuredTriggerWorkflow("topic-original"))
	defer func() { _ = resp.Body.Close() }()
	var created registerWorkflowResponse
	decodeEnvelope(t, resp, &created)

	original, err := base.GetWorkflow(context.Background(), created.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow(%q) before replace: %v", created.WorkflowID, err)
	}

	const (
		requestID  = "whole-request-response-loss"
		mutationID = "http:" + requestID
	)
	replacement := configuredTriggerWorkflow("topic-committed")
	firstPut := putWorkflowWithRequestID(t, srv.URL, "tok-full", string(created.WorkflowID), replacement, requestID)
	defer func() { _ = firstPut.Body.Close() }()
	if firstPut.StatusCode != http.StatusInternalServerError {
		t.Fatalf("first PUT status = %d, want 500 after bounded indeterminate retries", firstPut.StatusCode)
	}
	assertRequestIDEcho(t, firstPut, requestID)

	firstCalls, _, _, _ := hooked.observations()
	if firstCalls != 4 {
		t.Fatalf("CompareAndReplace calls after first PUT = %d, want 4 bounded attempts", firstCalls)
	}
	assertWorkflowTopic(t, cp.WorkflowRegistry(), created.WorkflowID, "topic-committed")
	committed, err := base.GetWorkflow(context.Background(), created.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow(%q) after first PUT: %v", created.WorkflowID, err)
	}
	if committed.RegistryRevision == original.RegistryRevision {
		t.Fatalf("registry revision after committed CAS = %d, want different from original %d", committed.RegistryRevision, original.RegistryRevision)
	}
	assertSinglePendingProjection(t, durable, "namespaceA", mutationID)
	claimIDs, _, ackIDs, _ := hooked.projectionObservations()
	if len(claimIDs) != 0 || len(ackIDs) != 0 {
		t.Fatalf("projection observations after indeterminate first PUT = claims:%v acks:%v, want none", claimIDs, ackIDs)
	}

	secondPut := putWorkflowWithRequestID(t, srv.URL, "tok-full", string(created.WorkflowID), replacement, requestID)
	defer func() { _ = secondPut.Body.Close() }()
	if secondPut.StatusCode != http.StatusOK {
		t.Fatalf("second PUT status = %d, want 200 after operation-ledger replay", secondPut.StatusCode)
	}
	assertRequestIDEcho(t, secondPut, requestID)

	calls, _, _, _ := hooked.observations()
	if calls != 5 {
		t.Fatalf("CompareAndReplace calls after second PUT = %d, want 5", calls)
	}
	requestsMu.Lock()
	gotRequests := append([]backend.WorkflowReplaceRequest(nil), requests...)
	requestsMu.Unlock()
	if len(gotRequests) != 5 {
		t.Fatalf("captured replace requests = %d, want 5", len(gotRequests))
	}
	wantOriginal := backend.RevisionOfWorkflow(original)
	wantCommitted := backend.RevisionOfWorkflow(committed)
	for i, req := range gotRequests {
		if req.MutationID != mutationID {
			t.Fatalf("replace request %d mutation ID = %q, want %q", i+1, req.MutationID, mutationID)
		}
		if req.Replacement.DefinitionHash != gotRequests[0].Replacement.DefinitionHash {
			t.Fatalf("replace request %d definition hash = %q, want %q", i+1, req.Replacement.DefinitionHash, gotRequests[0].Replacement.DefinitionHash)
		}
		if i < 4 && req.Expected != wantOriginal {
			t.Fatalf("replace request %d expected revision = %#v, want original %#v", i+1, req.Expected, wantOriginal)
		}
	}
	if gotRequests[4].Expected != wantCommitted {
		t.Fatalf("second HTTP request expected revision = %#v, want committed %#v", gotRequests[4].Expected, wantCommitted)
	}

	assertWorkflowTopic(t, cp.WorkflowRegistry(), created.WorkflowID, "topic-committed")
	assertWorkflowActivationDesired(t, cp.EntryActivationStore(), created.WorkflowID, "topic-committed")
	claimIDs, claimStates, ackIDs, acked := hooked.projectionObservations()
	if len(claimIDs) != 1 || claimIDs[0] != mutationID || claimStates[0] != backend.WorkflowProjectionClaimAcquired {
		t.Fatalf("projection claims = ids:%v states:%v, want one acquired claim for %q", claimIDs, claimStates, mutationID)
	}
	if len(ackIDs) != 1 || ackIDs[0] != mutationID || !acked[0] {
		t.Fatalf("projection acks = ids:%v acked:%v, want one successful ack for %q", ackIDs, acked, mutationID)
	}
	assertProjectionApplied(t, durable, "namespaceA", mutationID)
}

type observingActivationStore struct {
	engine.EntryActivationStore
	revisions engine.EntryActivationRevisionStore

	mu           sync.Mutex
	canceled     []bool
	namespaces   []namespace.Namespace
	hasDeadlines []bool
}

func newObservingActivationStore(store engine.EntryActivationStore) *observingActivationStore {
	return &observingActivationStore{
		EntryActivationStore: store,
		revisions:            store.(engine.EntryActivationRevisionStore),
	}
}

func (s *observingActivationStore) observe(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.canceled = append(s.canceled, ctx.Err() != nil)
	s.namespaces = append(s.namespaces, namespace.FromContext(ctx))
	_, hasDeadline := ctx.Deadline()
	s.hasDeadlines = append(s.hasDeadlines, hasDeadline)
}

func (s *observingActivationStore) Upsert(ctx context.Context, act engine.EntryActivation) error {
	s.observe(ctx)
	return s.EntryActivationStore.Upsert(ctx, act)
}

func (s *observingActivationStore) AdvanceWorkflowRevision(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, revision uint64) error {
	s.observe(ctx)
	return s.revisions.AdvanceWorkflowRevision(ctx, ns, workflowID, revision)
}

func (s *observingActivationStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.canceled = nil
	s.namespaces = nil
	s.hasDeadlines = nil
}

func (s *observingActivationStore) assertDetachedProjection(t *testing.T, want namespace.Namespace) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.canceled) == 0 {
		t.Fatal("activation projection did not access the store")
	}
	for i := range s.canceled {
		if s.canceled[i] || s.namespaces[i] != want || !s.hasDeadlines[i] {
			t.Fatalf("projection call %d: canceled=%v namespace=%q deadline=%v; want false,%q,true", i, s.canceled[i], s.namespaces[i], s.hasDeadlines[i], want)
		}
	}
}

func TestReplaceWorkflowProjectsCommittedRevisionAfterRequestCancellation(t *testing.T) {
	provider := local.New()
	hooked := newHookedAtomicWorkflowRegistry(provider.WorkflowRegistry())
	activationStore := newObservingActivationStore(control.NewMemoryEntryActivationStore())
	cp, err := control.NewControlPlane(control.Config{
		Backend:              provider,
		WorkflowRegistry:     hooked,
		EntryActivationStore: activationStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Concurrency: 8}, WithControlPlane(cp))
	if err != nil {
		t.Fatal(err)
	}
	ns := namespace.Namespace("namespaceA")
	ctx := namespace.WithNamespace(context.Background(), ns)
	id, _, err := srv.RegisterWorkflow(ctx, ns, configuredTriggerWorkflow("topic-original"))
	if err != nil {
		t.Fatal(err)
	}
	activationStore.reset()

	requestCtx, cancel := context.WithCancel(ctx)
	hooked.afterCall = func(call int, _ backend.WorkflowReplaceRequest, _ backend.WorkflowReplaceResult, err error) error {
		if call == 1 {
			cancel()
		}
		return err
	}
	_, _, err = srv.ctrl.replaceWorkflowByID(requestCtx, ns, id, configuredTriggerWorkflow("topic-committed"), "post-commit-cancel")
	if err != nil {
		t.Fatalf("replace after post-commit cancellation: %v", err)
	}
	if requestCtx.Err() != context.Canceled {
		t.Fatalf("request context error = %v, want context.Canceled", requestCtx.Err())
	}
	assertWorkflowTopic(t, cp.WorkflowRegistry(), id, "topic-committed")
	assertWorkflowActivationDesired(t, cp.EntryActivationStore(), id, "topic-committed")
	activationStore.assertDetachedProjection(t, ns)
	claimIDs, claimStates, ackIDs, acked := hooked.projectionObservations()
	if len(claimIDs) != 1 || claimIDs[0] != "post-commit-cancel" || claimStates[0] != backend.WorkflowProjectionClaimAcquired {
		t.Fatalf("detached projection claims = ids:%v states:%v, want one acquired claim", claimIDs, claimStates)
	}
	if len(ackIDs) != 1 || ackIDs[0] != "post-commit-cancel" || !acked[0] {
		t.Fatalf("detached projection acks = ids:%v acked:%v, want one successful ack", ackIDs, acked)
	}
	assertProjectionApplied(t, hooked.durable, ns, "post-commit-cancel")
}

func TestReplaceWorkflowFailsClosedWithoutDurableProjectionCapability(t *testing.T) {
	base := local.New().WorkflowRegistry()
	// Expose atomic replacement but intentionally hide the durable outbox. The
	// handler must reject before invoking the otherwise-valid CAS capability.
	wrapped := newAtomicOnlyWorkflowRegistry(base)
	srv, cp := newWorkflowReplaceDependencyTestServer(t, wrapped, control.NewMemoryEntryActivationStore(), nil)
	resp := postWorkflows(t, srv.URL, "tok-full", configuredTriggerWorkflow("topic-original"))
	defer func() { _ = resp.Body.Close() }()
	var created registerWorkflowResponse
	decodeEnvelope(t, resp, &created)

	putResp := putWorkflow(t, srv.URL, "tok-full", string(created.WorkflowID), configuredTriggerWorkflow("topic-rejected"))
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != 500 {
		t.Fatalf("status = %d, want 500", putResp.StatusCode)
	}
	if calls := wrapped.callCount(); calls != 0 {
		t.Fatalf("CompareAndReplaceWorkflow calls = %d, want 0 without durable projection capability", calls)
	}
	assertWorkflowTopic(t, cp.WorkflowRegistry(), created.WorkflowID, "topic-original")
}

func TestReplaceWorkflowCanceledBeforeCASDoesNotMutate(t *testing.T) {
	provider := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: provider})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Concurrency: 1}, WithControlPlane(cp))
	if err != nil {
		t.Fatal(err)
	}
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	id, _, err := srv.RegisterWorkflow(ctx, namespace.Default, configuredTriggerWorkflow("topic-original"))
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = srv.ctrl.replaceWorkflowByID(canceled, namespace.Default, id, configuredTriggerWorkflow("topic-canceled"), "cancel-before-cas")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("replace error = %v, want context.Canceled", err)
	}
	assertWorkflowTopic(t, cp.WorkflowRegistry(), types.WorkflowID(id), "topic-original")
}
