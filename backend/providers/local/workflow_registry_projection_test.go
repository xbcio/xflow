package local

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
)

func TestWorkflowRegistryProjectionOutboxCommitsUnchangedAndReplaced(t *testing.T) {
	reg := newWorkflowRegistry()
	ctx := context.Background()

	namespaces, err := reg.ListWorkflowActivationProjectionNamespaces(ctx)
	if err != nil {
		t.Fatalf("ListWorkflowActivationProjectionNamespaces before commit: %v", err)
	}
	if want := []namespace.Namespace{namespace.Default}; !reflect.DeepEqual(namespaces, want) {
		t.Fatalf("initial namespaces = %v, want %v", namespaces, want)
	}

	original := addProjectionTestWorkflow(t, reg)
	unchangedReq := backend.WorkflowReplaceRequest{
		MutationID:  "projection-unchanged",
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: original,
	}
	unchanged, err := reg.CompareAndReplaceWorkflow(ctx, unchangedReq)
	if err != nil {
		t.Fatalf("unchanged CompareAndReplaceWorkflow: %v", err)
	}
	if unchanged.Status != backend.WorkflowReplaceUnchanged {
		t.Fatalf("unchanged status = %q, want %q", unchanged.Status, backend.WorkflowReplaceUnchanged)
	}

	replacement := original
	replacement.DefinitionHash = "hash:replaced"
	replacement.RegistryRevision = 0
	replacedReq := backend.WorkflowReplaceRequest{
		MutationID:  "projection-replaced",
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: replacement,
	}
	replaced, err := reg.CompareAndReplaceWorkflow(ctx, replacedReq)
	if err != nil {
		t.Fatalf("replaced CompareAndReplaceWorkflow: %v", err)
	}
	if replaced.Status != backend.WorkflowReplaceReplaced {
		t.Fatalf("replaced status = %q, want %q", replaced.Status, backend.WorkflowReplaceReplaced)
	}

	namespaces, err = reg.ListWorkflowActivationProjectionNamespaces(ctx)
	if err != nil {
		t.Fatalf("ListWorkflowActivationProjectionNamespaces after commits: %v", err)
	}
	if want := []namespace.Namespace{namespace.Default, "tenant"}; !reflect.DeepEqual(namespaces, want) {
		t.Fatalf("namespaces = %v, want %v", namespaces, want)
	}

	firstPage, cursor, err := reg.ListPendingWorkflowActivationProjections(ctx, "tenant", 0, 1)
	if err != nil {
		t.Fatalf("ListPendingWorkflowActivationProjections first page: %v", err)
	}
	if len(firstPage) != 1 || firstPage[0].MutationID != unchangedReq.MutationID || cursor == 0 {
		t.Fatalf("first page = %#v cursor=%d, want unchanged and non-zero cursor", firstPage, cursor)
	}
	secondPage, next, err := reg.ListPendingWorkflowActivationProjections(ctx, "tenant", cursor, 1)
	if err != nil {
		t.Fatalf("ListPendingWorkflowActivationProjections second page: %v", err)
	}
	if len(secondPage) != 1 || secondPage[0].MutationID != replacedReq.MutationID || next != 0 {
		t.Fatalf("second page = %#v cursor=%d, want replaced and tail cursor", secondPage, next)
	}

	assertProjectionTestIntent(t, reg, unchangedReq.MutationID, "unchanged-token", unchanged)
	assertProjectionTestIntent(t, reg, replacedReq.MutationID, "replaced-token", replaced)

	stale := replaced.Current
	stale.DefinitionHash = "hash:stale"
	stale.RegistryRevision = 0
	_, err = reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID:  "projection-conflict",
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: stale,
	})
	if !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("stale replacement error = %v, want ErrWorkflowConflict", err)
	}
	pending, _, err := reg.ListPendingWorkflowActivationProjections(ctx, "tenant", 0, 10)
	if err != nil {
		t.Fatalf("ListPendingWorkflowActivationProjections after conflict: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending after conflict = %#v, want only two committed intents", pending)
	}
}

func TestWorkflowRegistryProjectionOutboxReplayDoesNotResurrectAppliedIntent(t *testing.T) {
	reg := newWorkflowRegistry()
	req, committed := commitProjectionTestReplacement(t, reg, "projection-replay")

	claim, err := reg.ClaimWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, "first-token", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if claim.State != backend.WorkflowActivationProjectionClaimAcquired {
		t.Fatalf("claim state = %q, want acquired", claim.State)
	}
	acked, err := reg.AckWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, "first-token")
	if err != nil || !acked {
		t.Fatalf("Ack = %v, %v; want true, nil", acked, err)
	}

	pending, _, err := reg.ListPendingWorkflowActivationProjections(context.Background(), "tenant", 0, 10)
	if err != nil {
		t.Fatalf("ListPendingWorkflowActivationProjections after Ack: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after Ack = %#v, want empty", pending)
	}

	replayed, err := reg.CompareAndReplaceWorkflow(context.Background(), req)
	if err != nil {
		t.Fatalf("CompareAndReplaceWorkflow replay: %v", err)
	}
	if !reflect.DeepEqual(replayed, committed) {
		t.Fatalf("replay = %#v, want %#v", replayed, committed)
	}
	pending, _, err = reg.ListPendingWorkflowActivationProjections(context.Background(), "tenant", 0, 10)
	if err != nil {
		t.Fatalf("ListPendingWorkflowActivationProjections after replay: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after replay = %#v, want empty", pending)
	}

	applied, err := reg.ClaimWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, "retry-token", time.Minute)
	if err != nil {
		t.Fatalf("Claim applied: %v", err)
	}
	if applied.State != backend.WorkflowActivationProjectionClaimApplied || !reflect.DeepEqual(applied.Intent.Current, committed.Current) {
		t.Fatalf("applied claim = %#v, want applied committed intent", applied)
	}
	acked, err = reg.AckWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, "stale-token")
	if err != nil || !acked {
		t.Fatalf("idempotent Ack = %v, %v; want true, nil", acked, err)
	}
}

func TestWorkflowRegistryProjectionOutboxClaimTokenFenceAndExpiry(t *testing.T) {
	reg := newWorkflowRegistry()
	now := time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)
	reg.clock = func() time.Time { return now }
	req, _ := commitProjectionTestReplacement(t, reg, "projection-lease")

	first, err := reg.ClaimWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, "token-a", time.Minute)
	if err != nil {
		t.Fatalf("first Claim: %v", err)
	}
	if first.State != backend.WorkflowActivationProjectionClaimAcquired || !first.LeaseDeadline.Equal(now.Add(time.Minute)) {
		t.Fatalf("first claim = %#v, want acquired with one-minute deadline", first)
	}

	now = now.Add(10 * time.Second)
	retry, err := reg.ClaimWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, "token-a", 10*time.Minute)
	if err != nil {
		t.Fatalf("same-token Claim: %v", err)
	}
	if retry.State != backend.WorkflowActivationProjectionClaimAcquired || !retry.LeaseDeadline.Equal(first.LeaseDeadline) {
		t.Fatalf("same-token retry = %#v, want original deadline %v", retry, first.LeaseDeadline)
	}

	busy, err := reg.ClaimWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, "token-b", time.Minute)
	if err != nil {
		t.Fatalf("busy Claim: %v", err)
	}
	if busy.State != backend.WorkflowActivationProjectionClaimBusy || !busy.LeaseDeadline.Equal(first.LeaseDeadline) {
		t.Fatalf("busy claim = %#v, want busy until %v", busy, first.LeaseDeadline)
	}

	now = first.LeaseDeadline
	reclaimed, err := reg.ClaimWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, "token-b", 2*time.Minute)
	if err != nil {
		t.Fatalf("expired Claim: %v", err)
	}
	if reclaimed.State != backend.WorkflowActivationProjectionClaimAcquired || !reclaimed.LeaseDeadline.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("reclaimed claim = %#v, want token-b acquisition", reclaimed)
	}

	acked, err := reg.AckWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, "token-a")
	if err != nil || acked {
		t.Fatalf("stale-token Ack = %v, %v; want false, nil", acked, err)
	}
	acked, err = reg.AckWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, "token-b")
	if err != nil || !acked {
		t.Fatalf("current-token Ack = %v, %v; want true, nil", acked, err)
	}

	pending, _, err := reg.ListPendingWorkflowActivationProjections(context.Background(), "tenant", 0, 10)
	if err != nil {
		t.Fatalf("ListPendingWorkflowActivationProjections after Ack: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after Ack = %#v, want empty", pending)
	}
}

func TestWorkflowRegistryProjectionOutboxConcurrentClaimHasOneOwner(t *testing.T) {
	reg := newWorkflowRegistry()
	now := time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)
	reg.clock = func() time.Time { return now }
	req, _ := commitProjectionTestReplacement(t, reg, "projection-concurrent-claim")

	const contenders = 32
	states := make(chan backend.WorkflowActivationProjectionClaimState, contenders)
	errs := make(chan error, contenders)
	var start sync.WaitGroup
	start.Add(1)
	var workers sync.WaitGroup
	workers.Add(contenders)
	for i := 0; i < contenders; i++ {
		token := string(rune('a' + i))
		go func() {
			defer workers.Done()
			start.Wait()
			claim, err := reg.ClaimWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, token, time.Minute)
			if err != nil {
				errs <- err
				return
			}
			states <- claim.State
		}()
	}
	start.Done()
	workers.Wait()
	close(states)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Claim: %v", err)
	}

	acquired := 0
	busy := 0
	for state := range states {
		switch state {
		case backend.WorkflowActivationProjectionClaimAcquired:
			acquired++
		case backend.WorkflowActivationProjectionClaimBusy:
			busy++
		default:
			t.Errorf("concurrent claim state = %q, want acquired or busy", state)
		}
	}
	if acquired != 1 || busy != contenders-1 {
		t.Fatalf("concurrent claims acquired=%d busy=%d, want 1/%d", acquired, busy, contenders-1)
	}
}

func TestWorkflowRegistryProjectionOutboxRejectsInvalidCalls(t *testing.T) {
	reg := newWorkflowRegistry()
	req, _ := commitProjectionTestReplacement(t, reg, "projection-invalid")

	if _, _, err := reg.ListPendingWorkflowActivationProjections(context.Background(), "tenant", 0, 0); err == nil {
		t.Fatal("ListPendingWorkflowActivationProjections accepted zero limit")
	}
	if _, err := reg.ClaimWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, "", time.Minute); err == nil {
		t.Fatal("Claim accepted empty token")
	}
	if _, err := reg.ClaimWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, "token", 0); err == nil {
		t.Fatal("Claim accepted zero TTL")
	}
	if acked, err := reg.AckWorkflowActivationProjection(context.Background(), "tenant", req.MutationID, ""); err == nil || acked {
		t.Fatalf("Ack with empty token = %v, %v; want false and error", acked, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reg.ListWorkflowActivationProjectionNamespaces(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListWorkflowActivationProjectionNamespaces canceled error = %v, want context.Canceled", err)
	}
	if _, _, err := reg.ListPendingWorkflowActivationProjections(ctx, "tenant", 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListPendingWorkflowActivationProjections canceled error = %v, want context.Canceled", err)
	}
	if _, err := reg.ClaimWorkflowActivationProjection(ctx, "tenant", req.MutationID, "token", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("Claim canceled error = %v, want context.Canceled", err)
	}
	if _, err := reg.AckWorkflowActivationProjection(ctx, "tenant", req.MutationID, "token"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Ack canceled error = %v, want context.Canceled", err)
	}
}

func addProjectionTestWorkflow(t *testing.T, reg *workflowRegistry) backend.WorkflowRecord {
	t.Helper()
	record, err := reg.AddWorkflow(context.Background(), backend.WorkflowRecord{
		ID:             "projection-workflow",
		Key:            "tenant/projection@v1",
		Namespace:      "tenant",
		Name:           "projection",
		Version:        "v1",
		DefinitionHash: "hash:original",
	})
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	return record
}

func commitProjectionTestReplacement(t *testing.T, reg *workflowRegistry, mutationID string) (backend.WorkflowReplaceRequest, backend.WorkflowReplaceResult) {
	t.Helper()
	original := addProjectionTestWorkflow(t, reg)
	replacement := original
	replacement.DefinitionHash = "hash:replacement"
	replacement.RegistryRevision = 0
	req := backend.WorkflowReplaceRequest{
		MutationID:  mutationID,
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: replacement,
	}
	result, err := reg.CompareAndReplaceWorkflow(context.Background(), req)
	if err != nil {
		t.Fatalf("CompareAndReplaceWorkflow: %v", err)
	}
	return req, result
}

func assertProjectionTestIntent(t *testing.T, reg *workflowRegistry, mutationID, token string, result backend.WorkflowReplaceResult) {
	t.Helper()
	claim, err := reg.ClaimWorkflowActivationProjection(context.Background(), "tenant", mutationID, token, time.Minute)
	if err != nil {
		t.Fatalf("ClaimWorkflowActivationProjection(%q): %v", mutationID, err)
	}
	if claim.State != backend.WorkflowActivationProjectionClaimAcquired {
		t.Fatalf("ClaimWorkflowActivationProjection(%q) state = %q, want acquired", mutationID, claim.State)
	}
	want := backend.WorkflowActivationProjectionIntent{
		Namespace:  "tenant",
		MutationID: mutationID,
		Previous:   result.Previous,
		Current:    result.Current,
	}
	if !reflect.DeepEqual(claim.Intent, want) {
		t.Fatalf("ClaimWorkflowActivationProjection(%q) intent = %#v, want %#v", mutationID, claim.Intent, want)
	}
}
