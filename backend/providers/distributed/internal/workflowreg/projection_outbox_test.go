package workflowreg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func TestWorkflowActivationProjectionOutboxCommitsCompleteIntent(t *testing.T) {
	tests := []struct {
		name       string
		changed    bool
		wantStatus backend.WorkflowReplaceStatus
	}{
		{name: "unchanged", wantStatus: backend.WorkflowReplaceUnchanged},
		{name: "replaced", changed: true, wantStatus: backend.WorkflowReplaceReplaced},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := namespace.Namespace("projection-" + tt.name)
			ctx := namespace.WithNamespace(context.Background(), ns)
			reg, _ := newTestRegistry(t)
			original := addDistributedAtomicWorkflow(t, ctx, reg, projectionWorkflowRecord(t, types.WorkflowID("old-"+tt.name), "old-"+tt.name, "v1", "hash:old"))
			beforeRevision := distributedRegistryRevision(t, ctx, reg)

			replacement := original
			if tt.changed {
				replacement = projectionWorkflowRecord(t, types.WorkflowID("new-"+tt.name), "new-"+tt.name, "v2", "hash:new")
			}
			request := backend.WorkflowReplaceRequest{
				MutationID:  "mutation-{caller}-" + tt.name,
				Expected:    backend.RevisionOfWorkflow(original),
				Replacement: replacement,
			}
			result, err := reg.CompareAndReplaceWorkflow(ctx, request)
			if err != nil {
				t.Fatalf("CompareAndReplaceWorkflow: %v", err)
			}
			if result.Status != tt.wantStatus {
				t.Fatalf("status = %q, want %q", result.Status, tt.wantStatus)
			}
			if tt.changed {
				if result.Current.RegistryRevision <= original.RegistryRevision {
					t.Fatalf("replacement revision = %d, want > %d", result.Current.RegistryRevision, original.RegistryRevision)
				}
			} else {
				if result.Current.RegistryRevision != original.RegistryRevision {
					t.Fatalf("unchanged revision = %d, want %d", result.Current.RegistryRevision, original.RegistryRevision)
				}
				if got := distributedRegistryRevision(t, ctx, reg); got != beforeRevision {
					t.Fatalf("unchanged mutation advanced authority revision from %d to %d", beforeRevision, got)
				}
			}

			operationKey := workflowOperationKey(ns, request.MutationID)
			pendingKey := workflowProjectionPendingKey(ns)
			leaseKey := workflowProjectionLeaseKey(ns, request.MutationID)
			wantSlot := slotTag(operationKey)
			for name, key := range map[string]string{
				"pending": pendingKey,
				"lease":   leaseKey,
			} {
				if got := slotTag(key); got != wantSlot {
					t.Fatalf("%s key slot = %q, want operation-ledger slot %q (key %q)", name, got, wantSlot, key)
				}
			}
			if wantSlot == "" || strings.Contains(wantSlot, request.MutationID) {
				t.Fatalf("authority slot %q is empty or contains caller-controlled mutation ID", wantSlot)
			}
			if globalSlot := slotTag(workflowProjectionNamespaceSetKey); globalSlot == wantSlot {
				t.Fatalf("global namespace set unexpectedly shares namespace authority slot %q", wantSlot)
			}

			ledger, err := reg.rdb.HGetAll(ctx, operationKey).Result()
			if err != nil {
				t.Fatalf("HGetAll operation ledger: %v", err)
			}
			wantPrevious := backend.RevisionOfWorkflow(original)
			wantLedgerFields := map[string]string{
				"mutation_id":          request.MutationID,
				"status":               string(tt.wantStatus),
				"projection_namespace": string(ns),
				"projection_state":     "pending",
				"previous_id":          string(wantPrevious.ID),
				"previous_key":         wantPrevious.Key,
				"previous_version":     wantPrevious.Version,
				"previous_hash":        wantPrevious.DefinitionHash,
				"previous_revision":    strconv.FormatUint(wantPrevious.RegistryRevision, 10),
			}
			for field, want := range wantLedgerFields {
				if got := ledger[field]; got != want {
					t.Errorf("ledger[%q] = %q, want %q", field, got, want)
				}
			}
			if ledger["current_payload"] == "" {
				t.Fatal("operation ledger omitted current_payload")
			}
			storedCurrent, err := unmarshalWorkflowRecord([]byte(ledger["current_payload"]))
			if err != nil {
				t.Fatalf("decode ledger current_payload: %v", err)
			}
			assertProjectionWorkflowRecord(t, storedCurrent, result.Current)
			pending, err := reg.rdb.SIsMember(ctx, pendingKey, request.MutationID).Result()
			if err != nil {
				t.Fatalf("SIsMember pending: %v", err)
			}
			if !pending {
				t.Fatal("committed operation is absent from namespace pending set")
			}

			refs, next, err := reg.ListPendingWorkflowActivationProjections(ctx, ns, 0, 10)
			if err != nil {
				t.Fatalf("ListPendingWorkflowActivationProjections: %v", err)
			}
			if next != 0 || len(refs) != 1 || refs[0].MutationID != request.MutationID {
				t.Fatalf("pending page = %#v, cursor %d; want only %q at tail", refs, next, request.MutationID)
			}
			claim, err := reg.ClaimWorkflowActivationProjection(ctx, ns, request.MutationID, "worker-1", time.Minute)
			if err != nil {
				t.Fatalf("ClaimWorkflowActivationProjection: %v", err)
			}
			if claim.State != backend.WorkflowActivationProjectionClaimAcquired || claim.LeaseDeadline.IsZero() {
				t.Fatalf("claim = state %q deadline %v, want acquired with deadline", claim.State, claim.LeaseDeadline)
			}
			assertProjectionIntent(t, claim.Intent, ns, request.MutationID, wantPrevious, result.Current)
		})
	}
}

func TestWorkflowActivationProjectionOutboxRegistersNamespaceBeforeConflict(t *testing.T) {
	ns := namespace.Namespace("projection-conflict")
	ctx := namespace.WithNamespace(context.Background(), ns)
	reg, _ := newTestRegistry(t)
	original := addDistributedAtomicWorkflow(t, ctx, reg, projectionWorkflowRecord(t, "conflict-old", "conflict-old", "v1", "hash:old"))
	beforeRevision := distributedRegistryRevision(t, ctx, reg)
	expected := backend.RevisionOfWorkflow(original)
	expected.RegistryRevision++
	replacement := original
	replacement.DefinitionHash = "hash:new"
	replacement.RegistryRevision = 0
	mutationID := "conflicting-mutation"

	_, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID:  mutationID,
		Expected:    expected,
		Replacement: replacement,
	})
	assertDistributedWorkflowConflict(t, err, backend.WorkflowReplaceConflictStaleRevision)

	registered, err := reg.rdb.SIsMember(ctx, workflowProjectionNamespaceSetKey, string(ns)).Result()
	if err != nil {
		t.Fatalf("SIsMember namespace: %v", err)
	}
	if !registered {
		t.Fatal("namespace was not registered before the conflicting CAS")
	}
	if reg.rdb.Exists(ctx, workflowOperationKey(ns, mutationID)).Val() != 0 {
		t.Fatal("conflicting CAS created an operation ledger")
	}
	if reg.rdb.SIsMember(ctx, workflowProjectionPendingKey(ns), mutationID).Val() {
		t.Fatal("conflicting CAS created a pending projection")
	}
	if got := distributedRegistryRevision(t, ctx, reg); got != beforeRevision {
		t.Fatalf("conflicting CAS advanced revision from %d to %d", beforeRevision, got)
	}
	assertProjectionWorkflowRecord(t, getDistributedAtomicWorkflow(t, ctx, reg, original.ID), original)
}

func TestWorkflowActivationProjectionOutboxNamespaceRegistrationFailurePreventsCAS(t *testing.T) {
	ns := namespace.Namespace("projection-sadd-failure")
	ctx := namespace.WithNamespace(context.Background(), ns)
	reg, srv := newTestRegistry(t)
	original := addDistributedAtomicWorkflow(t, ctx, reg, projectionWorkflowRecord(t, "sadd-old", "sadd-old", "v1", "hash:old"))
	beforeRevision := distributedRegistryRevision(t, ctx, reg)
	replacement := original
	replacement.DefinitionHash = "hash:new"
	replacement.RegistryRevision = 0
	mutationID := "must-not-cas"

	var mu sync.Mutex
	var sawNamespaceSAdd bool
	var sawEvalAfterFailure bool
	srv.Server().SetPreHook(server.Hook(func(peer *server.Peer, command string, args ...string) bool {
		if strings.EqualFold(command, "sadd") && len(args) > 0 && args[0] == workflowProjectionNamespaceSetKey {
			mu.Lock()
			sawNamespaceSAdd = true
			mu.Unlock()
			peer.WriteError("ERR injected namespace registration failure")
			return true
		}
		if strings.EqualFold(command, "eval") || strings.EqualFold(command, "evalsha") {
			mu.Lock()
			sawEvalAfterFailure = true
			mu.Unlock()
		}
		return false
	}))
	_, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
		MutationID:  mutationID,
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: replacement,
	})
	srv.Server().SetPreHook(nil)

	if !errors.Is(err, backend.ErrWorkflowMutationIndeterminate) {
		t.Fatalf("CompareAndReplaceWorkflow error = %v, want ErrWorkflowMutationIndeterminate", err)
	}
	mu.Lock()
	gotSAdd, gotEval := sawNamespaceSAdd, sawEvalAfterFailure
	mu.Unlock()
	if !gotSAdd {
		t.Fatal("CompareAndReplaceWorkflow did not attempt namespace SADD")
	}
	if gotEval {
		t.Fatal("CompareAndReplaceWorkflow attempted a Lua CAS after namespace SADD failed")
	}
	if reg.rdb.SIsMember(ctx, workflowProjectionNamespaceSetKey, string(ns)).Val() {
		t.Fatal("failed namespace SADD unexpectedly registered the namespace")
	}
	if reg.rdb.Exists(ctx, workflowOperationKey(ns, mutationID)).Val() != 0 {
		t.Fatal("failed namespace SADD was followed by an operation ledger write")
	}
	if reg.rdb.Exists(ctx, workflowProjectionPendingKey(ns)).Val() != 0 {
		t.Fatal("failed namespace SADD was followed by a pending outbox write")
	}
	if got := distributedRegistryRevision(t, ctx, reg); got != beforeRevision {
		t.Fatalf("failed namespace SADD advanced revision from %d to %d", beforeRevision, got)
	}
	assertProjectionWorkflowRecord(t, getDistributedAtomicWorkflow(t, ctx, reg, original.ID), original)
}

func TestWorkflowActivationProjectionOutboxClaimFenceExpiryAckAndReplay(t *testing.T) {
	ns := namespace.Namespace("projection-fencing")
	ctx := namespace.WithNamespace(context.Background(), ns)
	reg, srv := newTestRegistry(t)
	baseTime := time.Date(2035, time.June, 7, 8, 9, 10, 123_000_000, time.UTC)
	srv.SetTime(baseTime)
	request, result := createPendingProjection(t, ctx, reg, "fenced-mutation")
	leaseTTL := 2 * time.Second

	claimA, err := reg.ClaimWorkflowActivationProjection(ctx, ns, request.MutationID, "token-a", leaseTTL)
	if err != nil {
		t.Fatalf("claim token-a: %v", err)
	}
	if claimA.State != backend.WorkflowActivationProjectionClaimAcquired {
		t.Fatalf("token-a claim state = %q, want acquired", claimA.State)
	}
	if want := baseTime.Add(leaseTTL); !claimA.LeaseDeadline.Equal(want) {
		t.Fatalf("token-a deadline = %v, want Redis-time deadline %v", claimA.LeaseDeadline, want)
	}

	retryA, err := reg.ClaimWorkflowActivationProjection(ctx, ns, request.MutationID, "token-a", 30*time.Second)
	if err != nil {
		t.Fatalf("retry token-a: %v", err)
	}
	if retryA.State != backend.WorkflowActivationProjectionClaimAcquired || !retryA.LeaseDeadline.Equal(claimA.LeaseDeadline) {
		t.Fatalf("same-token retry = state %q deadline %v, want acquired with unchanged %v", retryA.State, retryA.LeaseDeadline, claimA.LeaseDeadline)
	}

	busyB, err := reg.ClaimWorkflowActivationProjection(ctx, ns, request.MutationID, "token-b", leaseTTL)
	if err != nil {
		t.Fatalf("busy token-b claim: %v", err)
	}
	if busyB.State != backend.WorkflowActivationProjectionClaimBusy || !busyB.LeaseDeadline.Equal(claimA.LeaseDeadline) {
		t.Fatalf("token-b claim = state %q deadline %v, want busy until %v", busyB.State, busyB.LeaseDeadline, claimA.LeaseDeadline)
	}
	if acked, err := reg.AckWorkflowActivationProjection(ctx, ns, request.MutationID, "token-b"); err != nil || acked {
		t.Fatalf("non-owner token-b Ack = %v, %v; want false, nil", acked, err)
	}

	srv.FastForward(leaseTTL + time.Millisecond)
	srv.SetTime(baseTime.Add(leaseTTL + time.Millisecond))
	claimB, err := reg.ClaimWorkflowActivationProjection(ctx, ns, request.MutationID, "token-b", leaseTTL)
	if err != nil {
		t.Fatalf("token-b takeover: %v", err)
	}
	if claimB.State != backend.WorkflowActivationProjectionClaimAcquired || !claimB.LeaseDeadline.After(claimA.LeaseDeadline) {
		t.Fatalf("token-b takeover = state %q deadline %v, want acquired after %v", claimB.State, claimB.LeaseDeadline, claimA.LeaseDeadline)
	}
	if acked, err := reg.AckWorkflowActivationProjection(ctx, ns, request.MutationID, "token-a"); err != nil || acked {
		t.Fatalf("stale token-a Ack = %v, %v; want false, nil", acked, err)
	}
	if acked, err := reg.AckWorkflowActivationProjection(ctx, ns, request.MutationID, "token-b"); err != nil || !acked {
		t.Fatalf("owner token-b Ack = %v, %v; want true, nil", acked, err)
	}
	if acked, err := reg.AckWorkflowActivationProjection(ctx, ns, request.MutationID, "token-b"); err != nil || !acked {
		t.Fatalf("idempotent token-b Ack = %v, %v; want true, nil", acked, err)
	}

	ledger, err := reg.rdb.HGetAll(ctx, workflowOperationKey(ns, request.MutationID)).Result()
	if err != nil {
		t.Fatalf("read applied ledger: %v", err)
	}
	if ledger["projection_state"] != "applied" {
		t.Fatalf("projection_state = %q, want applied", ledger["projection_state"])
	}
	if _, err := strconv.ParseInt(ledger["projection_applied_at_ms"], 10, 64); err != nil {
		t.Fatalf("projection_applied_at_ms = %q, want fixed decimal milliseconds: %v", ledger["projection_applied_at_ms"], err)
	}
	if reg.rdb.SIsMember(ctx, workflowProjectionPendingKey(ns), request.MutationID).Val() {
		t.Fatal("Ack left mutation in pending set")
	}
	if reg.rdb.Exists(ctx, workflowProjectionLeaseKey(ns, request.MutationID)).Val() != 0 {
		t.Fatal("Ack left projection lease behind")
	}

	replay, err := reg.CompareAndReplaceWorkflow(ctx, request)
	if err != nil {
		t.Fatalf("CompareAndReplaceWorkflow replay after Ack: %v", err)
	}
	if replay.Status != result.Status {
		t.Fatalf("replay status = %q, want %q", replay.Status, result.Status)
	}
	assertProjectionWorkflowRecord(t, replay.Current, result.Current)
	if reg.rdb.SIsMember(ctx, workflowProjectionPendingKey(ns), request.MutationID).Val() {
		t.Fatal("operation-ledger replay resurrected an acknowledged intent")
	}
	applied, err := reg.ClaimWorkflowActivationProjection(ctx, ns, request.MutationID, "token-c", leaseTTL)
	if err != nil {
		t.Fatalf("claim after Ack: %v", err)
	}
	if applied.State != backend.WorkflowActivationProjectionClaimApplied || !applied.LeaseDeadline.IsZero() {
		t.Fatalf("claim after Ack = state %q deadline %v, want applied with no lease", applied.State, applied.LeaseDeadline)
	}
	assertProjectionIntent(t, applied.Intent, ns, request.MutationID, result.Previous, result.Current)

	namespaces, err := reg.ListWorkflowActivationProjectionNamespaces(ctx)
	if err != nil {
		t.Fatalf("ListWorkflowActivationProjectionNamespaces: %v", err)
	}
	if len(namespaces) != 1 || namespaces[0] != ns {
		t.Fatalf("namespaces after Ack = %v, want append-only [%s]", namespaces, ns)
	}
}

func TestWorkflowActivationProjectionOutboxRecoversAfterRegistryRebuild(t *testing.T) {
	ns := namespace.Namespace("projection-recovery")
	ctx := namespace.WithNamespace(context.Background(), ns)
	reg, srv := newTestRegistry(t)
	baseTime := time.Date(2036, time.January, 2, 3, 4, 5, 0, time.UTC)
	srv.SetTime(baseTime)
	request, result := createPendingProjection(t, ctx, reg, "recoverable-mutation")
	firstClaim, err := reg.ClaimWorkflowActivationProjection(ctx, ns, request.MutationID, "recovering-worker", time.Minute)
	if err != nil {
		t.Fatalf("initial claim: %v", err)
	}

	freshClient := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = freshClient.Close() })
	rebuilt := New(freshClient)

	namespaces, err := rebuilt.ListWorkflowActivationProjectionNamespaces(context.Background())
	if err != nil {
		t.Fatalf("rebuilt ListWorkflowActivationProjectionNamespaces: %v", err)
	}
	if len(namespaces) != 1 || namespaces[0] != ns {
		t.Fatalf("rebuilt namespaces = %v, want [%s]", namespaces, ns)
	}
	refs, next, err := rebuilt.ListPendingWorkflowActivationProjections(context.Background(), ns, 0, 10)
	if err != nil {
		t.Fatalf("rebuilt ListPendingWorkflowActivationProjections: %v", err)
	}
	if next != 0 || len(refs) != 1 || refs[0].MutationID != request.MutationID {
		t.Fatalf("rebuilt pending page = %#v, cursor %d", refs, next)
	}
	busy, err := rebuilt.ClaimWorkflowActivationProjection(context.Background(), ns, request.MutationID, "other-worker", time.Minute)
	if err != nil {
		t.Fatalf("rebuilt competing claim: %v", err)
	}
	if busy.State != backend.WorkflowActivationProjectionClaimBusy || !busy.LeaseDeadline.Equal(firstClaim.LeaseDeadline) {
		t.Fatalf("rebuilt competing claim = state %q deadline %v, want busy until %v", busy.State, busy.LeaseDeadline, firstClaim.LeaseDeadline)
	}
	recovered, err := rebuilt.ClaimWorkflowActivationProjection(context.Background(), ns, request.MutationID, "recovering-worker", 10*time.Minute)
	if err != nil {
		t.Fatalf("rebuilt same-token claim: %v", err)
	}
	if recovered.State != backend.WorkflowActivationProjectionClaimAcquired || !recovered.LeaseDeadline.Equal(firstClaim.LeaseDeadline) {
		t.Fatalf("rebuilt same-token claim = state %q deadline %v, want acquired with %v", recovered.State, recovered.LeaseDeadline, firstClaim.LeaseDeadline)
	}
	assertProjectionIntent(t, recovered.Intent, ns, request.MutationID, result.Previous, result.Current)
}

func TestWorkflowActivationProjectionOutboxPaginationIsBoundedAndComplete(t *testing.T) {
	ns := namespace.Namespace("projection-pages")
	ctx := namespace.WithNamespace(context.Background(), ns)
	reg, _ := newTestRegistry(t)
	current := addDistributedAtomicWorkflow(t, ctx, reg, projectionWorkflowRecord(t, "page-id", "page-flow", "v1", "hash:0"))
	const mutationCount = 7
	want := make([]string, 0, mutationCount)
	for i := 1; i <= mutationCount; i++ {
		mutationID := fmt.Sprintf("page-mutation-%02d", i)
		replacement := current
		replacement.DefinitionHash = fmt.Sprintf("hash:%d", i)
		replacement.RegistryRevision = 0
		result, err := reg.CompareAndReplaceWorkflow(ctx, backend.WorkflowReplaceRequest{
			MutationID:  mutationID,
			Expected:    backend.RevisionOfWorkflow(current),
			Replacement: replacement,
		})
		if err != nil {
			t.Fatalf("CompareAndReplaceWorkflow %d: %v", i, err)
		}
		current = result.Current
		want = append(want, mutationID)
	}

	const pageLimit = 2
	var cursor uint64
	var got []string
	for page := 0; ; page++ {
		if page > mutationCount {
			t.Fatal("pagination cursor did not reach tail")
		}
		refs, next, err := reg.ListPendingWorkflowActivationProjections(ctx, ns, cursor, pageLimit)
		if err != nil {
			t.Fatalf("ListPendingWorkflowActivationProjections page %d: %v", page, err)
		}
		if len(refs) > pageLimit {
			t.Fatalf("page %d returned %d refs, limit %d", page, len(refs), pageLimit)
		}
		for _, ref := range refs {
			got = append(got, ref.MutationID)
		}
		if next == 0 {
			break
		}
		if next == cursor {
			t.Fatalf("page %d did not advance cursor %d", page, cursor)
		}
		cursor = next
	}
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("paged mutation count = %d (%v), want %d (%v)", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("paged mutations = %v, want %v", got, want)
		}
	}
}

func TestWorkflowActivationProjectionNamespacesAreSortedAndAppendOnly(t *testing.T) {
	reg, _ := newTestRegistry(t)
	input := []namespace.Namespace{"projection-zeta", "projection-alpha"}
	requests := make(map[namespace.Namespace]backend.WorkflowReplaceRequest, len(input))
	for i, ns := range input {
		ctx := namespace.WithNamespace(context.Background(), ns)
		original := addDistributedAtomicWorkflow(t, ctx, reg, projectionWorkflowRecord(t, types.WorkflowID(fmt.Sprintf("namespace-id-%d", i)), fmt.Sprintf("namespace-%d", i), "v1", fmt.Sprintf("hash:%d", i)))
		request := backend.WorkflowReplaceRequest{
			MutationID:  fmt.Sprintf("namespace-mutation-%d", i),
			Expected:    backend.RevisionOfWorkflow(original),
			Replacement: original,
		}
		if _, err := reg.CompareAndReplaceWorkflow(ctx, request); err != nil {
			t.Fatalf("CompareAndReplaceWorkflow(%s): %v", ns, err)
		}
		requests[ns] = request
	}

	ackedNamespace := input[0]
	request := requests[ackedNamespace]
	claim, err := reg.ClaimWorkflowActivationProjection(context.Background(), ackedNamespace, request.MutationID, "namespace-worker", time.Minute)
	if err != nil || claim.State != backend.WorkflowActivationProjectionClaimAcquired {
		t.Fatalf("claim before namespace Ack = %#v, %v", claim, err)
	}
	if acked, err := reg.AckWorkflowActivationProjection(context.Background(), ackedNamespace, request.MutationID, "namespace-worker"); err != nil || !acked {
		t.Fatalf("namespace Ack = %v, %v", acked, err)
	}

	got, err := reg.ListWorkflowActivationProjectionNamespaces(context.Background())
	if err != nil {
		t.Fatalf("ListWorkflowActivationProjectionNamespaces: %v", err)
	}
	want := append([]namespace.Namespace(nil), input...)
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if len(got) != len(want) {
		t.Fatalf("namespaces = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("namespaces = %v, want sorted append-only %v", got, want)
		}
	}
}

func projectionWorkflowRecord(t *testing.T, id types.WorkflowID, suffix, version, definitionHash string) backend.WorkflowRecord {
	t.Helper()
	record := testRecord(t, suffix, definitionHash)
	record.ID = id
	record.Version = version
	record.Definition.Version = version
	record.Key = record.Namespace + "/" + record.Name + "@" + version
	record.AuditFingerprint = "audit:" + suffix
	// Exercise the persisted full-payload path: the registry compiles and stores
	// Graph from Definition, and claims must decode that durable graph again.
	record.Graph = nil
	return record
}

func createPendingProjection(
	t *testing.T,
	ctx context.Context,
	reg *Registry,
	mutationID string,
) (backend.WorkflowReplaceRequest, backend.WorkflowReplaceResult) {
	t.Helper()
	original := addDistributedAtomicWorkflow(t, ctx, reg, projectionWorkflowRecord(t, types.WorkflowID(mutationID+"-id"), mutationID, "v1", "hash:old"))
	replacement := original
	replacement.DefinitionHash = "hash:new"
	replacement.AuditFingerprint = "audit:new"
	replacement.RegistryRevision = 0
	request := backend.WorkflowReplaceRequest{
		MutationID:  mutationID,
		Expected:    backend.RevisionOfWorkflow(original),
		Replacement: replacement,
	}
	result, err := reg.CompareAndReplaceWorkflow(ctx, request)
	if err != nil {
		t.Fatalf("CompareAndReplaceWorkflow: %v", err)
	}
	if result.Status != backend.WorkflowReplaceReplaced {
		t.Fatalf("CompareAndReplaceWorkflow status = %q, want replaced", result.Status)
	}
	return request, result
}

func assertProjectionIntent(
	t *testing.T,
	got backend.WorkflowActivationProjectionIntent,
	wantNamespace namespace.Namespace,
	wantMutationID string,
	wantPrevious backend.WorkflowRevision,
	wantCurrent backend.WorkflowRecord,
) {
	t.Helper()
	if got.Namespace != wantNamespace || got.MutationID != wantMutationID {
		t.Fatalf("intent identity = %q/%q, want %q/%q", got.Namespace, got.MutationID, wantNamespace, wantMutationID)
	}
	if got.Previous != wantPrevious {
		t.Fatalf("intent previous = %#v, want %#v", got.Previous, wantPrevious)
	}
	assertProjectionWorkflowRecord(t, got.Current, wantCurrent)
}

func assertProjectionWorkflowRecord(t *testing.T, got, want backend.WorkflowRecord) {
	t.Helper()
	if got.ID != want.ID ||
		got.Key != want.Key ||
		got.Namespace != want.Namespace ||
		got.Name != want.Name ||
		got.Version != want.Version ||
		got.DefinitionHash != want.DefinitionHash ||
		got.RegistryRevision != want.RegistryRevision ||
		got.AuditFingerprint != want.AuditFingerprint {
		t.Fatalf("workflow identity/payload metadata = %#v, want %#v", got, want)
	}
	assertProjectionJSONEqual(t, "Definition", got.Definition, want.Definition)
	if got.Graph == nil || want.Graph == nil {
		t.Fatalf("workflow Graph nil mismatch: got nil=%v want nil=%v", got.Graph == nil, want.Graph == nil)
	}
	assertProjectionJSONEqual(t, "Graph", got.Graph, want.Graph)
}

func assertProjectionJSONEqual(t *testing.T, name string, got, want any) {
	t.Helper()
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal got %s: %v", name, err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal want %s: %v", name, err)
	}
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("%s JSON differs:\n got %s\nwant %s", name, gotJSON, wantJSON)
	}
}
