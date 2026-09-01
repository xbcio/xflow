package apiserver

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

var errInjectedWorkflowRemove = errors.New("injected workflow removal failure")

type removeErrorWorkflowRegistry struct {
	backend.WorkflowRegistry
	commitBeforeError bool
}

func (r *removeErrorWorkflowRegistry) RemoveWorkflow(ctx context.Context, id types.WorkflowID) error {
	if r.commitBeforeError {
		if err := r.WorkflowRegistry.RemoveWorkflow(ctx, id); err != nil {
			return err
		}
	}
	return errInjectedWorkflowRemove
}

type removeErrorWorkflowFixture struct {
	base   backend.WorkflowRegistry
	store  engine.EntryActivationStore
	server string
	id     types.WorkflowID
	before backend.WorkflowRecord
}

func newRemoveErrorWorkflowFixture(t *testing.T, commitBeforeError bool) removeErrorWorkflowFixture {
	t.Helper()

	base := local.New().WorkflowRegistry()
	registry := &removeErrorWorkflowRegistry{
		WorkflowRegistry:  base,
		commitBeforeError: commitBeforeError,
	}
	store := control.NewMemoryEntryActivationStore()
	srv, _ := newWorkflowReplaceDependencyTestServer(t, registry, store, nil)

	resp := postWorkflows(t, srv.URL, "tok-full", configuredTriggerWorkflow("topic-original"))
	if resp.StatusCode != http.StatusCreated {
		defer func() { _ = resp.Body.Close() }()
		t.Fatalf("register status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	var registered registerWorkflowResponse
	decodeEnvelope(t, resp, &registered)
	_ = resp.Body.Close()

	before, err := base.GetWorkflow(context.Background(), registered.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow before DELETE: %v", err)
	}
	assertWorkflowActivationDesired(t, store, registered.WorkflowID, "topic-original")
	assertWorkflowActivationRevision(t, store, registered.WorkflowID, before.RegistryRevision, true)

	return removeErrorWorkflowFixture{
		base:   base,
		store:  store,
		server: srv.URL,
		id:     registered.WorkflowID,
		before: before,
	}
}

func deleteWorkflowExpectInjectedError(t *testing.T, fixture removeErrorWorkflowFixture) {
	t.Helper()

	req, err := http.NewRequest(http.MethodDelete, fixture.server+"/v1/workflows/"+string(fixture.id), nil)
	if err != nil {
		t.Fatalf("new DELETE request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer tok-full")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE workflow: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("DELETE status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
	if env := decodeEnvelope(t, resp, nil); env.Code != "internal_error" {
		t.Fatalf("DELETE code = %q, want internal_error", env.Code)
	}
}

func assertWorkflowActivationRevision(t *testing.T, store engine.EntryActivationStore, id types.WorkflowID, revision uint64, desired bool) {
	t.Helper()

	act, ok, err := store.Get(context.Background(), engine.EntryActivationKey{
		Namespace:       namespace.Namespace("namespaceA"),
		WorkflowID:      id,
		WorkflowVersion: "v1",
		EntryUnitID:     "trig",
	})
	if err != nil {
		t.Fatalf("Get activation: %v", err)
	}
	if !ok {
		t.Fatal("activation missing")
	}
	if act.Desired != desired {
		t.Fatalf("activation desired = %v, want %v", act.Desired, desired)
	}
	if act.RegistryRevision != revision {
		t.Fatalf("activation registry revision = %d, want %d", act.RegistryRevision, revision)
	}
}

func TestDeleteWorkflowRestoresActivationWhenRemoveFailsBeforeMutation(t *testing.T) {
	fixture := newRemoveErrorWorkflowFixture(t, false)
	deleteWorkflowExpectInjectedError(t, fixture)

	after, err := fixture.base.GetWorkflow(context.Background(), fixture.id)
	if err != nil {
		t.Fatalf("GetWorkflow after failed pre-mutation removal: %v", err)
	}
	if after.ID != fixture.before.ID ||
		after.Key != fixture.before.Key ||
		after.DefinitionHash != fixture.before.DefinitionHash ||
		after.RegistryRevision != fixture.before.RegistryRevision {
		t.Fatalf("workflow revision changed after pre-mutation failure: before=%+v after=%+v", fixture.before, after)
	}
	assertWorkflowActivationDesired(t, fixture.store, fixture.id, "topic-original")
	assertWorkflowActivationRevision(t, fixture.store, fixture.id, fixture.before.RegistryRevision, true)
}

func TestDeleteWorkflowDoesNotRestoreActivationAfterCommittedRemoveError(t *testing.T) {
	fixture := newRemoveErrorWorkflowFixture(t, true)
	deleteWorkflowExpectInjectedError(t, fixture)

	if _, err := fixture.base.GetWorkflow(context.Background(), fixture.id); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflow after committed removal error = %v, want ErrWorkflowNotFound", err)
	}
	assertWorkflowActivationNotDesired(t, fixture.store, fixture.id)
	assertWorkflowActivationRevision(t, fixture.store, fixture.id, fixture.before.RegistryRevision, false)
}
