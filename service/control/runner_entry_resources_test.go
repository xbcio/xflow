package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// seedResultEngine is the EngineFacade stub for the seed entry-point tests: it
// embeds fakeControlEngine for every method the seed path does not touch, and
// lets a test choose the exact admission result (or error) the Core must map.
type seedResultEngine struct {
	*fakeControlEngine
	resp  engine.SeedExecutionFromEntryResponse
	err   error
	calls int
	last  engine.SeedExecutionFromEntryRequest
}

func newSeedResultEngine(resp engine.SeedExecutionFromEntryResponse, err error) *seedResultEngine {
	return &seedResultEngine{fakeControlEngine: &fakeControlEngine{}, resp: resp, err: err}
}

func (e *seedResultEngine) SeedExecutionFromEntry(_ context.Context, req engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error) {
	e.calls++
	e.last = req
	return e.resp, e.err
}

func validWireSeedRequest() protocol.SeedExecutionRequest {
	return protocol.SeedExecutionRequest{
		ProtocolVersion: protocol.EntrySeedProtocolVersion,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		EntryUnitID:     "node-1",
		AdmissionKey:    "key-1",
		Outcome:         "success",
	}
}

// TestEntrySeedRequestFromWireRejectsMissingRequiredFields pins the one
// validation both the HTTP and in-process seed faces run.
func TestEntrySeedRequestFromWireRejectsMissingRequiredFields(t *testing.T) {
	cases := map[string]func(*protocol.SeedExecutionRequest){
		"admission_key": func(r *protocol.SeedExecutionRequest) { r.AdmissionKey = "" },
		"workflow_id":   func(r *protocol.SeedExecutionRequest) { r.WorkflowID = "" },
		"entry_unit_id": func(r *protocol.SeedExecutionRequest) { r.EntryUnitID = "" },
		"outcome":       func(r *protocol.SeedExecutionRequest) { r.Outcome = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			req := validWireSeedRequest()
			mutate(&req)
			if _, err := EntrySeedRequestFromWire(req); !errors.Is(err, ErrEntrySeedRequestInvalid) {
				t.Fatalf("error = %v, want ErrEntrySeedRequestInvalid", err)
			}
		})
	}
}

// TestEntrySeedRequestFromWireComputesResultHashServerSide asserts the client
// cannot influence the identity of the admitted result: the hash is derived
// from outcome + exits inside the conversion.
func TestEntrySeedRequestFromWireComputesResultHashServerSide(t *testing.T) {
	req := validWireSeedRequest()
	req.Exits = []protocol.BoundaryExit{{NodeName: "node-1", Port: "out", Data: map[string]any{"ok": true}}}
	got, err := EntrySeedRequestFromWire(req)
	if err != nil {
		t.Fatalf("EntrySeedRequestFromWire: %v", err)
	}
	wantExits := []engine.BoundaryExit{{NodeName: "node-1", Port: "out", Data: map[string]any{"ok": true}}}
	want := engine.ComputeResultHash(engine.GroupOutcome("success"), wantExits)
	if got.ResultHash != want {
		t.Fatalf("ResultHash = %q, want the server-computed %q", got.ResultHash, want)
	}
	if got.AdmissionKey != engine.AdmissionKey("key-1") || got.WorkflowID != types.WorkflowID("wf-1") ||
		got.EntryUnitID != "node-1" || got.Outcome != engine.GroupOutcome("success") {
		t.Fatalf("converted request = %+v, want the wire fields carried through", got)
	}
}

// TestCoreSeedEntryForRunnerMapsEngineResults is the in-process half of the
// HTTP semantics: each engine result must produce the types.EntrySeedResponse
// that writeSeedAdmissionResult + HTTPEntrySeedRuntime decode produce together.
func TestCoreSeedEntryForRunnerMapsEngineResults(t *testing.T) {
	sentinel := errors.New("backend exploded")
	accepted := engine.SeedExecutionFromEntryResponse{
		State:       engine.AdmissionStateAccepted,
		ExecutionID: types.ExecutionID("exec-1"),
		Duplicate:   true,
	}
	cases := []struct {
		name  string
		resp  engine.SeedExecutionFromEntryResponse
		err   error
		check func(t *testing.T, got types.EntrySeedResponse, err error)
	}{
		{
			name: "accepted",
			resp: accepted,
			check: func(t *testing.T, got types.EntrySeedResponse, err error) {
				if err != nil {
					t.Fatalf("error = %v, want nil", err)
				}
				if !got.Accepted || !got.Duplicate || got.ExecutionID != "exec-1" {
					t.Fatalf("response = %+v, want Accepted+Duplicate with the execution id", got)
				}
			},
		},
		{
			name: "conflict",
			resp: engine.SeedExecutionFromEntryResponse{State: engine.AdmissionStateConflict, ExecutionID: "exec-2"},
			check: func(t *testing.T, got types.EntrySeedResponse, err error) {
				if err != nil {
					t.Fatalf("error = %v, want nil", err)
				}
				if !got.Conflict || got.ExecutionID != "exec-2" || got.Accepted {
					t.Fatalf("response = %+v, want Conflict with the execution id", got)
				}
			},
		},
		{
			name: "over_limit",
			resp: engine.SeedExecutionFromEntryResponse{State: engine.AdmissionStateOverLimit},
			check: func(t *testing.T, got types.EntrySeedResponse, err error) {
				if err != nil {
					t.Fatalf("error = %v, want nil", err)
				}
				if !got.Refused || got.QueueFull || got.RetryAfter != protocol.ConcurrencyRetryAfterSeconds*time.Second {
					t.Fatalf("response = %+v, want Refused with RetryAfter=%ds", got, protocol.ConcurrencyRetryAfterSeconds)
				}
			},
		},
		{
			name: "queue_full",
			resp: engine.SeedExecutionFromEntryResponse{State: engine.AdmissionStateQueueFull},
			check: func(t *testing.T, got types.EntrySeedResponse, err error) {
				if err != nil {
					t.Fatalf("error = %v, want nil", err)
				}
				if !got.Refused || !got.QueueFull || got.RetryAfter != protocol.ConcurrencyRetryAfterSeconds*time.Second {
					t.Fatalf("response = %+v, want Refused+QueueFull with RetryAfter=%ds", got, protocol.ConcurrencyRetryAfterSeconds)
				}
			},
		},
		{
			name: "stale generation stays matchable",
			err:  ErrStaleGeneration,
			check: func(t *testing.T, _ types.EntrySeedResponse, err error) {
				if !errors.Is(err, ErrStaleGeneration) {
					t.Fatalf("error = %v, want ErrStaleGeneration", err)
				}
			},
		},
		{
			name: "unknown workflow stays matchable",
			err:  ErrEntrySeedWorkflowUnknown,
			check: func(t *testing.T, _ types.EntrySeedResponse, err error) {
				if !errors.Is(err, ErrEntrySeedWorkflowUnknown) {
					t.Fatalf("error = %v, want ErrEntrySeedWorkflowUnknown", err)
				}
			},
		},
		{
			name: "concurrency unsupported stays matchable",
			err:  engine.ErrConcurrencyUnsupported,
			check: func(t *testing.T, _ types.EntrySeedResponse, err error) {
				if !errors.Is(err, engine.ErrConcurrencyUnsupported) {
					t.Fatalf("error = %v, want engine.ErrConcurrencyUnsupported", err)
				}
			},
		},
		{
			name: "concurrency queue unsupported stays matchable",
			err:  engine.ErrConcurrencyQueueUnsupported,
			check: func(t *testing.T, _ types.EntrySeedResponse, err error) {
				if !errors.Is(err, engine.ErrConcurrencyQueueUnsupported) {
					t.Fatalf("error = %v, want engine.ErrConcurrencyQueueUnsupported", err)
				}
			},
		},
		{
			name: "other errors are not treated as handled",
			err:  sentinel,
			check: func(t *testing.T, got types.EntrySeedResponse, err error) {
				if !errors.Is(err, sentinel) {
					t.Fatalf("error = %v, want the engine error returned", err)
				}
				if got.Accepted || got.Conflict || got.Refused {
					t.Fatalf("response = %+v, want the zero response alongside an error", got)
				}
			},
		},
		{
			name: "unknown state is a protocol disagreement",
			resp: engine.SeedExecutionFromEntryResponse{State: engine.AdmissionState("mystery")},
			check: func(t *testing.T, _ types.EntrySeedResponse, err error) {
				if err == nil {
					t.Fatal("unknown admission state returned no error; a caller would treat it as handled")
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng := newSeedResultEngine(tc.resp, tc.err)
			core := &Core{engine: eng}
			got, err := core.seedEntryForRunner(context.Background(), "runner-1", "", "default", validWireSeedRequest(), inProcessTransportInfo())
			tc.check(t, got, err)
		})
	}
}

// TestCoreSeedEntryForRunnerRejectsBadCredentials: a credential failure is the
// same ErrUnauthenticated every runner-protocol method returns, and the engine
// is never reached.
func TestCoreSeedEntryForRunnerRejectsBadCredentials(t *testing.T) {
	auth, err := NewStaticTokenAuthenticator("runner-", "tok", []string{"*"}, []string{"*"})
	if err != nil {
		t.Fatal(err)
	}
	eng := newSeedResultEngine(engine.SeedExecutionFromEntryResponse{State: engine.AdmissionStateAccepted}, nil)
	core := &Core{engine: eng, auth: auth}

	if _, err := core.seedEntryForRunner(context.Background(), "runner-1", "wrong", "default", validWireSeedRequest(), inProcessTransportInfo()); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("error = %v, want ErrUnauthenticated", err)
	}
	if eng.calls != 0 {
		t.Fatalf("engine calls = %d, want 0: authentication failed but the seed was admitted", eng.calls)
	}
	if _, err := core.seedEntryForRunner(context.Background(), "", "tok", "default", validWireSeedRequest(), inProcessTransportInfo()); !errors.Is(err, ErrRunnerIDRequired) {
		t.Fatalf("missing runner id error = %v, want ErrRunnerIDRequired", err)
	}
}

// TestCoreSeedEntryForRunnerEnforcesNamespaceEntitlement: the runner may only
// seed into a namespace its policy grants, and the granted namespace is what the
// engine receives.
func TestCoreSeedEntryForRunnerEnforcesNamespaceEntitlement(t *testing.T) {
	auth, err := NewStaticTokenAuthenticator("runner-", "tok", []string{"ns-ok"}, []string{"*"})
	if err != nil {
		t.Fatal(err)
	}
	eng := newSeedResultEngine(engine.SeedExecutionFromEntryResponse{State: engine.AdmissionStateAccepted}, nil)
	core := &Core{engine: eng, auth: auth}

	if _, err := core.seedEntryForRunner(context.Background(), "runner-1", "tok", "ns-ok", validWireSeedRequest(), inProcessTransportInfo()); err != nil {
		t.Fatalf("granted namespace: %v", err)
	}
	if eng.calls != 1 || eng.last.Namespace != namespace.Namespace("ns-ok") {
		t.Fatalf("engine received namespace %q after %d calls, want ns-ok", eng.last.Namespace, eng.calls)
	}

	if _, err := core.seedEntryForRunner(context.Background(), "runner-1", "tok", "ns-no", validWireSeedRequest(), inProcessTransportInfo()); !errors.Is(err, ErrAuthNamespaceDenied) {
		t.Fatalf("ungranted namespace error = %v, want ErrAuthNamespaceDenied", err)
	}
	if eng.calls != 1 {
		t.Fatalf("engine calls = %d, want still 1: the denied seed was admitted", eng.calls)
	}

	// An empty declaration means default, which this policy does not grant.
	if _, err := core.seedEntryForRunner(context.Background(), "runner-1", "tok", "", validWireSeedRequest(), inProcessTransportInfo()); !errors.Is(err, ErrAuthNamespaceDenied) {
		t.Fatalf("empty namespace error = %v, want ErrAuthNamespaceDenied", err)
	}
}

// TestCoreSeedEntryForRunnerResolvesEmptyNamespaceToDefault is the positive half
// of the empty-declaration rule: a policy that grants default admits it, and the
// engine sees "default" rather than "".
func TestCoreSeedEntryForRunnerResolvesEmptyNamespaceToDefault(t *testing.T) {
	auth, err := NewStaticTokenAuthenticator("runner-", "tok", []string{"default"}, []string{"*"})
	if err != nil {
		t.Fatal(err)
	}
	eng := newSeedResultEngine(engine.SeedExecutionFromEntryResponse{State: engine.AdmissionStateAccepted}, nil)
	core := &Core{engine: eng, auth: auth}

	if _, err := core.seedEntryForRunner(context.Background(), "runner-1", "tok", "", validWireSeedRequest(), inProcessTransportInfo()); err != nil {
		t.Fatalf("empty namespace with default grant: %v", err)
	}
	if eng.last.Namespace != namespace.Default {
		t.Fatalf("engine namespace = %q, want %q", eng.last.Namespace, namespace.Default)
	}
}

// TestCoreSeedEntryForRunnerGenerationFence proves the in-process path runs the
// SAME fence as the HTTP face: an activation assigned at generation N rejects a
// seed at N-1 before the engine sees it, and admits the exact generation.
func TestCoreSeedEntryForRunnerGenerationFence(t *testing.T) {
	ctx := context.Background()
	acts := NewMemoryEntryActivationStore()
	if err := acts.Upsert(ctx, engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      types.WorkflowID("wf-1"),
		WorkflowVersion: "v1",
		EntryUnitID:     "node-1",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	key := engine.EntryActivationKey{
		Namespace:       namespace.Default,
		WorkflowID:      types.WorkflowID("wf-1"),
		WorkflowVersion: "v1",
		EntryUnitID:     "node-1",
	}
	if ok, err := acts.Assign(ctx, key, "runner-1", "sess-1", 5, time.Now().Add(time.Hour)); err != nil || !ok {
		t.Fatalf("Assign = %v, %v; want the activation owned at generation 5", ok, err)
	}

	eng := newSeedResultEngine(engine.SeedExecutionFromEntryResponse{State: engine.AdmissionStateAccepted}, nil)
	eng.inspectErr = engine.ErrExecutionNotFound
	core := &Core{engine: eng, entryActivations: acts}

	stale := validWireSeedRequest()
	stale.Generation = 4
	if _, err := core.seedEntryForRunner(ctx, "runner-1", "", "default", stale, inProcessTransportInfo()); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale generation error = %v, want ErrStaleGeneration", err)
	}
	if eng.calls != 0 {
		t.Fatalf("engine calls = %d, want 0: a stale-generation seed was admitted", eng.calls)
	}

	current := validWireSeedRequest()
	current.Generation = 5
	if _, err := core.seedEntryForRunner(ctx, "runner-1", "", "default", current, inProcessTransportInfo()); err != nil {
		t.Fatalf("current generation: %v", err)
	}
	if eng.calls != 1 {
		t.Fatalf("engine calls = %d, want 1 for the current generation", eng.calls)
	}
}

// TestCoreSeedEntryForRunnerRequiredFieldsReachTheEngine is the entry-point
// wiring of the shared validation: a missing field never reaches the engine.
func TestCoreSeedEntryForRunnerRequiredFieldsReachTheEngine(t *testing.T) {
	eng := newSeedResultEngine(engine.SeedExecutionFromEntryResponse{State: engine.AdmissionStateAccepted}, nil)
	core := &Core{engine: eng}
	req := validWireSeedRequest()
	req.AdmissionKey = ""
	if _, err := core.seedEntryForRunner(context.Background(), "runner-1", "", "default", req, inProcessTransportInfo()); !errors.Is(err, ErrEntrySeedRequestInvalid) {
		t.Fatalf("error = %v, want ErrEntrySeedRequestInvalid", err)
	}
	if eng.calls != 0 {
		t.Fatalf("engine calls = %d, want 0", eng.calls)
	}
}

// recordingSupplyStore is a SupplyHintSource stub that records the arguments it
// was called with.
type recordingSupplyStore struct {
	resources map[string]*store.SupplyResource
	err       error
	calls     int
	lastNS    string
	lastName  string
}

func (s *recordingSupplyStore) GetSupply(_ context.Context, ns, name string) (*store.SupplyResource, error) {
	s.calls++
	s.lastNS, s.lastName = ns, name
	if s.err != nil {
		return nil, s.err
	}
	if rec, ok := s.resources[ns+"/"+name]; ok {
		return rec, nil
	}
	return nil, store.ErrNotFound
}

func TestCoreFetchSupplyForRunnerReturnsContent(t *testing.T) {
	rec := &store.SupplyResource{
		Namespace:   "default",
		Name:        "rules",
		Content:     []byte(`{"rules":[]}`),
		ContentHash: "sha256:abc",
		Revision:    7,
	}
	supplies := &recordingSupplyStore{resources: map[string]*store.SupplyResource{"default/rules": rec}}
	core := &Core{supplies: supplies}

	content, hash, revision, err := core.fetchSupplyForRunner(context.Background(), "runner-1", "", "default", "rules", inProcessTransportInfo())
	if err != nil {
		t.Fatalf("fetchSupplyForRunner: %v", err)
	}
	if string(content) != string(rec.Content) || hash != rec.ContentHash || revision != rec.Revision {
		t.Fatalf("got (%q, %q, %d), want the stored content/hash/revision", content, hash, revision)
	}
	if supplies.lastNS != "default" || supplies.lastName != "rules" {
		t.Fatalf("store called with (%q, %q), want (default, rules)", supplies.lastNS, supplies.lastName)
	}
}

func TestCoreFetchSupplyForRunnerNotFoundPaths(t *testing.T) {
	cases := []struct {
		name     string
		supplies *recordingSupplyStore
		core     *Core
		ns       string
		wantErr  error
	}{
		{
			name:     "absent in the requested namespace",
			supplies: &recordingSupplyStore{resources: map[string]*store.SupplyResource{"other/rules": {}}},
			core:     &Core{},
			ns:       "default",
			wantErr:  store.ErrNotFound,
		},
		{
			name:    "no supply store configured",
			core:    &Core{},
			ns:      "default",
			wantErr: store.ErrNotFound,
		},
	}
	cases[0].core.supplies = cases[0].supplies
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, _, err := tc.core.fetchSupplyForRunner(context.Background(), "runner-1", "", tc.ns, "rules", inProcessTransportInfo()); !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestCoreFetchSupplyForRunnerStoreFailureIsNormalized(t *testing.T) {
	supplies := &recordingSupplyStore{err: errors.New("database is down")}
	core := &Core{supplies: supplies}
	if _, _, _, err := core.fetchSupplyForRunner(context.Background(), "runner-1", "", "default", "rules", inProcessTransportInfo()); !errors.Is(err, ErrInternalServer) {
		t.Fatalf("error = %v, want ErrInternalServer so backend details do not reach the caller", err)
	}
}

func TestCoreFetchSupplyForRunnerRejectsBadCredentialsAndNamespace(t *testing.T) {
	auth, err := NewStaticTokenAuthenticator("runner-", "tok", []string{"ns-ok"}, []string{"*"})
	if err != nil {
		t.Fatal(err)
	}
	supplies := &recordingSupplyStore{resources: map[string]*store.SupplyResource{"ns-ok/rules": {}}}
	core := &Core{auth: auth, supplies: supplies}

	if _, _, _, err := core.fetchSupplyForRunner(context.Background(), "runner-1", "wrong", "ns-ok", "rules", inProcessTransportInfo()); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("bad credential error = %v, want ErrUnauthenticated", err)
	}
	if supplies.calls != 0 {
		t.Fatalf("store calls = %d, want 0 after an authentication failure", supplies.calls)
	}

	if _, _, _, err := core.fetchSupplyForRunner(context.Background(), "runner-1", "tok", "ns-no", "rules", inProcessTransportInfo()); !errors.Is(err, ErrAuthNamespaceDenied) {
		t.Fatalf("ungranted namespace error = %v, want ErrAuthNamespaceDenied", err)
	}
	if supplies.calls != 0 {
		t.Fatalf("store calls = %d, want 0 after a namespace denial", supplies.calls)
	}
}
