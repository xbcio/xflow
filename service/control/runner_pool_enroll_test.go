package control

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type poolEnrollFixture struct {
	core       *Core
	codes      *MemoryRegistrationCodeStore
	identities *MemoryIssuedIdentityStore
	pools      *MemoryRunnerPoolStore
	codeID     string
	plaintext  string
	pool       RunnerPool
}

func newPoolEnrollFixture(t *testing.T, mutate func(*RunnerPool)) *poolEnrollFixture {
	t.Helper()
	ctx := context.Background()
	pool := RunnerPool{
		ID:                "pool-a",
		Name:              "pool a",
		OwnerKind:         store.PoolOwnerTenant,
		OwnerNamespace:    "tenant-a",
		AllowedNamespaces: []string{string(namespace.Default), "team-a"},
		AllowedNodeTypes:  []string{"xflow.http"},
		Labels:            map[string]string{"fleet": "primary"},
		CreatedAt:         time.Unix(1_700_000_000, 0).UTC(),
	}
	if mutate != nil {
		mutate(&pool)
	}
	pools := NewMemoryRunnerPoolStore()
	if err := pools.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	codes := NewMemoryRegistrationCodeStore()
	codeID, plaintext, err := GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	if err := codes.Create(ctx, RegistrationCode{
		ID:                codeID,
		CodeHash:          HashSecret(plaintext),
		AllowedNamespaces: append([]string(nil), pool.AllowedNamespaces...),
		AllowedNodeTypes:  append([]string(nil), pool.AllowedNodeTypes...),
		OwnerNamespace:    pool.OwnerNamespace,
		PoolID:            pool.ID,
		CreatedAt:         pool.CreatedAt,
	}); err != nil {
		t.Fatalf("Create registration code: %v", err)
	}
	identities := NewMemoryIssuedIdentityStore()
	return &poolEnrollFixture{
		core: &Core{
			registrationCodes: codes,
			issuedIdentities:  identities,
			enrollLimiter:     newEnrollLimiter(defaultEnrollFailureLimit, defaultEnrollLockout),
			pools:             pools,
			rotationGrace:     time.Minute,
		},
		codes: codes, identities: identities, pools: pools,
		codeID: codeID, plaintext: plaintext, pool: pool,
	}
}

func (f *poolEnrollFixture) enroll(t *testing.T, systemID, instanceUID string, namespaces []string) protocol.EnrollResponse {
	t.Helper()
	resp, err := f.core.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: f.plaintext,
		SystemID:         systemID,
		InstanceUID:      instanceUID,
		Namespaces:       namespaces,
		NodeTypes:        []string{"xflow.http"},
	}, TransportInfo{SourceIP: "192.0.2.10"})
	if err != nil {
		t.Fatalf("Enroll(system_id=%q): %v", systemID, err)
	}
	return resp
}

func (f *poolEnrollFixture) audit(t *testing.T) []EnrollAuditRecord {
	t.Helper()
	records, err := f.codes.EnrollAudit(context.Background(), f.codeID, OwnerScope{All: true})
	if err != nil {
		t.Fatalf("EnrollAudit: %v", err)
	}
	return records
}

func TestPoolEnrollFirstAndReenrollRotatesCredential(t *testing.T) {
	f := newPoolEnrollFixture(t, nil)
	first := f.enroll(t, "system-a", "instance-1", []string{"team-a"})
	if first.RunnerID == "" || first.Token == "" || first.CredentialGeneration != 1 {
		t.Fatalf("first response = %+v, want identity generation 1", first)
	}
	if !reflect.DeepEqual(first.Namespaces, []string{"team-a"}) || !reflect.DeepEqual(first.Labels, f.pool.Labels) {
		t.Fatalf("first response namespaces/labels = %v/%v, want [team-a]/%v", first.Namespaces, first.Labels, f.pool.Labels)
	}
	stored, ok, err := f.identities.Lookup(context.Background(), first.RunnerID)
	if err != nil || !ok {
		t.Fatalf("Lookup(first): ok=%v err=%v", ok, err)
	}
	if stored.PoolID != f.pool.ID || stored.CredentialGeneration != 1 {
		t.Fatalf("stored first identity = %+v, want pool %q generation 1", stored, f.pool.ID)
	}

	second := f.enroll(t, "system-a", "instance-2", []string{"team-a"})
	if second.RunnerID != first.RunnerID || second.CredentialGeneration != 2 || second.Token == first.Token {
		t.Fatalf("reenroll response = %+v, first = %+v; want same ID, generation 2, new token", second, first)
	}
	stored, ok, err = f.identities.Lookup(context.Background(), first.RunnerID)
	if err != nil || !ok {
		t.Fatalf("Lookup(reenroll): ok=%v err=%v", ok, err)
	}
	auth := NewIssuedIdentityAuthenticator(f.identities)
	auth.now = func() time.Time { return stored.PreviousTokenValidUntil.Add(-time.Nanosecond) }
	if _, err := auth.AuthenticateOngoing(first.RunnerID, first.Token, TransportInfo{}); err != nil {
		t.Fatalf("previous token rejected inside grace: %v", err)
	}
	if _, err := auth.AuthenticateOngoing(first.RunnerID, second.Token, TransportInfo{}); err != nil {
		t.Fatalf("current token rejected inside grace: %v", err)
	}
	auth.now = func() time.Time { return stored.PreviousTokenValidUntil }
	if _, err := auth.AuthenticateOngoing(first.RunnerID, first.Token, TransportInfo{}); !errors.Is(err, ErrAuthUnknownToken) {
		t.Fatalf("previous token at grace boundary error = %v, want ErrAuthUnknownToken", err)
	}
	if _, err := auth.AuthenticateOngoing(first.RunnerID, second.Token, TransportInfo{}); err != nil {
		t.Fatalf("current token rejected after grace: %v", err)
	}

	records := f.audit(t)
	if got := []string{records[0].Reason, records[1].Reason}; !reflect.DeepEqual(got, []string{"first", "reenroll"}) {
		t.Fatalf("audit reasons = %v, want [first reenroll]", got)
	}
}

func TestPoolEnrollConcurrentReenrollSameSystemID(t *testing.T) {
	f := newPoolEnrollFixture(t, nil)
	first := f.enroll(t, "system-a", "instance-1", []string{"team-a"})

	start := make(chan struct{})
	responses := make(chan protocol.EnrollResponse, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(instance string) {
			defer wg.Done()
			<-start
			resp, err := f.core.Enroll(context.Background(), protocol.EnrollRequest{
				RegistrationCode: f.plaintext,
				SystemID:         "system-a",
				InstanceUID:      instance,
				Namespaces:       []string{"team-a"},
				NodeTypes:        []string{"xflow.http"},
			}, TransportInfo{SourceIP: "192.0.2." + instance})
			responses <- resp
			errs <- err
		}(string(rune('1' + i)))
	}
	close(start)
	wg.Wait()
	close(responses)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent reenroll: %v", err)
		}
	}
	var generations []int
	for resp := range responses {
		if resp.RunnerID != first.RunnerID {
			t.Fatalf("concurrent runner ID = %q, want %q", resp.RunnerID, first.RunnerID)
		}
		generations = append(generations, int(resp.CredentialGeneration))
	}
	sort.Ints(generations)
	if !reflect.DeepEqual(generations, []int{2, 3}) {
		t.Fatalf("concurrent generations = %v, want [2 3]", generations)
	}
}

func TestPoolEnrollHealsMissingIssuedIdentity(t *testing.T) {
	f := newPoolEnrollFixture(t, nil)
	result, err := f.pools.EnrollInstance(context.Background(), store.EnrollInstanceRequest{
		PoolID: f.pool.ID, SystemID: "system-heal", InstanceUID: "old",
		CandidateRunnerID: "runner-healed", Now: time.Now().UTC(),
	})
	if err != nil || !result.Created {
		t.Fatalf("seed EnrollInstance: created=%v err=%v", result.Created, err)
	}
	resp := f.enroll(t, "system-heal", "new", []string{"team-a"})
	if resp.RunnerID != "runner-healed" || resp.CredentialGeneration != 1 {
		t.Fatalf("healed response = %+v, want runner-healed generation 1", resp)
	}
	records := f.audit(t)
	if len(records) != 1 || !records[0].Success || records[0].Reason != "healed" {
		t.Fatalf("healed audit = %+v, want one successful healed record", records)
	}
}

func TestPoolEnrollRequiresSystemIDAndInstanceUID(t *testing.T) {
	tests := []struct {
		name        string
		systemID    string
		instanceUID string
		wantReason  string
	}{
		{name: "system id", instanceUID: "instance-1", wantReason: "system id required"},
		{name: "instance uid", systemID: "system-a", wantReason: "instance uid required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newPoolEnrollFixture(t, nil)
			_, err := f.core.Enroll(context.Background(), protocol.EnrollRequest{
				RegistrationCode: f.plaintext, SystemID: tc.systemID, InstanceUID: tc.instanceUID,
				Namespaces: []string{"team-a"}, NodeTypes: []string{"xflow.http"},
			}, TransportInfo{SourceIP: "192.0.2.20"})
			if !errors.Is(err, ErrEnrollRejected) {
				t.Fatalf("Enroll error = %v, want ErrEnrollRejected", err)
			}
			if got := f.audit(t)[0].Reason; got != tc.wantReason {
				t.Fatalf("audit reason = %q, want %q", got, tc.wantReason)
			}
		})
	}
}

func TestPoolEnrollNamespaceSelection(t *testing.T) {
	many := []string{string(namespace.Default)}
	for i := 0; i < store.MaxInheritedNamespaces; i++ {
		many = append(many, "team-"+string(rune('a'+i)))
	}
	tests := []struct {
		name    string
		mutate  func(*RunnerPool)
		want    []string
		wantErr bool
	}{
		{
			name: "inherits concrete ceiling",
			mutate: func(pool *RunnerPool) {
				pool.InheritNamespaces = true
				pool.AllowedNamespaces = []string{"team-a", "team-b"}
			},
			want: []string{"team-a", "team-b"},
		},
		{name: "defaults when inheritance disabled", want: []string{string(namespace.Default)}},
		{
			name:    "wildcard requires declaration",
			mutate:  func(pool *RunnerPool) { pool.AllowedNamespaces = []string{"*"} },
			wantErr: true,
		},
		{
			name:    "concrete ceiling without default requires declaration",
			mutate:  func(pool *RunnerPool) { pool.AllowedNamespaces = []string{"team-a"} },
			wantErr: true,
		},
		{
			name: "inheritance cap falls back to default",
			mutate: func(pool *RunnerPool) {
				pool.InheritNamespaces = true
				pool.AllowedNamespaces = many
			},
			want: []string{string(namespace.Default)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newPoolEnrollFixture(t, tc.mutate)
			resp, err := f.core.Enroll(context.Background(), protocol.EnrollRequest{
				RegistrationCode: f.plaintext, SystemID: "system-a", InstanceUID: "instance-a", NodeTypes: []string{"xflow.http"},
			}, TransportInfo{SourceIP: "192.0.2.30"})
			if tc.wantErr {
				if !errors.Is(err, ErrEnrollRejected) {
					t.Fatalf("Enroll error = %v, want ErrEnrollRejected", err)
				}
				if got := f.audit(t)[0].Reason; got != "namespaces required" {
					t.Fatalf("audit reason = %q, want namespaces required", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Enroll: %v", err)
			}
			if !reflect.DeepEqual(resp.Namespaces, tc.want) {
				t.Fatalf("Namespaces = %v, want %v", resp.Namespaces, tc.want)
			}
		})
	}
}

func TestPoolEnrollRejectsPausedOrUnavailablePool(t *testing.T) {
	t.Run("paused", func(t *testing.T) {
		f := newPoolEnrollFixture(t, func(pool *RunnerPool) { pool.Paused = true })
		_, err := f.core.Enroll(context.Background(), protocol.EnrollRequest{
			RegistrationCode: f.plaintext, SystemID: "system-a", Namespaces: []string{"team-a"}, NodeTypes: []string{"xflow.http"},
		}, TransportInfo{SourceIP: "192.0.2.40"})
		if !errors.Is(err, ErrEnrollRejected) {
			t.Fatalf("paused error = %v, want ErrEnrollRejected", err)
		}
		if got := f.audit(t)[0].Reason; got != "runner pool paused" {
			t.Fatalf("audit reason = %q, want runner pool paused", got)
		}
	})

	t.Run("store not configured", func(t *testing.T) {
		f := newPoolEnrollFixture(t, nil)
		f.core.pools = nil
		_, err := f.core.Enroll(context.Background(), protocol.EnrollRequest{
			RegistrationCode: f.plaintext, SystemID: "system-a", Namespaces: []string{"team-a"}, NodeTypes: []string{"xflow.http"},
		}, TransportInfo{SourceIP: "192.0.2.41"})
		if !errors.Is(err, ErrEnrollRejected) {
			t.Fatalf("missing pool store error = %v, want ErrEnrollRejected", err)
		}
	})
}

func TestEnrollRejectsRegistrationCodeWithoutPool(t *testing.T) {
	codes := NewMemoryRegistrationCodeStore()
	ids := NewMemoryIssuedIdentityStore()
	codeID, plaintext, err := GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	if err := codes.Create(context.Background(), RegistrationCode{
		ID: codeID, CodeHash: HashSecret(plaintext), AllowedNamespaces: []string{"team-a"},
		AllowedNodeTypes: []string{"xflow.http"}, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	core := &Core{registrationCodes: codes, issuedIdentities: ids,
		enrollLimiter: newEnrollLimiter(defaultEnrollFailureLimit, defaultEnrollLockout)}
	_, err = core.Enroll(context.Background(), protocol.EnrollRequest{
		RegistrationCode: plaintext, SystemID: "system-a", InstanceUID: "instance-a",
		Namespaces: []string{"team-a"}, NodeTypes: []string{"xflow.http"},
	}, TransportInfo{SourceIP: "192.0.2.50"})
	if !errors.Is(err, ErrEnrollRejected) {
		t.Fatalf("Enroll error = %v, want ErrEnrollRejected", err)
	}
	audit, auditErr := codes.EnrollAudit(context.Background(), codeID, OwnerScope{All: true})
	if auditErr != nil || len(audit) != 1 || audit[0].Reason != "registration code has no pool" {
		t.Fatalf("audit = %+v, err=%v", audit, auditErr)
	}
}

type countingPoolStore struct {
	RunnerPoolStore
	getCalls int
}

func (s *countingPoolStore) GetPool(ctx context.Context, id string, scope OwnerScope) (RunnerPool, error) {
	s.getCalls++
	return s.RunnerPoolStore.GetPool(ctx, id, scope)
}

func newPoolRegisterCore(t *testing.T) (*Core, *MemoryRunnerDirectory, string) {
	t.Helper()
	ctx := context.Background()
	pools := NewMemoryRunnerPoolStore()
	if err := pools.CreatePool(ctx, RunnerPool{
		ID: "pool-register", OwnerKind: store.PoolOwnerTenant, OwnerNamespace: "tenant-a",
		AllowedNamespaces: []string{string(namespace.Default)}, AllowedNodeTypes: []string{"*"},
		Labels: map[string]string{"fleet": "primary", "region": "west"},
	}); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	identities := NewMemoryIssuedIdentityStore()
	const token = "issued-token"
	if err := identities.Issue(ctx, IssuedIdentity{
		RunnerID: "runner-pool", TokenHash: HashSecret(token), PoolID: "pool-register", CredentialGeneration: 1,
		Scope: RunnerPolicy{Name: "runner-pool", AllowedNamespaces: []string{string(namespace.Default)}, AllowedNodeTypes: []string{"*"}},
	}); err != nil {
		t.Fatalf("Issue: %v", err)
	}
	directory := NewMemoryRunnerDirectory()
	return &Core{runners: directory, auth: NewIssuedIdentityAuthenticator(identities), pools: pools}, directory, token
}

func TestRegisterRequiresInstanceUIDAcrossTransports(t *testing.T) {
	core := &Core{runners: NewMemoryRunnerDirectory()}
	_, err := core.register(context.Background(), protocol.RegisterRunnerRequest{
		RunnerID: "runner-no-instance", Concurrency: 1,
	}, TransportInfo{})
	if !errors.Is(err, ErrInstanceUIDRequired) {
		t.Fatalf("register error = %v, want ErrInstanceUIDRequired", err)
	}
	if got := normalizeRunnerError(err, nil, "register"); !errors.Is(got, ErrInstanceUIDRequired) {
		t.Fatalf("normalizeRunnerError = %v, want ErrInstanceUIDRequired", got)
	}
	recorder := httptest.NewRecorder()
	writeRunnerError(recorder, err)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("HTTP status = %d, want 400", recorder.Code)
	}
	if got := status.Code(runnerStatus(err)); got != codes.InvalidArgument {
		t.Fatalf("gRPC status = %v, want InvalidArgument", got)
	}
}

func TestRegisterMergesPoolLabelsAndRejectsConflict(t *testing.T) {
	t.Run("merge", func(t *testing.T) {
		core, directory, token := newPoolRegisterCore(t)
		_, err := core.register(context.Background(), protocol.RegisterRunnerRequest{
			RunnerID: "runner-pool", InstanceUID: "instance-pool", AuthToken: token, Concurrency: 1,
			Labels: map[string]string{"local": "yes", "fleet": "primary"},
		}, TransportInfo{})
		if err != nil {
			t.Fatalf("register: %v", err)
		}
		snapshot, ok := directory.Runner(context.Background(), "runner-pool")
		if !ok {
			t.Fatal("registered runner not found")
		}
		want := map[string]string{"local": "yes", "fleet": "primary", "region": "west"}
		if !reflect.DeepEqual(snapshot.Labels, want) {
			t.Fatalf("labels = %v, want %v", snapshot.Labels, want)
		}
	})

	t.Run("conflict", func(t *testing.T) {
		core, _, token := newPoolRegisterCore(t)
		_, err := core.register(context.Background(), protocol.RegisterRunnerRequest{
			RunnerID: "runner-pool", InstanceUID: "instance-pool", AuthToken: token, Concurrency: 1,
			Labels: map[string]string{"fleet": "other"},
		}, TransportInfo{})
		if !errors.Is(err, ErrLabelConflict) {
			t.Fatalf("register conflict error = %v, want ErrLabelConflict", err)
		}
		if got := normalizeRunnerError(err, nil, "register"); !errors.Is(got, ErrLabelConflict) {
			t.Fatalf("normalizeRunnerError = %v, want ErrLabelConflict", got)
		}
		recorder := httptest.NewRecorder()
		writeRunnerError(recorder, err)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("HTTP status = %d, want 400", recorder.Code)
		}
		if got := status.Code(runnerStatus(err)); got != codes.InvalidArgument {
			t.Fatalf("gRPC status = %v, want InvalidArgument", got)
		}
	})
}

func TestRegisterStaticAuthenticatorDoesNotReadPool(t *testing.T) {
	ctx := context.Background()
	pools := NewMemoryRunnerPoolStore()
	if err := pools.CreatePool(ctx, RunnerPool{
		ID: "pool-static", OwnerKind: store.PoolOwnerTenant, OwnerNamespace: "tenant-a",
		Labels: map[string]string{"server": "owned"},
	}); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	counting := &countingPoolStore{RunnerPoolStore: pools}
	identities := NewMemoryIssuedIdentityStore()
	if err := identities.Issue(ctx, IssuedIdentity{
		RunnerID: "runner-static", TokenHash: HashSecret("issued-token"), PoolID: "pool-static",
		Scope: RunnerPolicy{AllowedNodeTypes: []string{"*"}}, CredentialGeneration: 1,
	}); err != nil {
		t.Fatalf("Issue: %v", err)
	}
	static, err := NewFilePolicyStoreFromConfig(PolicyConfig{
		Version: 1,
		Runners: []PolicyEntry{{
			Name: "static", IDPrefix: "runner-", Token: "static-token",
			AllowedNamespaces: nil, AllowedNodeTypes: []string{"*"},
		}},
	}, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStoreFromConfig: %v", err)
	}
	directory := NewMemoryRunnerDirectory()
	core := &Core{
		runners: directory,
		auth:    NewMultiAuthenticator(static, NewIssuedIdentityAuthenticator(identities)),
		pools:   counting,
	}
	_, err = core.register(ctx, protocol.RegisterRunnerRequest{
		RunnerID: "runner-static", InstanceUID: "instance-static", AuthToken: "static-token", Concurrency: 1,
		Labels: map[string]string{"client": "kept"},
	}, TransportInfo{})
	if err != nil {
		t.Fatalf("static register: %v", err)
	}
	if counting.getCalls != 0 {
		t.Fatalf("pool GetPool calls = %d, want 0 for static-auth source", counting.getCalls)
	}
	snapshot, _ := directory.Runner(ctx, "runner-static")
	if !reflect.DeepEqual(snapshot.Labels, map[string]string{"client": "kept"}) {
		t.Fatalf("static labels = %v, want unchanged client label", snapshot.Labels)
	}
}

func TestRunnerPoolOptionsAndControlPlaneWiring(t *testing.T) {
	pools := NewMemoryRunnerPoolStore()
	srv := NewServer(nil, NewMemoryRunnerDirectory(), WithRunnerPools(pools))
	if srv.core.pools != pools || srv.core.rotationGrace != time.Minute {
		t.Fatalf("default server pools/grace = %T/%v, want supplied pool store/1m", srv.core.pools, srv.core.rotationGrace)
	}
	custom := NewServer(nil, NewMemoryRunnerDirectory(), WithEnrollRotationGrace(5*time.Second))
	if custom.core.rotationGrace != 5*time.Second {
		t.Fatalf("custom rotation grace = %v, want 5s", custom.core.rotationGrace)
	}

	cp, err := NewControlPlane(Config{Backend: backendlocal.New(), RunnerPools: pools})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	if cp.httpServer.core.pools != pools || cp.grpcServer.core.pools != pools {
		t.Fatalf("RunnerPools not wired to both cores: http=%T grpc=%T", cp.httpServer.core.pools, cp.grpcServer.core.pools)
	}
}
