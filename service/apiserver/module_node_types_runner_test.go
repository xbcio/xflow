package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// runnerAggregate builds one aggregated runner entry whose JSON is the
// encoding/json form of a minimal descriptor for typ.
func runnerAggregate(t *testing.T, typ string, version int, label string, pools []string, namespaces ...namespace.Namespace) control.AggregatedDescriptor {
	t.Helper()
	raw, err := json.Marshal(types.Descriptor{
		Type: typ, DisplayName: label,
		Params: []types.ParamSpec{{Name: "limit", Type: types.ParamNumber, Default: 10}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return control.AggregatedDescriptor{Type: typ, Version: version, Hash: label, JSON: raw, Pools: pools, Namespaces: namespaces}
}

// fakeRunnerNodeTypes returns a source serving entries and recording the
// namespaces it was asked for.
func fakeRunnerNodeTypes(calls *[]namespace.Namespace, err error, entries ...control.AggregatedDescriptor) runnerNodeTypeSource {
	return func(_ context.Context, ns namespace.Namespace) ([]control.AggregatedDescriptor, error) {
		if calls != nil {
			*calls = append(*calls, ns)
		}
		if err != nil {
			return nil, err
		}
		return entries, nil
	}
}

func listNodeTypeSchemas(t *testing.T, h http.Handler, path string) []nodeFormSchema {
	t.Helper()
	rec := getNodeTypes(t, h, path, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", path, rec.Code, rec.Body.String())
	}
	var resp nodeTypesResponse
	if err := json.Unmarshal(envelopeData(t, rec), &resp); err != nil {
		t.Fatal(err)
	}
	return resp.NodeTypes
}

func findNodeTypeSchema(schemas []nodeFormSchema, typ string, version int) (nodeFormSchema, bool) {
	for _, s := range schemas {
		if s.NodeType == typ && s.NodeVersion == version {
			return s, true
		}
	}
	return nodeFormSchema{}, false
}

func TestNodeTypesMergesRunnerEntries(t *testing.T) {
	var calls []namespace.Namespace
	m := &workflowControlModule{
		nodeDescriptors: fakeDescriptors(customDescriptor(1, "One")),
		runnerNodeTypes: fakeRunnerNodeTypes(&calls, nil,
			runnerAggregate(t, "acme.analyse", 1, "Analyse v1", []string{"gpu"}, namespace.Default),
			runnerAggregate(t, "acme.analyse", 2, "Analyse v2", []string{"cpu", "gpu"}, namespace.Default),
			runnerAggregate(t, "acme.nopool", 1, "No pool", nil, "*"),
		),
	}
	mux := nodeTypesMux(m)
	schemas := listNodeTypeSchemas(t, mux, "/v1/node-types?namespace=default")
	var got []string
	for _, s := range schemas {
		got = append(got, fmt.Sprintf("%s@%d", s.NodeType, s.NodeVersion))
	}
	if want := []string{"acme.analyse@1", "acme.analyse@2", "acme.nopool@1", "custom.x@1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("merged order = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(calls, []namespace.Namespace{namespace.Default}) {
		t.Errorf("source asked for %v, want [default]", calls)
	}
	v2, _ := findNodeTypeSchema(schemas, "acme.analyse", 2)
	if v2.Source != nodeFormSourceRunner || !reflect.DeepEqual(v2.RunnerPools, []string{"cpu", "gpu"}) || v2.DisplayName != "Analyse v2" {
		t.Errorf("acme.analyse@2 = source %q pools %v name %q", v2.Source, v2.RunnerPools, v2.DisplayName)
	}
	if len(v2.Fields) != 1 || v2.Fields[0].Name != "limit" {
		t.Errorf("acme.analyse@2 fields = %+v, want the reported limit param", v2.Fields)
	}
	server, _ := findNodeTypeSchema(schemas, "custom.x", 1)
	if server.Source != "" || server.RunnerPools != nil {
		t.Errorf("server entry carries source %q pools %v", server.Source, server.RunnerPools)
	}

	// The wire keys: source/runner_pools on runner entries, absent otherwise
	// (a pool-less runner has no runner_pools key).
	raw := envelopeData(t, getNodeTypes(t, mux, "/v1/node-types?namespace=default", nil))
	var wire struct {
		NodeTypes []map[string]json.RawMessage `json:"node_types"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	for _, s := range wire.NodeTypes {
		var typ string
		_ = json.Unmarshal(s["node_type"], &typ)
		_, hasSource := s["source"]
		_, hasPools := s["runner_pools"]
		switch typ {
		case "acme.analyse":
			if string(s["source"]) != `"runner"` || !hasPools {
				t.Errorf("%s: source %s, runner_pools present %v", typ, s["source"], hasPools)
			}
		case "acme.nopool":
			if !hasSource || hasPools {
				t.Errorf("%s: source present %v, runner_pools present %v; want source only", typ, hasSource, hasPools)
			}
		default:
			if hasSource || hasPools {
				t.Errorf("%s: server entry has source/runner_pools keys", typ)
			}
		}
	}

	for path, wantVersion := range map[string]int{
		"/v1/node-types/acme.analyse?namespace=default":           2,
		"/v1/node-types/acme.analyse?namespace=default&version=1": 1,
	} {
		rec := getNodeTypes(t, mux, path, nil)
		var s nodeFormSchema
		if err := json.Unmarshal(envelopeData(t, rec), &s); err != nil {
			t.Fatal(err)
		}
		if s.NodeVersion != wantVersion || s.Source != nodeFormSourceRunner {
			t.Errorf("%s: got @%d source %q, want @%d runner", path, s.NodeVersion, s.Source, wantVersion)
		}
	}
	if rec := getNodeTypes(t, mux, "/v1/node-types/acme.analyse", nil); rec.Code != http.StatusNotFound {
		t.Errorf("unscoped get of a runner type: status %d, want 404", rec.Code)
	}
}

func TestNodeTypesServerWinsByType(t *testing.T) {
	m := &workflowControlModule{
		nodeDescriptors: fakeDescriptors(customDescriptor(1, "Server")),
		runnerNodeTypes: fakeRunnerNodeTypes(nil, nil,
			// Same type as a server type, same and extra version.
			runnerAggregate(t, "custom.x", 1, "Runner shadow", []string{"p"}, namespace.Default),
			runnerAggregate(t, "custom.x", 2, "Runner extra", []string{"p"}, namespace.Default),
			// A builtin prefix the server registry here does not even carry.
			runnerAggregate(t, "xflow.fake", 1, "Builtin spoof", []string{"p"}, namespace.Default),
			// A builtin the real registry carries.
			runnerAggregate(t, "xflow.wait", 9, "Wait spoof", []string{"p"}, namespace.Default),
		),
	}
	mux := nodeTypesMux(m)
	schemas := listNodeTypeSchemas(t, mux, "/v1/node-types?namespace=default")
	if len(schemas) != 1 || schemas[0].NodeType != "custom.x" || schemas[0].NodeVersion != 1 || schemas[0].Source != "" || schemas[0].DisplayName != "Server" {
		t.Fatalf("schemas = %+v, want only the server custom.x@1", schemas)
	}
	for _, path := range []string{
		"/v1/node-types/custom.x?namespace=default&version=2",
		"/v1/node-types/xflow.fake?namespace=default",
	} {
		if rec := getNodeTypes(t, mux, path, nil); rec.Code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404", path, rec.Code)
		}
	}
	var s nodeFormSchema
	if err := json.Unmarshal(envelopeData(t, getNodeTypes(t, mux, "/v1/node-types/custom.x?namespace=default", nil)), &s); err != nil {
		t.Fatal(err)
	}
	if s.NodeVersion != 1 || s.Source != "" {
		t.Errorf("latest custom.x = @%d source %q, want the server's @1", s.NodeVersion, s.Source)
	}
}

func TestNodeTypesWithoutNamespaceUnchanged(t *testing.T) {
	var calls []namespace.Namespace
	withRunner := nodeTypesMux(&workflowControlModule{
		nodeDescriptors: fakeDescriptors(customDescriptor(1, "One")),
		runnerNodeTypes: fakeRunnerNodeTypes(&calls, nil, runnerAggregate(t, "acme.analyse", 1, "A", []string{"p"}, namespace.Default)),
	})
	without := nodeTypesMux(&workflowControlModule{nodeDescriptors: fakeDescriptors(customDescriptor(1, "One"))})
	for _, path := range []string{"/v1/node-types", "/v1/node-types/custom.x"} {
		got := getNodeTypes(t, withRunner, path, nil)
		want := getNodeTypes(t, without, path, nil)
		if string(envelopeData(t, got)) != string(envelopeData(t, want)) || got.Header().Get("ETag") != want.Header().Get("ETag") {
			t.Errorf("%s: unscoped response differs with a runner source set", path)
		}
	}
	if len(calls) != 0 {
		t.Errorf("unscoped requests consulted the runner source: %v", calls)
	}
}

func TestNodeTypesExcludesEntriesOutsideNamespace(t *testing.T) {
	m := &workflowControlModule{
		nodeDescriptors: fakeDescriptors(),
		runnerNodeTypes: fakeRunnerNodeTypes(nil, nil,
			runnerAggregate(t, "acme.other", 1, "Other tenant", []string{"p"}, "tenant-b"),
			runnerAggregate(t, "acme.mine", 1, "Mine", []string{"p"}, namespace.Default, "tenant-b"),
			runnerAggregate(t, "acme.shared", 1, "Shared", []string{"p"}, "*"),
		),
	}
	schemas := listNodeTypeSchemas(t, nodeTypesMux(m), "/v1/node-types?namespace=default")
	var got []string
	for _, s := range schemas {
		got = append(got, s.NodeType)
	}
	if want := []string{"acme.mine", "acme.shared"}; !reflect.DeepEqual(got, want) {
		t.Errorf("types = %v, want %v", got, want)
	}
}

func TestNodeTypesRunnerSourceErrorDegrades(t *testing.T) {
	m := &workflowControlModule{
		nodeDescriptors: fakeDescriptors(customDescriptor(1, "One")),
		runnerNodeTypes: fakeRunnerNodeTypes(nil, errors.New("directory down")),
	}
	schemas := listNodeTypeSchemas(t, nodeTypesMux(m), "/v1/node-types?namespace=default")
	if len(schemas) != 1 || schemas[0].NodeType != "custom.x" {
		t.Errorf("schemas = %+v, want the server entries only", schemas)
	}
}

func TestNodeTypesSkipsUndecodableRunnerEntry(t *testing.T) {
	bad := runnerAggregate(t, "acme.bad", 1, "Bad", nil, namespace.Default)
	bad.JSON = json.RawMessage(`{"Type":`)
	m := &workflowControlModule{
		nodeDescriptors: fakeDescriptors(),
		runnerNodeTypes: fakeRunnerNodeTypes(nil, nil, bad, runnerAggregate(t, "acme.good", 1, "Good", nil, namespace.Default)),
	}
	schemas := listNodeTypeSchemas(t, nodeTypesMux(m), "/v1/node-types?namespace=default")
	if len(schemas) != 1 || schemas[0].NodeType != "acme.good" {
		t.Errorf("schemas = %+v, want only acme.good", schemas)
	}
}

func TestNodeTypesETagChangesWithRunnerTypes(t *testing.T) {
	m := &workflowControlModule{nodeDescriptors: fakeDescriptors(customDescriptor(1, "One")), runnerNodeTypes: fakeRunnerNodeTypes(nil, nil)}
	mux := nodeTypesMux(m)
	before := getNodeTypes(t, mux, "/v1/node-types?namespace=default", nil).Header().Get("ETag")
	m.runnerNodeTypes = fakeRunnerNodeTypes(nil, nil, runnerAggregate(t, "acme.analyse", 1, "A", []string{"p"}, namespace.Default))
	after := getNodeTypes(t, mux, "/v1/node-types?namespace=default", nil)
	if after.Header().Get("ETag") == before {
		t.Error("ETag did not change when a runner type appeared")
	}
	if got := getNodeTypes(t, mux, "/v1/node-types?namespace=default", http.Header{"If-None-Match": {before}}); got.Code != http.StatusOK {
		t.Errorf("old ETag after a runner type appeared: status %d, want 200", got.Code)
	}
}

func TestNodeTypesNamespaceValidation(t *testing.T) {
	mux := nodeTypesMux(&workflowControlModule{nodeDescriptors: fakeDescriptors(customDescriptor(1, "One"))})
	for _, path := range []string{"/v1/node-types?namespace=", "/v1/node-types?namespace=a*b", "/v1/node-types/custom.x?namespace=*"} {
		rec := getNodeTypes(t, mux, path, nil)
		if rec.Code != http.StatusBadRequest || envelopeCode(t, rec) != "namespace_invalid" {
			t.Errorf("%s: status %d body %s, want 400 namespace_invalid", path, rec.Code, rec.Body.String())
		}
	}
}

// TestNodeTypesBareBranchRefusesForeignNamespace: the legacy bearer branch
// has no principal to authorize a namespace against, so only the request's
// own namespace is served.
func TestNodeTypesBareBranchRefusesForeignNamespace(t *testing.T) {
	mux := nodeTypesMux(&workflowControlModule{
		nodeDescriptors: fakeDescriptors(),
		runnerNodeTypes: fakeRunnerNodeTypes(nil, nil, runnerAggregate(t, "acme.b", 1, "B", nil, "tenant-b")),
	})
	for _, path := range []string{"/v1/node-types?namespace=tenant-b", "/v1/node-types/acme.b?namespace=tenant-b"} {
		if rec := getNodeTypes(t, mux, path, nil); rec.Code != http.StatusForbidden {
			t.Errorf("%s: status %d, want 403", path, rec.Code)
		}
	}
}

func TestNodeTypesAuthzBranchNamespace(t *testing.T) {
	cases := []struct {
		name string
		path string
		want int
	}{
		{"unscoped", "/v1/node-types", http.StatusOK},
		{"own namespace", "/v1/node-types?namespace=tenant-a", http.StatusOK},
		{"own namespace get", "/v1/node-types/acme.a?namespace=tenant-a", http.StatusOK},
		{"foreign namespace", "/v1/node-types?namespace=tenant-b", http.StatusForbidden},
		{"foreign namespace get", "/v1/node-types/acme.a?namespace=tenant-b", http.StatusForbidden},
	}
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Namespace: "tenant-a", Scopes: []string{"workflow"}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls []namespace.Namespace
			m := authzModule(t, auth, NamespaceAwareAuthorizer{}, NewInMemoryAuditSink())
			m.nodeDescriptors = fakeDescriptors()
			m.runnerNodeTypes = fakeRunnerNodeTypes(&calls, nil, runnerAggregate(t, "acme.a", 1, "A", []string{"pool-a"}, "tenant-a"))
			mux := http.NewServeMux()
			m.registerAuthzRoutes(mux)
			rec := getNodeTypes(t, mux, tc.path, nil)
			if rec.Code != tc.want {
				t.Fatalf("status %d, want %d: %s", rec.Code, tc.want, rec.Body.String())
			}
			if tc.want == http.StatusForbidden && len(calls) != 0 {
				t.Errorf("a denied request consulted the runner source: %v", calls)
			}
			if tc.name == "own namespace" {
				var resp nodeTypesResponse
				if err := json.Unmarshal(envelopeData(t, rec), &resp); err != nil {
					t.Fatal(err)
				}
				if len(resp.NodeTypes) != 1 || resp.NodeTypes[0].NodeType != "acme.a" || resp.NodeTypes[0].Source != nodeFormSourceRunner {
					t.Errorf("node_types = %+v, want the runner acme.a", resp.NodeTypes)
				}
			}
		})
	}
}

// The production constructor wires the control plane's live runner node
// types, so a runner registered on the control plane's directory reaches a
// namespace-scoped list without any host-side hook, and leaves it once its
// session is past the live TTL.
func TestNewWorkflowControlModuleServesControlPlaneRunnerNodeTypes(t *testing.T) {
	dir := control.NewMemoryRunnerDirectory()
	cp, err := control.NewControlPlane(control.Config{Backend: local.New(), RunnerDirectory: dir})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	raw, err := json.Marshal(types.Descriptor{Type: "acme.wired", DisplayName: "Wired"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dir.Register(context.Background(), control.RegisterRunnerRequest{
		RunnerID:     "runner-wired",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "acme.wired", NodeVersion: 1}},
		Descriptors:  []control.RunnerNodeDescriptor{{Type: "acme.wired", Version: 1, Hash: "h", JSON: raw}},
		Policy:       control.RunnerPolicy{AllowedNodeTypes: []string{"*"}, AllowedNamespaces: []string{"*"}},
		Namespaces:   []namespace.Namespace{namespace.Default},
		InstanceUID:  "instance-wired",
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	m := newWorkflowControlModule(cp, nil, nil, nil)
	now := time.Now()
	m.runnerNodeTypesNow = func() time.Time { return now }
	mux := nodeTypesMux(m)
	s, ok := findNodeTypeSchema(listNodeTypeSchemas(t, mux, "/v1/node-types?namespace=default"), "acme.wired", 1)
	if !ok || s.Source != nodeFormSourceRunner {
		t.Fatalf("acme.wired@1 = %+v (found %v), want a runner-sourced entry", s, ok)
	}
	if _, ok := findNodeTypeSchema(listNodeTypeSchemas(t, mux, "/v1/node-types"), "acme.wired", 1); ok {
		t.Fatal("unscoped list served a runner type")
	}

	// No heartbeat arrives: once the live TTL passes the type leaves the list.
	now = now.Add(control.DefaultRunnerLiveTTL + time.Second)
	if _, ok := findNodeTypeSchema(listNodeTypeSchemas(t, mux, "/v1/node-types?namespace=default"), "acme.wired", 1); ok {
		t.Fatal("runner type still listed after the live TTL")
	}
}

// TestNodeTypesAuthzBranchRefusesForeignNamespace pins the tenant boundary
// under ScopeAuthorizer, which never compares ResourceNamespace: a tenant-a
// principal asking for ?namespace=tenant-b must get 403, not tenant-b's runner
// schemas, while its own namespace is still served.
func TestNodeTypesAuthzBranchRefusesForeignNamespace(t *testing.T) {
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Namespace: "tenant-a", Scopes: []string{"workflow"}}}
	m := authzModule(t, auth, ScopeAuthorizer{}, NewInMemoryAuditSink())
	m.nodeDescriptors = fakeDescriptors()
	m.runnerNodeTypes = fakeRunnerNodeTypes(nil, nil,
		runnerAggregate(t, "acme.a", 1, "A", []string{"pool-a"}, "tenant-a"),
		runnerAggregate(t, "acme.b", 1, "B", []string{"pool-b"}, "tenant-b"),
	)
	mux := http.NewServeMux()
	m.registerAuthzRoutes(mux)
	for _, path := range []string{"/v1/node-types?namespace=tenant-b", "/v1/node-types/acme.b?namespace=tenant-b"} {
		if rec := getNodeTypes(t, mux, path, nil); rec.Code != http.StatusForbidden {
			t.Errorf("%s: status %d, want 403: %s", path, rec.Code, rec.Body.String())
		}
	}
	got := listNodeTypeSchemas(t, mux, "/v1/node-types?namespace=tenant-a")
	if _, ok := findNodeTypeSchema(got, "acme.a", 1); !ok {
		t.Errorf("own namespace: acme.a missing from %d schemas", len(got))
	}
	if _, ok := findNodeTypeSchema(got, "acme.b", 1); ok {
		t.Error("own namespace: acme.b from tenant-b leaked")
	}
}
