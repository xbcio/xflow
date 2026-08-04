package runner

import (
	"fmt"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

type fakeInventory struct {
	handlers    map[string]map[int]bool
	runtimes    []string
	resources   []string
	credentials []string
}

func (f *fakeInventory) Has(nodeType string, version int) bool {
	if versions, ok := f.handlers[nodeType]; ok {
		return versions[version]
	}
	return false
}
func (f *fakeInventory) Runtimes() []string    { return f.runtimes }
func (f *fakeInventory) Resources() []string   { return f.resources }
func (f *fakeInventory) Credentials() []string { return f.credentials }

func validInventory() *fakeInventory {
	return &fakeInventory{
		handlers: map[string]map[int]bool{
			"test.echo": {1: true},
		},
	}
}

func testPayload(t *testing.T) *engine.GroupLeasePayload {
	t.Helper()
	pkg := &graph.SubgraphPackage{
		Version:   1,
		GroupName: "g",
		EntryNode: "src",
		Def: &types.WorkflowDef{
			Name: "g",
			Nodes: []types.NodeDef{
				{Name: "src", Type: "test.echo", Version: 1},
				{Name: "__collector_src_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"src": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_src_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_src_main", SrcNode: "src", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "test.echo", NodeVersion: 1},
		},
	}
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatal(err)
	}
	return &engine.GroupLeasePayload{
		PackageHash: hash,
		Package:     pkg,
	}
}

func TestPackageCache_ResolveCachesOnHit(t *testing.T) {
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	payload := testPayload(t)
	inv := validInventory()

	g1, p1, err := cache.Resolve(payload, inv)
	if err != nil {
		t.Fatal(err)
	}
	if g1 == nil || p1 == nil {
		t.Fatal("expected non-nil graph and package")
	}

	// Second resolve with nil Package should hit cache.
	payload2 := &engine.GroupLeasePayload{
		PackageHash: payload.PackageHash,
		Package:     nil,
	}
	g2, p2, err := cache.Resolve(payload2, inv)
	if err != nil {
		t.Fatal(err)
	}
	if g2 != g1 || p2 != p1 {
		t.Fatal("expected cache hit to return same pointers")
	}
}

func TestPackageCache_HashMismatch(t *testing.T) {
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	payload := testPayload(t)
	payload.PackageHash = "pkg-sha256:v1:0000000000000000000000000000000000000000000000000000000000000000"
	inv := validInventory()

	_, _, err := cache.Resolve(payload, inv)
	if err == nil {
		t.Fatal("expected hash mismatch error")
	}
	var ve *PackageValidationError
	if !isValidationError(err, &ve) {
		t.Fatalf("expected PackageValidationError, got %T: %v", err, err)
	}
}

func TestPackageCache_HandlerMissing(t *testing.T) {
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	payload := testPayload(t)
	inv := &fakeInventory{handlers: map[string]map[int]bool{}}

	_, _, err := cache.Resolve(payload, inv)
	if err == nil {
		t.Fatal("expected handler missing error")
	}
	var ve *PackageValidationError
	if !isValidationError(err, &ve) {
		t.Fatalf("expected PackageValidationError, got %T: %v", err, err)
	}
}

func TestPackageCache_HandlerVersionMismatch(t *testing.T) {
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	payload := testPayload(t)
	inv := &fakeInventory{
		handlers: map[string]map[int]bool{
			"test.echo": {2: true},
		},
	}

	_, _, err := cache.Resolve(payload, inv)
	if err == nil {
		t.Fatal("expected version mismatch error")
	}
}

func TestPackageCache_RuntimeNotAllowed(t *testing.T) {
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10, AllowedRuntimes: []string{"go"}})
	payload := testPayload(t)
	payload.Package.Artifacts = []graph.GroupArtifact{
		{NodeName: "src", Runtime: "python"},
	}
	// Recompute hash after modifying.
	hash, _ := graph.ComputePackageHash(payload.Package)
	payload.PackageHash = hash
	inv := validInventory()

	_, _, err := cache.Resolve(payload, inv)
	if err == nil {
		t.Fatal("expected runtime not allowed error")
	}
}

func TestPackageCache_MaxPackageBytes(t *testing.T) {
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10, MaxPackageBytes: 100})
	payload := testPayload(t)
	payload.Package.Artifacts = []graph.GroupArtifact{
		{NodeName: "src", Size: 200},
	}
	hash, _ := graph.ComputePackageHash(payload.Package)
	payload.PackageHash = hash
	inv := validInventory()

	_, _, err := cache.Resolve(payload, inv)
	if err == nil {
		t.Fatal("expected max size error")
	}
}

func TestPackageCache_MissingPackageNotCached(t *testing.T) {
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	payload := &engine.GroupLeasePayload{
		PackageHash: "pkg-sha256:v1:abcd",
		Package:     nil,
	}
	inv := validInventory()

	_, _, err := cache.Resolve(payload, inv)
	if err != ErrPackageMissing {
		t.Fatalf("expected ErrPackageMissing, got %v", err)
	}
}

func TestPackageCache_EvictsOldEntries(t *testing.T) {
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 2})
	inv := validInventory()

	// Add 3 entries.
	for i := 0; i < 3; i++ {
		payload := testPayload(t)
		payload.Package.GroupName = fmt.Sprintf("g%d", i)
		hash, _ := graph.ComputePackageHash(payload.Package)
		payload.PackageHash = hash
		_, _, err := cache.Resolve(payload, inv)
		if err != nil {
			t.Fatalf("entry %d: %v", i, err)
		}
	}

	known := cache.Known()
	if len(known) > 2 {
		t.Fatalf("Known() = %d entries, want <= 2", len(known))
	}
}

func TestPackageCache_CredentialMissing(t *testing.T) {
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	payload := testPayload(t)
	payload.Package.Requirements = []graph.Requirement{
		{NodeType: "test.echo", NodeVersion: 1, Credentials: []string{"api-key"}},
	}
	hash, _ := graph.ComputePackageHash(payload.Package)
	payload.PackageHash = hash
	inv := validInventory()

	_, _, err := cache.Resolve(payload, inv)
	if err == nil {
		t.Fatal("expected credential missing error")
	}
	var ve *PackageValidationError
	if !isValidationError(err, &ve) {
		t.Fatalf("expected PackageValidationError, got %T", err)
	}
}

func TestPackageCache_ResourceMissing(t *testing.T) {
	cache := NewPackageCache(PackageCacheConfig{MaxEntries: 10})
	payload := testPayload(t)
	payload.Package.Requirements = []graph.Requirement{
		{NodeType: "test.echo", NodeVersion: 1, Resource: "gpu"},
	}
	hash, _ := graph.ComputePackageHash(payload.Package)
	payload.PackageHash = hash
	inv := validInventory()

	_, _, err := cache.Resolve(payload, inv)
	if err == nil {
		t.Fatal("expected resource missing error")
	}
}

func isValidationError(err error, target **PackageValidationError) bool {
	if pve, ok := err.(*PackageValidationError); ok {
		*target = pve
		return true
	}
	return false
}
