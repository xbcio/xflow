package graph

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// timeoutProbeDef is the shared definition used by the round-trip and the
// graph-hash compatibility tests. Its only Timeout-bearing node is "slow";
// "start" leaves Timeout unset so omitempty is exercised on the same snapshot.
func timeoutProbeDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "t",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "slow", Type: "xflow.noop", Timeout: 42 * time.Second},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "slow"}}}},
		},
	}
}

// TestRegisterNodesCarriesTimeout guards the main compile path: NodeDef.Timeout
// must reach NodeMeta.Timeout verbatim (raw, un-normalized -- zero is "not
// configured", negative is "no limit"; normalization is the engine's job in a
// later task, deliberately, because the engine's default-timeout option is
// invisible from this package and a resolved default would change graphHash for
// every graph that never configured one).
func TestRegisterNodesCarriesTimeout(t *testing.T) {
	def := timeoutProbeDef()
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	idx, ok := g.NodeIndex("slow")
	if !ok {
		t.Fatal("node \"slow\" not found")
	}
	if got := g.NodeAt(idx).Timeout; got != 42*time.Second {
		t.Fatalf("NodeMeta.Timeout = %v, want 42s (raw value must survive registerNodes)", got)
	}
}

// TestSnapshotRoundTripPreservesTimeout guards the wire format: a graph with a
// Timeout set must survive json.Marshal/Unmarshal with the value intact.
// Production reloads graphs from Redis via json.Unmarshal (rstate.Store.LoadGraph),
// so a Timeout that does not round-trip silently downgrades to the engine
// default on every cache-miss/reload -- the documented silent-degradation
// failure mode for field-by-field copy omissions.
func TestSnapshotRoundTripPreservesTimeout(t *testing.T) {
	g, err := Compile(timeoutProbeDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round Graph
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	idx, ok := round.NodeIndex("slow")
	if !ok {
		t.Fatal("round-tripped graph has no node \"slow\"")
	}
	if got := round.NodeAt(idx).Timeout; got != 42*time.Second {
		t.Fatalf("round-tripped NodeMeta.Timeout = %v, want 42s (wire format dropped the field)", got)
	}
	// The unset node must stay zero, not pick up a stray default.
	startIdx, ok := round.NodeIndex("start")
	if !ok {
		t.Fatal("round-tripped graph has no node \"start\"")
	}
	if got := round.NodeAt(startIdx).Timeout; got != 0 {
		t.Fatalf("round-tripped NodeMeta.Timeout for \"start\" = %v, want 0 (unset must stay unset)", got)
	}
}

// TestUnsetTimeoutDoesNotChangeGraphHash is the compatibility guard: a graph
// with no timeouts anywhere must hash byte-identically to before this field
// existed. graphHash is recorded on every persisted execution, so a single
// tag typo (missing omitempty on wireNodeMeta.Timeout, or carrying a resolved
// default into NodeMeta) moves every already-persisted graph's hash and
// invalidates them all. The expected hash below was captured from the parent
// commit (2da0b33) by running this exact def through Compile BEFORE the
// NodeMeta.Timeout field was added -- it is a pinned literal, not recomputed
// from current code. A test that recomputes the expected hash from the current
// codebase proves nothing.
func TestUnsetTimeoutDoesNotChangeGraphHash(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "t",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "slow", Type: "xflow.noop"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "slow"}}}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	const wantHash = "sha256:9a205c7fa88aa3b46788936f5aae62d1e526dae04e26344bfd3a68a9bd9dedab"
	if g.Hash() != wantHash {
		t.Fatalf("hash = %q, want %q (pinned at parent commit 2da0b33 before NodeMeta.Timeout "+
			"existed); an unset Timeout must keep omitempty and leave every pre-existing graph's "+
			"hash unchanged", g.Hash(), wantHash)
	}
}

// TestCompileTrustedCarriesTimeout guards the projected-package compile path,
// which has its own field-by-field NodeDef -> NodeMeta copy in compileTrusted.
// This is one of the three SILENT copy sites: missing it does not fail
// compilation, it silently downgrades every group member's timeout to the
// engine default. Projected packages are what runners actually execute for
// co-located groups, so a dropped Timeout here is the one that matters in
// production.
func TestCompileTrustedCarriesTimeout(t *testing.T) {
	pkg := &SubgraphPackage{
		Version:   SubgraphPackageVersion,
		GroupName: "grp",
		EntryNode: "m",
		Def: &types.WorkflowDef{
			Name: "grp",
			Nodes: []types.NodeDef{
				{Name: "m", Type: "test.noop", Timeout: 7 * time.Second},
			},
		},
	}
	g, err := CompileProjectedPackage(pkg)
	if err != nil {
		t.Fatalf("compile projected package: %v", err)
	}
	idx, ok := g.NodeIndex("m")
	if !ok {
		t.Fatal("compiled graph has no node \"m\"")
	}
	if got := g.NodeAt(idx).Timeout; got != 7*time.Second {
		t.Fatalf("NodeMeta.Timeout = %v, want 7s (compileTrusted dropped the field)", got)
	}
}
