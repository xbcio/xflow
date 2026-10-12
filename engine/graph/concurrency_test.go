package graph

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func concDef(settings *types.ConcurrencySettings) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:     "conc",
		Version:  "1",
		Settings: &types.WorkflowSettings{Concurrency: settings},
		Nodes:    []types.NodeDef{{Name: "a", Type: "test.echo"}},
	}
}

func TestResolveConcurrencyNormalizesMode(t *testing.T) {
	// A bare int carries no mode and must resolve to QUEUE: that is what the
	// shorthand means, and the store reads the resolved mode.
	g, err := Compile(concDef(&types.ConcurrencySettings{Limit: 3}))
	if err != nil {
		t.Fatalf("Compile error = %v", err)
	}
	got := g.Concurrency()
	if got == nil {
		t.Fatal("Concurrency() = nil, want a resolved block")
	}
	if got.Limit != 3 || got.Mode != types.ConcurrencyModeQueue {
		t.Errorf("Concurrency() = %+v, want limit 3 mode %q", got, types.ConcurrencyModeQueue)
	}
}

func TestResolveConcurrencyZeroLimitIsNoLimit(t *testing.T) {
	// A zero limit is the same behavior as no setting, so it stores nothing --
	// which is what keeps an unlimited graph's hash payload untouched.
	for _, settings := range []*types.ConcurrencySettings{
		nil,
		{Limit: 0},
		{Limit: 0, Mode: types.ConcurrencyModeReject},
	} {
		g, err := Compile(concDef(settings))
		if err != nil {
			t.Fatalf("Compile(%+v) error = %v", settings, err)
		}
		if g.Concurrency() != nil {
			t.Errorf("Compile(%+v): Concurrency() = %+v, want nil", settings, g.Concurrency())
		}
	}
}

func TestResolveConcurrencyRejectsBadSettings(t *testing.T) {
	cases := []struct {
		name     string
		settings *types.ConcurrencySettings
		wantSub  string
	}{
		{"negative limit", &types.ConcurrencySettings{Limit: -1}, "limit must be >= 0"},
		{"unknown mode", &types.ConcurrencySettings{Limit: 2, Mode: "bogus"}, "is not one of"},
		{"negative max_queued", &types.ConcurrencySettings{Limit: 2, MaxQueued: -1}, "max_queued must be >= 0"},
		{"negative queue_timeout", &types.ConcurrencySettings{Limit: 2, QueueTimeout: -1}, "queue_timeout must be >= 0"},
		{"reject with max_queued", &types.ConcurrencySettings{Limit: 2, Mode: types.ConcurrencyModeReject, MaxQueued: 5}, "require mode"},
		{"reject with timeout", &types.ConcurrencySettings{Limit: 2, Mode: types.ConcurrencyModeReject, QueueTimeout: 1}, "require mode"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(concDef(tc.settings))
			if err == nil {
				t.Fatalf("Compile(%+v) succeeded, want error", tc.settings)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error = %q, want it to mention %q", err, tc.wantSub)
			}
		})
	}
}

func TestResolveConcurrencyRejectsFAF(t *testing.T) {
	def := concDef(&types.ConcurrencySettings{Limit: 1})
	def.Options = &types.WorkflowOptions{FAF: true}
	_, err := Compile(def)
	if err == nil || !strings.Contains(err.Error(), "faf workflow cannot define settings.concurrency") {
		t.Fatalf("Compile(FAF + limit) error = %v, want the faf concurrency rejection", err)
	}
}

// TestResolveConcurrencyHashCoversSetting pins the two directions that matter
// for a stored execution: a graph that declares a limit hashes differently from
// one that does not (the limit is part of what the graph means), and an
// unlimited graph hashes exactly as it did before the field existed.
func TestResolveConcurrencyHashCoversSetting(t *testing.T) {
	unlimited, err := Compile(concDef(nil))
	if err != nil {
		t.Fatal(err)
	}
	zero, err := Compile(concDef(&types.ConcurrencySettings{Limit: 0}))
	if err != nil {
		t.Fatal(err)
	}
	if unlimited.Hash() != zero.Hash() {
		t.Errorf("limit 0 changed the graph hash: %s vs %s", unlimited.Hash(), zero.Hash())
	}

	limited, err := Compile(concDef(&types.ConcurrencySettings{Limit: 2}))
	if err != nil {
		t.Fatal(err)
	}
	if limited.Hash() == unlimited.Hash() {
		t.Error("a declared limit did not change the graph hash")
	}
}

// TestResolveConcurrencySnapshotRoundTrip covers the wire form: the resolved
// block survives Marshal/Unmarshal and the hash it re-derives is the stored one.
// A drift here would invalidate every persisted execution's recorded graph hash.
func TestResolveConcurrencySnapshotRoundTrip(t *testing.T) {
	for _, settings := range []*types.ConcurrencySettings{
		{Limit: 10},
		{Limit: 5, Mode: types.ConcurrencyModeReject},
		{Limit: 7, Mode: types.ConcurrencyModeQueue, MaxQueued: 4},
	} {
		g, err := Compile(concDef(settings))
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(g)
		if err != nil {
			t.Fatal(err)
		}
		var decoded Graph
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("Unmarshal(%+v): %v", settings, err)
		}
		// Re-deriving the hash proves the decoded block feeds the hash payload
		// identically to the compile-time one; comparing the stored string alone
		// would pass even if the field never round-tripped.
		if err := assignGraphHash(&decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Hash() != g.Hash() {
			t.Errorf("%+v: hash after round trip = %s, want %s", settings, decoded.Hash(), g.Hash())
		}
		if got := decoded.Concurrency(); got == nil || got.Limit != settings.Limit || got.Mode != settings.EffectiveMode() {
			t.Errorf("%+v: decoded Concurrency() = %+v", settings, got)
		}
	}
}

func TestConcurrencyKeySeparatesNamespacesAndNames(t *testing.T) {
	// Length prefixes are what make the concatenation unambiguous: without them
	// ("a", "bc") and ("ab", "c") would key the same slot set.
	if ConcurrencyKey("a", "bc") == ConcurrencyKey("ab", "c") {
		t.Error("ConcurrencyKey collided across a length boundary")
	}
	if ConcurrencyKey("ns", "wf") != ConcurrencyKey("ns", "wf") {
		t.Error("ConcurrencyKey is not deterministic")
	}
	if ConcurrencyKey("ns1", "wf") == ConcurrencyKey("ns2", "wf") {
		t.Error("ConcurrencyKey ignored the namespace")
	}
}
