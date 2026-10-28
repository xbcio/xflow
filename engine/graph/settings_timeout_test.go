package graph

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func settingsTimeoutDef(timeout time.Duration) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:     "to",
		Version:  "1",
		Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: timeout},
		Nodes:    []types.NodeDef{{Name: "a", Type: "test.echo"}},
	}
}

func TestSettingsTimeoutCompiles(t *testing.T) {
	g, err := Compile(settingsTimeoutDef(90 * time.Second))
	if err != nil {
		t.Fatalf("Compile error = %v", err)
	}
	if got := g.Timeout(); got != 90*time.Second {
		t.Errorf("Timeout() = %v, want 90s", got)
	}
}

func TestSettingsTimeoutUnsetIsZero(t *testing.T) {
	noSettings := settingsTimeoutDef(0)
	noSettings.Settings = nil
	for name, def := range map[string]*types.WorkflowDef{
		"nil settings":  noSettings,
		"zero timeout":  settingsTimeoutDef(0),
		"empty setting": {Name: "to", Settings: &types.WorkflowSettings{}, Nodes: []types.NodeDef{{Name: "a", Type: "test.echo"}}},
	} {
		g, err := Compile(def)
		if err != nil {
			t.Fatalf("%s: Compile error = %v", name, err)
		}
		if got := g.Timeout(); got != 0 {
			t.Errorf("%s: Timeout() = %v, want 0", name, got)
		}
	}
}

func TestSettingsTimeoutRejectsNegative(t *testing.T) {
	_, err := Compile(settingsTimeoutDef(-time.Second))
	if err == nil || !strings.Contains(err.Error(), "settings.timeout must be >= 0") {
		t.Fatalf("Compile(negative timeout) error = %v, want the settings.timeout rejection", err)
	}
}

func TestSettingsTimeoutFAF(t *testing.T) {
	def := settingsTimeoutDef(time.Minute)
	def.Options = &types.WorkflowOptions{FAF: true}
	_, err := Compile(def)
	if err == nil || !strings.Contains(err.Error(), "faf workflow cannot define settings.timeout") {
		t.Fatalf("Compile(FAF + timeout) error = %v, want the faf timeout rejection", err)
	}

	// An explicit zero is the unset spelling, so it must not trip the FAF guard.
	def = settingsTimeoutDef(0)
	def.Options = &types.WorkflowOptions{FAF: true}
	if _, err := Compile(def); err != nil {
		t.Fatalf("Compile(FAF + zero timeout) error = %v, want success", err)
	}
}

// TestSettingsTimeoutHash pins both directions. The expected literal was
// captured by compiling this exact definition (timeout unset) BEFORE
// graphHashPayload gained the Timeout field; it is not recomputed from current
// code. graphHash is recorded on every persisted execution, so a dropped
// omitempty would move the hash of every graph that sets no timeout.
func TestSettingsTimeoutHash(t *testing.T) {
	unset, err := Compile(settingsTimeoutDef(0))
	if err != nil {
		t.Fatal(err)
	}
	const wantHash = "sha256:53b131319a2125f13535469e21c9cb0929ae0eea71d618237295094dfad8acfc"
	if unset.Hash() != wantHash {
		t.Fatalf("hash = %q, want %q (pinned before graphHashPayload.Timeout existed); an unset "+
			"timeout must keep omitempty and leave every pre-existing graph's hash unchanged",
			unset.Hash(), wantHash)
	}

	set, err := Compile(settingsTimeoutDef(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if set.Hash() == unset.Hash() {
		t.Error("a declared timeout did not change the graph hash")
	}
	other, err := Compile(settingsTimeoutDef(2 * time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if other.Hash() == set.Hash() {
		t.Error("different timeouts produced the same graph hash")
	}
}

// TestSettingsTimeoutSnapshotRoundTrip covers the wire form: the timeout
// survives Marshal/Unmarshal, an unset one writes no field, and the hash
// re-derived from the decoded graph is the stored one.
func TestSettingsTimeoutSnapshotRoundTrip(t *testing.T) {
	g, err := Compile(settingsTimeoutDef(45 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"timeout":45000000000`) {
		t.Errorf("snapshot omits the timeout as integer nanoseconds: %s", data)
	}
	var decoded Graph
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got := decoded.Timeout(); got != 45*time.Second {
		t.Errorf("decoded Timeout() = %v, want 45s", got)
	}
	if err := assignGraphHash(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Hash() != g.Hash() {
		t.Errorf("hash after round trip = %s, want %s", decoded.Hash(), g.Hash())
	}

	unset, err := Compile(settingsTimeoutDef(0))
	if err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(unset)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"timeout"`) {
		t.Errorf("an unset timeout wrote a snapshot field: %s", data)
	}
}
