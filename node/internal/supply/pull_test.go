package supply

import (
	"testing"

	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

func TestPullDescriptorIsSupplyKind(t *testing.T) {
	d := Pull("https://config.example.com/rules.json").Descriptor()
	if d.Type != supply.NodeTypeHTTPPull {
		t.Fatalf("type = %q, want %q", d.Type, supply.NodeTypeHTTPPull)
	}
	if d.Kind != types.NodeKindSupply {
		t.Fatalf("kind = %q, want %q", d.Kind, types.NodeKindSupply)
	}
	if len(d.Outputs) != 0 {
		t.Fatalf("a supply node must declare no output ports, got %d", len(d.Outputs))
	}
}

// The descriptor is what the editor renders and what hash canonicalization
// reads: the url must be required, require_ready must default to the safe tier,
// and poll_interval must be absent rather than zero when unset.
func TestPullDescriptorParams(t *testing.T) {
	d := Pull("https://config.example.com/rules.json").Descriptor()
	byName := map[string]types.ParamSpec{}
	for _, p := range d.Params {
		byName[p.Name] = p
	}
	for _, name := range []string{supply.ParamURL, supply.ParamAuthentication, supply.ParamRequireReady, supply.ParamPollInterval} {
		if _, ok := byName[name]; !ok {
			t.Errorf("descriptor is missing the %q param", name)
		}
	}
	if !byName[supply.ParamURL].Required {
		t.Error("url must be required: a pull node without a source has nothing to fetch")
	}
	if got := byName[supply.ParamRequireReady].Default; got != true {
		t.Errorf("require_ready Default = %#v, want true", got)
	}
	if got := byName[supply.ParamPollInterval].Default; got != nil {
		t.Errorf("poll_interval Default = %#v, want unset (the runner's global cadence)", got)
	}
	// The dead keys of the other supply shapes must not be declared here: a
	// `resource` or `content` widget would invite exactly the configuration the
	// compiler rejects.
	for _, name := range []string{supply.ParamResource, supply.ParamContent} {
		if _, ok := byName[name]; ok {
			t.Errorf("descriptor declares %q, which a pull node does not accept", name)
		}
	}
}

func TestPullRawParams(t *testing.T) {
	got := Pull("https://config.example.com/rules.json").
		Authentication("rules_token").
		PollInterval("30s").
		RequireReady(false).
		RawParams()
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("RawParams = %T, want map[string]any", got)
	}
	if m[supply.ParamURL] != "https://config.example.com/rules.json" {
		t.Fatalf("url = %#v", m[supply.ParamURL])
	}
	if m[supply.ParamAuthentication] != "rules_token" {
		t.Fatalf("authentication = %#v", m[supply.ParamAuthentication])
	}
	if m[supply.ParamPollInterval] != "30s" {
		t.Fatalf("poll_interval = %#v", m[supply.ParamPollInterval])
	}
	if m[supply.ParamRequireReady] != false {
		t.Fatalf("require_ready = %#v, want false", m[supply.ParamRequireReady])
	}
}

// An unset optional param stays out of the map rather than landing as "": the
// parameters are hashed, and "" is a different definition from "absent" for
// every consumer that switches on presence.
func TestPullRawParamsOmitUnsetOptionalKeys(t *testing.T) {
	m := Pull("https://config.example.com/rules.json").RawParams().(map[string]any)
	for _, name := range []string{supply.ParamAuthentication, supply.ParamPollInterval} {
		if _, ok := m[name]; ok {
			t.Errorf("RawParams carries %q with no value set", name)
		}
	}
}
