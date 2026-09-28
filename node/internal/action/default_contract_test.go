package action

// Handler-layer Default contract for xflow.browser.cdp: parseBrowserCDPParams
// must produce the same effective params whether timeout_ms/total_timeout_ms
// are absent, set to the descriptor's Default, or set to that Default after a
// JSON round trip. See node/default_contract_test.go for the rationale.

import (
	"encoding/json"
	"maps"
	"reflect"
	"testing"

	nodeinternal "github.com/xbcio/xflow/node/internal"
)

func browserDescriptorDefault(t *testing.T, name string) any {
	t.Helper()
	for _, p := range (&CDPNode{}).Descriptor().Params {
		if p.Name == name {
			if p.Default == nil {
				t.Fatalf("xflow.browser.cdp/%s has no Default", name)
			}
			return p.Default
		}
	}
	t.Fatalf("xflow.browser.cdp has no param %q", name)
	return nil
}

func jsonRoundTrip(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDefaultContractBrowserTimeouts(t *testing.T) {
	snapshot := testBrowserSnapshot("chrome.test")
	policy := NewHostPolicy([]string{"app.test"}, nil)
	parse := func(t *testing.T, raw map[string]any) *browserParams {
		t.Helper()
		p, err := parseBrowserCDPParams(raw, snapshot, policy)
		if err != nil {
			t.Fatalf("parse %v: %v", raw, err)
		}
		// Policy is a func and never DeepEqual; it is not what the param sets.
		p.Policy = nil
		return p
	}

	for _, name := range []string{"timeout_ms", "total_timeout_ms"} {
		t.Run(name, func(t *testing.T) {
			def := browserDescriptorDefault(t, name)
			absent := parse(t, validBrowserParams())
			for variant, value := range map[string]any{"default": def, "default_json": jsonRoundTrip(t, def)} {
				raw := maps.Clone(validBrowserParams())
				raw[name] = value
				if got := parse(t, raw); !reflect.DeepEqual(got, absent) {
					t.Errorf("%s=%#v (%s): parsed %+v, want %+v (param absent)", name, value, variant, got, absent)
				}
			}
		})
	}
}

// TestFallbackContractBrowser exercises every BuiltinFallbacks entry for
// xflow.browser.cdp against the parser that applies it.
func TestFallbackContractBrowser(t *testing.T) {
	snapshot := testBrowserSnapshot("chrome.test")
	policy := NewHostPolicy([]string{"app.test"}, nil)
	checks := map[string]func(t *testing.T, f nodeinternal.Fallback){
		// Derived: absent target_host means the entry_url host.
		"target_host": func(t *testing.T, _ nodeinternal.Fallback) {
			absent, err := parseBrowserCDPParams(validBrowserParams(), snapshot, policy)
			if err != nil {
				t.Fatal(err)
			}
			explicit := maps.Clone(validBrowserParams())
			explicit["target_host"] = absent.EntryURL.Hostname()
			set, err := parseBrowserCDPParams(explicit, snapshot, policy)
			if err != nil {
				t.Fatal(err)
			}
			absent.Policy, set.Policy = nil, nil
			if absent.TargetHost != "app.test" || !reflect.DeepEqual(absent, set) {
				t.Fatalf("absent target_host parsed %+v, want it equal to target_host=%q: %+v", absent, explicit["target_host"], set)
			}
		},
	}
	runFallbackChecks(t, "xflow.browser.cdp", 1, checks)
}

// runFallbackChecks runs one check per BuiltinFallbacks entry for typ@version
// and fails on an entry without a check or a check without an entry, so the
// table and the handler cannot drift apart silently.
func runFallbackChecks(t *testing.T, typ string, version int, checks map[string]func(*testing.T, nodeinternal.Fallback)) {
	t.Helper()
	entries := nodeinternal.BuiltinFallbacksFor(typ, version)
	if len(entries) == 0 {
		t.Fatalf("BuiltinFallbacks has no entry for %s@%d", typ, version)
	}
	seen := map[string]bool{}
	for _, f := range entries {
		seen[f.Param] = true
		check, ok := checks[f.Param]
		if !ok {
			t.Errorf("BuiltinFallbacks entry %s@%d/%s has no handler check", typ, version, f.Param)
			continue
		}
		t.Run(f.Param, func(t *testing.T) { check(t, f) })
	}
	for param := range checks {
		if !seen[param] {
			t.Errorf("check for %s@%d/%s has no BuiltinFallbacks entry", typ, version, param)
		}
	}
}
