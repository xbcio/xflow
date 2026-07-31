package supply

import "testing"

// The default MUST be the safe tier: a missing parameter means
// require_ready=true. cast.ToBool(nil) is false, so a naive read flips the
// default to the dangerous setting (traffic served with no rules at all).
func TestRequireReadyDefaultsTrue(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]any
		want   bool
	}{
		{"nil params", nil, true},
		{"absent key", map[string]any{"resource": "r"}, true},
		{"explicit true", map[string]any{"require_ready": true}, true},
		{"explicit false", map[string]any{"require_ready": false}, false},
		{"string false (YAML)", map[string]any{"require_ready": "false"}, false},
		{"string true (YAML)", map[string]any{"require_ready": "true"}, true},
		{"garbage value", map[string]any{"require_ready": "yes-please"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RequireReady(tc.params); got != tc.want {
				t.Fatalf("RequireReady(%#v) = %v, want %v", tc.params, got, tc.want)
			}
		})
	}
}

func TestResourceNameFallsBackToNodeName(t *testing.T) {
	if got := ResourceName("rules", nil); got != "rules" {
		t.Fatalf("ResourceName with no param = %q, want the node name", got)
	}
	if got := ResourceName("rules", map[string]any{"resource": "shared"}); got != "shared" {
		t.Fatalf("ResourceName = %q, want shared", got)
	}
	// An empty string must not shadow the node name.
	if got := ResourceName("rules", map[string]any{"resource": ""}); got != "rules" {
		t.Fatalf("ResourceName with empty param = %q, want the node name", got)
	}
}

func TestStaticContentRequiresContent(t *testing.T) {
	got, err := StaticContent(map[string]any{"content": `{"a":1}`})
	if err != nil {
		t.Fatalf("StaticContent: %v", err)
	}
	if string(got) != `{"a":1}` {
		t.Fatalf("content = %s", got)
	}
	if _, err := StaticContent(map[string]any{}); err == nil {
		t.Fatal("missing content must be an error, not empty bytes")
	}
}
