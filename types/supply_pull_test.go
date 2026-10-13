package types

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The map every valid case varies one key of. Every key is present in at least
// one case, so a rule that only fires for a full parameter set is caught.
func validPullParams() map[string]any {
	return map[string]any{
		SupplyPullParamURL: "https://config.example.com/rules.json",
	}
}

func TestParseSupplyPullSourceValid(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]any
		want   SupplyPullSource
	}{
		{
			name:   "url only",
			params: validPullParams(),
			want:   SupplyPullSource{URL: "https://config.example.com/rules.json"},
		},
		{
			name: "all optional keys set",
			params: map[string]any{
				SupplyPullParamURL:            "https://config.example.com/rules.json",
				SupplyPullParamAuthentication: "rules_token",
				SupplyPullParamPollInterval:   "30s",
				SupplyParamRequireReady:       false,
			},
			want: SupplyPullSource{
				URL:            "https://config.example.com/rules.json",
				Authentication: "rules_token",
				PollInterval:   30 * time.Second,
			},
		},
		{
			name: "interval at the floor",
			params: map[string]any{
				SupplyPullParamURL:          "https://config.example.com/rules.json",
				SupplyPullParamPollInterval: MinSupplyPullInterval.String(),
			},
			want: SupplyPullSource{
				URL:          "https://config.example.com/rules.json",
				PollInterval: MinSupplyPullInterval,
			},
		},
		{
			// require_ready is a legal key on every supply type; its VALUE is
			// read by node/supply.RequireReady, not here, so even a malformed
			// one must not fail the parse (the reader keeps the safe default).
			name: "require_ready value is not this parser's business",
			params: map[string]any{
				SupplyPullParamURL:      "https://config.example.com/rules.json",
				SupplyParamRequireReady: "not-a-bool",
			},
			want: SupplyPullSource{URL: "https://config.example.com/rules.json"},
		},
		{
			name: "https scheme is case-insensitive per RFC 3986",
			params: map[string]any{
				SupplyPullParamURL: "HTTPS://config.example.com/rules.json",
			},
			// The URL is stored as written: the definition's spelling is what
			// the hash covers, so normalizing here would move workflow hashes.
			want: SupplyPullSource{URL: "HTTPS://config.example.com/rules.json"},
		},
		{
			name: "non-default port and path are preserved",
			params: map[string]any{
				SupplyPullParamURL: "https://config.example.com:8443/a/b.json?rev=2",
			},
			want: SupplyPullSource{URL: "https://config.example.com:8443/a/b.json?rev=2"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseSupplyPullSource(tc.params)
			if err != nil {
				t.Fatalf("ParseSupplyPullSource: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("source = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseSupplyPullSourceRejects(t *testing.T) {
	cases := []struct {
		name    string
		params  map[string]any
		wantErr string // substring the error must carry
	}{
		{
			name:    "url missing",
			params:  map[string]any{},
			wantErr: `"url" parameter is required`,
		},
		{
			name:    "url empty",
			params:  map[string]any{SupplyPullParamURL: ""},
			wantErr: `"url" parameter is required`,
		},
		{
			name:    "url not a string",
			params:  map[string]any{SupplyPullParamURL: 42},
			wantErr: `"url" must be a string`,
		},
		{
			name:    "plain http has no opt-out",
			params:  map[string]any{SupplyPullParamURL: "http://config.example.com/rules.json"},
			wantErr: "must be an https URL",
		},
		{
			name:    "other schemes rejected",
			params:  map[string]any{SupplyPullParamURL: "file:///etc/passwd"},
			wantErr: "must be an https URL",
		},
		{
			name:    "no host",
			params:  map[string]any{SupplyPullParamURL: "https:///rules.json"},
			wantErr: "has no host",
		},
		{
			name:    "relative url",
			params:  map[string]any{SupplyPullParamURL: "/rules.json"},
			wantErr: "must be an https URL",
		},
		{
			name:    "malformed url",
			params:  map[string]any{SupplyPullParamURL: "https://exa mple.com/rules.json"},
			wantErr: "not a valid URL",
		},
		{
			name:    "credentials embedded in the url",
			params:  map[string]any{SupplyPullParamURL: "https://user:pass@config.example.com/rules.json"},
			wantErr: "must not embed credentials",
		},
		{
			name: "resource is not accepted on a pull node",
			params: map[string]any{
				SupplyPullParamURL:  "https://config.example.com/rules.json",
				SupplyParamResource: "shared_rules",
			},
			wantErr: `"resource" is not accepted on a pull node`,
		},
		{
			// A resource with no url at all is still reported as the resource,
			// since it is the specific diagnosis and does not depend on the URL.
			name:    "resource is rejected even without a url",
			params:  map[string]any{SupplyParamResource: "shared_rules"},
			wantErr: `"resource" is not accepted`,
		},
		{
			name: "unknown parameter",
			params: map[string]any{
				SupplyPullParamURL: "https://config.example.com/rules.json",
				"retries":          3,
			},
			wantErr: `unknown parameter "retries"`,
		},
		{
			name: "authentication empty",
			params: map[string]any{
				SupplyPullParamURL:            "https://config.example.com/rules.json",
				SupplyPullParamAuthentication: "",
			},
			wantErr: "must name a credential",
		},
		{
			name: "authentication not a string",
			params: map[string]any{
				SupplyPullParamURL:            "https://config.example.com/rules.json",
				SupplyPullParamAuthentication: []any{"a"},
			},
			wantErr: `"authentication" must be a string`,
		},
		{
			name: "interval below the floor",
			params: map[string]any{
				SupplyPullParamURL:          "https://config.example.com/rules.json",
				SupplyPullParamPollInterval: "9s",
			},
			wantErr: "below the 10s minimum",
		},
		{
			name: "interval cannot disable polling",
			params: map[string]any{
				SupplyPullParamURL:          "https://config.example.com/rules.json",
				SupplyPullParamPollInterval: "0s",
			},
			wantErr: "below the 10s minimum",
		},
		{
			name: "negative interval",
			params: map[string]any{
				SupplyPullParamURL:          "https://config.example.com/rules.json",
				SupplyPullParamPollInterval: "-30s",
			},
			wantErr: "below the 10s minimum",
		},
		{
			name: "interval without a unit",
			params: map[string]any{
				SupplyPullParamURL:          "https://config.example.com/rules.json",
				SupplyPullParamPollInterval: "30",
			},
			wantErr: "poll_interval",
		},
		{
			name: "interval not a string",
			params: map[string]any{
				SupplyPullParamURL:          "https://config.example.com/rules.json",
				SupplyPullParamPollInterval: 30,
			},
			wantErr: "must be a duration string",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseSupplyPullSource(tc.params)
			if err == nil {
				t.Fatalf("ParseSupplyPullSource = %+v, want an error containing %q", got, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
			if got != (SupplyPullSource{}) {
				t.Fatalf("a rejected parse returned %+v; want the zero source", got)
			}
		})
	}
}

// The error for a definition with several unknown keys must not depend on Go's
// map iteration order: the same bad definition has to produce the same
// message every time, or a failing apply-validate loop becomes unreproducible.
func TestParseSupplyPullSourceUnknownKeyErrorIsStable(t *testing.T) {
	params := map[string]any{
		SupplyPullParamURL: "https://config.example.com/rules.json",
		"zeta":             1,
		"alpha":            2,
	}
	for i := 0; i < 50; i++ {
		_, err := ParseSupplyPullSource(params)
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), `unknown parameter "alpha"`) {
			t.Fatalf("iteration %d: error %q; want the alphabetically first unknown key", i, err)
		}
	}
}

// The DTO crosses the activation wire as JSON, so its tags are a contract, not
// a detail: renamed or omitted keys break a runner one version behind.
func TestSupplyPullSourceJSON(t *testing.T) {
	raw, err := json.Marshal(SupplyPullSource{
		URL:            "https://config.example.com/rules.json",
		Authentication: "rules_token",
		PollInterval:   30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["url"] != "https://config.example.com/rules.json" {
		t.Errorf("url = %#v", decoded["url"])
	}
	if decoded["authentication"] != "rules_token" {
		t.Errorf("authentication = %#v", decoded["authentication"])
	}
	if decoded["poll_interval"] != float64(30*time.Second) {
		t.Errorf("poll_interval = %#v, want %v as nanoseconds", decoded["poll_interval"], 30*time.Second)
	}

	// Unset optional keys stay out of the document: an anonymous source with
	// the global cadence must not carry `"authentication":""`.
	raw, err = json.Marshal(SupplyPullSource{URL: "https://config.example.com/rules.json"})
	if err != nil {
		t.Fatal(err)
	}
	decoded = nil
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"authentication", "poll_interval"} {
		if _, ok := decoded[key]; ok {
			t.Errorf("zero-value source marshals %q", key)
		}
	}

	// Round trip.
	var back SupplyPullSource
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.URL != "https://config.example.com/rules.json" || back.Authentication != "" || back.PollInterval != 0 {
		t.Fatalf("round trip = %+v", back)
	}
}
