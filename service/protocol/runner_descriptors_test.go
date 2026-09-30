package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// updateRunnerDescriptors rewrites the runner descriptor envelope golden:
// go test ./service/protocol -run RunnerDescriptor -update-runner-descriptors
var updateRunnerDescriptors = flag.Bool("update-runner-descriptors", false, "rewrite the runner descriptor envelope golden")

const runnerDescriptorGolden = "testdata/runner_descriptors_v1.golden.json"

// fullRunnerDescriptor sets every types.Descriptor member (recursively), so a
// rename, removal, or addition of any field changes the encoded envelope and
// fails TestRunnerDescriptorEnvelopeGolden. It also pins the semantics the
// design relies on: a nil Eq, nil-vs-empty In, and an integer Default above
// 2^53.
func fullRunnerDescriptor() types.Descriptor {
	f := func(v float64) *float64 { return &v }
	i := func(v int) *int { return &v }
	yes, no := true, false
	return types.Descriptor{
		Type:               "acme.analyse",
		Kind:               types.NodeKindAction,
		DisplayName:        "Analyse",
		Credentials:        []string{"acme-api"},
		Capabilities:       []string{"acme.experimental"},
		Docs:               "# Analyse\nRuns the analysis.",
		Inputs:             []types.PortSpec{{Name: "main", DisplayName: "Input"}},
		Outputs:            []types.PortSpec{{Name: "main", DisplayName: "Output"}, {Name: "error", DisplayName: "Error"}},
		Groups:             []types.GroupSpec{{Key: "advanced", DisplayName: "Advanced", Description: "Tuning", Collapsed: true}},
		OneOf:              []types.OneOfGroup{{Params: []string{"query", "script"}, Mode: types.OneOfAtMost}},
		DynamicOutputsFrom: "routes",
		Params: []types.ParamSpec{
			{
				Name:        "limit",
				DisplayName: "Limit",
				Type:        types.ParamNumber,
				Required:    true,
				Default:     int64(1)<<60 + 1, // above 2^53: must survive as an exact integer
				Description: "Row limit",
				Enum: []types.EnumOption{
					{Value: 10, DisplayName: "Ten", Description: "small"},
					{Value: "all", DisplayName: "All", Description: "everything"},
				},
				EnumWhen: []types.ConditionalEnum{{
					When: types.Condition{Param: "mode", Eq: "fast"},
					Enum: []types.EnumOption{{Value: 1, DisplayName: "One", Description: "fast only"}},
				}},
				Secret: true,
				Constraints: &types.Constraints{
					Min: f(0), Max: f(1e6),
					MinLength: i(1), MaxLength: i(8),
					Pattern:  "^[0-9]+$",
					Format:   "json",
					MinItems: i(0), MaxItems: i(3),
					UniqueItems: true,
				},
				VisibleWhen: &types.Condition{
					Param:  "mode",
					Eq:     nil, // nil Eq encodes as null and decodes back to nil
					In:     []any{},
					Truthy: &yes,
					AllOf:  []types.Condition{{Param: "a", In: nil}},
					AnyOf:  []types.Condition{{Param: "b", Eq: true}},
					Not:    &types.Condition{Param: "c", Truthy: &no},
				},
				RequiredWhen: &types.Condition{Param: "mode", In: []any{"x", 2}},
				Group:        "advanced",
				Order:        3,
				Widget:       "slider",
				Deprecated:   "use rows",
			},
			{
				Name: "routes",
				Type: types.ParamArray,
				Item: &types.ParamSpec{Name: "route", Type: types.ParamString},
			},
			{
				Name:   "options",
				Type:   types.ParamObject,
				Fields: []types.ParamSpec{{Name: "verbose", Type: types.ParamBool, Default: false}},
			},
		},
	}
}

func fullRunnerDescriptorSet() []RunnerDescriptor {
	full := fullRunnerDescriptor()
	return []RunnerDescriptor{
		// Deliberately unsorted: encoding sorts by type, then version.
		{Type: "acme.analyse", Version: 2, Descriptor: full},
		{Type: "acme.analyse", Version: 1, Descriptor: types.Descriptor{Type: "acme.analyse", Kind: types.NodeKindAction}},
	}
}

func indentJSON(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		t.Fatalf("indent %s: %v", raw, err)
	}
	buf.WriteByte('\n')
	return buf.Bytes()
}

// TestRunnerDescriptorEnvelopeGolden fails on any change to the encoded
// envelope or types.Descriptor shape. A deliberate compatible change updates
// the golden; an incompatible one also bumps RunnerDescriptorSchema.
func TestRunnerDescriptorEnvelopeGolden(t *testing.T) {
	raw, err := EncodeRunnerDescriptors(fullRunnerDescriptorSet())
	if err != nil {
		t.Fatalf("EncodeRunnerDescriptors: %v", err)
	}
	got := indentJSON(t, raw)
	if *updateRunnerDescriptors {
		if err := os.MkdirAll(filepath.Dir(runnerDescriptorGolden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(runnerDescriptorGolden, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(runnerDescriptorGolden)
	if err != nil {
		t.Fatalf("%v (run with -update-runner-descriptors to create)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s is stale; if the change is intended, rerun with -update-runner-descriptors, review the diff, and bump RunnerDescriptorSchema when it is incompatible\ngot:\n%s", runnerDescriptorGolden, got)
	}
}

func TestRunnerDescriptorEnvelopeRoundTrip(t *testing.T) {
	raw, err := EncodeRunnerDescriptors(fullRunnerDescriptorSet())
	if err != nil {
		t.Fatalf("EncodeRunnerDescriptors: %v", err)
	}
	env, err := DecodeRunnerDescriptorEnvelope(raw)
	if err != nil {
		t.Fatalf("DecodeRunnerDescriptorEnvelope: %v", err)
	}
	if env.Schema != RunnerDescriptorSchema {
		t.Fatalf("schema = %q, want %q", env.Schema, RunnerDescriptorSchema)
	}
	if len(env.Descriptors) != 2 || env.Descriptors[0].Version != 1 || env.Descriptors[1].Version != 2 {
		t.Fatalf("entries = %+v, want acme.analyse v1 then v2", env.Descriptors)
	}

	decoded := make([]RunnerDescriptor, 0, len(env.Descriptors))
	for _, e := range env.Descriptors {
		d, err := DecodeRunnerDescriptor(e.Descriptor)
		if err != nil {
			t.Fatalf("DecodeRunnerDescriptor(%s v%d): %v", e.Type, e.Version, err)
		}
		decoded = append(decoded, RunnerDescriptor{Type: e.Type, Version: e.Version, Descriptor: d})
	}

	t.Run("ReencodesByteIdentical", func(t *testing.T) {
		again, err := EncodeRunnerDescriptors(decoded)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if !bytes.Equal(again, raw) {
			t.Errorf("re-encoded envelope differs\n got: %s\nwant: %s", again, raw)
		}
	})

	d := decoded[1].Descriptor
	p := d.Params[0]
	t.Run("LargeIntegerDefaultKeepsPrecision", func(t *testing.T) {
		n, ok := p.Default.(json.Number)
		if !ok {
			t.Fatalf("Default = %T(%v), want json.Number", p.Default, p.Default)
		}
		if got, want := n.String(), "1152921504606846977"; got != want {
			t.Errorf("Default = %s, want %s", got, want)
		}
	})
	t.Run("NilEqStaysNil", func(t *testing.T) {
		if p.VisibleWhen.Eq != nil {
			t.Errorf("VisibleWhen.Eq = %#v, want nil", p.VisibleWhen.Eq)
		}
	})
	t.Run("EmptyVersusNilInSurvives", func(t *testing.T) {
		if p.VisibleWhen.In == nil || len(p.VisibleWhen.In) != 0 {
			t.Errorf("VisibleWhen.In = %#v, want empty non-nil", p.VisibleWhen.In)
		}
		if p.VisibleWhen.AllOf[0].In != nil {
			t.Errorf("VisibleWhen.AllOf[0].In = %#v, want nil", p.VisibleWhen.AllOf[0].In)
		}
	})
	t.Run("DescriptorTypePreserved", func(t *testing.T) {
		if d.Type != "acme.analyse" || d.Kind != types.NodeKindAction {
			t.Errorf("Type/Kind = %q/%q, want acme.analyse/action", d.Type, d.Kind)
		}
	})
}

func TestEncodeRunnerDescriptorsEmpty(t *testing.T) {
	raw, err := EncodeRunnerDescriptors(nil)
	if err != nil || raw != nil {
		t.Fatalf("EncodeRunnerDescriptors(nil) = %q, %v; want nil, nil", raw, err)
	}
}

func TestDecodeRunnerDescriptorEnvelope(t *testing.T) {
	entry := `{"type":"acme.x","version":1,"descriptor":{"Type":"acme.x"}}`
	many := `{"schema":"` + RunnerDescriptorSchema + `","descriptors":[` +
		strings.TrimSuffix(strings.Repeat(entry+",", MaxRunnerDescriptorEntries+1), ",") + `]}`
	huge := `{"schema":"` + RunnerDescriptorSchema + `","descriptors":[],"pad":"` +
		strings.Repeat("x", MaxRunnerDescriptorEnvelopeBytes) + `"}`

	tests := []struct {
		name    string
		raw     string
		wantErr error
		wantN   int
	}{
		{"empty reports none", "", nil, 0},
		{"null reports none", "null", nil, 0},
		{"valid", `{"schema":"` + RunnerDescriptorSchema + `","descriptors":[` + entry + `]}`, nil, 1},
		{"unknown fields ignored", `{"schema":"` + RunnerDescriptorSchema + `","extra":1,"descriptors":[{"type":"acme.x","version":1,"new":true,"descriptor":{}}]}`, nil, 1},
		{"unknown schema", `{"schema":"xflow.runner-descriptors/v2","descriptors":[` + entry + `]}`, ErrRunnerDescriptorsUnknownSchema, 0},
		{"missing schema", `{"descriptors":[]}`, ErrRunnerDescriptorsUnknownSchema, 0},
		{"malformed", `{"schema":`, ErrRunnerDescriptorsMalformed, 0},
		{"too many entries", many, ErrRunnerDescriptorsTooMany, 0},
		{"too large", huge, ErrRunnerDescriptorsTooLarge, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := DecodeRunnerDescriptorEnvelope([]byte(tt.raw))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("DecodeRunnerDescriptorEnvelope err = %v, want %v", err, tt.wantErr)
			}
			if len(env.Descriptors) != tt.wantN {
				t.Errorf("entries = %d, want %d", len(env.Descriptors), tt.wantN)
			}
		})
	}
}

func TestDecodeRunnerDescriptor(t *testing.T) {
	tooLarge := `{"Type":"acme.x","Docs":"` + strings.Repeat("x", MaxRunnerDescriptorBytes) + `"}`
	tests := []struct {
		name    string
		raw     string
		wantErr error
	}{
		{"valid", `{"Type":"acme.x","FutureField":1}`, nil},
		{"too large", tooLarge, ErrRunnerDescriptorTooLarge},
		{"malformed", `{"Type":`, ErrRunnerDescriptorMalformed},
		{"wrong shape", `{"Params":"nope"}`, ErrRunnerDescriptorMalformed},
		{"trailing data", `{"Type":"acme.x"} {}`, ErrRunnerDescriptorMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeRunnerDescriptor(json.RawMessage(tt.raw))
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("DecodeRunnerDescriptor(%.40s) err = %v, want %v", tt.raw, err, tt.wantErr)
			}
		})
	}
}
