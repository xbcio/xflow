package graph

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

func fptr(v float64) *float64 { return &v }
func iptr(v int) *int         { return &v }
func bptr(v bool) *bool       { return &v }

// issueKeys renders issues as "path code severity" for compact assertions.
func issueKeys(issues []ParamIssue) []string {
	out := make([]string, len(issues))
	for i, is := range issues {
		out[i] = is.Path + " " + is.Code + " " + is.Severity
	}
	return out
}

func assertIssues(t *testing.T, issues []ParamIssue, want ...string) {
	t.Helper()
	got := issueKeys(issues)
	if len(want) == 0 {
		want = []string{}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("issues = %q\nwant     %q", got, want)
	}
}

func deepCopyJSON(t *testing.T, v map[string]any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestValidateParamsRequired(t *testing.T) {
	desc := types.Descriptor{Params: []types.ParamSpec{
		{Name: "url", Type: types.ParamString, Required: true},
		{Name: "opt", Type: types.ParamString},
	}}
	for name, params := range map[string]map[string]any{
		"missing": {},
		"nil":     {"url": nil},
		"empty":   {"url": ""},
	} {
		t.Run(name, func(t *testing.T) {
			issues := ValidateParams(desc, params)
			assertIssues(t, issues, "/parameters/url required error")
			if issues[0].Node != "" {
				t.Fatalf("ValidateParams must leave Node empty, got %q", issues[0].Node)
			}
		})
	}
	assertIssues(t, ValidateParams(desc, map[string]any{"url": "x"}))
	// Zero values other than "" and nil are set.
	desc.Params[0].Type = types.ParamNumber
	assertIssues(t, ValidateParams(desc, map[string]any{"url": 0}))
}

func TestValidateParamsRequiredWhen(t *testing.T) {
	desc := types.Descriptor{Params: []types.ParamSpec{
		{Name: "operation", Type: types.ParamString},
		{Name: "where", Type: types.ParamObject, RequiredWhen: &types.Condition{Param: "operation", In: []any{"update", "delete"}}},
	}}
	assertIssues(t, ValidateParams(desc, map[string]any{"operation": "delete"}), "/parameters/where required error")
	assertIssues(t, ValidateParams(desc, map[string]any{"operation": "select"}))
	assertIssues(t, ValidateParams(desc, map[string]any{"operation": "update", "where": map[string]any{"id": 1}}))
}

func TestValidateParamsEnumAndEnumWhen(t *testing.T) {
	desc := types.Descriptor{Params: []types.ParamSpec{
		{Name: "language", Type: types.ParamString, Enum: []types.EnumOption{{Value: "js"}, {Value: "wasm"}}},
		{Name: "runtime", Type: types.ParamString,
			Enum: []types.EnumOption{{Value: "goja"}, {Value: "qjs"}, {Value: "wazero"}},
			EnumWhen: []types.ConditionalEnum{
				{When: types.Condition{Param: "language", Eq: "js"}, Enum: []types.EnumOption{{Value: "goja"}, {Value: "qjs"}}},
				{When: types.Condition{Param: "language", Eq: "wasm"}, Enum: []types.EnumOption{{Value: "wazero"}}},
			}},
		{Name: "level", Type: types.ParamNumber, Enum: []types.EnumOption{{Value: 1}, {Value: 2}}},
	}}
	assertIssues(t, ValidateParams(desc, map[string]any{"language": "js", "runtime": "qjs"}))
	assertIssues(t, ValidateParams(desc, map[string]any{"language": "js", "runtime": "wazero"}), "/parameters/runtime enum error")
	assertIssues(t, ValidateParams(desc, map[string]any{"language": "wasm", "runtime": "goja"}), "/parameters/runtime enum error")
	// No EnumWhen matches: fall back to Enum.
	assertIssues(t, ValidateParams(desc, map[string]any{"runtime": "wazero"}))
	assertIssues(t, ValidateParams(desc, map[string]any{"language": "python"}), "/parameters/language enum error")
	// JSON float64 equals the int literal.
	assertIssues(t, ValidateParams(desc, map[string]any{"level": float64(2)}))
	assertIssues(t, ValidateParams(desc, map[string]any{"level": "2"}), "/parameters/level enum error")
	// An unset value is not checked against the Enum.
	assertIssues(t, ValidateParams(desc, map[string]any{"language": ""}))
}

func TestValidateParamsConstraints(t *testing.T) {
	desc := types.Descriptor{Params: []types.ParamSpec{
		{Name: "n", Type: types.ParamNumber, Constraints: &types.Constraints{Min: fptr(1), Max: fptr(10)}},
		{Name: "s", Type: types.ParamString, Constraints: &types.Constraints{MinLength: iptr(2), MaxLength: iptr(3), Pattern: `^[a-z]+$`}},
		{Name: "a", Type: types.ParamArray, Constraints: &types.Constraints{MinItems: iptr(1), MaxItems: iptr(2), UniqueItems: true}},
	}}
	cases := []struct {
		name   string
		params map[string]any
		want   []string
	}{
		{"all valid", map[string]any{"n": 5, "s": "ab", "a": []any{"x"}}, nil},
		{"min", map[string]any{"n": 0.5}, []string{"/parameters/n constraint.min error"}},
		{"max", map[string]any{"n": float64(11)}, []string{"/parameters/n constraint.max error"}},
		{"bounds inclusive", map[string]any{"n": 10, "s": "abc", "a": []any{1, 2}}, nil},
		{"min_length", map[string]any{"s": "a"}, []string{"/parameters/s constraint.min_length error"}},
		{"max_length counts runes", map[string]any{"s": "äöü"}, []string{"/parameters/s constraint.pattern error"}},
		{"max_length", map[string]any{"s": "abcd"}, []string{"/parameters/s constraint.max_length error"}},
		{"pattern", map[string]any{"s": "A1"}, []string{"/parameters/s constraint.pattern error"}},
		{"min_items", map[string]any{"a": []any{}}, []string{"/parameters/a constraint.min_items error"}},
		{"max_items", map[string]any{"a": []string{"x", "y", "z"}}, []string{"/parameters/a constraint.max_items error"}},
		{"unique_items numeric", map[string]any{"a": []any{1, float64(1)}}, []string{"/parameters/a constraint.unique_items error"}},
		// No type checks: a bound whose value has another type is not applied.
		{"wrong type ignored", map[string]any{"n": "zero", "s": 5, "a": "x"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertIssues(t, ValidateParams(desc, tc.params), tc.want...)
		})
	}
}

func TestValidateParamsFormats(t *testing.T) {
	spec := func(name, format string) types.ParamSpec {
		return types.ParamSpec{Name: name, Type: types.ParamString, Constraints: &types.Constraints{Format: format}}
	}
	desc := types.Descriptor{Params: []types.ParamSpec{
		spec("duration", FormatDuration),
		spec("cron", FormatCron),
		spec("expression", FormatExpression),
		spec("digest", FormatSHA256Digest),
		spec("json", FormatJSON),
		spec("url", "url"),
		spec("hostport", "host-port"),
		spec("code", "code"),
	}}
	valid := map[string]any{
		"duration":   "1h30m",
		"cron":       "*/5 * * * *",
		"expression": `$input.amount >= 100 && sprintf("%.2f", 1.5) != ""`,
		"digest":     "sha256:" + strings.Repeat("a", 64),
		"json":       `{"a":[1,2]}`,
		// Advisory formats are never checked.
		"url":      "not a url",
		"hostport": "nope",
		"code":     "}{",
	}
	assertIssues(t, ValidateParams(desc, valid))
	assertIssues(t, ValidateParams(desc, map[string]any{"cron": "@every 1s"}))

	invalid := map[string]any{
		"duration":   "5 minutes",
		"cron":       "* * *",
		"expression": "a +",
		"digest":     "sha256:ABC",
		"json":       "{",
	}
	assertIssues(t, ValidateParams(desc, invalid),
		"/parameters/duration format.duration error",
		"/parameters/cron format.cron error",
		"/parameters/expression format.expression error",
		"/parameters/digest format.sha256-digest error",
		"/parameters/json format.json error",
	)
	// A structured value is JSON by construction; a non-string duration is
	// not type-checked.
	assertIssues(t, ValidateParams(desc, map[string]any{"json": map[string]any{"a": 1}, "duration": 5}))
}

func TestValidateParamsMessagesNeverCarryValues(t *testing.T) {
	desc := types.Descriptor{Params: []types.ParamSpec{
		{Name: "token", Type: types.ParamString, Enum: []types.EnumOption{{Value: "a"}}, Constraints: &types.Constraints{Pattern: `^a$`, Format: FormatJSON}},
	}}
	const secret = "s3cr3t-value"
	for _, is := range ValidateParams(desc, map[string]any{"token": secret}) {
		if strings.Contains(is.Message, secret) {
			t.Fatalf("message leaks the value: %q", is.Message)
		}
	}
}

func TestValidateParamsTemplatesOnlyCheckedForSet(t *testing.T) {
	desc := types.Descriptor{Params: []types.ParamSpec{
		{Name: "mode", Type: types.ParamString, Required: true, Enum: []types.EnumOption{{Value: "a"}}},
		{Name: "n", Type: types.ParamNumber, Constraints: &types.Constraints{Min: fptr(1)}},
		{Name: "d", Type: types.ParamString, Constraints: &types.Constraints{Format: FormatDuration}},
		{Name: "obj", Type: types.ParamObject, Fields: []types.ParamSpec{{Name: "x", Type: types.ParamString, Enum: []types.EnumOption{{Value: "ok"}}}}},
	}}
	params := map[string]any{
		"mode": "${{ $config.mode }}",
		"n":    "{{ $vars.n }}",
		"d":    "{{ $config.minutes }}m", // partial template counts
		"obj":  map[string]any{"x": "prefix-{{ $vars.x }}"},
	}
	assertIssues(t, ValidateParams(desc, params))
	// A template in a required param still counts as set.
	assertIssues(t, ValidateParams(desc, map[string]any{"mode": "{{ $vars.m }}"}))
	// An unterminated "{{" is not a template: the value is checked.
	assertIssues(t, ValidateParams(desc, map[string]any{"mode": "{{ oops"}), "/parameters/mode enum error")
}

func TestValidateParamsHiddenParams(t *testing.T) {
	desc := types.Descriptor{
		Params: []types.ParamSpec{
			{Name: "mode", Type: types.ParamString},
			{Name: "expression", Type: types.ParamString, Required: true,
				VisibleWhen: &types.Condition{Param: "mode", Eq: "expression"},
				Constraints: &types.Constraints{Format: FormatExpression}},
			{Name: "code", Type: types.ParamString},
			{Name: "placeholder", Type: types.ParamString, VisibleWhen: &types.Condition{Not: &types.Condition{}},
				Enum: []types.EnumOption{{Value: "never"}}},
		},
		OneOf: []types.OneOfGroup{{Params: []string{"code", "placeholder"}}},
	}
	// Hidden: Required, Enum, and Format are all skipped...
	assertIssues(t, ValidateParams(desc, map[string]any{"mode": "rules", "expression": "a +", "placeholder": "x"}))
	// ...but the hidden placeholder still counts toward OneOf.
	assertIssues(t, ValidateParams(desc, map[string]any{"code": "c", "placeholder": "x"}), "/parameters/code one_of error")
	// Visible again: the rules apply.
	assertIssues(t, ValidateParams(desc, map[string]any{"mode": "expression", "code": "c"}), "/parameters/expression required error")
}

func TestValidateParamsOneOfModes(t *testing.T) {
	group := func(mode string) types.Descriptor {
		return types.Descriptor{
			Params: []types.ParamSpec{{Name: "a"}, {Name: "b"}},
			OneOf:  []types.OneOfGroup{{Params: []string{"a", "b"}, Mode: mode}},
		}
	}
	none := map[string]any{"a": "", "b": nil}
	one := map[string]any{"a": "x"}
	both := map[string]any{"a": "x", "b": "{{ $vars.b }}"} // a template counts as set
	cases := []struct {
		mode            string
		none, one, both bool // whether an issue is expected
	}{
		{"", true, false, true},
		{types.OneOfExactly, true, false, true},
		{types.OneOfAtMost, false, false, true},
		{types.OneOfAtLeast, true, false, false},
	}
	for _, tc := range cases {
		desc := group(tc.mode)
		for name, c := range map[string]struct {
			params map[string]any
			want   bool
		}{"none": {none, tc.none}, "one": {one, tc.one}, "both": {both, tc.both}} {
			got := ValidateParams(desc, c.params)
			if (len(got) > 0) != c.want {
				t.Errorf("mode %q %s: issues %q, want issue=%v", tc.mode, name, issueKeys(got), c.want)
			}
			if len(got) > 0 && (got[0].Code != ParamIssueCodeOneOf || got[0].Path != "/parameters/a") {
				t.Errorf("mode %q %s: issue %+v", tc.mode, name, got[0])
			}
		}
		if got := ValidateOneOf(desc, both); (len(got) > 0) != tc.both {
			t.Errorf("ValidateOneOf mode %q: %q", tc.mode, issueKeys(got))
		}
	}
}

func TestValidateParamsRecursesIntoFieldsAndItems(t *testing.T) {
	desc := types.Descriptor{Params: []types.ParamSpec{
		{Name: "aggregate", Type: types.ParamObject, Fields: []types.ParamSpec{
			{Name: "on_overflow", Type: types.ParamString, Enum: []types.EnumOption{{Value: "discard"}, {Value: "dead_letter"}}},
			{Name: "dead_letter_topic", Type: types.ParamString,
				VisibleWhen:  &types.Condition{Param: "on_overflow", Eq: "dead_letter"},
				RequiredWhen: &types.Condition{Param: "on_overflow", Eq: "dead_letter"}},
		}},
		{Name: "rules", Type: types.ParamArray, Item: &types.ParamSpec{Type: types.ParamObject, Fields: []types.ParamSpec{
			{Name: "condition", Type: types.ParamString, Required: true, Constraints: &types.Constraints{Format: FormatExpression}},
		}}},
		{Name: "tags", Type: types.ParamArray, Item: &types.ParamSpec{Type: types.ParamString, Constraints: &types.Constraints{Pattern: `^[a-z]+$`}}},
		{Name: "a/b~c", Type: types.ParamString, Required: true},
	}}
	params := map[string]any{
		"aggregate": map[string]any{"on_overflow": "dead_letter"},
		"rules":     []any{map[string]any{"condition": "x > 1"}, map[string]any{}, map[string]any{"condition": "a +"}},
		"tags":      []string{"ok", "NO", ""},
	}
	// A template anywhere in a value exempts the whole value from shape checks.
	assertIssues(t, ValidateParams(desc, map[string]any{"a/b~c": "x", "tags": []any{"NO", "{{ $vars.t }}"}}))
	assertIssues(t, ValidateParams(desc, params),
		"/parameters/aggregate/dead_letter_topic required error",
		"/parameters/rules/1/condition required error",
		"/parameters/rules/2/condition format.expression error",
		"/parameters/tags/1 constraint.pattern error",
		"/parameters/a~1b~0c required error",
	)
}

func TestValidateParamsDoesNotMutate(t *testing.T) {
	desc := types.Descriptor{Params: []types.ParamSpec{
		{Name: "mode", Type: types.ParamString, Default: "signal"},
		{Name: "obj", Type: types.ParamObject, Fields: []types.ParamSpec{{Name: "x", Default: 1, Required: true}}},
	}}
	params := map[string]any{"obj": map[string]any{"y": []any{float64(1)}}}
	before := deepCopyJSON(t, params)
	_ = ValidateParams(desc, params)
	if !reflect.DeepEqual(params, before) {
		t.Fatalf("ValidateParams mutated params: %v, was %v", params, before)
	}
}

func TestValidateParamsAdvisoryAllowlist(t *testing.T) {
	enum := []types.EnumOption{{Value: "GET"}, {Value: "POST"}}
	httpDesc := types.Descriptor{Type: "xflow.http", Params: []types.ParamSpec{
		{Name: "method", Type: types.ParamString, Enum: enum},
		{Name: "mode", Type: types.ParamString, Enum: []types.EnumOption{{Value: "json"}}},
	}}
	issues := ValidateParams(httpDesc, map[string]any{"method": "OPTIONS", "mode": "xml"})
	assertIssues(t, issues, "/parameters/method enum warning", "/parameters/mode enum error")

	// The allowlist is keyed by node type: the same path elsewhere is an error.
	other := httpDesc
	other.Type = "custom.http"
	assertIssues(t, ValidateParams(other, map[string]any{"method": "OPTIONS"}), "/parameters/method enum error")

	// Warnings alone never count as errors (what enforce rejects).
	if HasParamErrors(issues[:1]) {
		t.Fatal("a warning-only issue list must not count as errors")
	}
	if !HasParamErrors(issues) {
		t.Fatal("an error issue must count")
	}

	waitDesc := types.Descriptor{Type: "xflow.wait", Params: []types.ParamSpec{
		{Name: "timeout", Type: types.ParamString, Constraints: &types.Constraints{Format: FormatDuration}},
		{Name: "duration", Type: types.ParamString, Constraints: &types.Constraints{Format: FormatDuration}},
	}}
	assertIssues(t, ValidateParams(waitDesc, map[string]any{"timeout": "2 days", "duration": "2 days"}),
		"/parameters/timeout format.duration warning", "/parameters/duration format.duration error")

	kafkaDesc := types.Descriptor{Type: "xflow.trigger.kafka", Params: []types.ParamSpec{
		{Name: "aggregate", Type: types.ParamObject, Fields: []types.ParamSpec{{Name: "on_overflow", Enum: []types.EnumOption{{Value: "block"}}}}},
		{Name: "message_schema", Type: types.ParamObject, Fields: []types.ParamSpec{{Name: "on_invalid", Enum: []types.EnumOption{{Value: "fail"}}}}},
	}}
	assertIssues(t, ValidateParams(kafkaDesc, map[string]any{
		"aggregate":      map[string]any{"on_overflow": "BLOCK"},
		"message_schema": map[string]any{"on_invalid": "FAIL"},
	}), "/parameters/aggregate/on_overflow enum warning", "/parameters/message_schema/on_invalid enum warning")
}

func TestValidateParamsSDKShapedValues(t *testing.T) {
	// SDK params are not JSON-normalized: typed slices/maps and typed nils.
	type rule struct {
		Condition string `json:"condition"`
	}
	desc := types.Descriptor{Params: []types.ParamSpec{
		{Name: "signals", Type: types.ParamArray, Required: true, Constraints: &types.Constraints{UniqueItems: true}},
		{Name: "headers", Type: types.ParamObject, Fields: []types.ParamSpec{{Name: "X", Enum: []types.EnumOption{{Value: "1"}}}}},
		{Name: "quorum", Type: types.ParamNumber, Constraints: &types.Constraints{Min: fptr(1)},
			VisibleWhen: &types.Condition{Param: "signals", Truthy: bptr(true)}},
	}}
	var nilSlice []string
	assertIssues(t, ValidateParams(desc, map[string]any{"signals": nilSlice, "quorum": 0}), "/parameters/signals required error")
	assertIssues(t, ValidateParams(desc, map[string]any{
		"signals": []string{"a", "a"},
		"headers": map[string]string{"X": "2"},
		"quorum":  int64(0),
	}), "/parameters/signals constraint.unique_items error", "/parameters/headers/X enum error", "/parameters/quorum constraint.min error")
	_ = rule{}
}
