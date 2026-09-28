package xflow

import (
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/types"
)

// rawParamsBuilder is a Builder whose params are exactly what the test says,
// so a param the real node builders always emit (http "method", redis "mode")
// can be left out.
type rawParamsBuilder struct {
	desc   types.Descriptor
	params map[string]any
}

func (b *rawParamsBuilder) NodeType() string                    { return b.desc.Type }
func (b *rawParamsBuilder) RawParams() any                      { return b.params }
func (b *rawParamsBuilder) OnError(types.OnError) types.Builder { return b }
func (b *rawParamsBuilder) OnErrorStrategy() types.OnError      { return "" }
func (b *rawParamsBuilder) Descriptor() types.Descriptor        { return b.desc }
func (b *rawParamsBuilder) withParam(k string, v any) *rawParamsBuilder {
	b.params[k] = v
	return b
}

// http.method and trigger.redis.mode carry a Default and are not Required,
// so an absent (or nil) value is filled with the Default instead of failing
// the build. They used to be Required as well, which made the Default
// unreachable: validateParams returned the "missing" error first.
func TestValidateParamsWritesDefaultForOptionalParamsWithDefault(t *testing.T) {
	cases := []struct {
		name     string
		desc     types.Descriptor
		required map[string]any // the node's other Required params
		param    string
		want     any
	}{
		{"http.method", node.HTTP("", "").Descriptor(), map[string]any{"url": "https://example.invalid/"}, "method", "GET"},
		{"trigger.redis.mode", trigger.Redis().Descriptor(), map[string]any{"addr": "localhost:6379"}, "mode", "stream"},
	}
	for _, tc := range cases {
		for _, absent := range []struct {
			label  string
			params map[string]any
		}{
			{"missing", map[string]any{}},
			{"nil", map[string]any{tc.param: nil}},
		} {
			t.Run(tc.name+"/"+absent.label, func(t *testing.T) {
				for k, v := range tc.required {
					absent.params[k] = v
				}
				if err := validateParams("n", tc.desc.Params, absent.params); err != nil {
					t.Fatalf("validateParams: %v", err)
				}
				if got := absent.params[tc.param]; got != tc.want {
					t.Fatalf("%s = %#v, want Default %#v written", tc.param, got, tc.want)
				}
			})
		}
	}
}

// End to end through WorkflowBuilder.build: the normalized node params carry
// the Default.
func TestWorkflowBuildFillsHTTPMethodAndRedisModeDefaults(t *testing.T) {
	wf := Workflow("param-defaults")
	wf.Node("redis", &rawParamsBuilder{
		desc:   trigger.Redis().Descriptor(),
		params: map[string]any{"addr": "localhost:6379", "stream": "s", "group": "g"},
	})
	wf.Node("http", (&rawParamsBuilder{
		desc:   node.HTTP("", "").Descriptor(),
		params: map[string]any{},
	}).withParam("url", "https://example.invalid/"))

	def, err := wf.build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	got := map[string]map[string]any{}
	for _, n := range def.Nodes {
		got[n.Name] = n.Parameters
	}
	if m := got["http"]["method"]; m != "GET" {
		t.Errorf("http method = %#v, want GET", m)
	}
	if m := got["redis"]["mode"]; m != "stream" {
		t.Errorf("redis mode = %#v, want stream", m)
	}
}

// A param that is still Required keeps failing the build when absent.
func TestValidateParamsStillRejectsMissingRequired(t *testing.T) {
	desc := node.HTTP("", "").Descriptor()
	if err := validateParams("n", desc.Params, map[string]any{}); err == nil {
		t.Fatal("validateParams accepted an http node without url")
	}
}
