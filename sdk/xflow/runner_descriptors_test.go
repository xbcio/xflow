package xflow

import (
	"context"
	"log/slog"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// runnerDescriptorHandler is registered in the global node registry under
// type names unique to this file, so other suites never see it.
type runnerDescriptorHandler struct {
	typ     string
	version int
	params  []types.ParamSpec
}

func (h runnerDescriptorHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.typ, Kind: types.NodeKindAction, DisplayName: h.typ, Params: h.params}
}

func (h runnerDescriptorHandler) NodeVersion() int { return h.version }

func (runnerDescriptorHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: input.Data}, nil
}

func decodeRunnerDescriptorEnvelope(t *testing.T, raw []byte) []protocol.RunnerDescriptor {
	t.Helper()
	env, err := protocol.DecodeRunnerDescriptorEnvelope(raw)
	if err != nil {
		t.Fatalf("DecodeRunnerDescriptorEnvelope: %v", err)
	}
	out := make([]protocol.RunnerDescriptor, 0, len(env.Descriptors))
	for _, e := range env.Descriptors {
		d, err := protocol.DecodeRunnerDescriptor(e.Descriptor)
		if err != nil {
			t.Fatalf("DecodeRunnerDescriptor(%s v%d): %v", e.Type, e.Version, err)
		}
		out = append(out, protocol.RunnerDescriptor{Type: e.Type, Version: e.Version, Descriptor: d})
	}
	return out
}

func TestDeclaredRunnerDescriptors(t *testing.T) {
	registered := []registry.RegisteredDescriptor{
		{Type: "acme.analyse", Version: 1, Descriptor: types.Descriptor{Type: "acme.analyse", DisplayName: "v1"}},
		{Type: "acme.analyse", Version: 2, Descriptor: types.Descriptor{Type: "acme.analyse", DisplayName: "v2"}},
		{Type: "acme.other", Version: 1, Descriptor: types.Descriptor{Type: "acme.other"}},
		{Type: "xflow.function", Version: 1, Descriptor: types.Descriptor{Type: "xflow.function"}},
	}

	t.Run("KeepsOnlyDeclaredTypesAllVersions", func(t *testing.T) {
		got := declaredRunnerDescriptors([]string{" acme.analyse ", "xflow.function", "acme.unregistered"}, registered)
		want := []protocol.RunnerDescriptor{
			{Type: "acme.analyse", Version: 1, Descriptor: types.Descriptor{Type: "acme.analyse", DisplayName: "v1"}},
			{Type: "acme.analyse", Version: 2, Descriptor: types.Descriptor{Type: "acme.analyse", DisplayName: "v2"}},
			{Type: "xflow.function", Version: 1, Descriptor: types.Descriptor{Type: "xflow.function"}},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("declaredRunnerDescriptors = %+v, want %+v", got, want)
		}
	})

	t.Run("NilWhenNothingDeclared", func(t *testing.T) {
		if got := declaredRunnerDescriptors(nil, registered); got != nil {
			t.Fatalf("declaredRunnerDescriptors(nil) = %+v, want nil", got)
		}
		if got := declaredRunnerDescriptors([]string{"", "  "}, registered); got != nil {
			t.Fatalf("declaredRunnerDescriptors(blank) = %+v, want nil", got)
		}
	})

	t.Run("NilWhenNoDeclaredTypeIsRegistered", func(t *testing.T) {
		if got := declaredRunnerDescriptors([]string{"acme.unregistered"}, registered); got != nil {
			t.Fatalf("declaredRunnerDescriptors = %+v, want nil", got)
		}
		if raw := runnerDescriptorsJSON([]string{"acme.unregistered"}, registered, slog.Default()); raw != nil {
			t.Fatalf("runnerDescriptorsJSON = %s, want nil", raw)
		}
	})

	t.Run("EncodeFailureReportsNone", func(t *testing.T) {
		bad := []registry.RegisteredDescriptor{{
			Type: "acme.bad", Version: 1,
			Descriptor: types.Descriptor{Type: "acme.bad", Params: []types.ParamSpec{{Name: "p", Default: func() {}}}},
		}}
		if raw := runnerDescriptorsJSON([]string{"acme.bad"}, bad, slog.Default()); raw != nil {
			t.Fatalf("runnerDescriptorsJSON = %s, want nil on encode failure", raw)
		}
	})
}

// The service config a runner registers with carries the envelope of its
// declared types from the global node registry, and nothing for a runner whose
// declared types are not registered there.
func TestBuildRunnerServiceConfigReportsDeclaredDescriptors(t *testing.T) {
	const typ = "test.runner-descriptors/sdk-config"
	const undeclared = "test.runner-descriptors/sdk-undeclared"
	params := []types.ParamSpec{{Name: "limit", Type: types.ParamNumber, Default: 3}}
	registry.Register(runnerDescriptorHandler{typ: typ, version: 1, params: params})
	registry.Register(runnerDescriptorHandler{typ: typ, version: 2, params: params})
	registry.Register(runnerDescriptorHandler{typ: undeclared, version: 1})

	svcCfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{typ},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	got := decodeRunnerDescriptorEnvelope(t, svcCfg.DescriptorsJSON)
	if len(got) != 2 {
		t.Fatalf("reported %d descriptors, want 2 (both versions of %s): %+v", len(got), typ, got)
	}
	for i, d := range got {
		if d.Type != typ || d.Version != i+1 || d.Descriptor.Type != typ {
			t.Fatalf("descriptor[%d] = %s v%d (Descriptor.Type %q), want %s v%d", i, d.Type, d.Version, d.Descriptor.Type, typ, i+1)
		}
		if len(d.Descriptor.Params) != 1 || d.Descriptor.Params[0].Name != "limit" {
			t.Fatalf("descriptor[%d] params = %+v, want the registered limit param", i, d.Descriptor.Params)
		}
	}

	none, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://server:8080",
		Capabilities: []string{"test.runner-descriptors/sdk-never-registered"},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if none.DescriptorsJSON != nil {
		t.Fatalf("DescriptorsJSON = %s, want nil when no declared type is registered", none.DescriptorsJSON)
	}
}
