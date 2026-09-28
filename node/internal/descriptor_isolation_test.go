package internal

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestDefinitionDescriptorIsolation pins that Definition.Descriptor hands out
// a copy in both directions: a caller writing into the returned descriptor
// cannot reach the definition, and builder calls made after the caller took
// its copy cannot write into the caller's slices.
func TestDefinitionDescriptorIsolation(t *testing.T) {
	def := Define("xflow.test.descriptor_isolation", func(context.Context, *types.Input) (*types.Output, error) {
		return &types.Output{}, nil
	}).
		Param(types.ParamSpec{Name: "p", Type: types.ParamObject, Default: map[string]any{"k": "v"}}).
		Output("main").
		Credential("cred-a")

	got := def.Descriptor()
	got.Params[0].Name = "mutated"
	got.Params[0].Default.(map[string]any)["k"] = "mutated"
	got.Outputs[0].Name = "mutated"
	got.Credentials[0] = "mutated"

	fresh := def.Descriptor()
	if fresh.Params[0].Name != "p" || fresh.Params[0].Default.(map[string]any)["k"] != "v" {
		t.Fatalf("caller write reached the definition's params: %#v", fresh.Params)
	}
	if fresh.Outputs[0].Name != "main" || fresh.Credentials[0] != "cred-a" {
		t.Fatalf("caller write reached the definition's ports/credentials: %#v / %#v", fresh.Outputs, fresh.Credentials)
	}

	held := def.Descriptor()
	def.Param(types.ParamSpec{Name: "late"}).Output("late").Credential("late")
	if len(held.Params) != 1 || len(held.Outputs) != 1 || len(held.Credentials) != 1 {
		t.Fatalf("later builder calls changed a descriptor the caller already held: %#v", held)
	}
	if n := len(def.Descriptor().Params); n != 2 {
		t.Fatalf("definition lost its own later Param call: %d params, want 2", n)
	}
}

// TestTriggerDefinitionDescriptorIsolation pins the same copy contract for
// trigger definitions, whose default Outputs slice is populated at
// DefineTrigger time.
func TestTriggerDefinitionDescriptorIsolation(t *testing.T) {
	def := DefineTrigger("xflow.test.trigger_descriptor_isolation",
		func(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
			return nil, nil
		})

	got := def.Descriptor()
	got.Outputs[0].Name = "mutated"

	if name := def.Descriptor().Outputs[0].Name; name != "main" {
		t.Fatalf("caller write reached the trigger definition: Outputs[0].Name = %q, want main", name)
	}
}
