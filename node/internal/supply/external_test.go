package supply

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestExternalDescriptorIsSupplyKind(t *testing.T) {
	d := External("rules").Descriptor()
	if d.Type != "xflow.supply.external" {
		t.Fatalf("type = %q", d.Type)
	}
	if d.Kind != types.NodeKindSupply {
		t.Fatalf("kind = %q, want %q", d.Kind, types.NodeKindSupply)
	}
	if len(d.Outputs) != 0 {
		t.Fatalf("a supply node must declare no output ports, got %d", len(d.Outputs))
	}
}

func TestExternalRawParams(t *testing.T) {
	got := External("shared-rules").RequireReady(false).RawParams()
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("RawParams = %T, want map[string]any", got)
	}
	if m["resource"] != "shared-rules" {
		t.Fatalf("resource = %#v", m["resource"])
	}
	if m["require_ready"] != false {
		t.Fatalf("require_ready = %#v, want false", m["require_ready"])
	}
}
