package supply

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

func TestStaticDescriptorAndParams(t *testing.T) {
	n := Static([]byte(`{"rules":[]}`))
	d := n.Descriptor()
	if d.Type != "xflow.supply.static" || d.Kind != types.NodeKindSupply {
		t.Fatalf("descriptor = %+v", d)
	}
	if len(d.Outputs) != 0 {
		t.Fatalf("a supply node must declare no output ports, got %d", len(d.Outputs))
	}
	m := n.RawParams().(map[string]any)
	if m["content"] != `{"rules":[]}` {
		t.Fatalf("content = %#v", m["content"])
	}
	// static is always ready: the content travels with the definition.
	if m["require_ready"] != true {
		t.Fatalf("require_ready = %#v, want true", m["require_ready"])
	}
}
