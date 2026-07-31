package supply

import (
	nodeinternal "github.com/xbcio/xflow/node/internal"
	xsupply "github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

// StaticNode declares supply content that travels with the workflow definition.
// It is the legitimate shape of "there is no remote source at all" — distinct
// from "there is a source but the content has not arrived yet", which the
// readiness gate handles. Static content is therefore always ready.
//
// This is NOT a fallback for an external supply. The spec deliberately removed
// the `default: <bytes>` tier: serving live traffic with stale embedded rules
// produces wrong data that looks right, which is worse than not serving.
type StaticNode struct {
	nodeinternal.BaseNode
	ContentValue []byte
}

// Static returns a supply declaration whose content is the given bytes.
func Static(content []byte) *StaticNode {
	return &StaticNode{ContentValue: content}
}

func (n *StaticNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.supply.static",
		Kind:        types.NodeKindSupply,
		DisplayName: "Static Supply",
		Params: []types.ParamSpec{
			{Name: xsupply.ParamContent, DisplayName: "Content", Type: types.ParamString, Required: true,
				Description: "Literal supply content"},
			{Name: xsupply.ParamRequireReady, DisplayName: "Require Ready", Type: types.ParamBool, Default: true},
		},
	}
}

func (n *StaticNode) NodeType() string { return "xflow.supply.static" }

func (n *StaticNode) RawParams() any {
	return map[string]any{
		xsupply.ParamContent: string(n.ContentValue),
		// Static content is present by construction, so the gate is trivially
		// satisfied. Keeping the key makes the readiness read path uniform.
		xsupply.ParamRequireReady: true,
	}
}

func (n *StaticNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
