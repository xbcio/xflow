package supply

import (
	"fmt"

	"github.com/spf13/cast"
)

// Parameter names shared by both supply node types. They live in this exported
// package rather than next to the node types because the control plane reads
// them when deriving activations, and node/internal/... is invisible to service/.
const (
	ParamResource     = "resource"
	ParamRequireReady = "require_ready"
	ParamContent      = "content"
)

// RequireReady reads the readiness policy. It defaults to TRUE — and the default
// must survive every malformed input, because false is the dangerous setting: it
// lets a runner take over an entry activation with no rules at all, tagging live
// traffic as if no rule matched.
//
// cast.ToBool(nil) returns false, so reading the key straight through cast would
// silently flip the default. Only an unambiguous false (bool false, "false",
// "0", "f", …) turns the gate off.
func RequireReady(params map[string]any) bool {
	if params == nil {
		return true
	}
	raw, ok := params[ParamRequireReady]
	if !ok || raw == nil {
		return true
	}
	v, err := cast.ToBoolE(raw)
	if err != nil {
		// Unparseable value: keep the safe default rather than guessing.
		return true
	}
	return v
}

// ResourceName resolves the SupplyResource name a supply node refers to: the
// "resource" parameter when set, otherwise the node's own name. Keeping them
// separable lets two workflows consume the same content under different local
// node names.
func ResourceName(nodeName string, params map[string]any) string {
	if s := cast.ToString(params[ParamResource]); s != "" {
		return s
	}
	return nodeName
}

// StaticContent extracts the literal content of an xflow.supply.static node.
// A missing or empty value is an error rather than empty bytes: "" and
// "the source says the rule set is empty" are different states (spec §5.10.4),
// and a static node with no content is a definition mistake.
func StaticContent(params map[string]any) ([]byte, error) {
	s := cast.ToString(params[ParamContent])
	if s == "" {
		return nil, fmt.Errorf("xflow.supply.static: %q parameter is required", ParamContent)
	}
	return []byte(s), nil
}
