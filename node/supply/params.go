package supply

import (
	"fmt"
	"reflect"

	"github.com/spf13/cast"
	"github.com/xbcio/xflow/types"
)

// Parameter names shared by the supply node types. They live in this exported
// package rather than next to the node types because the control plane reads
// them when deriving activations, and node/internal/... is invisible to service/.
//
// The three names the compiler also validates are aliases of the types package
// constants: engine/graph cannot import node/, so types is the only spelling
// both layers can share. ParamContent stays local — it is read only here.
const (
	ParamResource     = types.SupplyParamResource
	ParamRequireReady = types.SupplyParamRequireReady
	ParamContent      = "content"
)

// NodeTypeHTTPPull is the DSL node type of a runner-side pull supply: content
// fetched by the runner from an external HTTPS source. It is declared here
// (rather than only inside node/internal/supply) because the graph compiler and
// the control plane must recognize the same string.
const NodeTypeHTTPPull = types.SupplyPullNodeType

// Parameters of an xflow.supply.http node beyond the shared require_ready.
// ParamURL is required and https-only; ParamAuthentication names a credential
// in the RUNNER's process configuration (the definition never carries the
// credential itself); ParamPollInterval is an optional Go duration string that
// may only speed the runner's poll up.
const (
	ParamURL            = types.SupplyPullParamURL
	ParamAuthentication = types.SupplyPullParamAuthentication
	ParamPollInterval   = types.SupplyPullParamPollInterval
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
	if !ok || isNilValue(raw) {
		return true
	}
	v, err := cast.ToBoolE(raw)
	if err != nil {
		// Unparseable value: keep the safe default rather than guessing.
		return true
	}
	return v
}

// isNilValue reports whether v is nil — including typed nils (e.g. (*string)(nil)).
//
// In Go's interface semantics, a typed nil stored in an any variable is NOT
// == nil: the interface holds a non-nil type descriptor paired with a nil value.
// cast.ToBoolE treats a *string nil pointer as false with no error, which would
// silently flip the safe default to the dangerous tier. We use reflect to catch
// all nil-able kinds (Ptr, Map, Slice, Interface, Chan, Func).
func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface, reflect.Chan, reflect.Func:
		return rv.IsNil()
	}
	return false
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
