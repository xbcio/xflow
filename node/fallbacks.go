package node

import core "github.com/xbcio/xflow/node/internal"

// Fallback records a builtin param whose handler substitutes a value when the
// param is absent although its ParamSpec declares no Default. See the field
// docs on the underlying type: Param is the dotted ParamSpec path
// ("tuning.start_id"), Value is the JSON-shaped constant (string, float64 or
// bool), and Derived describes a computed fallback (Value is then nil).
type Fallback = core.Fallback

// BuiltinFallbacks returns a copy of the builtin handler-fallback table, the
// read-only source the node-types projection exports as each field's
// display-only "fallback" hint. The entries are deliberately not ParamSpec
// Defaults (adding a Default moves SDK definition hashes). Mutating the
// returned slice does not affect the table: every Value is a scalar.
func BuiltinFallbacks() []Fallback { return core.BuiltinFallbacks() }
