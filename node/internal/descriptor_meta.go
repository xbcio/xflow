package internal

import "github.com/xbcio/xflow/types"

// Helpers for the editor metadata builtin descriptors carry on their
// ParamSpecs (Enum, VisibleWhen, RequiredWhen, Constraints, Widget, ...).
// They only build values; nothing here is read at execution time.

// Widget names for ParamSpec.Widget. Set one only where the editor cannot
// infer the control from Type, Enum, Item, and Fields.
const (
	WidgetCode             = "code"
	WidgetBase64           = "base64"
	WidgetDuration         = "duration"
	WidgetDateTime         = "datetime"
	WidgetCron             = "cron"
	WidgetExpression       = "expression"
	WidgetCredentialSelect = "credential-select"
	WidgetPortSelect       = "port-select"
	WidgetKeyValue         = "key-value"
	WidgetKeyExpression    = "key-expression"
)

// Constraints.Format names. duration, cron, expression, and sha256-digest are
// enforced by the backend validator; url and host-port are advisory hints.
const (
	FormatDuration     = "duration"
	FormatCron         = "cron"
	FormatExpression   = "expression"
	FormatSHA256Digest = "sha256-digest"
	FormatURL          = "url"
	FormatHostPort     = "host-port"
)

// Options builds an Enum from bare string values.
func Options(values ...string) []types.EnumOption {
	out := make([]types.EnumOption, len(values))
	for i, v := range values {
		out[i] = types.EnumOption{Value: v}
	}
	return out
}

// StringItem is the Item schema of an array of strings.
func StringItem() *types.ParamSpec {
	return &types.ParamSpec{Type: types.ParamString}
}

// Float returns a pointer to v, for Constraints.Min / Constraints.Max.
func Float(v float64) *float64 { return &v }

// Format returns Constraints carrying only a Format.
func Format(format string) *types.Constraints {
	return &types.Constraints{Format: format}
}

// CondEq holds when param equals v.
func CondEq(param string, v any) *types.Condition {
	return &types.Condition{Param: param, Eq: v}
}

// CondIn holds when param equals any of vs. A nil entry matches a missing
// param, which is how "unset means the default" is spelled.
func CondIn(param string, vs ...any) *types.Condition {
	return &types.Condition{Param: param, In: vs}
}

// CondTruthy holds when param is truthy.
func CondTruthy(param string) *types.Condition {
	t := true
	return &types.Condition{Param: param, Truthy: &t}
}

// CondFalsy holds when param is falsy (missing, "", 0, false, or null).
func CondFalsy(param string) *types.Condition {
	f := false
	return &types.Condition{Param: param, Truthy: &f}
}

// CondAll holds when every condition holds.
func CondAll(cs ...*types.Condition) *types.Condition {
	all := make([]types.Condition, len(cs))
	for i, c := range cs {
		all[i] = *c
	}
	return &types.Condition{AllOf: all}
}

// CondNever never holds: a Condition with no clauses holds, so its negation
// never does. Used as VisibleWhen for params that exist in the wire format but
// must never be shown in an editor.
func CondNever() *types.Condition {
	return &types.Condition{Not: &types.Condition{}}
}
