package exprx

import "github.com/xbcio/xflow/node/supply"

// SuppliesEnv returns the extra-env fragment that exposes $supplies over an
// explicit registry. Tests and embedded callers use it; production goes through
// BuildExprEnv's default, which reads supply.Default.
func SuppliesEnv(reg *supply.Registry) map[string]any {
	return map[string]any{"$supplies": reg.Decoded()}
}
