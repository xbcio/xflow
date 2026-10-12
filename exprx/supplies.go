package exprx

import (
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/supply"
)

// SuppliesEnv returns the extra-env fragment that exposes $supplies over an
// explicit registry, scoped to one namespace. Tests and embedded callers use
// it; production goes through BuildExprEnv's default, which reads
// supply.Default for the executing input's namespace.
//
// The namespace is a required argument rather than a default: supply content is
// namespace-scoped, and a view that silently fell back to the default namespace
// would resolve a same-named supply from the wrong tenant.
func SuppliesEnv(reg *supply.Registry, ns namespace.Namespace) map[string]any {
	return map[string]any{"$supplies": reg.Decoded(ns)}
}
