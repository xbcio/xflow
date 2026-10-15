package namespace

import (
	"fmt"
	"sort"
)

// NormalizeOwned normalizes a caller-supplied owned-namespace set: empty
// entries are dropped, every remaining name is validated, duplicates are
// removed, and the result is sorted. An input that carries no usable
// namespace resolves to nil, which every consumer treats as "legacy mode"
// (no owned-set scoping at all).
//
// The returned slice is a private copy; callers may not mutate it in place
// to influence other holders of the same set.
func NormalizeOwned(in []Namespace) ([]Namespace, error) {
	seen := make(map[Namespace]struct{}, len(in))
	out := make([]Namespace, 0, len(in))
	for _, ns := range in {
		if ns == "" {
			continue
		}
		if err := Validate(ns); err != nil {
			return nil, fmt.Errorf("owned namespace %q: %w", ns, err)
		}
		if _, dup := seen[ns]; dup {
			continue
		}
		seen[ns] = struct{}{}
		out = append(out, ns)
	}
	if len(out) == 0 {
		return nil, nil
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// DeploymentSuffix derives the per-deployment key and queue suffix for an
// owned namespace set, or an error for sets that cannot share one deployment
// identity unambiguously:
//
//	nil / empty        -> ""                      (legacy: unscoped process)
//	contains Default   -> ""                      (legacy: the default deployment must keep
//	                                              byte-identical key and queue names so that
//	                                              v0.0.47 and v0.0.48 pods coexist during a
//	                                              rolling upgrade)
//	one non-default ns -> string(ns)              (a dedicated deployment, e.g. a local dev
//	                                              process on shared infrastructure)
//	>=2 non-default without Default -> error      (namespaces may contain '+' and '-', so any
//	                                              join scheme is ambiguous; reject at
//	                                              construction instead of guessing)
//
// Every element is validated again here even though NormalizeOwned already
// did: the suffix is embedded into Redis key names and asynq queue names, and
// an unvalidated element containing ':' or a glob metacharacter would corrupt
// the key schema. Namespace.Validate rejects exactly those characters.
func DeploymentSuffix(owned []Namespace) (string, error) {
	nonDefault := make([]Namespace, 0, len(owned))
	for _, ns := range owned {
		if err := Validate(ns); err != nil {
			return "", fmt.Errorf("owned namespace %q: %w", ns, err)
		}
		if ns != Default {
			nonDefault = append(nonDefault, ns)
		}
	}
	hasDefault := len(nonDefault) != len(owned)
	switch {
	case len(owned) == 0, hasDefault:
		// Unscoped process or a default-containing deployment: legacy names.
		// The default check must come before the single-namespace check:
		// {default, x} owns both but still shares the default deployment's
		// byte-identical legacy names.
		return "", nil
	case len(nonDefault) == 1:
		return string(nonDefault[0]), nil
	default:
		return "", fmt.Errorf("owned namespace set %v must contain %q or exactly one non-%q namespace; "+
			"a process cannot own multiple dedicated namespaces under one unambiguous deployment identity",
			owned, Default, Default)
	}
}
