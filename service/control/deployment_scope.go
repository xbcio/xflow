package control

import (
	"fmt"

	"github.com/xbcio/xflow/namespace"
)

// ownedNamespacesProvider is implemented by backends that carry a deployment
// scope of their own (the distributed backend). The control plane reads it to
// inherit the embedder's scope and to refuse a config that contradicts it.
type ownedNamespacesProvider interface {
	OwnedNamespaces() []namespace.Namespace
}

// deploymentScope is the resolved namespace identity of one control plane: the
// set it owns, the key suffix derived from it, and the namespaces its entry
// reconciler and supply hinter enumerate. The zero value is the legacy
// unscoped deployment.
type deploymentScope struct {
	// owned is nil for a legacy unscoped process; every scoped worker treats nil
	// as "do not filter" and non-nil as a hard filter.
	owned []namespace.Namespace
	// entry is what the entry reconciler and hinter are configured with. nil
	// keeps their own {namespace.Default} fallback.
	entry []namespace.Namespace
}

// resolveDeploymentScope derives the control plane's scope from cfg and its
// backend. It exists as one function so the three consumers of the scope —
// the Redis key prefix, the leader-gated workers, and the entry namespace list
// — can never be configured from different answers.
//
// The owned set is cfg.OwnedNamespaces, or the backend's when cfg leaves it
// empty. A backend that reports a set different from cfg's is an error rather
// than a preference: the backend's leader lease and maintenance sweeps are
// already keyed by its set, so a control plane filtering by another one would
// run workers for namespaces whose leadership it does not hold.
//
// Entry namespaces default to the owned set. An explicit EntryNamespaces list
// is kept verbatim (the reconciler normalizes it itself) but must stay inside
// the owned set: a namespace this deployment does not own would be activated
// here and swept elsewhere.
func resolveDeploymentScope(cfg Config) (deploymentScope, error) {
	owned, err := namespace.NormalizeOwned(cfg.OwnedNamespaces)
	if err != nil {
		return deploymentScope{}, fmt.Errorf("control: Config.OwnedNamespaces: %w", err)
	}
	if provider, ok := cfg.Backend.(ownedNamespacesProvider); ok {
		backendOwned, err := namespace.NormalizeOwned(provider.OwnedNamespaces())
		if err != nil {
			return deploymentScope{}, fmt.Errorf("control: backend owned namespaces: %w", err)
		}
		switch {
		case owned == nil:
			owned = backendOwned
		case !sameNamespaces(owned, backendOwned):
			return deploymentScope{}, fmt.Errorf(
				"control: Config.OwnedNamespaces %v differs from the backend's owned namespaces %v; "+
					"configure the backend and the control plane with the same set", owned, backendOwned)
		}
	}
	if _, err := namespace.DeploymentSuffix(owned); err != nil {
		return deploymentScope{}, fmt.Errorf("control: Config.OwnedNamespaces: %w", err)
	}

	entry := cfg.EntryNamespaces
	if len(entry) == 0 {
		entry = owned
	} else if owned != nil {
		inOwned := make(map[namespace.Namespace]struct{}, len(owned))
		for _, ns := range owned {
			inOwned[ns] = struct{}{}
		}
		for _, ns := range entry {
			if ns == "" {
				continue
			}
			if _, ok := inOwned[ns]; !ok {
				return deploymentScope{}, fmt.Errorf(
					"control: Config.EntryNamespaces contains %q, which is outside the owned namespaces %v", ns, owned)
			}
		}
	}
	return deploymentScope{owned: owned, entry: entry}, nil
}

func sameNamespaces(a, b []namespace.Namespace) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// scopeSet builds a lookup set from a namespace list, dropping empty entries.
// It returns nil when nothing usable remains, which callers read as
// "unscoped" — the same collapse namespace.NormalizeOwned applies. It is not
// namespaceSet (memory_runner_directory.go): that one keeps empty entries and
// never returns nil, which would turn "no scope" into "match nothing".
func scopeSet(in []namespace.Namespace) map[namespace.Namespace]struct{} {
	var set map[namespace.Namespace]struct{}
	for _, ns := range in {
		if ns == "" {
			continue
		}
		if set == nil {
			set = make(map[namespace.Namespace]struct{}, len(in))
		}
		set[ns] = struct{}{}
	}
	return set
}
