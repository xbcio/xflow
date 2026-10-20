package control

import (
	"reflect"
	"strings"
	"testing"

	"github.com/xbcio/xflow/backend"
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/namespace"
)

// nsList builds a namespace list; no arguments yields nil (not an empty slice)
// so a test can tell "unset" from "set to nothing".
func nsList(names ...string) []namespace.Namespace {
	if names == nil {
		return nil
	}
	out := make([]namespace.Namespace, len(names))
	for i, n := range names {
		out[i] = namespace.Namespace(n)
	}
	return out
}

// ownedRedisBackend is a Redis-exposing backend that also carries its own
// owned-namespace set, the way the distributed backend does.
type ownedRedisBackend struct {
	redisBackendStub
	owned []namespace.Namespace
}

func (b ownedRedisBackend) OwnedNamespaces() []namespace.Namespace { return b.owned }

// ownedLocalBackend is the same without a Redis client.
type ownedLocalBackend struct {
	backend.Provider
	owned []namespace.Namespace
}

func (b ownedLocalBackend) OwnedNamespaces() []namespace.Namespace { return b.owned }

func TestResolveDeploymentScope(t *testing.T) {
	tests := []struct {
		name        string
		cfgOwned    []namespace.Namespace
		cfgEntry    []namespace.Namespace
		backend     backend.Provider // nil -> a plain local backend with no owned set
		wantOwned   []namespace.Namespace
		wantEntry   []namespace.Namespace
		wantErrPart string
	}{
		// Mutation: defaulting entry to {Default} when nothing is configured would
		// change every legacy deployment's reconciler list.
		{name: "legacy unscoped keeps everything nil"},
		{name: "owned only: entry follows owned",
			cfgOwned: nsList("local-jack"), wantOwned: nsList("local-jack"), wantEntry: nsList("local-jack")},
		{name: "owned is normalized (sorted, deduped, empties dropped)",
			cfgOwned: nsList("x", "", "default", "x"), wantOwned: nsList("default", "x"), wantEntry: nsList("default", "x")},
		{name: "default plus another namespace is a default deployment",
			cfgOwned: nsList("default", "x"), wantOwned: nsList("default", "x"), wantEntry: nsList("default", "x")},
		{name: "explicit entry inside owned is kept verbatim",
			cfgOwned: nsList("default", "x"), cfgEntry: nsList("x"),
			wantOwned: nsList("default", "x"), wantEntry: nsList("x")},
		// Mutation: dropping the subset check lets a deployment activate a
		// namespace that another deployment sweeps.
		{name: "entry outside owned is rejected",
			cfgOwned: nsList("local-jack"), cfgEntry: nsList("default"),
			wantErrPart: `"default", which is outside the owned namespaces`},
		{name: "empty entry elements are skipped by the subset check",
			cfgOwned: nsList("local-jack"), cfgEntry: nsList("", "local-jack"),
			wantOwned: nsList("local-jack"), wantEntry: nsList("", "local-jack")},
		{name: "entry without owned is untouched (existing SDK usage)",
			cfgEntry: nsList("tenant-a"), wantEntry: nsList("tenant-a")},
		{name: "two dedicated namespaces are ambiguous",
			cfgOwned: nsList("a", "b"), wantErrPart: "must contain"},
		{name: "invalid owned namespace names the config field",
			cfgOwned: nsList("a:b"), wantErrPart: "Config.OwnedNamespaces"},
		// Mutation: not inheriting leaves a backend-scoped process running its
		// workers and runner directory unscoped.
		{name: "backend supplies owned when config is empty",
			backend:   ownedLocalBackend{Provider: backendlocal.New(), owned: nsList("local-jack")},
			wantOwned: nsList("local-jack"), wantEntry: nsList("local-jack")},
		{name: "backend and config agree",
			backend:  ownedLocalBackend{Provider: backendlocal.New(), owned: nsList("local-jack")},
			cfgOwned: nsList("local-jack"), wantOwned: nsList("local-jack"), wantEntry: nsList("local-jack")},
		// Mutation: letting config win silently runs workers for namespaces whose
		// leader lease the backend does not hold.
		{name: "backend and config differ",
			backend:  ownedLocalBackend{Provider: backendlocal.New(), owned: nsList("default")},
			cfgOwned: nsList("local-jack"), wantErrPart: "differs from the backend's owned namespaces"},
		{name: "unscoped backend contradicts a scoped config",
			backend:  ownedLocalBackend{Provider: backendlocal.New()},
			cfgOwned: nsList("local-jack"), wantErrPart: "differs from the backend's owned namespaces"},
		{name: "backend with an ambiguous set is rejected",
			backend:     ownedLocalBackend{Provider: backendlocal.New(), owned: nsList("a", "b")},
			wantErrPart: "must contain"},
		{name: "backend with an invalid name is rejected",
			backend:     ownedLocalBackend{Provider: backendlocal.New(), owned: nsList("a:b")},
			wantErrPart: "backend owned namespaces"},
		{name: "entry outside the inherited backend set is rejected",
			backend:  ownedLocalBackend{Provider: backendlocal.New(), owned: nsList("local-jack")},
			cfgEntry: nsList("default"), wantErrPart: "outside the owned namespaces"},
		{name: "a backend without OwnedNamespaces imposes nothing",
			cfgOwned: nsList("local-jack"), wantOwned: nsList("local-jack"), wantEntry: nsList("local-jack")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var b backend.Provider = backendlocal.New()
			if tc.backend != nil {
				b = tc.backend
			}
			got, err := resolveDeploymentScope(Config{
				Backend: b, OwnedNamespaces: tc.cfgOwned, EntryNamespaces: tc.cfgEntry,
			})
			if tc.wantErrPart != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrPart) {
					t.Fatalf("resolveDeploymentScope error = %v, want it to contain %q", err, tc.wantErrPart)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveDeploymentScope: %v", err)
			}
			if !reflect.DeepEqual(got.owned, tc.wantOwned) {
				t.Fatalf("owned = %#v, want %#v", got.owned, tc.wantOwned)
			}
			if !reflect.DeepEqual(got.entry, tc.wantEntry) {
				t.Fatalf("entry = %#v, want %#v", got.entry, tc.wantEntry)
			}
		})
	}
}
