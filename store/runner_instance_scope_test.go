package store

import (
	"reflect"
	"testing"

	"github.com/xbcio/xflow/namespace"
)

func TestRunnerInstanceScopeFor(t *testing.T) {
	tests := []struct {
		name  string
		owned []namespace.Namespace
		want  RunnerInstanceScope
	}{
		{"nil covers nothing", nil, RunnerInstanceScope{}},
		{"empty covers nothing", []namespace.Namespace{}, RunnerInstanceScope{}},
		// Mutation: dropping the platform flag for default leaves the shared
		// fleet with no deployment to prune it.
		{"default owns platform pools", []namespace.Namespace{namespace.Default}, RunnerInstanceScope{TenantNamespaces: []string{"default"}, IncludePlatform: true}},
		// Mutation: setting the flag for any namespace lets a dedicated
		// deployment drain shared infrastructure.
		{"dedicated does not", []namespace.Namespace{"local-jack"}, RunnerInstanceScope{TenantNamespaces: []string{"local-jack"}}},
		{"mixed keeps order", []namespace.Namespace{"local-jack", namespace.Default}, RunnerInstanceScope{TenantNamespaces: []string{"local-jack", "default"}, IncludePlatform: true}},
		{"duplicates collapse", []namespace.Namespace{"a", "a", namespace.Default, namespace.Default}, RunnerInstanceScope{TenantNamespaces: []string{"a", "default"}, IncludePlatform: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := RunnerInstanceScopeFor(tc.owned)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("RunnerInstanceScopeFor(%v) = %#v, want %#v", tc.owned, got, tc.want)
			}
			if wantEmpty := len(tc.owned) == 0; got.Empty() != wantEmpty {
				t.Fatalf("Empty() = %v, want %v", got.Empty(), wantEmpty)
			}
		})
	}
}
