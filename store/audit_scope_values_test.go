package store

import (
	"reflect"
	"testing"

	"github.com/xbcio/xflow/namespace"
)

func TestAuditScopeValues(t *testing.T) {
	tests := []struct {
		name  string
		owned []namespace.Namespace
		want  []string
	}{
		{"nil matches nothing", nil, nil},
		{"empty matches nothing", []namespace.Namespace{}, nil},
		// Mutation: dropping the "" companion makes every legacy empty-namespace
		// audit row unreachable by the default deployment.
		{"default also owns empty-namespace rows", []namespace.Namespace{namespace.Default}, []string{"default", ""}},
		// Mutation: adding "" for any namespace lets a dedicated deployment settle
		// rows that belong to the default deployment.
		{"dedicated does not inherit empty-namespace rows", []namespace.Namespace{"local-jack"}, []string{"local-jack"}},
		{"mixed set keeps order and adds one empty", []namespace.Namespace{"local-jack", namespace.Default}, []string{"local-jack", "default", ""}},
		{"duplicates collapse", []namespace.Namespace{namespace.Default, namespace.Default, "local-jack", "local-jack"}, []string{"default", "", "local-jack"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := AuditScopeValues(tc.owned); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("AuditScopeValues(%v) = %#v, want %#v", tc.owned, got, tc.want)
			}
		})
	}
}
