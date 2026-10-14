package apiserver

import (
	"testing"

	"github.com/xbcio/xflow/namespace"
)

// buildControlPlane copies apiserver.Config onto control.Config field by
// field. A missing EntryNamespaces line breaks the namespace chain at exactly
// this hop: the embedded host (SAS) sets the option, the SDK threads it into
// apiserver.Config, and it stops right here — the control plane falls back to
// reconciling {namespace.Default} only and every workflow registered in the
// host's local namespace silently never activates.
func TestBuildControlPlaneThreadsEntryNamespaces(t *testing.T) {
	cp, err := buildControlPlane(Config{
		Concurrency:     1,
		EntryNamespaces: []namespace.Namespace{"local-dev"},
	})
	if err != nil {
		t.Fatalf("buildControlPlane: %v", err)
	}
	rec := cp.EntryActivationReconciler()
	if rec == nil {
		t.Fatal("the in-memory backend path must assemble the reconciler")
	}
	got := rec.Namespaces()
	if len(got) != 1 || got[0] != "local-dev" {
		t.Fatalf("reconciler namespaces = %v, want [local-dev]: EntryNamespaces was lost in the apiserver→control config copy", got)
	}
}
