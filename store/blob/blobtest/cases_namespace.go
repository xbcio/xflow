package blobtest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
)

// invalidNamespaces are values no Store may accept as a namespace.
var invalidNamespaces = []namespace.Namespace{"a/b", "..", "", "a b"}

func caseNamespaceIsolation(t *testing.T, s blob.Store) {
	ctx := context.Background()
	refA, err := s.Put(ctx, nsA, []byte("belongs to A"))
	if err != nil {
		t.Fatalf("Put(%q) error = %v", nsA, err)
	}
	if _, err := s.Get(ctx, nsB, refA); !errors.Is(err, blob.ErrNamespaceMismatch) {
		t.Errorf("Get(%q, ref of %q) error = %v, want ErrNamespaceMismatch", nsB, nsA, err)
	}
	// A forged ref that claims namespace B with A's digest must not reach A's bytes.
	forged := refA
	forged.Namespace = string(nsB)
	if _, err := s.Get(ctx, nsB, forged); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Get(%q, %+v) error = %v, want ErrNotFound", nsB, forged, err)
	}

	// Namespaces that differ only in case must stay isolated: a case-insensitive
	// filesystem folds such names together, and a Store must not let that leak.
	const upper, lower namespace.Namespace = "Tenant", "tenant"
	data := []byte("identical content in both")
	refUpper, err := s.Put(ctx, upper, data)
	if err != nil {
		t.Fatalf("Put(%q) error = %v", upper, err)
	}
	refLower, err := s.Put(ctx, lower, data)
	if err != nil {
		t.Fatalf("Put(%q) error = %v", lower, err)
	}
	if _, err := s.Get(ctx, lower, refUpper); !errors.Is(err, blob.ErrNamespaceMismatch) {
		t.Errorf("Get(%q, ref of %q) error = %v, want ErrNamespaceMismatch", lower, upper, err)
	}
	if _, err := s.Get(ctx, upper, refLower); !errors.Is(err, blob.ErrNamespaceMismatch) {
		t.Errorf("Get(%q, ref of %q) error = %v, want ErrNamespaceMismatch", upper, lower, err)
	}
	for ns, ref := range map[namespace.Namespace]blob.Ref{upper: refUpper, lower: refLower} {
		if got, err := s.Get(ctx, ns, ref); err != nil || !bytes.Equal(got, data) {
			t.Errorf("Get(%q, own ref) = (%q, %v), want (%q, nil)", ns, got, err, data)
		}
	}
}

func casePutRejectsInvalidNamespace(t *testing.T, s blob.Store) {
	for _, ns := range invalidNamespaces {
		if ref, err := s.Put(context.Background(), ns, []byte("data")); !errors.Is(err, blob.ErrInvalidRef) {
			t.Errorf("Put(%q) = (%+v, %v), want ErrInvalidRef", string(ns), ref, err)
		}
	}
}

// caseInvalidRefNamespace checks that Get and Delete reject a Ref whose
// Namespace is itself invalid, whatever namespace the caller passes.
func caseInvalidRefNamespace(t *testing.T, s blob.Store) {
	ctx := context.Background()
	good, err := s.Put(ctx, nsA, []byte("source of a valid ref"))
	if err != nil {
		t.Fatalf("Put error = %v", err)
	}
	for _, bad := range invalidNamespaces {
		ref := good
		ref.Namespace = string(bad)
		for _, callerNS := range []namespace.Namespace{bad, nsA} {
			if _, err := s.Get(ctx, callerNS, ref); !errors.Is(err, blob.ErrInvalidRef) {
				t.Errorf("Get(%q, ref namespace %q) error = %v, want ErrInvalidRef", string(callerNS), ref.Namespace, err)
			}
			if err := s.Delete(ctx, callerNS, ref); !errors.Is(err, blob.ErrInvalidRef) {
				t.Errorf("Delete(%q, ref namespace %q) error = %v, want ErrInvalidRef", string(callerNS), ref.Namespace, err)
			}
		}
	}
	if _, err := s.Get(ctx, nsA, good); err != nil {
		t.Errorf("Get of the valid blob after rejected calls error = %v, want nil", err)
	}
}

func caseDeleteChecksNamespace(t *testing.T, s blob.Store) {
	ctx := context.Background()
	data := []byte("owned by A")
	refA, err := s.Put(ctx, nsA, data)
	if err != nil {
		t.Fatalf("Put error = %v", err)
	}
	if err := s.Delete(ctx, nsB, refA); !errors.Is(err, blob.ErrNamespaceMismatch) {
		t.Fatalf("Delete(%q, ref of %q) error = %v, want ErrNamespaceMismatch", nsB, nsA, err)
	}
	if got, err := s.Get(ctx, nsA, refA); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Get after a rejected cross-namespace Delete = (%q, %v), want (%q, nil)", got, err, data)
	}
}

func caseDeleteIsPerNamespace(t *testing.T, s blob.Store) {
	ctx := context.Background()
	data := []byte("same bytes in two namespaces")
	refA, err := s.Put(ctx, nsA, data)
	if err != nil {
		t.Fatalf("Put(%q) error = %v", nsA, err)
	}
	refB, err := s.Put(ctx, nsB, data)
	if err != nil {
		t.Fatalf("Put(%q) error = %v", nsB, err)
	}
	if err := s.Delete(ctx, nsA, refA); err != nil {
		t.Fatalf("Delete(%q) error = %v", nsA, err)
	}
	if got, err := s.Get(ctx, nsB, refB); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Get(%q) after deleting the same content in %q = (%q, %v), want (%q, nil)", nsB, nsA, got, err, data)
	}
	if _, err := s.Get(ctx, nsA, refA); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Get(%q) after Delete error = %v, want ErrNotFound", nsA, err)
	}
}
