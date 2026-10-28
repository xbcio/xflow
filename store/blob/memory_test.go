package blob_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/blob/blobtest"
)

// This file is an external test package because blobtest imports blob; an
// in-package test importing blobtest would be an import cycle. Cases that need
// unexported state live in memory_internal_test.go.

func TestMemory_Contract(t *testing.T) {
	blobtest.RunStoreContract(t, func(t *testing.T) blob.Store {
		t.Helper()
		return blob.NewMemory()
	})
}

func TestMemory_MaxBytes(t *testing.T) {
	ctx := context.Background()
	const ns namespace.Namespace = "limits"

	t.Run("OverLimit", func(t *testing.T) {
		m := blob.NewMemory().WithMaxBytes(8)
		if _, err := m.Put(ctx, ns, make([]byte, 9)); !errors.Is(err, blob.ErrTooLarge) {
			t.Fatalf("Put(9 bytes) with limit 8 error = %v, want ErrTooLarge", err)
		}
		if n := m.Len(); n != 0 {
			t.Fatalf("Len() after a rejected Put = %d, want 0", n)
		}
	})

	t.Run("AtLimit", func(t *testing.T) {
		m := blob.NewMemory().WithMaxBytes(8)
		if _, err := m.Put(ctx, ns, make([]byte, 8)); err != nil {
			t.Fatalf("Put(8 bytes) with limit 8 error = %v, want nil", err)
		}
	})

	t.Run("NonPositiveMeansUnlimited", func(t *testing.T) {
		for _, limit := range []int64{0, -1} {
			m := blob.NewMemory().WithMaxBytes(limit)
			if _, err := m.Put(ctx, ns, make([]byte, 1<<16)); err != nil {
				t.Fatalf("Put(64 KiB) with limit %d error = %v, want nil", limit, err)
			}
		}
	})
}

func TestMemory_InvalidNamespaceLeavesNothing(t *testing.T) {
	ctx := context.Background()
	m := blob.NewMemory()
	for _, ns := range []namespace.Namespace{"a/b", "..", "", "a b"} {
		if _, err := m.Put(ctx, ns, []byte("data")); !errors.Is(err, blob.ErrInvalidRef) {
			t.Fatalf("Put(%q) error = %v, want ErrInvalidRef", string(ns), err)
		}
	}
	if n := m.Len(); n != 0 {
		t.Fatalf("Len() after Puts into invalid namespaces = %d, want 0", n)
	}
}

func TestMemory_Len(t *testing.T) {
	ctx := context.Background()
	const ns namespace.Namespace = "len"
	m := blob.NewMemory()
	a, err := m.Put(ctx, ns, []byte("a"))
	if err != nil {
		t.Fatalf("Put(a) error = %v", err)
	}
	if _, err := m.Put(ctx, ns, []byte("a")); err != nil {
		t.Fatalf("second Put(a) error = %v", err)
	}
	if _, err := m.Put(ctx, ns, []byte("b")); err != nil {
		t.Fatalf("Put(b) error = %v", err)
	}
	if n := m.Len(); n != 2 {
		t.Fatalf("Len() after two distinct contents = %d, want 2", n)
	}
	if err := m.Delete(ctx, ns, a); err != nil {
		t.Fatalf("Delete(a) error = %v", err)
	}
	if n := m.Len(); n != 1 {
		t.Fatalf("Len() after one Delete = %d, want 1", n)
	}
}
