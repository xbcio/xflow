package blobtest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/store/blob"
)

func caseDeleteThenGet(t *testing.T, s blob.Store) {
	ctx := context.Background()
	ref, err := s.Put(ctx, nsA, []byte("to be deleted"))
	if err != nil {
		t.Fatalf("Put error = %v", err)
	}
	if err := s.Delete(ctx, nsA, ref); err != nil {
		t.Fatalf("Delete(%q, %+v) error = %v", nsA, ref, err)
	}
	if _, err := s.Get(ctx, nsA, ref); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Get after Delete error = %v, want ErrNotFound", err)
	}
}

func caseDeleteMissing(t *testing.T, s blob.Store) {
	ref := blob.Ref{
		Namespace: string(nsA),
		Digest:    sha256Hex([]byte("never written")),
		Size:      13,
		Stored:    13,
	}
	if err := s.Delete(context.Background(), nsA, ref); err != nil {
		t.Fatalf("Delete(%q, %+v) of a missing blob error = %v, want nil", nsA, ref, err)
	}
}

func caseDeleteKeepsOthers(t *testing.T, s blob.Store) {
	ctx := context.Background()
	keepData := []byte("keep me")
	keep, err := s.Put(ctx, nsA, keepData)
	if err != nil {
		t.Fatalf("Put(keep) error = %v", err)
	}
	drop, err := s.Put(ctx, nsA, []byte("drop me"))
	if err != nil {
		t.Fatalf("Put(drop) error = %v", err)
	}
	if err := s.Delete(ctx, nsA, drop); err != nil {
		t.Fatalf("Delete(drop) error = %v", err)
	}
	got, err := s.Get(ctx, nsA, keep)
	if err != nil || !bytes.Equal(got, keepData) {
		t.Fatalf("Get(keep) after deleting another blob = (%q, %v), want (%q, nil)", got, err, keepData)
	}
}

func caseNoAliasing(t *testing.T, s blob.Store) {
	ctx := context.Background()
	input := []byte("original content")
	want := append([]byte(nil), input...)
	ref, err := s.Put(ctx, nsA, input)
	if err != nil {
		t.Fatalf("Put error = %v", err)
	}
	input[0] ^= 0xff // The caller reuses its buffer after Put returns.

	got, err := s.Get(ctx, nsA, ref)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("Get after mutating the Put input = (%q, %v), want (%q, nil)", got, err, want)
	}
	got[0] ^= 0xff // The caller owns the slice Get returned.
	again, err := s.Get(ctx, nsA, ref)
	if err != nil || !bytes.Equal(again, want) {
		t.Fatalf("second Get after mutating the first result = (%q, %v), want (%q, nil)", again, err, want)
	}
}
