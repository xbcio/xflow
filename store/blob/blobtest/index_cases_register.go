package blobtest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
)

const (
	idxNS  namespace.Namespace = "index-a"
	idxNS2 namespace.Namespace = "index-b"
)

func mustRegister(t *testing.T, h IndexHarness, ref blob.Ref) blob.Registration {
	t.Helper()
	reg, err := h.Index.Register(context.Background(), ref)
	if err != nil {
		t.Fatalf("Register(%+v) error = %v", ref, err)
	}
	return reg
}

func mustLink(t *testing.T, h IndexHarness, execID string, ref blob.Ref, ttl time.Duration) {
	t.Helper()
	if err := h.Index.Link(context.Background(), execID, ref, h.Clock.Now().Add(ttl)); err != nil {
		t.Fatalf("Link(%q, %s, +%s) error = %v", execID, ref.Digest[:8], ttl, err)
	}
}

func candidates(t *testing.T, h IndexHarness) []blob.Ref {
	t.Helper()
	refs, err := h.Index.Candidates(context.Background(), grace, batch)
	if err != nil {
		t.Fatalf("Candidates error = %v", err)
	}
	return refs
}

// wantCandidates fails unless the current candidates are exactly want, in order.
func wantCandidates(t *testing.T, h IndexHarness, why string, want ...blob.Ref) {
	t.Helper()
	got := candidates(t, h)
	if len(got) != len(want) {
		t.Fatalf("%s: Candidates = %d refs %v, want %d refs %v", why, len(got), digests(got), len(want), digests(want))
	}
	for i := range want {
		if got[i].Namespace != want[i].Namespace || got[i].Digest != want[i].Digest {
			t.Fatalf("%s: Candidates[%d] = %s/%s, want %s/%s", why, i, got[i].Namespace, got[i].Digest[:8], want[i].Namespace, want[i].Digest[:8])
		}
	}
}

func digests(refs []blob.Ref) []string {
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.Namespace + "/" + r.Digest[:8]
	}
	return out
}

func mustMark(t *testing.T, h IndexHarness, ref blob.Ref) blob.Fence {
	t.Helper()
	f, err := h.Index.MarkDeleting(context.Background(), ref, grace)
	if err != nil {
		t.Fatalf("MarkDeleting(%s) error = %v", ref.Digest[:8], err)
	}
	return f
}

func caseRegisterCreates(t *testing.T, h IndexHarness) {
	ref := refFor(idxNS, "fresh object")
	reg := mustRegister(t, h, ref)
	if reg.Revived {
		t.Errorf("Register of a new object: Revived = true, want false")
	}
	if reg.Ref != ref {
		t.Errorf("Register of a new object: Ref = %+v, want the writer's %+v", reg.Ref, ref)
	}
	// A registered, unlinked object is garbage once the grace period passes.
	wantCandidates(t, h, "inside the grace period")
	h.Clock.Advance(grace + time.Millisecond)
	wantCandidates(t, h, "after the grace period", ref)
}

// caseRegisterReadsBack pins design 4.4 item 8: a second writer's Codec and
// Stored must come from the index, not from what that writer encoded locally.
func caseRegisterReadsBack(t *testing.T, h IndexHarness) {
	first := refFor(idxNS, "two writers, two codecs")
	first.Codec, first.Stored = blob.CodecZstd, first.Size-3
	second := refFor(idxNS, "two writers, two codecs") // Raw: Stored == Size.

	mustRegister(t, h, first)
	reg := mustRegister(t, h, second)
	if reg.Ref != first {
		t.Fatalf("second writer's Registration.Ref = %+v, want the recorded %+v", reg.Ref, first)
	}
	if reg.Revived {
		t.Errorf("Register of a live object: Revived = true, want false")
	}
}

func caseRegisterSizeConflict(t *testing.T, h IndexHarness) {
	ref := refFor(idxNS, "size is part of identity")
	mustRegister(t, h, ref)

	wrong := ref
	wrong.Size, wrong.Stored = ref.Size+1, ref.Stored+1
	if _, err := h.Index.Register(context.Background(), wrong); !errors.Is(err, blob.ErrMetadataConflict) {
		t.Fatalf("Register with a different size error = %v, want ErrMetadataConflict", err)
	}
	if reg := mustRegister(t, h, ref); reg.Ref != ref {
		t.Errorf("after the rejected Register, Ref = %+v, want the original %+v", reg.Ref, ref)
	}
}

func caseRegisterInvalid(t *testing.T, h IndexHarness) {
	good := refFor(idxNS, "invalid variants")
	bad := map[string]blob.Ref{
		"zero":            {},
		"empty namespace": {Digest: good.Digest, Size: good.Size, Stored: good.Stored},
		"slash namespace": {Namespace: "a/b", Digest: good.Digest, Size: good.Size, Stored: good.Stored},
		"short digest":    {Namespace: good.Namespace, Digest: good.Digest[:10], Size: good.Size, Stored: good.Stored},
		"upper digest":    {Namespace: good.Namespace, Digest: strings.ToUpper(good.Digest), Size: good.Size, Stored: good.Stored},
		"negative size":   {Namespace: good.Namespace, Digest: good.Digest, Size: -1, Stored: -1},
		"unknown codec":   {Namespace: good.Namespace, Digest: good.Digest, Size: good.Size, Stored: good.Stored, Codec: "lz4"},
	}
	for name, ref := range bad {
		if _, err := h.Index.Register(context.Background(), ref); !errors.Is(err, blob.ErrInvalidRef) {
			t.Errorf("Register(%s) error = %v, want ErrInvalidRef", name, err)
		}
	}
}

// caseRegisterConcurrent has many writers race to create one object, each with
// its own codec. Exactly one record can win, and every writer must be told the
// winner's.
func caseRegisterConcurrent(t *testing.T, h IndexHarness) {
	const writers = 16
	base := refFor(idxNS, "contended registration payload")
	regs := make([]blob.Registration, writers)
	errs := make([]error, writers)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ref := base
			if i%2 == 1 {
				ref.Codec, ref.Stored = blob.CodecZstd, base.Size-int64(i)
			}
			<-start
			regs[i], errs[i] = h.Index.Register(context.Background(), ref)
		}()
	}
	close(start)
	wg.Wait()

	for i := range writers {
		if errs[i] != nil {
			t.Fatalf("Register[%d] error = %v", i, errs[i])
		}
		if regs[i].Revived {
			t.Errorf("Register[%d]: Revived = true on a never-claimed object", i)
		}
		if regs[i].Ref != regs[0].Ref {
			t.Errorf("Register[%d] Ref = %+v, differs from Register[0] Ref = %+v", i, regs[i].Ref, regs[0].Ref)
		}
	}
}

// caseIndexNamespaces guards the primary key: namespaces are case sensitive
// ("Tenant" and "tenant" are two tenants) and the same digest in two namespaces
// is two objects. A case-insensitive column collation would fold the first pair
// into one row and let one tenant's link protect the other's blob.
func caseIndexNamespaces(t *testing.T, h IndexHarness) {
	upper := refFor("Tenant-Idx", "same bytes, three tenants")
	lower := refFor("tenant-idx", "same bytes, three tenants")
	other := refFor(idxNS2, "same bytes, three tenants")
	for _, r := range []blob.Ref{upper, lower, other} {
		mustRegister(t, h, r)
	}

	mustLink(t, h, "exec-1", upper, horizon)
	h.Clock.Advance(grace + time.Millisecond)

	got := map[string]bool{}
	for _, r := range candidates(t, h) {
		got[r.Namespace] = true
	}
	if got["Tenant-Idx"] || !got["tenant-idx"] || !got[string(idxNS2)] || len(got) != 2 {
		t.Fatalf("candidates by namespace = %v, want only tenant-idx and %s (Tenant-Idx is linked)", got, idxNS2)
	}
}

func caseLinkUnregistered(t *testing.T, h IndexHarness) {
	ref := refFor(idxNS, "never registered")
	err := h.Index.Link(context.Background(), "exec-1", ref, h.Clock.Now().Add(horizon))
	if !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Link of an unregistered object error = %v, want ErrNotFound", err)
	}
}

func caseLinkBadArguments(t *testing.T, h IndexHarness) {
	ref := refFor(idxNS, "link arguments")
	mustRegister(t, h, ref)
	future := h.Clock.Now().Add(horizon)

	for name, execID := range map[string]string{
		"empty":      "",
		"too long":   strings.Repeat("e", blob.MaxExecIDLen+1),
		"whitespace": "exec 1",
		"control":    "exec\x00",
	} {
		if err := h.Index.Link(context.Background(), execID, ref, future); !errors.Is(err, blob.ErrInvalidRef) {
			t.Errorf("Link with a %s execution id error = %v, want ErrInvalidRef", name, err)
		}
	}
	if err := h.Index.Link(context.Background(), strings.Repeat("e", blob.MaxExecIDLen), ref, future); err != nil {
		t.Errorf("Link with a %d byte execution id error = %v, want nil", blob.MaxExecIDLen, err)
	}
	for name, at := range map[string]time.Time{
		"now":  h.Clock.Now(),
		"past": h.Clock.Now().Add(-time.Minute),
		"zero": {},
	} {
		if err := h.Index.Link(context.Background(), "exec-2", ref, at); err == nil {
			t.Errorf("Link with an expiry of %s: error = nil, want an error", name)
		}
	}
	if err := h.Index.Link(context.Background(), "exec-3", blob.Ref{}, future); !errors.Is(err, blob.ErrInvalidRef) {
		t.Errorf("Link with a zero ref error = %v, want ErrInvalidRef", err)
	}
}
