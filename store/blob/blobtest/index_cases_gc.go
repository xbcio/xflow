package blobtest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/store/blob"
)

func caseLinkMovesLater(t *testing.T, h IndexHarness) {
	ref := refFor(idxNS, "link only extends")
	mustRegister(t, h, ref)
	mustLink(t, h, "exec-1", ref, 3*grace)
	// A second, earlier expiry for the same execution must not shorten the first.
	mustLink(t, h, "exec-1", ref, grace/2)

	h.Clock.Advance(2 * grace)
	wantCandidates(t, h, "the later expiry survived the earlier second Link")

	h.Clock.Advance(2 * grace)
	wantCandidates(t, h, "link expired", ref)
}

func caseCandidateRules(t *testing.T, h IndexHarness) {
	linked := refFor(idxNS, "protected by a link")
	idle := refFor(idxNS, "idle with no link")
	for _, r := range []blob.Ref{linked, idle} {
		mustRegister(t, h, r)
	}
	mustLink(t, h, "exec-1", linked, 2*grace)

	h.Clock.Advance(grace/2 + time.Millisecond)
	wantCandidates(t, h, "both inside the grace period")

	// Past grace for both, inside the link's life for one: Link refreshes the
	// last reference too, but the unexpired link is what protects it here.
	h.Clock.Advance(grace)
	wantCandidates(t, h, "linked object has an unexpired link", idle)

	// Re-registering is a reference: it restarts the grace period.
	mustRegister(t, h, idle)
	wantCandidates(t, h, "idle object was just re-registered")

	// The link lapses; the object still needs a full grace period of silence.
	h.Clock.Advance(2 * grace)
	wantCandidates(t, h, "everything expired, oldest reference first", linked, idle)
}

func caseManyLinks(t *testing.T, h IndexHarness) {
	ref := refFor(idxNS, "shared by two executions")
	mustRegister(t, h, ref)
	mustLink(t, h, "exec-short", ref, grace/2)
	mustLink(t, h, "exec-long", ref, 4*grace)

	h.Clock.Advance(2 * grace)
	wantCandidates(t, h, "exec-long still holds it")

	h.Clock.Advance(3 * grace)
	wantCandidates(t, h, "both links lapsed", ref)
}

func caseCandidatesOrder(t *testing.T, h IndexHarness) {
	var refs []blob.Ref
	for i := range 5 {
		ref := refFor(idxNS, fmt.Sprintf("ordered object %d", i))
		mustRegister(t, h, ref)
		refs = append(refs, ref)
		h.Clock.Advance(time.Second) // Distinct last-reference times, oldest first.
	}
	h.Clock.Advance(grace)

	wantCandidates(t, h, "all five, oldest first", refs...)
	got, err := h.Index.Candidates(context.Background(), grace, 2)
	if err != nil {
		t.Fatalf("Candidates(limit 2) error = %v", err)
	}
	if len(got) != 2 || got[0].Digest != refs[0].Digest || got[1].Digest != refs[1].Digest {
		t.Fatalf("Candidates(limit 2) = %v, want the two oldest %v", digests(got), digests(refs[:2]))
	}
	for _, bad := range []struct {
		grace time.Duration
		limit int
	}{{0, 1}, {-time.Second, 1}, {grace, 0}, {grace, -1}} {
		if _, err := h.Index.Candidates(context.Background(), bad.grace, bad.limit); err == nil {
			t.Errorf("Candidates(grace %s, limit %d) error = nil, want an error", bad.grace, bad.limit)
		}
	}
}

func casePruneLinks(t *testing.T, h IndexHarness) {
	ref := refFor(idxNS, "pruned links")
	mustRegister(t, h, ref)
	mustLink(t, h, "exec-a", ref, grace/4)
	mustLink(t, h, "exec-b", ref, grace/4)
	mustLink(t, h, "exec-c", ref, 6*grace)

	if n, err := h.Index.PruneLinks(context.Background(), batch); err != nil || n != 0 {
		t.Fatalf("PruneLinks with nothing expired = (%d, %v), want (0, nil)", n, err)
	}
	h.Clock.Advance(grace / 2)
	if n, err := h.Index.PruneLinks(context.Background(), 1); err != nil || n != 1 {
		t.Fatalf("PruneLinks(limit 1) = (%d, %v), want (1, nil)", n, err)
	}
	if n, err := h.Index.PruneLinks(context.Background(), batch); err != nil || n != 1 {
		t.Fatalf("PruneLinks(rest) = (%d, %v), want (1, nil)", n, err)
	}
	if _, err := h.Index.PruneLinks(context.Background(), 0); err == nil {
		t.Errorf("PruneLinks(limit 0) error = nil, want an error")
	}

	// Pruning removed only dead links: exec-c still protects the object.
	h.Clock.Advance(2 * grace)
	wantCandidates(t, h, "exec-c is still linked")
	h.Clock.Advance(5 * grace)
	wantCandidates(t, h, "exec-c expired", ref)
}

func caseMarkRechecks(t *testing.T, h IndexHarness) {
	ctx := context.Background()
	ref := refFor(idxNS, "rechecked at claim time")
	mustRegister(t, h, ref)

	// The collector listed it, then the grace period had not actually passed.
	if _, err := h.Index.MarkDeleting(ctx, ref, grace); !errors.Is(err, blob.ErrNotCandidate) {
		t.Fatalf("MarkDeleting inside the grace period error = %v, want ErrNotCandidate", err)
	}

	h.Clock.Advance(grace + time.Millisecond)
	// ...or a link appeared between listing and claiming.
	mustLink(t, h, "exec-1", ref, horizon)
	if _, err := h.Index.MarkDeleting(ctx, ref, grace); !errors.Is(err, blob.ErrNotCandidate) {
		t.Fatalf("MarkDeleting of a linked object error = %v, want ErrNotCandidate", err)
	}

	missing := refFor(idxNS, "unknown to the index")
	if _, err := h.Index.MarkDeleting(ctx, missing, grace); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("MarkDeleting of an unknown object error = %v, want ErrNotFound", err)
	}
	if _, err := h.Index.MarkDeleting(ctx, blob.Ref{}, grace); !errors.Is(err, blob.ErrInvalidRef) {
		t.Errorf("MarkDeleting of a zero ref error = %v, want ErrInvalidRef", err)
	}
	if _, err := h.Index.MarkDeleting(ctx, ref, 0); err == nil {
		t.Errorf("MarkDeleting with grace 0 error = nil, want an error")
	}
}

// caseMarkIdempotent covers a collector that crashed after claiming: the next
// run finds the object still listed and must be able to claim it again, with a
// fence that stays valid.
func caseMarkIdempotent(t *testing.T, h IndexHarness) {
	ref := refFor(idxNS, "claimed twice")
	mustRegister(t, h, ref)
	h.Clock.Advance(grace + time.Millisecond)

	first := mustMark(t, h, ref)
	if first.Generation < 1 || first.Ref != ref {
		t.Fatalf("MarkDeleting fence = %+v, want generation >= 1 and Ref %+v", first, ref)
	}
	wantCandidates(t, h, "a claimed object is still a candidate", ref)
	second := mustMark(t, h, ref)
	if second != first {
		t.Fatalf("second MarkDeleting fence = %+v, want the same fence %+v", second, first)
	}
}

func caseLinkFenced(t *testing.T, h IndexHarness) {
	ref := refFor(idxNS, "link after claim")
	mustRegister(t, h, ref)
	h.Clock.Advance(grace + time.Millisecond)
	mustMark(t, h, ref)

	err := h.Index.Link(context.Background(), "exec-1", ref, h.Clock.Now().Add(horizon))
	if !errors.Is(err, blob.ErrFenced) {
		t.Fatalf("Link of a claimed object error = %v, want ErrFenced", err)
	}
}

func casePurgeRemoves(t *testing.T, h IndexHarness) {
	ctx := context.Background()
	ref := refFor(idxNS, "purged with its links")
	mustRegister(t, h, ref)
	mustLink(t, h, "exec-1", ref, grace/2)
	h.Clock.Advance(grace + time.Millisecond)
	fence := mustMark(t, h, ref)

	called := 0
	if err := h.Index.Purge(ctx, fence, func(context.Context) error { called++; return nil }); err != nil {
		t.Fatalf("Purge error = %v", err)
	}
	if called != 1 {
		t.Errorf("delete callback ran %d times, want 1", called)
	}
	wantCandidates(t, h, "after Purge")
	if _, err := h.Index.MarkDeleting(ctx, ref, grace); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("MarkDeleting after Purge error = %v, want ErrNotFound", err)
	}

	// The digest is free again: registering it starts a new object, generation 1
	// from the writer's point of view, with the writer's own metadata.
	again := ref
	again.Codec, again.Stored = blob.CodecZstd, ref.Size-1
	reg := mustRegister(t, h, again)
	if reg.Revived || reg.Ref != again {
		t.Errorf("Register after Purge = %+v, want a new object %+v, not revived", reg, again)
	}
}

// caseLastRefNeverMovesBack pins the rule that a reference stamped by a clock
// that runs behind (another node, or a clock step) cannot rewind the object's
// last-reference time and thereby make a recently used object look idle.
func caseLastRefNeverMovesBack(t *testing.T, h IndexHarness) {
	ref := refFor(idxNS, "clock goes backwards")
	mustRegister(t, h, ref)
	h.Clock.Advance(grace / 2)
	mustRegister(t, h, ref) // Last reference: t0 + grace/2.
	h.Clock.Advance(-grace / 2)
	mustRegister(t, h, ref) // A slow clock must not pull it back to t0.

	h.Clock.Advance(3 * grace / 2) // Now t0 + 1.5*grace: exactly grace after the last reference.
	wantCandidates(t, h, "exactly one grace period after the newest reference")
	h.Clock.Advance(time.Millisecond)
	wantCandidates(t, h, "just past one grace period", ref)
}
