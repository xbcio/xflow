package blobtest

import (
	"bytes"
	"context"
	"errors"

	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/objectstore"
)

// The three scenarios below replay, one deterministic step at a time, the
// interleavings the generation fence exists for. HookIndex runs a writer from
// inside the collector's own goroutine at the instant between "claimed" and
// "confirm", so none of them sleeps or depends on scheduling.

// caseRaceRevive is the headline race: the collector marked the object deleting
// and read its generation, THEN a Put of the same digest arrived. The collector's
// confirm must fail, the bytes must still be there, and the Ref the Put returned
// must be readable.
func caseRaceRevive(r *gcRig) {
	data := compressible("revived after the claim")
	first := r.put(data)
	r.age()

	var during blob.Ref
	calls := 0
	r.hook.AfterMark = func(fence blob.Fence) {
		calls++
		during = r.put(data)
	}
	res, err := r.gc.Run(context.Background())
	if err != nil {
		r.t.Fatalf("Run error = %v", err)
	}
	if calls != 1 {
		r.t.Fatalf("the claim hook ran %d times, want once", calls)
	}
	if want := (blob.CollectResult{Candidates: 1, Fenced: 1}); res != want {
		r.t.Fatalf("Run = %+v, want %+v: the confirm must lose to the Put", res, want)
	}
	if during != first {
		r.t.Fatalf("Put during the claim returned %+v, want %+v", during, first)
	}
	r.wantReadable("Ref returned by the Put that beat the confirm", during, data)
	r.wantReadable("Ref from before the claim", first, data)
	// The Put took the object back and refreshed it, so it is live and no longer
	// garbage: a later pass has nothing to do.
	r.hook.AfterMark = nil
	if res := r.run(); res != (blob.CollectResult{}) {
		r.t.Fatalf("Run after the lost confirm = %+v, want nothing to do", res)
	}
	r.wantReadable("after the next pass", during, data)
}

// caseRaceRewrites is why a revived Put overwrites instead of deduplicating: the
// object was claimed, then its bytes were damaged, then a writer arrived. A Put
// that trusted the existing bytes would hand out a Ref that cannot be read; the
// revive path rewrites them.
func caseRaceRewrites(r *gcRig) {
	data := compressible("damaged under a claim")
	ref := r.put(data)
	key := r.fs.keyOfLastPut()
	r.age()
	fence := mustMark(r.t, r.h, ref)

	junk := append([]byte{0x00}, bytes.Repeat([]byte("x"), 64)...)
	if _, err := r.fs.FSStore.PutObject(context.Background(), key, bytes.NewReader(junk), int64(len(junk)), objectstore.PutOptions{}); err != nil {
		r.t.Fatalf("damage the stored bytes: %v", err)
	}
	if _, err := r.store.Get(context.Background(), idxNS, ref); !errors.Is(err, blob.ErrCorrupt) {
		r.t.Fatalf("Get of the damaged object error = %v, want ErrCorrupt (fixture check)", err)
	}

	again := r.put(data)
	if again != ref {
		r.t.Fatalf("Put of a claimed object returned %+v, want %+v", again, ref)
	}
	r.wantReadable("after the reviving Put rewrote the bytes", again, data)

	called := false
	err := r.h.Index.Purge(context.Background(), fence, func(context.Context) error { called = true; return nil })
	if !errors.Is(err, blob.ErrFenced) || called {
		r.t.Fatalf("Purge with the pre-revive fence = (called %v, %v), want ErrFenced without a delete", called, err)
	}
}

// caseRaceStaleClaim is the ABA case, and the one only the generation can
// catch: the object is claimed (generation g), a writer revives it, it goes idle
// and is claimed again, and the first collector's confirm finally runs. State
// alone cannot tell the two claims apart, because both left the object
// "deleting"; the first claim's fence must still lose, so the second claim's
// owner is the only one that deletes.
func caseRaceStaleClaim(r *gcRig) {
	data := compressible("claimed twice")
	first := r.put(data)
	r.age()

	var second, stale blob.Fence
	r.hook.AfterMark = func(claim blob.Fence) {
		stale = claim
		r.put(data)
		r.age()
		var err error
		if second, err = r.h.Index.MarkDeleting(context.Background(), first, grace); err != nil {
			r.t.Fatalf("second claim error = %v", err)
		}
	}
	res, err := r.gc.Run(context.Background())
	if err != nil {
		r.t.Fatalf("Run error = %v", err)
	}
	if want := (blob.CollectResult{Candidates: 1, Fenced: 1}); res != want {
		r.t.Fatalf("Run = %+v, want %+v: the first claim's confirm must lose to the second claim", res, want)
	}
	r.wantReadable("after the stale confirm lost", first, data)
	if second.Generation <= stale.Generation {
		r.t.Fatalf("second claim has generation %d, want one later than the first claim's %d", second.Generation, stale.Generation)
	}

	// The second claim is the live one and deletes normally.
	r.hook.AfterMark = nil
	if err := r.h.Index.Purge(context.Background(), second, func(ctx context.Context) error {
		return r.store.Delete(ctx, idxNS, second.Ref)
	}); err != nil {
		r.t.Fatalf("Purge with the second claim's fence error = %v", err)
	}
	r.wantGone("after the second claim purged", first)
}
