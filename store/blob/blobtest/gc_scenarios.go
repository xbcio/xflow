package blobtest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
)

// gcBatch is the batch cap of the rig every scenario gets.
const gcBatch = 10

// RunGCScenarios runs the collector scenarios against the stack newIndex builds:
// a real ObjectBacked over a real FSStore, registering with that Index, and a
// Collector deleting through it. The deterministic race scenarios are the point;
// the rest pin the behavior around them.
func RunGCScenarios(t *testing.T, newIndex IndexFactory) {
	t.Helper()
	cases := []struct {
		name string
		run  func(r *gcRig)
	}{
		{"CollectsOnlyUnreferencedBlobs", caseGCCollects},
		{"LinkProtectsUntilExpiry", caseGCLinkProtects},
		{"BatchCapBoundsAPass", caseGCBatchCap},
		{"CancelledPassLeavesClaimForNextRun", caseGCCancelled},
		{"FailedDeleteKeepsClaimAndPassContinues", caseGCDeleteFails},
		{"UnsupportedDeleteStopsThePass", caseGCDeleteUnsupported},
		{"PutAfterCollectionStoresFreshBytes", caseGCPutAfterCollect},
		{"SecondWriterReadsBackRecordedMetadata", caseGCMetadataReadBack},
		{"RaceWriterRevivesAfterMark", caseRaceRevive},
		{"RaceRevivedObjectRewritesDamagedBytes", caseRaceRewrites},
		{"RaceStaleClaimAfterReclaim", caseRaceStaleClaim},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(newGCRig(t, newIndex(t), gcBatch))
		})
	}
}

func caseGCCollects(r *gcRig) {
	dead, kept := compressible("dead"), compressible("kept")
	deadRef, keptRef := r.put(dead), r.put(kept)
	mustLink(r.t, r.h, "exec-kept", keptRef, horizon)
	r.age()

	if res, want := r.run(), (blob.CollectResult{Candidates: 1, Deleted: 1}); res != want {
		r.t.Fatalf("Run = %+v, want %+v", res, want)
	}
	r.wantGone("unreferenced blob", deadRef)
	r.wantReadable("linked blob", keptRef, kept)
}

func caseGCLinkProtects(r *gcRig) {
	data := compressible("linked")
	ref := r.put(data)
	mustLink(r.t, r.h, "exec-1", ref, horizon)

	r.age()
	if res := r.run(); res != (blob.CollectResult{}) {
		r.t.Fatalf("Run while linked = %+v, want nothing to do", res)
	}
	r.wantReadable("blob under an unexpired link", ref, data)

	r.h.Clock.Advance(horizon)
	if res, want := r.run(), (blob.CollectResult{PrunedLinks: 1, Candidates: 1, Deleted: 1}); res != want {
		r.t.Fatalf("Run after the link expired = %+v, want %+v", res, want)
	}
	r.wantGone("blob whose link expired", ref)
}

func caseGCBatchCap(r *gcRig) {
	const total = 2*gcBatch + 5
	refs := make([]blob.Ref, total)
	for i := range refs {
		refs[i] = r.put(compressible(fmt.Sprintf("batched %d", i)))
	}
	r.age()

	for _, want := range []int{gcBatch, gcBatch, 5, 0} {
		res := r.run()
		if res.Candidates != want || res.Deleted != want {
			r.t.Fatalf("Run = %+v, want %d candidates and %d deleted", res, want, want)
		}
	}
	for i, ref := range refs {
		r.wantGone(fmt.Sprintf("after the batched passes, blob %d", i), ref)
	}
}

func caseGCCancelled(r *gcRig) {
	data := []string{"one", "two", "three"}
	refs := make([]blob.Ref, len(data))
	for i, d := range data {
		refs[i] = r.put(compressible(d))
	}
	r.age()

	done, cancel := context.WithCancel(context.Background())
	cancel()
	if res, err := r.gc.Run(done); !errors.Is(err, context.Canceled) || res != (blob.CollectResult{}) {
		r.t.Fatalf("Run with a cancelled context = (%+v, %v), want Canceled and no work", res, err)
	}

	// Cancel after the first claim: the pass stops with that object claimed but
	// not deleted, and says so.
	ctx, cancelMid := context.WithCancel(context.Background())
	defer cancelMid()
	claims := 0
	r.hook.AfterMark = func(blob.Fence) {
		if claims++; claims == 1 {
			cancelMid()
		}
	}
	res, err := r.gc.Run(ctx)
	if !errors.Is(err, context.Canceled) || res.Deleted != 0 {
		r.t.Fatalf("Run cancelled mid-pass = (%+v, %v), want Canceled and nothing deleted", res, err)
	}
	for i, ref := range refs {
		r.wantReadable("after the cancelled pass, blob "+data[i], ref, compressible(data[i]))
	}

	// The claimed object is still garbage, so the next pass finishes it.
	r.hook.AfterMark = nil
	if res, want := r.run(), (blob.CollectResult{Candidates: 3, Deleted: 3}); res != want {
		r.t.Fatalf("Run after the cancelled pass = %+v, want %+v", res, want)
	}
	for _, ref := range refs {
		r.wantGone("after the finishing pass", ref)
	}
}

// failingStore fails Delete for chosen digests and forwards everything else.
type failingStore struct {
	blob.Store
	fail map[string]error
}

func (f *failingStore) Delete(ctx context.Context, ns namespace.Namespace, ref blob.Ref) error {
	if err := f.fail[ref.Digest]; err != nil {
		return err
	}
	return f.Store.Delete(ctx, ns, ref)
}

func caseGCDeleteFails(r *gcRig) {
	names := []string{"first", "second", "third"}
	refs := make([]blob.Ref, len(names))
	for i, n := range names {
		refs[i] = r.put(compressible(n))
	}
	r.age()

	boom := errors.New("object store is down")
	broken := &failingStore{Store: r.store, fail: map[string]error{refs[1].Digest: boom}}
	gc, err := blob.NewCollector(r.hook, broken, blob.CollectorOptions{Grace: grace, Batch: gcBatch})
	if err != nil {
		r.t.Fatalf("NewCollector: %v", err)
	}
	res, err := gc.Run(context.Background())
	if !errors.Is(err, boom) {
		r.t.Fatalf("Run error = %v, want the store's error", err)
	}
	// One broken object must not starve the ones behind it.
	if want := (blob.CollectResult{Candidates: 3, Deleted: 2, Failed: 1}); res != want {
		r.t.Fatalf("Run = %+v, want %+v", res, want)
	}
	r.wantReadable("blob whose delete failed", refs[1], compressible(names[1]))
	r.wantGone("blob before the failure", refs[0])
	r.wantGone("blob after the failure", refs[2])

	// The failed object stayed claimed, so a healthy pass retries it.
	if res, want := r.run(), (blob.CollectResult{Candidates: 1, Deleted: 1}); res != want {
		r.t.Fatalf("retry Run = %+v, want %+v", res, want)
	}
	r.wantGone("blob after the retry", refs[1])
}

func caseGCDeleteUnsupported(r *gcRig) {
	refs := []blob.Ref{r.put(compressible("a")), r.put(compressible("b"))}
	r.age()

	broken := &failingStore{Store: r.store, fail: map[string]error{}}
	for _, ref := range refs {
		broken.fail[ref.Digest] = blob.ErrDeleteUnsupported
	}
	gc, err := blob.NewCollector(r.hook, broken, blob.CollectorOptions{Grace: grace, Batch: gcBatch})
	if err != nil {
		r.t.Fatalf("NewCollector: %v", err)
	}
	// Every object would fail the same way, so the pass stops at the first.
	res, err := gc.Run(context.Background())
	if !errors.Is(err, blob.ErrDeleteUnsupported) || res.Deleted != 0 || res.Failed != 0 {
		r.t.Fatalf("Run = (%+v, %v), want ErrDeleteUnsupported and no per-object failures", res, err)
	}
}

func caseGCPutAfterCollect(r *gcRig) {
	data := compressible("collected, then written again")
	first := r.put(data)
	r.age()
	r.run()
	r.wantGone("collected blob", first)

	second := r.put(data)
	if second != first {
		r.t.Fatalf("Put after collection = %+v, want %+v", second, first)
	}
	r.wantReadable("rewritten blob", second, data)
}

func caseGCMetadataReadBack(r *gcRig) {
	data := compressible("two writers, two codecs")
	first := r.put(data)
	if first.Codec != blob.CodecZstd {
		r.t.Fatalf("fixture error: first Put used codec %q, want zstd", first.Codec)
	}
	// A second writer that does not compress registers the same digest. It
	// dedups against the first writer's bytes and must report the first writer's
	// Codec and Stored, which the index recorded, not its own.
	plain := blob.NewObjectBacked(r.fs, blob.ObjectBackedOptions{Index: r.hook})
	second, err := plain.Put(context.Background(), idxNS, data)
	if err != nil {
		r.t.Fatalf("second writer Put error = %v", err)
	}
	if second != first {
		r.t.Fatalf("second writer's Ref = %+v, want the recorded %+v", second, first)
	}
	got, err := plain.Get(context.Background(), idxNS, second)
	if err != nil || string(got) != string(data) {
		r.t.Fatalf("second writer Get = (%d bytes, %v), want the content", len(got), err)
	}
}
