package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// bombInput is the uncompressed size of the hostile frames below. It is far
// above the Size the Ref promises (10), so a decoder that honours the frame
// instead of the Ref would allocate this much.
const bombInput = 64 << 20

// bombFrames returns two small, valid zstd objects that expand to bombInput
// zero bytes: one declares its content size in the frame header (what
// Encoder.EncodeAll writes), the other does not (what a streaming writer
// writes), which is the harder case because the decoder cannot tell up front.
func bombFrames(t *testing.T) map[string][]byte {
	t.Helper()
	zeros := make([]byte, bombInput)
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd.NewWriter error = %v", err)
	}
	declared := enc.EncodeAll(zeros, []byte{frameZstd})

	var buf bytes.Buffer
	buf.WriteByte(frameZstd)
	w, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatalf("zstd.NewWriter(stream) error = %v", err)
	}
	if _, err := w.Write(zeros); err != nil {
		t.Fatalf("stream Write error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("stream Close error = %v", err)
	}
	return map[string][]byte{"DeclaredContentSize": declared, "UnknownContentSize": buf.Bytes()}
}

// TestObjectBacked_DecompressionBomb stores a tiny frame that expands to 64 MiB
// under a Ref that promises far fewer bytes. Get must fail with ErrCorrupt and
// must not allocate anything near the expanded size.
//
// The frames are about 7 KiB. Under a Ref size of 16 KiB they fit inside the
// read bound (frameReadLimit is about 17 KiB), so only the decoder's own
// protections stop the expansion: the content-size check when the frame declares
// its size, and the output cap when it does not. Under a Ref size of 10 the
// read bound (about 1 KiB) is exceeded as well, but both defences end in
// ErrCorrupt, so this test cannot tell which one fired and does not pin the
// read bound. TestObjectBacked_ReadIsBounded does that.
func TestObjectBacked_DecompressionBomb(t *testing.T) {
	// A real 64 MiB allocation dwarfs this bound, while the buffers a correct
	// Get touches (a few tens of KiB) stay far below it.
	const allocBound = 8 << 20

	for name, frame := range bombFrames(t) {
		for _, claimed := range []int{10, 16 << 10} {
			t.Run(fmt.Sprintf("%s/RefSize%d", name, claimed), func(t *testing.T) {
				f := newCorruptionFixture(t)
				ref := f.refFor(make([]byte, claimed)) // Digest of claimed zero bytes.
				f.plant(ref, frame)

				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				got, err := f.store.Get(context.Background(), f.ns, ref)
				runtime.ReadMemStats(&after)

				if !errors.Is(err, ErrCorrupt) {
					t.Fatalf("Get of a %d byte frame expanding to %d bytes under ref size %d = (%d bytes, %v), want ErrCorrupt", len(frame), bombInput, claimed, len(got), err)
				}
				// TotalAlloc counts every allocation process-wide, so this is a
				// rough bound: other goroutines can only add to it, never hide a
				// 64 MiB allocation made by Get.
				if grown := after.TotalAlloc - before.TotalAlloc; grown > allocBound {
					t.Errorf("Get allocated %d bytes while rejecting the bomb, want under %d", grown, allocBound)
				}
			})
		}
	}
}
