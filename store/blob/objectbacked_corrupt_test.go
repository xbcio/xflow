package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/objectstore"
)

// corruptionFixture is an ObjectBacked store over a FSStore whose objects the
// test can overwrite directly, as damaged storage would.
type corruptionFixture struct {
	t     *testing.T
	fs    *objectstore.FSStore
	store *ObjectBacked
	ns    namespace.Namespace
}

func newCorruptionFixture(t *testing.T) *corruptionFixture {
	t.Helper()
	fs := objectstore.NewFSStore(t.TempDir())
	return &corruptionFixture{t: t, fs: fs, store: NewObjectBacked(fs, ObjectBackedOptions{}), ns: "corrupt"}
}

// refFor returns the Ref a correct Put of data would return, without storing it.
func (f *corruptionFixture) refFor(data []byte) Ref {
	sum := sha256.Sum256(data)
	size := int64(len(data))
	return Ref{Namespace: string(f.ns), Digest: hex.EncodeToString(sum[:]), Size: size, Stored: size}
}

// plant writes object as the raw stored bytes (frame header included) for ref,
// bypassing ObjectBacked.Put.
func (f *corruptionFixture) plant(ref Ref, object []byte) {
	f.t.Helper()
	key, err := objectKey(f.ns, ref.Digest)
	if err != nil {
		f.t.Fatalf("objectKey error = %v", err)
	}
	_, err = f.fs.PutObject(context.Background(), key, bytes.NewReader(object), int64(len(object)), objectstore.PutOptions{})
	if err != nil {
		f.t.Fatalf("PutObject(%q) error = %v", key, err)
	}
}

func (f *corruptionFixture) wantCorrupt(ref Ref) {
	f.t.Helper()
	got, err := f.store.Get(context.Background(), f.ns, ref)
	if !errors.Is(err, ErrCorrupt) {
		f.t.Fatalf("Get(%+v) = (%d bytes, %v), want ErrCorrupt", ref, len(got), err)
	}
}

// zstdFrame returns a complete object (header byte plus zstd payload) for data.
func zstdFrame(t *testing.T, data []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd.NewWriter error = %v", err)
	}
	return enc.EncodeAll(data, []byte{frameZstd})
}

func TestObjectBacked_Corruption(t *testing.T) {
	data := bytes.Repeat([]byte("integrity "), 1024)

	t.Run("UnknownFrameHeader", func(t *testing.T) {
		f := newCorruptionFixture(t)
		ref := f.refFor(data)
		f.plant(ref, append([]byte{0x7f}, data...))
		f.wantCorrupt(ref)
	})

	t.Run("EmptyObject", func(t *testing.T) {
		f := newCorruptionFixture(t)
		ref := f.refFor(data)
		f.plant(ref, nil)
		f.wantCorrupt(ref)
	})

	t.Run("TruncatedZstd", func(t *testing.T) {
		f := newCorruptionFixture(t)
		ref := f.refFor(data)
		frame := zstdFrame(t, data)
		f.plant(ref, frame[:len(frame)/2])
		f.wantCorrupt(ref)
	})

	t.Run("ZstdDecodesToWrongLength", func(t *testing.T) {
		f := newCorruptionFixture(t)
		ref := f.refFor(data)
		f.plant(ref, zstdFrame(t, data[:len(data)-1]))
		f.wantCorrupt(ref)
	})

	t.Run("RawSameLengthOtherBytes", func(t *testing.T) {
		f := newCorruptionFixture(t)
		ref := f.refFor(data)
		other := bytes.Repeat([]byte("tampered! "), 1024)
		f.plant(ref, append([]byte{frameRaw}, other...))
		f.wantCorrupt(ref)
	})

	t.Run("ZstdSameLengthOtherBytes", func(t *testing.T) {
		f := newCorruptionFixture(t)
		ref := f.refFor(data)
		other := bytes.Repeat([]byte("tampered! "), 1024)
		f.plant(ref, zstdFrame(t, other))
		f.wantCorrupt(ref)
	})

	t.Run("RawWrongLength", func(t *testing.T) {
		f := newCorruptionFixture(t)
		ref := f.refFor(data)
		f.plant(ref, append([]byte{frameRaw}, data[:len(data)-1]...))
		f.wantCorrupt(ref)
	})

	t.Run("ObjectLongerThanAnyValidFrame", func(t *testing.T) {
		f := newCorruptionFixture(t)
		small := []byte("tiny")
		ref := f.refFor(small)
		f.plant(ref, append([]byte{frameRaw}, bytes.Repeat([]byte("x"), 1<<20)...))
		f.wantCorrupt(ref)
	})
}
