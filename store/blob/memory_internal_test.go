package blob

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/namespace"
)

// TestMemory_Corruption tampers with the stored bytes directly, which needs
// access to unexported fields, so it lives in package blob.
func TestMemory_Corruption(t *testing.T) {
	ctx := context.Background()
	const ns namespace.Namespace = "corrupt"
	data := []byte("trustworthy bytes")

	t.Run("FlippedByte", func(t *testing.T) {
		m := NewMemory()
		ref, err := m.Put(ctx, ns, data)
		if err != nil {
			t.Fatalf("Put error = %v", err)
		}
		m.blobs[memKey{ns: ns, digest: ref.Digest}][0] ^= 0xff
		if _, err := m.Get(ctx, ns, ref); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Get after flipping a stored byte error = %v, want ErrCorrupt", err)
		}
	})

	t.Run("Truncated", func(t *testing.T) {
		m := NewMemory()
		ref, err := m.Put(ctx, ns, data)
		if err != nil {
			t.Fatalf("Put error = %v", err)
		}
		key := memKey{ns: ns, digest: ref.Digest}
		m.blobs[key] = m.blobs[key][:len(data)-1]
		if _, err := m.Get(ctx, ns, ref); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Get after truncating the stored bytes error = %v, want ErrCorrupt", err)
		}
	})

	t.Run("WrongSizeInRef", func(t *testing.T) {
		m := NewMemory()
		ref, err := m.Put(ctx, ns, data)
		if err != nil {
			t.Fatalf("Put error = %v", err)
		}
		ref.Size++
		ref.Stored++ // Keep the Ref well formed: no codec means Stored == Size.
		if _, err := m.Get(ctx, ns, ref); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Get with ref.Size = %d (stored %d bytes) error = %v, want ErrCorrupt", ref.Size, len(data), err)
		}
	})
}
