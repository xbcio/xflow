package blob

import (
	"fmt"

	"github.com/xbcio/xflow/namespace"
)

// digestLen is the length of a hex-encoded SHA-256 digest.
const digestLen = 64

// Codec values for Ref.Codec.
const (
	// CodecNone marks a payload stored as the original, uncompressed bytes.
	CodecNone = ""
	// CodecZstd marks a payload stored as a zstd frame of the original bytes.
	CodecZstd = "zstd"
)

// Ref points at a blob inside one namespace. It is the only thing Redis and Kafka
// carry for an externalized payload. A Ref received from a client must never be
// trusted: run Validate and let the Store check it against the stored bytes.
type Ref struct {
	// Namespace is the namespace that owns the blob.
	Namespace string `json:"ns"`
	// Digest is the lowercase hex SHA-256 of the uncompressed bytes (64 characters).
	Digest string `json:"sha256"`
	// Size is the uncompressed byte count.
	Size int64 `json:"size"`
	// Stored is the byte count of the payload as stored, after compression.
	Stored int64 `json:"stored"`
	// Codec names how the payload is encoded at rest: "" for none, "zstd" for zstd.
	Codec string `json:"codec,omitempty"`
}

// Validate reports whether r is well formed. It checks shape only and does not
// touch storage. Every error wraps ErrInvalidRef.
func (r Ref) Validate() error {
	if err := validateNamespace(namespace.Namespace(r.Namespace)); err != nil {
		return fmt.Errorf("%w: namespace %q: %w", ErrInvalidRef, r.Namespace, err)
	}
	if !validDigest(r.Digest) {
		return fmt.Errorf("%w: digest %q is not 64 lowercase hex characters", ErrInvalidRef, r.Digest)
	}
	if r.Size < 0 {
		return fmt.Errorf("%w: size %d is negative", ErrInvalidRef, r.Size)
	}
	if r.Stored < 0 {
		return fmt.Errorf("%w: stored %d is negative", ErrInvalidRef, r.Stored)
	}
	if r.Codec != CodecNone && r.Codec != CodecZstd {
		return fmt.Errorf("%w: unknown codec %q", ErrInvalidRef, r.Codec)
	}
	// Without a codec the payload is the original bytes, so the two counts must
	// agree. A zstd payload may be larger than the original (incompressible
	// data plus frame overhead), so only its sign is checked above.
	if r.Codec == CodecNone && r.Stored != r.Size {
		return fmt.Errorf("%w: stored %d differs from size %d with no codec", ErrInvalidRef, r.Stored, r.Size)
	}
	return nil
}

// checkRef validates ref and confirms it belongs to the caller's namespace ns.
// A malformed ref returns an error wrapping ErrInvalidRef; a well-formed ref
// issued for another namespace returns ErrNamespaceMismatch. Get and Delete run
// it before touching storage.
func checkRef(ns namespace.Namespace, ref Ref) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	if ref.Namespace != string(ns) {
		return fmt.Errorf("%w: ref %q, caller %q", ErrNamespaceMismatch, ref.Namespace, string(ns))
	}
	return nil
}

// validDigest reports whether s is exactly 64 lowercase hexadecimal characters.
func validDigest(s string) bool {
	if len(s) != digestLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
