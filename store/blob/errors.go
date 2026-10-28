package blob

import "errors"

// Sentinel errors returned by Store implementations. Callers test for them with
// errors.Is; implementations may wrap them with extra context.
var (
	// ErrNotFound reports that the blob a Ref points at does not exist in the
	// Store.
	ErrNotFound = errors.New("blob: not found")

	// ErrCorrupt reports that the bytes read back do not match the Ref: their
	// SHA-256 differs from Ref.Digest or their length differs from Ref.Size. It
	// signals damaged or tampered storage, not a caller mistake.
	ErrCorrupt = errors.New("blob: content does not match ref")

	// ErrTooLarge reports that the data exceeds the maximum size the Store
	// accepts. The condition is permanent for that data, so retrying cannot
	// succeed.
	ErrTooLarge = errors.New("blob: data too large")

	// ErrNamespaceMismatch reports that Ref.Namespace differs from the namespace
	// the caller passed. A Ref is only valid inside the namespace that issued it.
	ErrNamespaceMismatch = errors.New("blob: ref namespace does not match caller namespace")

	// ErrInvalidRef reports that a Ref failed Ref.Validate. Store methods wrap it
	// when they reject a malformed Ref, before touching storage.
	ErrInvalidRef = errors.New("blob: invalid ref")

	// ErrDeleteUnsupported reports that the underlying storage cannot delete
	// objects, so Store.Delete is not available on this implementation.
	ErrDeleteUnsupported = errors.New("blob: delete not supported")
)
