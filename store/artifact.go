package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"path/filepath"
	"strings"

	"github.com/xbcio/xflow/store/objectstore"
)

// MaxArtifactBytes caps a single artifact at 16 MiB, matching the MEDIUMBLOB
// column that backs the authoritative store and the max_allowed_packet the
// deployment already requires for xflow_supplies.
const MaxArtifactBytes = 16 << 20

var (
	// ErrArtifactTooLarge is returned when content exceeds MaxArtifactBytes.
	ErrArtifactTooLarge = errors.New("store: artifact exceeds size limit")

	// ErrInvalidFilename is returned when a filename is not a bare base name.
	// Rejecting rather than silently sanitizing is deliberate: a caller that
	// passed a path wants to find out, not to have it quietly rewritten.
	ErrInvalidFilename = errors.New("store: invalid artifact filename")

	// ErrVersionConflict is returned when (namespace, filename, version) already
	// resolves to different content. A version binding is immutable once made —
	// the Docker "tag moved under me" failure mode is not reproduced here.
	ErrVersionConflict = errors.New("store: artifact version already bound to different content")

	// ErrDigestMismatch is returned by Open when the bytes a backend served do
	// not hash to the digest they were addressed by. It is a permanent
	// condition, not a transient one: the read is content-addressed, so
	// retrying the same digest against the same backend can only produce the
	// same wrong bytes.
	ErrDigestMismatch = errors.New("store: artifact content does not match its digest")
)

// ArtifactRef is the small descriptor that travels through node parameters,
// Graph snapshots and TaskLeases. It never carries bytes.
type ArtifactRef struct {
	// Digest is "sha256:<hex>" — the only addressing key, and by construction
	// also the version identity of the bytes.
	Digest string
	Size   int64
	// Filename is the original base name from upload. Purely descriptive:
	// addressing is entirely by Digest, and no filesystem path is ever derived
	// from this field. It is the only untrusted input in this design.
	Filename string
}

// ArtifactMeta is the naming information supplied at upload time.
type ArtifactMeta struct {
	// Filename must be a bare base name; see ValidateFilename.
	Filename string
	// Version defaults to DefaultVersion(digest) when empty.
	Version string
	// Namespace is the tenant, and participates in the identity unique key.
	Namespace string
	// ContentType is advisory. Empty means "infer from the Filename suffix",
	// falling back to application/octet-stream.
	ContentType string
}

// ArtifactIdentity is one (namespace, filename, version) -> digest binding.
type ArtifactIdentity struct {
	Namespace   string
	Filename    string
	Version     string
	Digest      string
	ContentType string
}

// ArtifactIndex is the identity layer: tenant-scoped names pointing at
// content-addressed bytes. It is separate from objectstore.Store because the
// two layers answer different questions — the object store deduplicates bytes
// globally and cannot be tenant-aware, while the index is exactly what makes
// tenant authorization possible without breaking that deduplication.
//
// Only the authoritative (server-side) store has an index. A runner-side
// ArtifactStore resolves purely by digest and carries a nil index.
type ArtifactIndex interface {
	// Bind records that (namespace, filename, version) refers to id.Digest.
	// Re-binding the same triple to the same digest is a no-op success;
	// re-binding it to different content returns ErrVersionConflict.
	Bind(ctx context.Context, id ArtifactIdentity) error

	// HasReference reports whether digest is referenced by any identity row in
	// namespace. This is the authorization predicate for artifact retrieval.
	HasReference(ctx context.Context, namespace, digest string) (bool, error)

	// CountReferences returns how many identity rows point at digest across all
	// namespaces. Used for operator-facing reference reporting; there is no
	// automatic garbage collection.
	CountReferences(ctx context.Context, digest string) (int64, error)
}

// ArtifactStore adds "content-addressed and immutable" semantics on top of any
// objectstore.Store. It is a concrete type rather than an interface because
// there is only one possible behaviour; everything that varies lives in the
// backend below it.
type ArtifactStore struct {
	objects objectstore.Store
	index   ArtifactIndex
}

// NewArtifactStore builds an authoritative store: bytes in objects, names in
// index. index may be nil, which yields a digest-only store — Open and Stat
// work, Put refuses to record a name, and there is no authorization predicate.
// That is the correct shape for a runner-side cache.
func NewArtifactStore(objects objectstore.Store, index ArtifactIndex) *ArtifactStore {
	return &ArtifactStore{objects: objects, index: index}
}

// Put stores content and, when an index is present, binds meta's name to it.
//
// It takes []byte rather than an io.Reader, deliberately breaking symmetry with
// objectstore.PutObject: the digest must be computed by this layer from the
// complete content, so a caller is structurally unable to supply its own digest
// and thus unable to claim a hash that does not match the bytes. Streaming would
// require either a temp file or after-the-fact verification, and the 16 MiB
// ceiling already bounds the memory cost.
func (s *ArtifactStore) Put(ctx context.Context, content []byte, meta ArtifactMeta) (ArtifactRef, error) {
	if int64(len(content)) > MaxArtifactBytes {
		return ArtifactRef{}, fmt.Errorf("%w: %d > %d", ErrArtifactTooLarge, len(content), int64(MaxArtifactBytes))
	}
	name, err := ValidateFilename(meta.Filename)
	if err != nil {
		return ArtifactRef{}, err
	}

	digest := ContentHash(content)
	ref := ArtifactRef{Digest: digest, Size: int64(len(content)), Filename: name}

	contentType := meta.ContentType
	if contentType == "" {
		contentType = inferContentType(name)
	}

	// IfNoneMatch: identical bytes already stored means there is nothing to
	// write. Deduplication is the expected outcome, not an error.
	_, err = s.objects.PutObject(ctx, ObjectKeyForDigest(digest), bytes.NewReader(content), ref.Size, objectstore.PutOptions{
		ContentType: contentType,
		Metadata:    map[string]string{"filename": name},
		IfNoneMatch: true,
	})
	if err != nil && !errors.Is(err, objectstore.ErrPreconditionFailed) {
		return ArtifactRef{}, err
	}

	if s.index == nil {
		return ref, nil
	}
	version := meta.Version
	if version == "" {
		version = DefaultVersion(digest)
	}
	if err := s.index.Bind(ctx, ArtifactIdentity{
		Namespace:   meta.Namespace,
		Filename:    name,
		Version:     version,
		Digest:      digest,
		ContentType: contentType,
	}); err != nil {
		return ArtifactRef{}, err
	}
	return ref, nil
}

// Open returns the content for digest. The caller closes the reader.
//
// The bytes are hashed and compared against digest before any of them are
// handed back — Put computes the digest from the complete content so that a
// caller cannot claim a hash that does not match its bytes, and this is the
// read-side half of that same guarantee. Without it the digest is only a name:
// ValidateDigest checks the shape of the requested string, objectstore derives
// Object.ETag by parsing the digest back out of the key, and nothing anywhere
// recomputes it. The runner resolves a script node's artifact_digest through
// here and feeds the result straight to the wasm compiler, so the digest is the
// only thing binding a workflow definition to the code that actually runs.
//
// The cost is one sha256 over at most MaxArtifactBytes, once per call. That is
// not the hot path: node/internal/code/script memoises resolved artifact bytes
// per (namespace, digest, language), so a runner hashes each module once per
// process, not once per message.
//
// Verification requires the whole body, so Open buffers rather than streams.
// Every backend already does: sqlstore reads the MEDIUMBLOB into memory,
// objectstore.ReadThrough buffers the origin response to write it through to
// cache, and MaxArtifactBytes bounds all of it at 16 MiB.
func (s *ArtifactStore) Open(ctx context.Context, digest string) (io.ReadCloser, ArtifactRef, error) {
	if err := ValidateDigest(digest); err != nil {
		return nil, ArtifactRef{}, err
	}
	body, obj, err := s.objects.GetObject(ctx, ObjectKeyForDigest(digest))
	if err != nil {
		return nil, ArtifactRef{}, err
	}
	defer func() { _ = body.Close() }()

	// One byte past the ceiling, so content of exactly MaxArtifactBytes — which
	// Put admits — reads through cleanly and only a larger body is rejected.
	content, err := io.ReadAll(io.LimitReader(body, MaxArtifactBytes+1))
	if err != nil {
		return nil, ArtifactRef{}, fmt.Errorf("store: read artifact %s: %w", digest, err)
	}
	if int64(len(content)) > MaxArtifactBytes {
		return nil, ArtifactRef{}, fmt.Errorf("%w: %s is larger than %d", ErrArtifactTooLarge, digest, int64(MaxArtifactBytes))
	}
	if got := ContentHash(content); got != digest {
		// Name both digests so an operator can tell "the backend lost my bytes"
		// from "the backend has someone else's bytes", and never echo the
		// content: it is source or a compiled module, and this error is logged.
		return nil, ArtifactRef{}, fmt.Errorf("%w: %s resolved to content hashing %s", ErrDigestMismatch, digest, got)
	}

	ref := refFromObject(digest, obj)
	// The bytes are now known authentic, so their count is the authoritative
	// size regardless of what the backend reported (HTTPStore, for one, reports
	// -1 for a chunked response).
	ref.Size = int64(len(content))
	return io.NopCloser(bytes.NewReader(content)), ref, nil
}

// Stat returns metadata for digest without reading the content.
func (s *ArtifactStore) Stat(ctx context.Context, digest string) (ArtifactRef, error) {
	if err := ValidateDigest(digest); err != nil {
		return ArtifactRef{}, err
	}
	obj, err := s.objects.HeadObject(ctx, ObjectKeyForDigest(digest))
	if err != nil {
		return ArtifactRef{}, err
	}
	return refFromObject(digest, obj), nil
}

// HasReference reports whether namespace may read digest. A store with no index
// has no authorization layer and answers false — failing closed.
func (s *ArtifactStore) HasReference(ctx context.Context, namespace, digest string) (bool, error) {
	if s.index == nil {
		return false, nil
	}
	if err := ValidateDigest(digest); err != nil {
		return false, err
	}
	return s.index.HasReference(ctx, namespace, digest)
}

func refFromObject(digest string, obj *objectstore.Object) ArtifactRef {
	ref := ArtifactRef{Digest: digest}
	if obj != nil {
		ref.Size = obj.Size
		ref.Filename = obj.Metadata["filename"]
	}
	return ref
}

// ObjectKeyForDigest maps a digest to its object key, sharding two levels deep
// the way git objects and most content-addressed stores do, so no single
// directory or index prefix accumulates millions of entries.
//
//	sha256:abcdef... -> artifacts/sha256/ab/cd/abcdef...
//
// The key derives solely from the digest. Filename never participates, which is
// why even a filename validation bypass could not influence file placement.
func ObjectKeyForDigest(digest string) string {
	alg, hex, ok := strings.Cut(digest, ":")
	if !ok || len(hex) < 4 {
		// ValidateDigest gates every caller; this branch keeps the function
		// total rather than panicking on a value that cannot reach it.
		return "artifacts/invalid/" + digest
	}
	return "artifacts/" + alg + "/" + hex[0:2] + "/" + hex[2:4] + "/" + hex
}

// DefaultVersion is the version assigned when the caller supplies none. It is a
// pure function of the digest, so publishing the same file twice produces the
// same identity row rather than a second one.
func DefaultVersion(digest string) string {
	_, hex, ok := strings.Cut(digest, ":")
	if !ok || len(hex) < 12 {
		return "sha256-unknown"
	}
	return "sha256-" + hex[:12]
}

// ValidateDigest accepts only the canonical "sha256:<64 lowercase hex>" form.
// Retrieval keys are derived from this value, so anything else is rejected
// before it can reach a backend.
func ValidateDigest(digest string) error {
	const prefix = "sha256:"
	hex, ok := strings.CutPrefix(digest, prefix)
	if !ok || len(hex) != 64 {
		return fmt.Errorf("store: invalid artifact digest %q: want sha256:<64 hex>", digest)
	}
	for i := 0; i < len(hex); i++ {
		c := hex[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf("store: invalid artifact digest %q: non-hex character", digest)
		}
	}
	return nil
}

// ValidateFilename accepts only a bare base name and returns it unchanged.
//
// It rejects rather than sanitizes so that a caller passing "build/tagger.wasm"
// learns about it instead of silently getting "tagger.wasm". Backslash is
// rejected explicitly: filepath.Base on Linux does not treat it as a separator,
// so a Windows-style path would otherwise pass through as one long "name".
func ValidateFilename(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: empty", ErrInvalidFilename)
	}
	if len(name) > 255 {
		return "", fmt.Errorf("%w: longer than 255 bytes", ErrInvalidFilename)
	}
	if strings.ContainsAny(name, "\x00\\") {
		return "", fmt.Errorf("%w: %q contains a NUL or backslash", ErrInvalidFilename, name)
	}
	if name == "." || name == ".." {
		return "", fmt.Errorf("%w: %q is a directory reference", ErrInvalidFilename, name)
	}
	if filepath.Base(name) != name {
		return "", fmt.Errorf("%w: %q is not a bare base name", ErrInvalidFilename, name)
	}
	return name, nil
}

// inferContentType guesses a MIME type from the filename suffix. The value is
// advisory only — nothing in the platform interprets artifact content.
func inferContentType(name string) string {
	if ct := mime.TypeByExtension(filepath.Ext(name)); ct != "" {
		return ct
	}
	switch strings.ToLower(filepath.Ext(name)) {
	case ".wasm":
		return "application/wasm"
	case ".js", ".mjs":
		return "text/javascript"
	case ".py":
		return "text/x-python"
	}
	return "application/octet-stream"
}
