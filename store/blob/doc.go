// Package blob defines a content-addressed contract for moving large payloads
// out of the state store.
//
// A node output, task body, or map items list that grows past a size threshold
// is kept as a blob: Redis and Kafka carry only a small Ref, and the bytes live
// in a Store. The contract describes nothing but moving bytes by content. It
// carries no Redis, SQL, or object-storage detail, so the same contract serves
// the local (file) and distributed (objectstore) backends.
//
// Addressing. A blob is identified by (namespace, digest). The digest is the
// SHA-256 of the UNCOMPRESSED bytes, never of the stored payload, so a change of
// compression codec or level does not change the identity and deduplication
// stays stable. A Store always computes the digest itself and never accepts one
// from the caller; Ref.Codec records only how the payload is encoded at rest.
//
// Keys. Backends that map a blob to a path use the key from objectKey, in which
// the namespace appears as lowercase hex rather than as text. A case-insensitive
// or Unicode-normalizing filesystem would otherwise fold two different
// namespaces ("Tenant" and "tenant") into one directory and let them read each
// other's blobs.
//
// Ordering. Writers follow write-before-reference: Put the bytes first, then
// commit the Ref into state. A failed Put must fail the commit rather than fall
// back to inlining. A failed commit leaves an orphan blob, which garbage
// collection reclaims later.
//
// Frame format. ObjectBacked writes each blob to the object store as one header
// byte followed by the payload: 0x00 means the payload is the original bytes,
// 0x01 means it is one zstd frame of them. Get decodes by that header alone and
// never by Ref.Codec or Ref.Stored, which are only the writer's record. Two
// writers can compress the same digest differently, and an object store need not
// persist any metadata, so the encoding has to live in the stored bytes. An
// unknown header byte, an empty object, or a payload that does not decode to
// exactly Ref.Size bytes with the Ref's SHA-256 is ErrCorrupt.
//
// Scope. Store moves bytes only. Retention, link tracking, and garbage
// collection of orphans belong to the layers above it, not to this package.
package blob
