package sqlstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
)

// compile-time interface assertions
var (
	_ objectstore.Store   = (*artifactBlobRepo)(nil)
	_ store.ArtifactIndex = (*artifactIndexRepo)(nil)
)

// ---------------------------------------------------------------------------
// artifactBlobRepo implements objectstore.Store against xflow_artifact_blobs.
// ---------------------------------------------------------------------------

type artifactBlobRepo struct {
	db *gorm.DB
}

// digestFromKey recovers the canonical "sha256:<hex>" digest from an object key
// like "artifacts/sha256/ab/cd/abcdef...", which is the shape
// store.ObjectKeyForDigest produces. The algorithm is the second segment and the
// full hex is the last; the two sharding segments in between are redundant with
// the hex prefix and are not consulted.
//
// A key this repo did not generate yields a digest that matches no row, so the
// caller sees store.ErrNotFound. That is the right outcome: this layer stores
// bytes and must not be the place that decides what a valid digest looks like
// (store.ValidateDigest owns that, above).
func digestFromKey(key string) string {
	parts := strings.Split(key, "/")
	if len(parts) < 3 {
		return key
	}
	return parts[1] + ":" + parts[len(parts)-1]
}

func (r *artifactBlobRepo) PutObject(ctx context.Context, key string, body io.Reader, size int64, opts objectstore.PutOptions) (*objectstore.Object, error) {
	content, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("put artifact blob: read body: %w", err)
	}
	// size is the caller's claim; content is the truth. Storing an unverified
	// claim would put a size in the column that a later HeadObject reports and
	// the runner-side cache validates against — a mismatch there would look like
	// corruption in the retrieval path rather than a bad write here.
	if int64(len(content)) != size {
		return nil, fmt.Errorf("put artifact blob %q: body is %d bytes, caller declared %d", key, len(content), size)
	}
	if int64(len(content)) > store.MaxArtifactBytes {
		return nil, fmt.Errorf("put artifact blob %q: %d bytes exceeds %d limit", key, len(content), int64(store.MaxArtifactBytes))
	}

	digest := digestFromKey(key)

	row := dbArtifactBlob{
		ContentHash: digest,
		Content:     content,
		SizeBytes:   uint64(size),
	}

	if opts.IfNoneMatch {
		// INSERT ... ON DUPLICATE KEY UPDATE (no-op update) avoids rewriting
		// the 16 MiB BLOB when the row already exists. RowsAffected == 0 means
		// the row existed — signal deduplication via ErrPreconditionFailed.
		result := r.db.WithContext(ctx).
			Clauses(clause.OnConflict{DoNothing: true}).
			Create(&row)
		if result.Error != nil {
			return nil, fmt.Errorf("put artifact blob %q: %w", digest, result.Error)
		}
		if result.RowsAffected == 0 {
			return &objectstore.Object{
				Key:  key,
				Size: size,
				ETag: digest,
			}, objectstore.ErrPreconditionFailed
		}
	} else {
		if err := r.db.WithContext(ctx).Create(&row).Error; err != nil {
			return nil, fmt.Errorf("put artifact blob %q: %w", digest, err)
		}
	}

	return &objectstore.Object{
		Key:          key,
		Size:         size,
		ETag:         digest,
		LastModified: time.Now(),
	}, nil
}

func (r *artifactBlobRepo) GetObject(ctx context.Context, key string) (io.ReadCloser, *objectstore.Object, error) {
	digest := digestFromKey(key)
	var row dbArtifactBlob
	err := r.db.WithContext(ctx).
		Where("content_hash = ?", digest).
		First(&row).Error
	if err != nil {
		return nil, nil, wrapBlobErr(fmt.Sprintf("get artifact blob %q", digest), err)
	}
	obj := &objectstore.Object{
		Key:          key,
		Size:         int64(row.SizeBytes),
		ETag:         row.ContentHash,
		LastModified: row.CreatedAt,
	}
	return io.NopCloser(bytes.NewReader(row.Content)), obj, nil
}

func (r *artifactBlobRepo) HeadObject(ctx context.Context, key string) (*objectstore.Object, error) {
	digest := digestFromKey(key)
	var row dbArtifactBlob
	// Explicitly select only metadata columns — NEVER the content column.
	// This is a hard requirement: HeadObject must not pull multi-MiB BLOBs.
	err := r.db.WithContext(ctx).
		Select("content_hash", "size_bytes", "created_at").
		Where("content_hash = ?", digest).
		First(&row).Error
	if err != nil {
		return nil, wrapBlobErr(fmt.Sprintf("head artifact blob %q", digest), err)
	}
	return &objectstore.Object{
		Key:          key,
		Size:         int64(row.SizeBytes),
		ETag:         row.ContentHash,
		LastModified: row.CreatedAt,
	}, nil
}

// wrapBlobErr is wrapDBErr's counterpart for the blob repo. It maps a missing
// row to objectstore.ErrNotFound rather than store.ErrNotFound: what this repo
// implements is objectstore.Store, and a composition like objectstore.ReadThrough
// distinguishes "cache miss" from "backend broken" against that sentinel without
// knowing which backend it holds. Returning the store-level sentinel here would
// make a MySQL miss indistinguishable from a real failure to such a caller.
func wrapBlobErr(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("%s: %w", op, objectstore.ErrNotFound)
	}
	return fmt.Errorf("%s: %w", op, err)
}

// ---------------------------------------------------------------------------
// artifactIndexRepo implements store.ArtifactIndex against xflow_artifacts.
// ---------------------------------------------------------------------------

type artifactIndexRepo struct {
	db *gorm.DB
}

// Bind records an identity binding. Same triple + same digest is idempotent;
// same triple + different digest returns ErrVersionConflict.
func (r *artifactIndexRepo) Bind(ctx context.Context, id store.ArtifactIdentity) error {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var cur dbArtifact
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("namespace = ? AND filename = ? AND version = ?", id.Namespace, id.Filename, id.Version).
			First(&cur).Error

		if errors.Is(err, gorm.ErrRecordNotFound) {
			// No existing row — insert. The row lock above cannot guard a row
			// that does not exist yet, so two concurrent binds of the same
			// triple both reach here; uk_identity is what actually serializes
			// them, and the loser surfaces as a duplicate-key error rather than
			// a silent second row.
			row := dbArtifact{
				Namespace:   id.Namespace,
				Filename:    id.Filename,
				Version:     id.Version,
				ContentHash: id.Digest,
				ContentType: id.ContentType,
			}
			return tx.Create(&row).Error
		}
		if err != nil {
			return err
		}

		// Row exists. A version binding is immutable: the same triple may only
		// ever name the same bytes.
		if cur.ContentHash != id.Digest {
			return store.ErrVersionConflict
		}
		// Idempotent: same triple, same digest — nothing to do.
		return nil
	})

	// Return ErrVersionConflict unwrapped so callers can match the sentinel
	// without wrapDBErr's operation prefix (mirrors PutSupply's handling of
	// ErrRevisionConflict).
	if errors.Is(err, store.ErrVersionConflict) {
		return err
	}
	return wrapDBErr("bind artifact identity", err)
}

// HasReference reports whether digest is referenced by any identity row in
// namespace. This is the authorization predicate for artifact retrieval, so it
// runs on the hot path of every runner fetch: LIMIT 1 against idx_content_hash
// rather than COUNT(*), which would scan every matching row to answer a
// yes/no question.
func (r *artifactIndexRepo) HasReference(ctx context.Context, namespace, digest string) (bool, error) {
	// A pointer target distinguishes "no row" (stays nil) from "row with value
	// 0"; scanning into a bare int would rely on the zero value meaning absent.
	var found *int
	err := r.db.WithContext(ctx).
		Raw("SELECT 1 FROM xflow_artifacts WHERE namespace = ? AND content_hash = ? LIMIT 1", namespace, digest).
		Scan(&found).Error
	if err != nil {
		return false, wrapDBErr("has artifact reference", err)
	}
	return found != nil, nil
}

// CountReferences returns how many identity rows point at digest across all
// namespaces.
func (r *artifactIndexRepo) CountReferences(ctx context.Context, digest string) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&dbArtifact{}).
		Where("content_hash = ?", digest).
		Count(&count).Error
	if err != nil {
		return 0, wrapDBErr("count artifact references", err)
	}
	return count, nil
}

// latestRowPredicate is the group-wise maximum: keep the row that is newest for
// its own (namespace, filename).
//
// It is a correlated subquery rather than a window function on purpose —
// nothing else in this tree uses ROW_NUMBER(), and taking a hard MySQL 8.0
// dependency for one ops-page query is not a trade worth making. Both the outer
// scan and the subquery are served by uk_identity (namespace, filename,
// version); EXPLAIN shows a covering index lookup on the outer side and an
// index lookup on the inner.
//
// The `id DESC` tiebreak is load-bearing, not decoration. created_at is
// DATETIME(3) and GORM stamps it client-side (autoCreateTime:milli), so two
// versions bound within the same millisecond compare equal — without the
// tiebreak "the latest" has no answer and the page flips between them across
// refreshes.
//
// The outer row is referenced by table name because GORM's Model() emits no
// alias; the inner one is aliased `y` so the two are distinguishable.
const latestRowPredicate = `id = (
	SELECT y.id FROM xflow_artifacts y
	WHERE y.namespace = xflow_artifacts.namespace AND y.filename = xflow_artifacts.filename
	ORDER BY y.created_at DESC, y.id DESC LIMIT 1
)`

// ListLatestVersions returns the newest identity row per filename in namespace.
//
// namespace is matched exactly and is never a wildcard — an empty string here
// selects only rows literally stored with an empty namespace. Treating it as
// "all namespaces" would be a horizontal privilege escalation, which is why the
// scope is a plain equality predicate exactly as in HasReference above.
func (r *artifactIndexRepo) ListLatestVersions(ctx context.Context, namespace string, opts store.ListOptions) ([]*store.ArtifactVersion, error) {
	var ds []*dbArtifact
	q := r.db.WithContext(ctx).
		Model(&dbArtifact{}).
		Where("namespace = ?", namespace).
		Where(latestRowPredicate).
		Order("filename, id")
	err := applyPagination(q, opts).Find(&ds).Error
	if err := wrapDBErr(fmt.Sprintf("list latest artifact versions in %q", namespace), err); err != nil {
		return nil, err
	}
	return fromDBArtifacts(ds), nil
}

// ListVersions returns every version of one file, most recent first. Same
// namespace rule as ListLatestVersions.
func (r *artifactIndexRepo) ListVersions(ctx context.Context, namespace, filename string, opts store.ListOptions) ([]*store.ArtifactVersion, error) {
	var ds []*dbArtifact
	q := r.db.WithContext(ctx).
		Model(&dbArtifact{}).
		Where("namespace = ? AND filename = ?", namespace, filename).
		Order("created_at DESC, id DESC")
	err := applyPagination(q, opts).Find(&ds).Error
	if err := wrapDBErr(fmt.Sprintf("list artifact versions %q/%q", namespace, filename), err); err != nil {
		return nil, err
	}
	return fromDBArtifacts(ds), nil
}

func fromDBArtifacts(ds []*dbArtifact) []*store.ArtifactVersion {
	out := make([]*store.ArtifactVersion, 0, len(ds))
	for _, d := range ds {
		out = append(out, &store.ArtifactVersion{
			ArtifactIdentity: store.ArtifactIdentity{
				Namespace:   d.Namespace,
				Filename:    d.Filename,
				Version:     d.Version,
				Digest:      d.ContentHash,
				ContentType: d.ContentType,
			},
			CreatedAt: d.CreatedAt,
		})
	}
	return out
}
