package sqlstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xbcio/xflow/store/blob"
)

// BlobIndex is the SQL implementation of blob.Index: the lifecycle and
// garbage-collection metadata of the blobs a blob.Store holds, kept in
// xflow_blob_objects and xflow_blob_links. It stores no bytes and takes no part
// in scheduling.
//
// The fence. Every method that decides anything about one object first takes
// that object's row lock (SELECT ... FOR UPDATE inside a transaction), so a
// writer's Register, a Link, a collector's MarkDeleting, and a collector's
// Purge are totally ordered per object. Purge runs the byte-delete callback
// while it still holds the lock, which is the point: a writer cannot revive the
// object between the collector's last check and the delete, because the
// writer's Register waits for the lock. Whichever commits first wins, and the
// loser sees the winner's state: a Purge that finds the generation changed
// returns blob.ErrFenced without deleting; a Register that arrives after a
// Purge finds no row and creates a fresh object.
//
// Consequences worth knowing:
//
//   - The delete callback runs inside a database transaction. It must not use
//     this database (on SQLite the single pooled connection is the one the
//     transaction holds, so it would deadlock) and should be bounded by the
//     context, since the object stays locked while it runs.
//   - Register inserts before it locks. Locking first would take a gap lock on
//     the missing key, and two writers that both locked a missing key and then
//     inserted would deadlock under InnoDB's default isolation.
//   - A transaction the database aborts as a deadlock victim is run again a few
//     times (see inTx). That needs the dialect's transient-error classifier to be
//     registered, which importing store/sqlstore/mysqlstore does.
//   - Times are bound as Go UTC values, truncated to milliseconds, and compared
//     in SQL only against other bound values. No session-time-zone function is
//     used, so the result does not depend on the server's time_zone.
type BlobIndex struct {
	db  *gorm.DB
	now func() time.Time
}

var _ blob.Index = (*BlobIndex)(nil)

// pruneChunk bounds how many link keys one DELETE names, which keeps the
// statement under the bind-parameter limits of both dialects (3 per key).
const pruneChunk = 200

// NewBlobIndex returns a BlobIndex over db, reading the time from now (nil
// means time.Now). Blob metadata needs a database, so a nil db fails here
// rather than at the first call. The tables must exist: apply
// db/xflow_schema.sql, or AutoMigrate in development and tests.
func NewBlobIndex(db *gorm.DB, now func() time.Time) (*BlobIndex, error) {
	if db == nil {
		return nil, errors.New("sqlstore: blob index needs a database")
	}
	if now == nil {
		now = time.Now
	}
	return &BlobIndex{db: db, now: func() time.Time { return blob.IndexTime(now()) }}, nil
}

// blobTxAttempts bounds how often a transaction that the database aborted as a
// deadlock victim is run again before its error is returned.
const blobTxAttempts = 5

// inTx runs fn in a transaction and runs it again, up to blobTxAttempts times in
// all, when the database aborts it as a deadlock victim. InnoDB documents that
// as the way to handle a deadlock, and it is not avoidable here: two writers that
// insert into the same index gap each hold a gap lock the other's insert needs,
// whatever order the statements are in. Rerunning is safe because every
// transaction in this file is idempotent and starts from a fresh read, and
// Purge's delete callback is required to be idempotent.
//
// A deadlock is recognised through the classifiers dialect packages register
// (see RegisterTransientClassifier), so the retry exists for the dialect whose
// package the binary imports, and no other error is ever retried. When the
// attempts run out the last error is returned and, being classified, wraps
// store.ErrTransient for the caller.
func (b *BlobIndex) inTx(ctx context.Context, fn func(tx *gorm.DB) error) error {
	for attempt := 1; ; attempt++ {
		err := b.db.WithContext(ctx).Transaction(fn)
		if err == nil || attempt == blobTxAttempts || !isRegisteredTransient(err) {
			return err
		}
		// Back off a little longer each time so the transaction that won the
		// deadlock can finish before this one asks for the same locks again.
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(attempt) * 2 * time.Millisecond):
		}
	}
}

// errBlobObjectMissing reports that the object row a method needed to lock does
// not exist. Each method maps it to the sentinel its contract names.
var errBlobObjectMissing = errors.New("sqlstore: blob object row missing")

// lockBlobObject reads one object row with a row lock. It must run inside a
// transaction; the lock lasts until that transaction ends. A missing row is an
// ordinary outcome (a Purge removed it), so it is read with Find rather than
// First, which GORM logs as an error.
func lockBlobObject(tx *gorm.DB, ns, digest string) (*dbBlobObject, error) {
	var rows []dbBlobObject
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("namespace = ? AND digest = ?", ns, digest).
		Limit(1).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errBlobObjectMissing
	}
	return &rows[0], nil
}

// ref converts a row to the Ref the index records for it.
func (o *dbBlobObject) ref() blob.Ref {
	return blob.Ref{Namespace: o.Namespace, Digest: o.Digest, Size: o.SizeBytes, Stored: o.StoredBytes, Codec: o.Codec}
}

// touchBlobObject moves the object's last-reference time forward to now. It
// never moves it back: nodes sharing one database have different clocks, and a
// reference stamped by a slow clock must not make a recently used object look
// idle. The caller holds the object's row lock.
func touchBlobObject(tx *gorm.DB, obj *dbBlobObject, now time.Time) error {
	if !now.After(obj.LastRefAt) {
		return nil
	}
	return tx.Model(&dbBlobObject{}).
		Where("namespace = ? AND digest = ?", obj.Namespace, obj.Digest).
		Update("last_ref_at", now).Error
}

// Register implements blob.Index.
func (b *BlobIndex) Register(ctx context.Context, ref blob.Ref) (blob.Registration, error) {
	if err := ctx.Err(); err != nil {
		return blob.Registration{}, err
	}
	if err := ref.Validate(); err != nil {
		return blob.Registration{}, err
	}
	var reg blob.Registration
	err := b.inTx(ctx, func(tx *gorm.DB) error {
		now := b.now()
		fresh := dbBlobObject{
			Namespace: ref.Namespace, Digest: ref.Digest,
			SizeBytes: ref.Size, StoredBytes: ref.Stored, Codec: ref.Codec,
			Generation: 1, State: blob.ObjectLive, LastRefAt: now,
		}
		// Insert first and ignore a duplicate: see the type comment. Whether the
		// row was created here or already existed, the locked read below is the
		// single source of truth, so the outcome of this statement is not read.
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&fresh).Error; err != nil {
			return err
		}
		obj, err := lockBlobObject(tx, ref.Namespace, ref.Digest)
		if err != nil {
			if errors.Is(err, errBlobObjectMissing) {
				return fmt.Errorf("blob object %s/%s vanished during register", ref.Namespace, ref.Digest)
			}
			return err
		}
		if obj.SizeBytes != ref.Size {
			return fmt.Errorf("%w: %s/%s is recorded with size %d, ref has %d",
				blob.ErrMetadataConflict, obj.Namespace, obj.Digest, obj.SizeBytes, ref.Size)
		}
		if obj.State == blob.ObjectDeleting {
			// Revive: the generation increment is what invalidates every claim
			// taken before this moment, and the writer's codec replaces the
			// recorded one because the writer is about to rewrite the bytes.
			next := obj.Generation + 1
			if err := tx.Model(&dbBlobObject{}).
				Where("namespace = ? AND digest = ?", obj.Namespace, obj.Digest).
				Updates(map[string]any{
					"state": blob.ObjectLive, "generation": next,
					"stored_bytes": ref.Stored, "codec": ref.Codec,
					"last_ref_at": maxTime(obj.LastRefAt, now),
				}).Error; err != nil {
				return err
			}
			obj.State, obj.Generation, obj.StoredBytes, obj.Codec = blob.ObjectLive, next, ref.Stored, ref.Codec
			reg = blob.Registration{Ref: obj.ref(), Revived: true}
			return nil
		}
		if err := touchBlobObject(tx, obj, now); err != nil {
			return err
		}
		reg = blob.Registration{Ref: obj.ref()}
		return nil
	})
	if err != nil {
		return blob.Registration{}, wrapBlobIndexErr("register blob", err)
	}
	return reg, nil
}

// maxTime returns the later of a and b.
func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// Link implements blob.Index.
func (b *BlobIndex) Link(ctx context.Context, execID string, ref blob.Ref, expireAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := blob.ValidateExecID(execID); err != nil {
		return fmt.Errorf("%w: %w", blob.ErrInvalidRef, err)
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	expireAt = blob.IndexTime(expireAt)
	err := b.inTx(ctx, func(tx *gorm.DB) error {
		now := b.now()
		if !expireAt.After(now) {
			return fmt.Errorf("blob: link expiry %s is not after the current time %s",
				expireAt.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
		}
		obj, err := lockBlobObject(tx, ref.Namespace, ref.Digest)
		if errors.Is(err, errBlobObjectMissing) {
			return fmt.Errorf("%w: %s/%s is not registered", blob.ErrNotFound, ref.Namespace, ref.Digest)
		}
		if err != nil {
			return err
		}
		if obj.State != blob.ObjectLive {
			return fmt.Errorf("%w: %s/%s is claimed for deletion", blob.ErrFenced, ref.Namespace, ref.Digest)
		}
		// Create the link if there is none, then move an existing one later. The
		// insert comes first for the reason Register inserts before it locks: an
		// UPDATE whose key is missing takes a gap lock under InnoDB's default
		// isolation, and two Links for different objects that each took one and
		// then inserted into the same gap deadlock. Inserting first holds no gap
		// lock, and the update then targets a row that exists. Both statements are
		// idempotent and neither reads the other's outcome, so a concurrent
		// PruneLinks that deletes the expired row in between only turns the update
		// into a no-op. A link never moves earlier: the insert ignores an existing
		// row and the update only matches a smaller expiry.
		link := dbBlobLink{Namespace: ref.Namespace, ExecID: execID, Digest: ref.Digest, ExpireAt: expireAt}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&link).Error; err != nil {
			return err
		}
		if err := tx.Model(&dbBlobLink{}).
			Where("namespace = ? AND exec_id = ? AND digest = ? AND expire_at < ?", ref.Namespace, execID, ref.Digest, expireAt).
			Update("expire_at", expireAt).Error; err != nil {
			return err
		}
		return touchBlobObject(tx, obj, now)
	})
	return wrapBlobIndexErr("link blob", err)
}

// PruneLinks implements blob.Index.
func (b *BlobIndex) PruneLinks(ctx context.Context, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if limit <= 0 {
		return 0, fmt.Errorf("blob: prune limit %d is not positive", limit)
	}
	now := b.now()
	var keys []dbBlobLink
	if err := b.db.WithContext(ctx).
		Where("expire_at <= ?", now).
		Order("expire_at").
		Limit(limit).
		Find(&keys).Error; err != nil {
		return 0, wrapBlobIndexErr("select expired blob links", err)
	}
	removed := 0
	for start := 0; start < len(keys); start += pruneChunk {
		chunk := keys[start:min(start+pruneChunk, len(keys))]
		args := make([]any, 0, 3*len(chunk)+1)
		for _, k := range chunk {
			args = append(args, k.Namespace, k.ExecID, k.Digest)
		}
		args = append(args, now)
		// expire_at is checked again so a link that a concurrent Link extended
		// after the select above is left alone.
		res := b.db.WithContext(ctx).Exec(
			"DELETE FROM xflow_blob_links WHERE (namespace, exec_id, digest) IN ("+
				strings.TrimSuffix(strings.Repeat("(?, ?, ?),", len(chunk)), ",")+") AND expire_at <= ?", args...)
		if res.Error != nil {
			return removed, wrapBlobIndexErr("prune blob links", res.Error)
		}
		removed += int(res.RowsAffected)
	}
	return removed, nil
}

// Candidates implements blob.Index.
func (b *BlobIndex) Candidates(ctx context.Context, grace time.Duration, limit int) ([]blob.Ref, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := blob.ValidateGCArgs(grace, limit); err != nil {
		return nil, err
	}
	now := b.now()
	cutoff := blob.IndexTime(now.Add(-grace))
	var rows []dbBlobObject
	err := b.db.WithContext(ctx).Raw(
		"SELECT o.namespace, o.digest, o.size_bytes, o.stored_bytes, o.codec "+
			"FROM xflow_blob_objects o "+
			"WHERE o.last_ref_at < ? "+
			"AND NOT EXISTS (SELECT 1 FROM xflow_blob_links l "+
			"WHERE l.namespace = o.namespace AND l.digest = o.digest AND l.expire_at > ?) "+
			"ORDER BY o.last_ref_at, o.namespace, o.digest LIMIT ?",
		cutoff, now, limit).Scan(&rows).Error
	if err != nil {
		return nil, wrapBlobIndexErr("list blob candidates", err)
	}
	out := make([]blob.Ref, len(rows))
	for i := range rows {
		out[i] = rows[i].ref()
	}
	return out, nil
}

// MarkDeleting implements blob.Index.
func (b *BlobIndex) MarkDeleting(ctx context.Context, ref blob.Ref, grace time.Duration) (blob.Fence, error) {
	if err := ctx.Err(); err != nil {
		return blob.Fence{}, err
	}
	if err := ref.Validate(); err != nil {
		return blob.Fence{}, err
	}
	if err := blob.ValidateGCArgs(grace, 1); err != nil {
		return blob.Fence{}, err
	}
	var fence blob.Fence
	err := b.inTx(ctx, func(tx *gorm.DB) error {
		now := b.now()
		obj, err := lockBlobObject(tx, ref.Namespace, ref.Digest)
		if errors.Is(err, errBlobObjectMissing) {
			return fmt.Errorf("%w: %s/%s is not registered", blob.ErrNotFound, ref.Namespace, ref.Digest)
		}
		if err != nil {
			return err
		}
		// An object that is already claimed is still garbage: Link refuses it
		// and a writer's Register would have revived it, so nothing can have
		// changed since the claim.
		if obj.State == blob.ObjectLive {
			if !obj.LastRefAt.Before(blob.IndexTime(now.Add(-grace))) {
				return fmt.Errorf("%w: %s/%s", blob.ErrNotCandidate, obj.Namespace, obj.Digest)
			}
			var linked *int
			if err := tx.Raw(
				"SELECT 1 FROM xflow_blob_links WHERE namespace = ? AND digest = ? AND expire_at > ? LIMIT 1",
				obj.Namespace, obj.Digest, now).Scan(&linked).Error; err != nil {
				return err
			}
			if linked != nil {
				return fmt.Errorf("%w: %s/%s", blob.ErrNotCandidate, obj.Namespace, obj.Digest)
			}
			if err := tx.Model(&dbBlobObject{}).
				Where("namespace = ? AND digest = ?", obj.Namespace, obj.Digest).
				Update("state", blob.ObjectDeleting).Error; err != nil {
				return err
			}
		}
		fence = blob.Fence{Ref: obj.ref(), Generation: obj.Generation}
		return nil
	})
	if err != nil {
		return blob.Fence{}, wrapBlobIndexErr("claim blob for deletion", err)
	}
	return fence, nil
}

// Purge implements blob.Index.
func (b *BlobIndex) Purge(ctx context.Context, fence blob.Fence, del func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fence.Validate(); err != nil {
		return err
	}
	if del == nil {
		return errors.New("blob: purge needs a delete callback")
	}
	ns, digest := fence.Ref.Namespace, fence.Ref.Digest
	err := b.inTx(ctx, func(tx *gorm.DB) error {
		obj, err := lockBlobObject(tx, ns, digest)
		if errors.Is(err, errBlobObjectMissing) {
			return fmt.Errorf("%w: %s/%s no longer exists", blob.ErrFenced, ns, digest)
		}
		if err != nil {
			return err
		}
		if obj.State != blob.ObjectDeleting || obj.Generation != fence.Generation {
			return fmt.Errorf("%w: %s/%s is %s at generation %d, the claim was generation %d",
				blob.ErrFenced, ns, digest, obj.State, obj.Generation, fence.Generation)
		}
		// The bytes go while the row lock is held, so no writer can revive the
		// object between the check above and this call. If del fails the
		// transaction rolls back and the object stays claimed for a retry.
		if err := del(ctx); err != nil {
			return err
		}
		if err := tx.Where("namespace = ? AND digest = ?", ns, digest).Delete(&dbBlobLink{}).Error; err != nil {
			return err
		}
		return tx.Where("namespace = ? AND digest = ?", ns, digest).Delete(&dbBlobObject{}).Error
	})
	return wrapBlobIndexErr("purge blob", err)
}

// wrapBlobIndexErr is wrapDBErr for the blob index: errors that already carry a
// blob sentinel are returned as they are, so the operation prefix does not bury
// them, and everything else gets the usual transient classification.
func wrapBlobIndexErr(op string, err error) error {
	if err == nil {
		return nil
	}
	for _, sentinel := range []error{
		blob.ErrFenced, blob.ErrNotCandidate, blob.ErrNotFound, blob.ErrInvalidRef, blob.ErrMetadataConflict,
		context.Canceled, context.DeadlineExceeded,
	} {
		if errors.Is(err, sentinel) {
			return err
		}
	}
	return wrapDBErr(op, err)
}
