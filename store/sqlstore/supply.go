package sqlstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xbcio/xflow/store"
)

// supplyRepo implements store.Supplies.
type supplyRepo struct {
	db *gorm.DB
	// atRest, when non-nil, encrypts the content column. Nil keeps the
	// pre-encryption behaviour byte for byte, so the feature can be rolled out
	// without changing what an existing deployment stores.
	atRest SupplyEncryption
}

var _ store.Supplies = (*supplyRepo)(nil)

// normSupplyNS returns "default" for an empty namespace, mirroring the
// normalisation on the write path in sdk/xflow.Server.UpdateSupply so that
// callers which pass "" for namespace find the same row they wrote.
func normSupplyNS(ns string) string {
	if ns == "" {
		return "default"
	}
	return ns
}

func (r *supplyRepo) GetSupply(ctx context.Context, namespace, name string) (*store.SupplyResource, error) {
	namespace = normSupplyNS(namespace)
	var d dbSupply
	err := r.db.WithContext(ctx).
		Where("namespace = ? AND name = ?", namespace, name).
		First(&d).Error
	if err := wrapDBErr(fmt.Sprintf("get supply %q/%q", namespace, name), err); err != nil {
		return nil, err
	}
	out := fromDBSupply(&d)
	if r.atRest != nil {
		plaintext, decErr := r.atRest.Open(out.Content)
		if decErr != nil {
			// Do not include the stored bytes in the error: they are ciphertext
			// of a value that may be a credential.
			return nil, fmt.Errorf("get supply %q/%q: decrypt: %w", namespace, name, decErr)
		}
		out.Content = plaintext
	}
	// General integrity check, independent of encryption: content_hash is
	// written alongside content on every PutSupply (see below), so it must
	// still match the content just read back, whether or not atRest is set.
	// A mismatch means the row was damaged after the fact — truncation, a bad
	// migration, a direct SQL edit — and serving it as if it were fine would
	// be strictly worse than erroring. PutSupply always sets Content and
	// ContentHash together from the same rec.Content, in the same write, so a
	// non-empty content_hash can never be stale for a legitimate row: it is
	// never left over from a previous value while content moves on without
	// it. An empty content_hash is treated as "no hash to check" rather than
	// a mismatch, so this cannot break reads of rows that predate the
	// content_hash column (schema default is '').
	//
	// Encryption is one case this happens to cover, not the reason it exists:
	// an at-rest implementation may identify pre-encryption plaintext from an
	// envelope prefix, so corruption that destroys that prefix can make Open
	// return mangled bytes as plaintext. The hash closes that residual gap, but
	// it applies equally when atRest is nil and no encryption is involved.
	if out.ContentHash != "" && out.ContentHash != store.ContentHash(out.Content) {
		return nil, fmt.Errorf("get supply %q/%q: content hash mismatch, stored content may be corrupted", namespace, name)
	}
	return out, nil
}

// PutSupply performs the compare-and-set inside a transaction with a row lock so
// two concurrent writers carrying the same If-Match cannot both succeed.
func (r *supplyRepo) PutSupply(ctx context.Context, rec *store.SupplyResource, ifMatch *uint64) (*store.SupplyResource, error) {
	// Normalise namespace before touching the DB so that callers which pass ""
	// store and find rows under "default", matching GetSupply's normalisation.
	if rec.Namespace == "" {
		cp := *rec
		cp.Namespace = "default"
		rec = &cp
	}
	var out *store.SupplyResource
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var cur dbSupply
		found := true
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("namespace = ? AND name = ?", rec.Namespace, rec.Name).
			First(&cur).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			found = false
		} else if err != nil {
			return err
		}
		var curRev uint64
		if found {
			curRev = cur.Revision
		}
		if ifMatch != nil && *ifMatch != curRev {
			return store.ErrRevisionConflict
		}

		// Convert domain zero time.Time to DB NULL (*time.Time nil).
		var lastFetchAt *time.Time
		if !rec.LastFetchAt.IsZero() {
			t := rec.LastFetchAt
			lastFetchAt = &t
		}

		// The hash is computed on the PLAINTEXT and the ciphertext is stored.
		// Hashing the ciphertext would defeat both idempotent-write detection
		// and the consumer-side "hash unchanged, do not rebuild" check, because
		// AES-GCM's random nonce makes every encryption of the same content a
		// different byte string.
		contentHash := store.ContentHash(rec.Content)
		storedContent := rec.Content
		if r.atRest != nil {
			sealed, encErr := r.atRest.Seal(rec.Content)
			if encErr != nil {
				return fmt.Errorf("put supply %q/%q: encrypt: %w", rec.Namespace, rec.Name, encErr)
			}
			storedContent = sealed
		}

		next := dbSupply{
			ID:          cur.ID,
			Namespace:   rec.Namespace,
			Name:        rec.Name,
			Content:     storedContent,
			ContentType: rec.ContentType,
			Revision:    curRev + 1,
			ContentHash: contentHash,
			UpdatedBy:   rec.UpdatedBy,
			LastFetchAt: lastFetchAt,
			LastError:   rec.LastError,
		}
		if found {
			if err := tx.Model(&dbSupply{}).Where("id = ?", cur.ID).Updates(map[string]any{
				"content":       next.Content,
				"content_type":  next.ContentType,
				"revision":      next.Revision,
				"content_hash":  next.ContentHash,
				"updated_by":    next.UpdatedBy,
				"last_fetch_at": lastFetchAt,
				"last_error":    next.LastError,
			}).Error; err != nil {
				return err
			}
		} else if err := tx.Create(&next).Error; err != nil {
			return err
		}

		var stored dbSupply
		if err := tx.Where("namespace = ? AND name = ?", rec.Namespace, rec.Name).
			First(&stored).Error; err != nil {
			return err
		}
		out = fromDBSupply(&stored)
		if r.atRest != nil {
			// The row was just read back from the DB, so Content is ciphertext.
			// Callers (and the store contract tests) expect plaintext.
			plaintext, decErr := r.atRest.Open(out.Content)
			if decErr != nil {
				return fmt.Errorf("put supply %q/%q: decrypt readback: %w", rec.Namespace, rec.Name, decErr)
			}
			out.Content = plaintext
		}
		return nil
	})
	if errors.Is(err, store.ErrRevisionConflict) {
		return nil, err
	}
	if err := wrapDBErr(fmt.Sprintf("put supply %q/%q", rec.Namespace, rec.Name), err); err != nil {
		return nil, err
	}
	return out, nil
}
