package sqlstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/store"
)

// supplyRepo implements store.Supplies.
type supplyRepo struct {
	db *gorm.DB
	// atRest, when non-nil, encrypts the content column. Nil keeps the
	// pre-encryption behaviour byte for byte, so the feature can be rolled out
	// without changing what an existing deployment stores.
	atRest *supplyenc.AtRest
}

var _ store.Supplies = (*supplyRepo)(nil)

func (r *supplyRepo) GetSupply(ctx context.Context, namespace, name string) (*store.SupplyResource, error) {
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
	return out, nil
}

// PutSupply performs the compare-and-set inside a transaction with a row lock so
// two concurrent writers carrying the same If-Match cannot both succeed.
func (r *supplyRepo) PutSupply(ctx context.Context, rec *store.SupplyResource, ifMatch *uint64) (*store.SupplyResource, error) {
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
