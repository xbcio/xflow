package sqlstore

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xbcio/xflow/store"
)

// SupplyResealer is the at-rest transform a reseal pass needs on top of
// SupplyEncryption: a way to tell, without decrypting, whether a stored value
// is already sealed under the current key.
type SupplyResealer interface {
	SupplyEncryption
	NeedsReseal(stored []byte) bool
}

// ErrResealNotConfigured means the provider has no at-rest encryption, or its
// encryption cannot classify rows. Resealing without a key would be a no-op
// that reports success, so it is refused.
var ErrResealNotConfigured = errors.New("sqlstore: supply reseal requires at-rest encryption with NeedsReseal")

// defaultResealBatch bounds how many row IDs one page reads. Each row is still
// resealed in its own transaction, so this only sizes the ID scan.
const defaultResealBatch = 100

// maxResealFailures caps how many failures the report itemises so a store full
// of undecryptable rows cannot produce an unbounded report. Failed still
// counts all of them.
const maxResealFailures = 100

// ResealOptions configures ResealSupplies.
type ResealOptions struct {
	// BatchSize is the ID page size; <= 0 uses defaultResealBatch.
	BatchSize int
	// DryRun classifies rows without writing anything.
	DryRun bool
	// Namespace, when non-empty, limits the pass to one namespace. Empty means
	// every row; a rotation is finished only by an unscoped pass.
	Namespace string
}

// ResealFailure names one row that could not be resealed. Reason never carries
// stored bytes: they are ciphertext of a value that may be a credential.
type ResealFailure struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Reason    string `json:"reason"`
}

// ResealReport summarises one ResealSupplies pass. The previous key may be
// removed only after a pass (not a dry run) reports Failed == 0; Resealed
// counts rows rewritten by this pass, or rows that would be under DryRun.
type ResealReport struct {
	Scanned  int             `json:"scanned"`
	Resealed int             `json:"resealed"`
	Current  int             `json:"already_current"`
	Failed   int             `json:"failed"`
	DryRun   bool            `json:"dry_run"`
	Failures []ResealFailure `json:"failures,omitempty"`
}

// ResealSupplies rewrites every supply row that is not sealed under the
// current at-rest key: rows sealed under the previous key, and pre-encryption
// plaintext rows. It is the step that makes a KEK rotation finishable — until
// it reports zero failures, the previous key must stay configured.
//
// Contract:
//   - Plaintext is unchanged, so content_hash, revision and updated_at are left
//     alone. Consumers compare content_hash to decide whether to rebuild; a
//     reseal must not look like a content change to them.
//   - Each row is locked (SELECT ... FOR UPDATE) and resealed in its own
//     transaction, the same lock PutSupply takes, so a concurrent write is
//     serialised against it rather than overwritten.
//   - A row that cannot be opened, or whose plaintext no longer matches its
//     content_hash, is reported and left untouched; the pass continues.
//   - Idempotent and resumable: already-current rows are skipped, so rerunning
//     after an interruption only touches what is left.
func (p *Provider) ResealSupplies(ctx context.Context, opts ResealOptions) (*ResealReport, error) {
	resealer, ok := p.supplyRepo.atRest.(SupplyResealer)
	if !ok || resealer == nil {
		return nil, ErrResealNotConfigured
	}
	batch := opts.BatchSize
	if batch <= 0 {
		batch = defaultResealBatch
	}
	report := &ResealReport{DryRun: opts.DryRun}
	var cursor uint64
	for {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		var ids []uint64
		q := p.db.WithContext(ctx).Model(&dbSupply{}).Where("id > ?", cursor)
		if opts.Namespace != "" {
			q = q.Where("namespace = ?", opts.Namespace)
		}
		if err := q.Order("id ASC").Limit(batch).
			Pluck("id", &ids).Error; err != nil {
			return report, fmt.Errorf("reseal supplies: list ids after %d: %w", cursor, err)
		}
		if len(ids) == 0 {
			return report, nil
		}
		for _, id := range ids {
			if err := p.resealSupplyRow(ctx, resealer, id, opts.DryRun, report); err != nil {
				return report, err
			}
			cursor = id
		}
	}
}

// resealSupplyRow handles one row. It returns an error only for failures that
// should stop the pass (DB errors, cancellation); per-row content problems are
// recorded in report.
func (p *Provider) resealSupplyRow(ctx context.Context, resealer SupplyResealer, id uint64, dryRun bool, report *ResealReport) error {
	return p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row dbSupply
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).First(&row).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Deleted between the ID scan and now: nothing to reseal.
			return nil
		}
		if err != nil {
			return fmt.Errorf("reseal supplies: lock row %d: %w", id, err)
		}
		report.Scanned++
		stored := []byte(row.Content)
		if !resealer.NeedsReseal(stored) {
			report.Current++
			return nil
		}
		fail := func(reason string) {
			report.Failed++
			if len(report.Failures) < maxResealFailures {
				report.Failures = append(report.Failures, ResealFailure{Namespace: row.Namespace, Name: row.Name, Reason: reason})
			}
		}
		plaintext, err := resealer.Open(stored)
		if err != nil {
			fail(fmt.Sprintf("open: %v", err))
			return nil
		}
		// Refuse to launder a damaged row: re-sealing corrupted bytes under the
		// current key would give them a fresh, valid envelope.
		if row.ContentHash != "" && row.ContentHash != store.ContentHash(plaintext) {
			fail("content hash mismatch, stored content may be corrupted")
			return nil
		}
		if dryRun {
			report.Resealed++
			return nil
		}
		sealed, err := resealer.Seal(plaintext)
		if err != nil {
			fail(fmt.Sprintf("seal: %v", err))
			return nil
		}
		// UpdateColumn skips hooks and updated_at tracking: the content did not
		// change, only its envelope. b64Bytes keeps the driver.Valuer on the
		// static type (see PutSupply).
		if err := tx.Model(&dbSupply{}).Where("id = ?", id).
			UpdateColumn("content", b64Bytes(sealed)).Error; err != nil {
			return fmt.Errorf("reseal supplies: write row %d: %w", id, err)
		}
		report.Resealed++
		return nil
	})
}
