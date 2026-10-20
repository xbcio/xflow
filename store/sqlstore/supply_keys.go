package sqlstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/xbcio/xflow/store"
)

// SupplyKeyInspector is the read-only capability InspectSupplyKeys needs from
// an at-rest encryptor: it extends SupplyResealer with the two facts only the
// key layer knows, the current key's fingerprint and the key ID a stored value
// claims. It stays an optional interface (rather than growing
// SupplyEncryption) so the storage layer keeps owning no key management: a
// caller whose encryptor cannot classify rows is refused instead of being
// handed a report that guessed.
type SupplyKeyInspector interface {
	SupplyResealer
	CurrentKeyID() string
	EnvelopeKeyID(stored []byte) (kid string, isEnvelope bool)
}

// ErrSupplyKeyInspectionNotConfigured means the provider has no at-rest
// encryption, or its encryption cannot identify keys. Reporting a per-kid
// distribution without that capability would be a fabricated answer, so it is
// refused.
var ErrSupplyKeyInspectionNotConfigured = errors.New("sqlstore: supply key inspection requires at-rest encryption with key identification")

// maxSupplyKeyIssues caps how many unreadable rows one report itemises, so a
// store full of damaged rows cannot produce an unbounded report. Unreadable
// still counts all of them.
const maxSupplyKeyIssues = 100

// SupplyKeyStatusOptions configures InspectSupplyKeys.
type SupplyKeyStatusOptions struct {
	// BatchSize is the row page size; <= 0 uses defaultResealBatch.
	BatchSize int
	// Namespace, when non-empty, limits the scan to one namespace.
	Namespace string
}

// SupplyKeyIssue names one row that could not be read with the configured
// keyring. Reason never carries stored bytes: they may be ciphertext of a
// credential.
type SupplyKeyIssue struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	KeyID     string `json:"key_id,omitempty"`
	Reason    string `json:"reason"`
}

// SupplyKeyStatus is a read-only classification of every scanned supply row.
//
// ByKeyID counts envelopes whose kid parsed; Plaintext counts non-envelopes
// (rows written before at-rest encryption was enabled); Unparsable counts
// envelope-prefixed values with no usable kid (damaged rows, or legacy
// plaintext that happens to start like an envelope). Those three buckets and
// ByKeyID are mutually exclusive and cover every scanned row.
//
// NeedsReseal counts rows the provider's reseal pass would still rewrite, using
// the same NeedsReseal predicate the pass uses. Unreadable counts rows the
// caller's keyring cannot open, or whose plaintext no longer matches
// content_hash. A rotation is safe to finish only when a scan built from the
// CURRENT key alone reports NeedsReseal == 0 and Unreadable == 0; a status scan
// given the current+previous keyring instead reports old-kid rows as readable
// and leaves them in NeedsReseal.
type SupplyKeyStatus struct {
	CurrentKeyID string           `json:"current_key_id"`
	Scanned      int              `json:"scanned"`
	ByKeyID      map[string]int   `json:"by_key_id"`
	Plaintext    int              `json:"plaintext"`
	Unparsable   int              `json:"unparsable"`
	NeedsReseal  int              `json:"needs_reseal"`
	Unreadable   int              `json:"unreadable"`
	Issues       []SupplyKeyIssue `json:"issues,omitempty"`
}

// InspectSupplyKeys scans supply rows without writing anything and classifies
// them by at-rest key ID. It takes no row locks and changes no column, so it is
// safe to run against a live database; it is a point-in-time diagnostic, not a
// snapshot, so concurrent PutSupply writes can appear between pages.
//
// The caller chooses what "readable" means through encryption: pass an
// encryptor built with the current+previous keys for a status report, or one
// built with the current key alone to verify that a reseal finished and the
// previous key can be retired. Only an encryptor that also identifies keys
// (SupplyKeyInspector) is accepted; anything else yields
// ErrSupplyKeyInspectionNotConfigured rather than a guessed report. The report
// is returned even alongside a scan error so a partial classification is not
// lost.
func (p *Provider) InspectSupplyKeys(ctx context.Context, encryption SupplyEncryption, opts SupplyKeyStatusOptions) (*SupplyKeyStatus, error) {
	if encryption == nil {
		return nil, ErrSupplyKeyInspectionNotConfigured
	}
	inspector, ok := encryption.(SupplyKeyInspector)
	if !ok {
		return nil, ErrSupplyKeyInspectionNotConfigured
	}
	batch := opts.BatchSize
	if batch <= 0 {
		batch = defaultResealBatch
	}
	status := &SupplyKeyStatus{
		CurrentKeyID: inspector.CurrentKeyID(),
		ByKeyID:      make(map[string]int),
	}
	var cursor uint64
	for {
		if err := ctx.Err(); err != nil {
			return status, err
		}
		// Page on the primary key, like ResealSupplies and audit_repo: keyset
		// pagination cannot skip or repeat rows when concurrent writers insert,
		// which OFFSET would.
		var rows []dbSupply
		q := p.db.WithContext(ctx).Model(&dbSupply{}).
			Select("id", "namespace", "name", "content", "content_hash").
			Where("id > ?", cursor).
			Order("id ASC").
			Limit(batch)
		if opts.Namespace != "" {
			q = q.Where("namespace = ?", opts.Namespace)
		}
		if err := q.Find(&rows).Error; err != nil {
			return status, fmt.Errorf("inspect supply keys: list rows after %d: %w", cursor, err)
		}
		if len(rows) == 0 {
			return status, nil
		}
		for i := range rows {
			inspectSupplyRow(inspector, &rows[i], status)
			cursor = rows[i].ID
		}
	}
}

// inspectSupplyRow classifies one row and checks that it really reads back
// under the caller's keyring. It never records stored bytes in the report.
func inspectSupplyRow(inspector SupplyKeyInspector, row *dbSupply, status *SupplyKeyStatus) {
	status.Scanned++
	stored := []byte(row.Content)
	kid, isEnvelope := inspector.EnvelopeKeyID(stored)
	switch {
	case !isEnvelope:
		status.Plaintext++
	case kid == "":
		status.Unparsable++
	default:
		status.ByKeyID[kid]++
	}
	if inspector.NeedsReseal(stored) {
		status.NeedsReseal++
	}
	if isEnvelope && kid == "" {
		// Open would fail in the JSON parse; report the classification problem
		// directly so the operator is not handed a parse error as the reason.
		recordSupplyKeyIssue(status, row, kid, "envelope has no readable key id")
		return
	}
	plaintext, err := inspector.Open(stored)
	if err != nil {
		recordSupplyKeyIssue(status, row, kid, fmt.Sprintf("open: %v", err))
		return
	}
	// The same integrity check GetSupply applies on every read: a reseal that
	// rewrote the wrong bytes, or a row damaged after the fact, must show up
	// as a hard failure rather than pass as a healthy current-kid row.
	if row.ContentHash != "" && row.ContentHash != store.ContentHash(plaintext) {
		recordSupplyKeyIssue(status, row, kid, "content hash mismatch, stored content may be corrupted")
	}
}

func recordSupplyKeyIssue(status *SupplyKeyStatus, row *dbSupply, kid, reason string) {
	status.Unreadable++
	if len(status.Issues) >= maxSupplyKeyIssues {
		return
	}
	status.Issues = append(status.Issues, SupplyKeyIssue{
		Namespace: row.Namespace,
		Name:      row.Name,
		KeyID:     kid,
		Reason:    reason,
	})
}
