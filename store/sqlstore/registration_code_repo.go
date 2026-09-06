package sqlstore

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/store"
	"gorm.io/gorm"
)

type registrationCodeRepo struct{ db *gorm.DB }

var _ store.RegistrationCodeStore = (*registrationCodeRepo)(nil)

// NewRegistrationCodeStore returns the SQL-backed registration code store.
func NewRegistrationCodeStore(db *gorm.DB) store.RegistrationCodeStore {
	return &registrationCodeRepo{db: db}
}

func encodeList(list []string) string {
	if len(list) == 0 {
		return "[]"
	}
	b, err := json.Marshal(list)
	if err != nil {
		// Unreachable: json.Marshal on a []string cannot fail (no cycles, no
		// unsupported types, no channels/funcs to reject). Kept as a defensive
		// fallback rather than a panic so a future refactor that widens list's
		// element type does not turn an edge case into a crash; decodeList's
		// error path below is the one that actually matters (Ruling U).
		return "[]"
	}
	return string(b)
}

// decodeList reverses encodeList. s == "" is the legitimate empty state —
// either encodeList's own "[]" was never written (a row created before this
// column existed) or the column is genuinely empty — and returns (nil, nil).
//
// Any other unparseable content is corruption, not emptiness, and must not be
// swallowed into nil: RunnerPolicy.AllowsNamespace treats an empty
// AllowedNamespaces as "default namespace only", which is a WIDER grant than
// most non-empty scopes ever set. Silently returning nil on a JSON error
// would turn a damaged allowed_namespaces column into a privilege escalation
// on the very next enroll (Ruling U). Callers must propagate the error
// instead of falling back to a zero-value list.
func decodeList(s string) ([]string, error) {
	if s == "" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, fmt.Errorf("%w: %v", store.ErrEnrollScopeCorrupted, err)
	}
	return out, nil
}

func (r *registrationCodeRepo) Create(ctx context.Context, code store.RegistrationCode) error {
	return r.db.WithContext(ctx).Create(&dbRegistrationCode{
		ID:                code.ID,
		CodeHash:          code.CodeHash[:],
		AllowedNamespaces: encodeList(code.AllowedNamespaces),
		AllowedNodeTypes:  encodeList(code.AllowedNodeTypes),
		Revoked:           code.Revoked,
		CreatedAt:         code.CreatedAt,
	}).Error
}

func (r *registrationCodeRepo) ResolveByPlaintext(ctx context.Context, plaintext string) (store.RegistrationCode, error) {
	want := store.HashSecret(plaintext)
	// Look the row up by hash equality in SQL — code_hash is uniquely indexed,
	// so this is one indexed read rather than a full-table scan that would grow
	// with the code count. The subtle.ConstantTimeCompare below is the defense
	// that matters: the index lookup's timing depends on the hash, not on the
	// plaintext, and sha256 already decouples the two.
	var row dbRegistrationCode
	err := r.db.WithContext(ctx).Where("code_hash = ?", want[:]).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return store.RegistrationCode{}, store.ErrRegistrationCodeUnknown
	}
	if err != nil {
		return store.RegistrationCode{}, err
	}
	code, err := rowToRegistrationCode(row)
	if err != nil {
		return store.RegistrationCode{}, err
	}
	if subtle.ConstantTimeCompare(want[:], code.CodeHash[:]) != 1 {
		// Defense in depth against a column type that silently truncates or
		// pads: the WHERE clause matched but the bytes did not.
		return store.RegistrationCode{}, store.ErrRegistrationCodeUnknown
	}
	if code.Revoked {
		return store.RegistrationCode{}, store.ErrRegistrationCodeRevoked
	}
	return code, nil
}

func rowToRegistrationCode(row dbRegistrationCode) (store.RegistrationCode, error) {
	namespaces, err := decodeList(row.AllowedNamespaces)
	if err != nil {
		return store.RegistrationCode{}, fmt.Errorf("registration code %s: allowed_namespaces: %w", row.ID, err)
	}
	nodeTypes, err := decodeList(row.AllowedNodeTypes)
	if err != nil {
		return store.RegistrationCode{}, fmt.Errorf("registration code %s: allowed_node_types: %w", row.ID, err)
	}
	code := store.RegistrationCode{
		ID:                row.ID,
		AllowedNamespaces: namespaces,
		AllowedNodeTypes:  nodeTypes,
		Revoked:           row.Revoked,
		CreatedAt:         row.CreatedAt,
	}
	copy(code.CodeHash[:], row.CodeHash)
	return code, nil
}

func (r *registrationCodeRepo) List(ctx context.Context) ([]store.RegistrationCode, error) {
	var rows []dbRegistrationCode
	if err := r.db.WithContext(ctx).Order("created_at ASC, id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]store.RegistrationCode, 0, len(rows))
	for _, row := range rows {
		code, err := rowToRegistrationCode(row)
		if err != nil {
			return nil, err
		}
		out = append(out, code)
	}
	return out, nil
}

func (r *registrationCodeRepo) Revoke(ctx context.Context, id string) error {
	res := r.db.WithContext(ctx).Model(&dbRegistrationCode{}).
		Where("id = ?", id).Update("revoked", true)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return store.ErrRegistrationCodeNotFound
	}
	return nil
}

func (r *registrationCodeRepo) AppendEnrollAudit(ctx context.Context, rec store.EnrollAuditRecord) error {
	return r.db.WithContext(ctx).Create(&dbEnrollAudit{
		CodeID:   rec.CodeID,
		Success:  rec.Success,
		Reason:   rec.Reason,
		RunnerID: rec.RunnerID,
		SourceIP: rec.SourceIP,
		At:       rec.At,
	}).Error
}

func (r *registrationCodeRepo) EnrollAudit(ctx context.Context, codeID string) ([]store.EnrollAuditRecord, error) {
	var rows []dbEnrollAudit
	if err := r.db.WithContext(ctx).Where("code_id = ?", codeID).Order("id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]store.EnrollAuditRecord, 0, len(rows))
	for _, row := range rows {
		out = append(out, store.EnrollAuditRecord{
			CodeID:   row.CodeID,
			Success:  row.Success,
			Reason:   row.Reason,
			RunnerID: row.RunnerID,
			SourceIP: row.SourceIP,
			At:       row.At,
		})
	}
	return out, nil
}

type issuedIdentityRepo struct{ db *gorm.DB }

var _ store.IssuedIdentityStore = (*issuedIdentityRepo)(nil)

// NewIssuedIdentityStore returns the SQL-backed issued identity store.
func NewIssuedIdentityStore(db *gorm.DB) store.IssuedIdentityStore {
	return &issuedIdentityRepo{db: db}
}

func (r *issuedIdentityRepo) Issue(ctx context.Context, id store.IssuedIdentity) error {
	var issuedAt *time.Time
	if !id.IssuedAt.IsZero() {
		issuedAt = &id.IssuedAt
	}
	return r.db.WithContext(ctx).Save(&dbIssuedIdentity{
		RunnerID:        id.RunnerID,
		TokenHash:       id.TokenHash[:],
		IDPrefix:        id.Scope.IDPrefix,
		ScopeNamespaces: encodeList(id.Scope.AllowedNamespaces),
		ScopeNodeTypes:  encodeList(id.Scope.AllowedNodeTypes),
		CodeID:          id.CodeID,
		IssuedAt:        issuedAt,
	}).Error
}

func (r *issuedIdentityRepo) Lookup(ctx context.Context, runnerID string) (store.IssuedIdentity, bool, error) {
	var row dbIssuedIdentity
	err := r.db.WithContext(ctx).Where("runner_id = ?", runnerID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return store.IssuedIdentity{}, false, nil
	}
	if err != nil {
		return store.IssuedIdentity{}, false, err
	}
	id, err := rowToIssuedIdentity(row)
	if err != nil {
		return store.IssuedIdentity{}, false, err
	}
	return id, true, nil
}

func rowToIssuedIdentity(row dbIssuedIdentity) (store.IssuedIdentity, error) {
	var issuedAt time.Time
	if row.IssuedAt != nil {
		issuedAt = *row.IssuedAt
	}
	namespaces, err := decodeList(row.ScopeNamespaces)
	if err != nil {
		return store.IssuedIdentity{}, fmt.Errorf("issued identity %s: scope_namespaces: %w", row.RunnerID, err)
	}
	nodeTypes, err := decodeList(row.ScopeNodeTypes)
	if err != nil {
		return store.IssuedIdentity{}, fmt.Errorf("issued identity %s: scope_node_types: %w", row.RunnerID, err)
	}
	id := store.IssuedIdentity{
		RunnerID: row.RunnerID,
		Scope: store.RunnerPolicy{
			Name:              row.RunnerID,
			IDPrefix:          row.IDPrefix,
			AllowedNamespaces: namespaces,
			AllowedNodeTypes:  nodeTypes,
		},
		CodeID:   row.CodeID,
		IssuedAt: issuedAt,
	}
	copy(id.TokenHash[:], row.TokenHash)
	return id, nil
}

func (r *issuedIdentityRepo) List(ctx context.Context) ([]store.IssuedIdentity, error) {
	var rows []dbIssuedIdentity
	if err := r.db.WithContext(ctx).Order("issued_at ASC, runner_id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]store.IssuedIdentity, 0, len(rows))
	for _, row := range rows {
		id, err := rowToIssuedIdentity(row)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}
