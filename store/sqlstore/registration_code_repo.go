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

type registrationCodeRepo struct {
	db *gorm.DB
	// now is injected so code expiry can be tested at the exact boundary
	// without sleeping, and so this store and MemoryRegistrationCodeStore
	// agree on where "now" comes from. Deliberately NOT the database clock:
	// pushing the predicate into SQL (WHERE expires_at > NOW()) would make the
	// two implementations disagree the moment the app and DB hosts drift, and
	// the storecontract test could no longer hold them to one answer.
	now func() time.Time
}

var _ store.RegistrationCodeStore = (*registrationCodeRepo)(nil)

// NewRegistrationCodeStore returns the SQL-backed registration code store.
func NewRegistrationCodeStore(db *gorm.DB) store.RegistrationCodeStore {
	return &registrationCodeRepo{db: db}
}

func (r *registrationCodeRepo) clock() time.Time {
	if r == nil || r.now == nil {
		return time.Now().UTC()
	}
	return r.now().UTC()
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
	var expires *time.Time
	if !code.ExpiresAt.IsZero() {
		t := code.ExpiresAt.UTC()
		expires = &t
	}
	return r.db.WithContext(ctx).Create(&dbRegistrationCode{
		ID:                code.ID,
		CodeHash:          code.CodeHash[:],
		AllowedNamespaces: encodeList(code.AllowedNamespaces),
		AllowedNodeTypes:  encodeList(code.AllowedNodeTypes),
		OwnerNamespace:    code.OwnerNamespace,
		Revoked:           code.Revoked,
		CreatedAt:         code.CreatedAt,
		ExpiresAt:         expires,
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
	// Expiry is evaluated in Go against the injected clock, not pushed into the
	// WHERE clause above — see the `now` field's doc on why the database clock
	// is the wrong authority here.
	if code.IsExpired(r.clock()) {
		return store.RegistrationCode{}, store.ErrRegistrationCodeExpired
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
		OwnerNamespace:    row.OwnerNamespace,
		Revoked:           row.Revoked,
		CreatedAt:         row.CreatedAt,
	}
	copy(code.CodeHash[:], row.CodeHash)
	if row.ExpiresAt != nil {
		code.ExpiresAt = row.ExpiresAt.UTC()
	}
	return code, nil
}

func (r *registrationCodeRepo) List(ctx context.Context, scope store.OwnerScope) ([]store.RegistrationCode, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	q := r.db.WithContext(ctx).Model(&dbRegistrationCode{})
	if !scope.All {
		q = q.Where("owner_namespace = ?", scope.Namespace)
	}
	var rows []dbRegistrationCode
	if err := q.Order("created_at ASC, id ASC").Find(&rows).Error; err != nil {
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

func (r *registrationCodeRepo) Revoke(ctx context.Context, id string, scope store.OwnerScope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	q := r.db.WithContext(ctx).Model(&dbRegistrationCode{}).Where("id = ?", id)
	if !scope.All {
		q = q.Where("owner_namespace = ?", scope.Namespace)
	}
	res := q.Update("revoked", true)
	if res.Error != nil {
		return res.Error
	}
	// Zero rows covers three cases that must be indistinguishable from
	// outside: no such id, an id owned by another namespace, and an id already
	// revoked. Revoking twice being reported as not-found is the accepted cost
	// of not building an existence oracle.
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

func (r *registrationCodeRepo) EnrollAudit(ctx context.Context, codeID string, scope store.OwnerScope) ([]store.EnrollAuditRecord, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	own := r.db.WithContext(ctx).Model(&dbRegistrationCode{}).Where("id = ?", codeID)
	if !scope.All {
		own = own.Where("owner_namespace = ?", scope.Namespace)
	}
	var n int64
	if err := own.Count(&n).Error; err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, store.ErrRegistrationCodeNotFound
	}
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
	var expires *time.Time
	if !id.ExpiresAt.IsZero() {
		t := id.ExpiresAt.UTC()
		expires = &t
	}
	var revoked *time.Time
	if !id.RevokedAt.IsZero() {
		t := id.RevokedAt.UTC()
		revoked = &t
	}
	return r.db.WithContext(ctx).Save(&dbIssuedIdentity{
		RunnerID:        id.RunnerID,
		TokenHash:       id.TokenHash[:],
		IDPrefix:        id.Scope.IDPrefix,
		ScopeNamespaces: encodeList(id.Scope.AllowedNamespaces),
		ScopeNodeTypes:  encodeList(id.Scope.AllowedNodeTypes),
		CodeID:          id.CodeID,
		OwnerNamespace:  id.OwnerNamespace,
		IssuedAt:        issuedAt,
		ExpiresAt:       expires,
		RevokedAt:       revoked,
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
		CodeID:         row.CodeID,
		OwnerNamespace: row.OwnerNamespace,
		IssuedAt:       issuedAt,
	}
	if row.ExpiresAt != nil {
		id.ExpiresAt = row.ExpiresAt.UTC()
	}
	if row.RevokedAt != nil {
		id.RevokedAt = row.RevokedAt.UTC()
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

// Revoke stamps revoked_at on the identity owned by scope. Revoking an
// already-revoked identity is a no-op that returns nil: revocation is a state,
// not an event, and an operator retrying after a timeout must not see a
// spurious failure.
//
// An identity outside scope reports ErrIssuedIdentityNotFound, the same verdict
// as one that does not exist, so this endpoint is not an existence oracle for
// other tenants' runner ids.
func (r *issuedIdentityRepo) Revoke(ctx context.Context, runnerID string, scope store.OwnerScope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	now := time.Now().UTC()
	q := r.db.WithContext(ctx).Model(&dbIssuedIdentity{}).
		Where("runner_id = ? AND revoked_at IS NULL", runnerID)
	if !scope.All {
		q = q.Where("owner_namespace = ?", scope.Namespace)
	}
	res := q.Update("revoked_at", now)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		// Zero rows is either "no such runner" or "already revoked". Distinguish
		// them, because already-revoked must be a nil no-op while missing must
		// be an error.
		//
		// The COUNT carries the same scope predicate as the UPDATE above, and
		// must keep carrying it: an unscoped COUNT would find another tenant's
		// row, report n > 0, and return nil — telling the caller "that runner
		// exists and is already revoked" about a runner it may not even see.
		var n int64
		cq := r.db.WithContext(ctx).Model(&dbIssuedIdentity{}).
			Where("runner_id = ?", runnerID)
		if !scope.All {
			cq = cq.Where("owner_namespace = ?", scope.Namespace)
		}
		if err := cq.Count(&n).Error; err != nil {
			return err
		}
		if n == 0 {
			return store.ErrIssuedIdentityNotFound
		}
	}
	return nil
}

// Renew extends expires_at. The WHERE clause is the whole enforcement: an
// identity that is revoked, or whose expires_at has already passed, matches
// zero rows and cannot renew itself back to life.
func (r *issuedIdentityRepo) Renew(ctx context.Context, runnerID string, expiresAt time.Time) error {
	now := time.Now().UTC()
	var next *time.Time
	if !expiresAt.IsZero() {
		t := expiresAt.UTC()
		next = &t
	}
	res := r.db.WithContext(ctx).Model(&dbIssuedIdentity{}).
		Where("runner_id = ? AND revoked_at IS NULL", runnerID).
		Where("expires_at IS NULL OR expires_at > ?", now).
		Update("expires_at", next)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return store.ErrIssuedIdentityNotFound
	}
	return nil
}
