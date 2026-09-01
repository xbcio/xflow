package sqlstore

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
)

// Provider is the GORM-backed implementation of store.Store and
// store.Transactor. It embeds the per-domain repos bound to the root *gorm.DB,
// so it satisfies the full store.Store surface, while Transaction rebinds the
// repos to a shared transaction for cross-domain atomic writes.
type Provider struct {
	db *gorm.DB
	*executionRepo
	*nodeRepo
	*signalRepo
	*auditRepo
	*supplyRepo
	artifactBlobs *artifactBlobRepo
	artifactIndex *artifactIndexRepo
}

// compile-time interface checks
var (
	_ store.Store      = (*Provider)(nil)
	_ store.Transactor = (*Provider)(nil)
)

// SupplyEncryption is the minimal at-rest transform required by the supply
// repository. Key management, envelope formats, and concrete cryptography stay
// outside the storage layer.
type SupplyEncryption interface {
	Seal(plaintext []byte) ([]byte, error)
	Open(stored []byte) ([]byte, error)
}

// options holds construction-time settings for New. It stays unexported: the
// only supported way to configure it is through an Option function, so adding
// a new option never breaks existing call sites.
type options struct {
	supplyAtRest SupplyEncryption
}

// Option configures a Provider at construction time.
type Option func(*options)

// WithSupplyEncryption enables at-rest encryption of the supply content
// column. When omitted or passed a nil implementation (including a typed nil),
// supply content is stored and read back exactly as it was before encryption
// existed.
func WithSupplyEncryption(encryption SupplyEncryption) Option {
	return func(o *options) {
		o.supplyAtRest = normalizeSupplyEncryption(encryption)
	}
}

func normalizeSupplyEncryption(encryption SupplyEncryption) SupplyEncryption {
	if encryption == nil {
		return nil
	}

	value := reflect.ValueOf(encryption)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		if value.IsNil() {
			return nil
		}
	}
	return encryption
}

// New creates a Provider from an already-configured *gorm.DB. The caller owns
// the connection lifecycle and chooses the dialector (MySQL/Postgres/SQLite)
// and pool settings.
func New(db *gorm.DB, opts ...Option) *Provider {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	return &Provider{
		db:            db,
		executionRepo: &executionRepo{db: db},
		nodeRepo:      &nodeRepo{db: db},
		signalRepo:    &signalRepo{db: db},
		auditRepo:     &auditRepo{db: db},
		supplyRepo:    &supplyRepo{db: db, atRest: o.supplyAtRest},
		artifactBlobs: &artifactBlobRepo{db: db},
		artifactIndex: &artifactIndexRepo{db: db},
	}
}

// DB returns the underlying *gorm.DB. It exists for tests that need to
// observe raw column contents (e.g. verifying the supply content column is
// ciphertext) without going through a repo's encode/decode path; production
// code should prefer the per-domain repo methods.
func (p *Provider) DB() *gorm.DB {
	return p.db
}

// CheckReadiness verifies that the underlying SQL connection pool can reach
// the database, honoring cancellation and deadlines from the probe request.
func (p *Provider) CheckReadiness(ctx context.Context) error {
	if p == nil || p.db == nil {
		return errors.New("sqlstore: database is not configured")
	}
	sqlDB, err := p.db.DB()
	if err != nil {
		return fmt.Errorf("sqlstore: resolve database connection: %w", err)
	}
	if sqlDB == nil {
		return errors.New("sqlstore: database connection is not configured")
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("sqlstore: ping database: %w", err)
	}
	return nil
}

// ArtifactObjects returns the objectstore.Store backed by xflow_artifact_blobs.
// It is not included in store.Store or store.Set because the object store
// contract is independent of per-domain transactional boundaries.
func (p *Provider) ArtifactObjects() objectstore.Store {
	return p.artifactBlobs
}

// ArtifactIndex returns the store.ArtifactIndex backed by xflow_artifacts.
func (p *Provider) ArtifactIndex() store.ArtifactIndex {
	return p.artifactIndex
}

// Transaction runs fn within a single DB transaction. Every store in the
// supplied bundle is bound to that transaction.
func (p *Provider) Transaction(ctx context.Context, fn func(s store.Set) error) error {
	return p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(storesFor(tx))
	})
}

// storesFor binds a fresh set of repos to the given *gorm.DB (root or tx).
func storesFor(db *gorm.DB) store.Set {
	return store.Set{
		Execution: &executionRepo{db: db},
		Node:      &nodeRepo{db: db},
		Signal:    &signalRepo{db: db},
		Audit:     &auditRepo{db: db},
	}
}

// AutoMigrate creates or updates tables. Use only for development/testing;
// production schema is managed via db/xflow_schema.sql.
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&dbExecution{},
		&dbNode{},
		&dbSignal{},
		&dbAuditEvent{},
		&dbSupply{},
		&dbArtifactBlob{},
		&dbArtifact{},
	)
}
