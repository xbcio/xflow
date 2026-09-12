package sqlstore

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

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

// parameterizedSQLLogger preserves the caller's GORM logger while ensuring
// bound values never get interpolated into SQL traces. sqlstore binds base64
// artifact content and supply ciphertext, neither of which belongs in logs.
//
// LogMode must retain this wrapper because GORM's Debug method replaces its
// logger with the result of LogMode.
type parameterizedSQLLogger struct {
	gormlogger.Interface
}

func (l parameterizedSQLLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	return parameterizedSQLLogger{Interface: l.Interface.LogMode(level)}
}

func (parameterizedSQLLogger) ParamsFilter(_ context.Context, sql string, _ ...interface{}) (string, []interface{}) {
	return sql, nil
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
	// Protect all Provider construction paths, including callers that use
	// sqlstore.New directly with a GORM logger configured to interpolate SQL
	// parameters. Session preserves the caller's connection and logger sink.
	if db != nil {
		db = db.Session(&gorm.Session{
			Logger: parameterizedSQLLogger{Interface: db.Logger},
		})
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

// autoMigrateModels is the model list AutoMigrate applies. It is a
// package-level var — not inlined into AutoMigrate — so
// schema_pairing_test.go can reflect over it and assert, in both directions,
// that it stays in lockstep with db/xflow_schema.sql's CREATE TABLE
// statements. Without that pairing test, a model added here but not there
// would go unnoticed: CI seeds a fresh database from db/xflow_schema.sql and
// then calls AutoMigrate on top of it. AutoMigrate can add missing
// columns/tables but never removes them, so a missing CREATE TABLE is silently
// patched over in CI while production (which never runs AutoMigrate; see the
// doc comment below) would be missing the table entirely.
var autoMigrateModels = []any{
	&dbExecution{},
	&dbNode{},
	&dbSignal{},
	&dbAuditEvent{},
	&dbSupply{},
	&dbArtifactBlob{},
	&dbArtifact{},
	&dbRegistrationCode{},
	&dbEnrollAudit{},
	&dbIssuedIdentity{},
}

// AutoMigrate creates or updates tables. Use only for development/testing;
// production schema is managed via db/xflow_schema.sql.
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(autoMigrateModels...)
}
