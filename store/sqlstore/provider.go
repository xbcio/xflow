package sqlstore

import (
	"context"

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

// New creates a Provider from an already-configured *gorm.DB. The caller owns
// the connection lifecycle and chooses the dialector (MySQL/Postgres/SQLite)
// and pool settings.
func New(db *gorm.DB) *Provider {
	return &Provider{
		db:            db,
		executionRepo: &executionRepo{db: db},
		nodeRepo:      &nodeRepo{db: db},
		signalRepo:    &signalRepo{db: db},
		auditRepo:     &auditRepo{db: db},
		supplyRepo:    &supplyRepo{db: db},
		artifactBlobs: &artifactBlobRepo{db: db},
		artifactIndex: &artifactIndexRepo{db: db},
	}
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
