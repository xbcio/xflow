package sqlstore

import (
	"context"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
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
	}
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
	)
}
