package xbcinfra

import (
	"context"
	"fmt"

	xbcgorm "github.com/xbcio/xbc/extensions/storage/gorm"
	"gorm.io/gorm"
)

// OpenDatabase opens the durable database described by cfg and verifies it is
// reachable before returning. It mirrors xflow's MySQL store constructor: a
// wrong DSN or an unavailable server fails startup here instead of surfacing on
// the first query. The returned close function releases the pool and is safe to
// call more than once.
func OpenDatabase(ctx context.Context, cfg xbcgorm.Config) (*gorm.DB, func() error, error) {
	db, err := xbcgorm.Open(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("xbcinfra: open database: %w", err)
	}
	closeDB := func() error { return xbcgorm.Close(db) }

	if ctx == nil {
		ctx = context.Background()
	}
	sqlDB, err := db.DB()
	if err != nil {
		_ = closeDB()
		return nil, nil, fmt.Errorf("xbcinfra: obtain %s connection pool: %w", cfg.Driver, err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = closeDB()
		return nil, nil, fmt.Errorf("xbcinfra: ping %s database: %w", cfg.Driver, err)
	}
	return db, closeDB, nil
}
