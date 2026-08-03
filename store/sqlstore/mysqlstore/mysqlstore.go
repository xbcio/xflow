// Package mysqlstore provides a MySQL-backed store.Store built on GORM.
// It is a thin dialect entry point over the dialect-agnostic sqlstore core:
// it opens a MySQL connection, configures the pool, and returns a
// *sqlstore.Provider. Callers who already hold a *gorm.DB (or want another
// dialect) should use sqlstore.New directly.
package mysqlstore

import (
	"fmt"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/store/sqlstore"
)

// Option configures the MySQL connection pool and GORM behavior.
type Option func(*config)

type config struct {
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
	gormCfg         *gorm.Config
	// sqlstoreOpts is forwarded to sqlstore.New verbatim. It exists because
	// mysqlstore.Option configures the connection pool, a different concern
	// from sqlstore.Option, which configures the Provider itself (e.g. supply
	// encryption); mysqlstore.WithSupplyEncryption bridges the two.
	sqlstoreOpts []sqlstore.Option
}

func defaultConfig() *config {
	return &config{
		maxOpenConns:    25,
		maxIdleConns:    5,
		connMaxLifetime: 5 * time.Minute,
		// TranslateError maps MySQL duplicate-key (1062) and not-found errors
		// to gorm.ErrDuplicatedKey / gorm.ErrRecordNotFound so the
		// dialect-agnostic sqlstore core can branch on sentinels. Required by
		// the T9 audit reconcile worker's idempotent AppendOutcomeIfAbsent,
		// which treats a duplicate outcome insert (concurrent worker / leader
		// switch racing two sweeps) as a benign idempotent skip.
		gormCfg: &gorm.Config{TranslateError: true},
	}
}

// WithMaxOpenConns sets the maximum number of open connections. Default 25.
func WithMaxOpenConns(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxOpenConns = n
		}
	}
}

// WithMaxIdleConns sets the maximum number of idle connections. Default 5.
func WithMaxIdleConns(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxIdleConns = n
		}
	}
}

// WithConnMaxLifetime sets the maximum lifetime of a connection. Default 5m.
func WithConnMaxLifetime(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.connMaxLifetime = d
		}
	}
}

// WithGormConfig overrides the GORM config (e.g. to set a custom logger).
func WithGormConfig(cfg *gorm.Config) Option {
	return func(c *config) {
		if cfg != nil {
			c.gormCfg = cfg
		}
	}
}

// WithSupplyEncryption enables at-rest encryption of the supply content
// column. This is a passthrough: mysqlstore.Option configures the connection
// pool, which is a different type from sqlstore.Option, so this collects the
// underlying sqlstore.WithSupplyEncryption option and forwards it to
// sqlstore.New at the end of New.
func WithSupplyEncryption(a *supplyenc.AtRest) Option {
	return func(c *config) {
		c.sqlstoreOpts = append(c.sqlstoreOpts, sqlstore.WithSupplyEncryption(a))
	}
}

// New opens a MySQL connection pool and returns a ready-to-use Provider.
// The dsn must include parseTime=true for correct time.Time scanning.
func New(dsn string, opts ...Option) (*sqlstore.Provider, error) {
	c := defaultConfig()
	for _, o := range opts {
		o(c)
	}

	db, err := gorm.Open(gormmysql.Open(dsn), c.gormCfg)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql.DB: %w", err)
	}
	sqlDB.SetMaxOpenConns(c.maxOpenConns)
	sqlDB.SetMaxIdleConns(c.maxIdleConns)
	sqlDB.SetConnMaxLifetime(c.connMaxLifetime)
	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}

	return sqlstore.New(db, c.sqlstoreOpts...), nil
}
