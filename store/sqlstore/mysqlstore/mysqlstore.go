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

	"github.com/xbcio/xflow/store/sqlstore"
)

// Option configures the MySQL connection pool and GORM behavior.
type Option func(*config)

type config struct {
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
	gormCfg         *gorm.Config
}

func defaultConfig() *config {
	return &config{
		maxOpenConns:    25,
		maxIdleConns:    5,
		connMaxLifetime: 5 * time.Minute,
		gormCfg:         &gorm.Config{},
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

	return sqlstore.New(db), nil
}
