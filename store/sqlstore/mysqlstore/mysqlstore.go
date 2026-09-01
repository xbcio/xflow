// Package mysqlstore provides a MySQL-backed store.Store built on GORM.
// It is a thin dialect entry point over the dialect-agnostic sqlstore core:
// it opens a MySQL connection, configures the pool, and returns a
// *sqlstore.Provider. Callers who already hold a *gorm.DB (or want another
// dialect) should use sqlstore.New directly -- this is the documented,
// supported embedding path (see sdk/xflow/cluster.go's own example), not an
// edge case.
//
// This package's init() below registers this package's MySQL-specific
// transient-error classification (a deadlock, error 1213) with sqlstore's
// dialect-agnostic core via sqlstore.RegisterTransientClassifier, so
// store/sqlstore/errors.go itself never needs to import this package's
// driver. Registration happens purely from this package being imported --
// NOT from New running -- specifically so that a caller who constructs its
// own *gorm.DB and calls sqlstore.New directly (never touching this
// package's New) still gets MySQL-deadlock classification, as long as it
// imports this package (even as a blank import, `_
// "github.com/xbcio/xflow/store/sqlstore/mysqlstore"`) somewhere in its
// binary. This used to be gated on New running instead; that gated an entire
// class of legitimate embedders out of classification entirely -- see
// store/sqlstore/errors.go's RegisterTransientClassifier doc comment for the
// full retraction.
package mysqlstore

import (
	"errors"
	"fmt"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/xbcio/xflow/store/sqlstore"
)

// mysqlErrDeadlock is the MySQL server error number for "Deadlock found when
// trying to get lock; try restarting transaction" (ER_LOCK_DEADLOCK). It is
// the only error number isMySQLDeadlock classifies as safe to retry. Lock
// wait timeout (1205) is deliberately not included: it can reflect a
// long-running transaction rather than a genuine deadlock, and retrying it
// blindly can make contention worse rather than resolve it.
const mysqlErrDeadlock = 1213

// isMySQLDeadlock is this package's store/sqlstore.RegisterTransientClassifier
// predicate: it recognizes a MySQL deadlock (error 1213) via errors.As
// against *mysqldriver.MySQLError, not a substring match. Registered under
// the dialect name "mysql" by init() below.
//
// Registering this unconditionally at import time (rather than gating it on
// a live connection) is safe for any deployment that never opens a MySQL
// connection: isMySQLDeadlock can only ever match a *mysqldriver.MySQLError
// with this specific error number, a value that can only originate from a
// real MySQL driver error in the first place. A deployment that never talks
// to MySQL will never produce one, so this predicate is completely inert for
// it regardless of whether it is registered.
func isMySQLDeadlock(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDeadlock
}

// init registers isMySQLDeadlock with sqlstore's dialect-agnostic core the
// moment this package is imported -- the same idiom database/sql driver
// packages use (e.g. `import _ "github.com/go-sql-driver/mysql"` registering
// itself with database/sql.Register from its own init()). This is what makes
// a bare blank import of this package (with no call to New at all) enough to
// wire MySQL-deadlock classification into any *sqlstore.Provider built
// however the caller likes, including via sqlstore.New(db) directly over the
// caller's own *gorm.DB.
func init() {
	sqlstore.RegisterTransientClassifier("mysql", isMySQLDeadlock)
}

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
		// TranslateError normalizes common MySQL failures for callers. Audit
		// outcome idempotency does not depend on this setting: it uses an
		// explicit conflict-ignore insert and RowsAffected instead.
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
func WithSupplyEncryption(encryption sqlstore.SupplyEncryption) Option {
	return func(c *config) {
		c.sqlstoreOpts = append(c.sqlstoreOpts, sqlstore.WithSupplyEncryption(encryption))
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
