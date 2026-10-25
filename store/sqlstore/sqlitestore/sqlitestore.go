// Package sqlitestore provides a SQLite-backed store.Store built on GORM.
// It is a thin dialect entry point over the dialect-agnostic sqlstore core:
// it opens a SQLite database file, configures the connection, and returns a
// *sqlstore.Provider. Callers who already hold a *gorm.DB (or want another
// dialect) should use sqlstore.New directly -- this is the documented,
// supported embedding path (see sdk/xflow/cluster.go's own example), not an
// edge case.
//
// The driver is github.com/ncruces/go-sqlite3, which compiles SQLite to WASM
// and executes it on a pure-Go runtime (via github.com/ncruces/go-sqlite3-wasm).
// This package therefore needs no cgo and no platform SQLite library: it
// builds and runs anywhere the Go toolchain does.
//
// This package's init() below registers this package's SQLite-specific
// transient-error classification (SQLITE_BUSY / SQLITE_LOCKED) with sqlstore's
// dialect-agnostic core via sqlstore.RegisterTransientClassifier, so
// store/sqlstore/errors.go itself never needs to import this package's
// driver. Registration happens purely from this package being imported --
// NOT from New running -- specifically so that a caller who constructs its
// own *gorm.DB and calls sqlstore.New directly (never touching this
// package's New) still gets SQLite lock-conflict classification, as long as
// it imports this package (even as a blank import, `_
// "github.com/xbcio/xflow/store/sqlstore/sqlitestore"`) somewhere in its
// binary. See store/sqlstore/errors.go's RegisterTransientClassifier doc
// comment for why import-time registration is the intended idiom.
package sqlitestore

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/gormlite"
	"github.com/ncruces/go-sqlite3/vfs/memdb"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"

	"github.com/xbcio/xflow/store/sqlstore"
)

// defaultBusyTimeout is how long a SQLite connection waits for a competing
// writer before returning SQLITE_BUSY. New's single-connection pool means
// normal operation never contends, but callers who raise WithMaxOpenConns
// (or share the file with another process) get a bounded wait instead of an
// immediate failure.
const defaultBusyTimeout = 5 * time.Second

// sqliteTimeFormat is sqlite3.TimeFormat4 ("YYYY-MM-DD HH:MM:SS.SSS"), which
// the driver always encodes in UTC and SQLite's date functions understand.
// Sub-millisecond digits are truncated, whereas MySQL's DATETIME(3) rounds by
// default, so a value within half a millisecond of a "created_at < ?" bound can
// land on different sides of it across the two dialects.
const sqliteTimeFormat = string(sqlite3.TimeFormat4)

// isSQLiteBusy is this package's store/sqlstore.RegisterTransientClassifier
// predicate: it recognizes the two SQLite result codes that mean "another
// connection holds a conflicting lock, retry after a pause" -- SQLITE_BUSY
// (5) and SQLITE_LOCKED (6) -- via errors.Is against the driver's typed
// error codes, not a substring match. Registered under the dialect name "sqlite" by init()
// below.
//
// The primary code is checked, so every extended code derived from BUSY or
// LOCKED (BUSY_TIMEOUT, BUSY_SNAPSHOT, LOCKED_SHAREDCACHE, ...) is covered by
// the same test. Other codes are deliberately excluded: SQLITE_IOERR and
// SQLITE_CORRUPT also surface during ordinary file-backed operation, but
// retrying them blindly would turn a persistent fault into an infinite retry
// loop rather than resolve contention.
func isSQLiteBusy(err error) bool {
	// The driver reports a lock conflict as *sqlite3.Error from statement
	// execution but as a bare sqlite3.ExtendedErrorCode from some connection
	// paths (e.g. BEGIN). Both implement Is against the primary ErrorCode, so
	// errors.Is covers every shape without depending on which one surfaced.
	return errors.Is(err, sqlite3.BUSY) || errors.Is(err, sqlite3.LOCKED)
}

// init registers isSQLiteBusy with sqlstore's dialect-agnostic core the
// moment this package is imported -- the same idiom database/sql driver
// packages use (see mysqlstore's equivalent init for the full rationale).
// This is what makes a bare blank import of this package (with no call to New
// at all) enough to wire SQLite lock-conflict classification into any
// *sqlstore.Provider built however the caller likes, including via
// sqlstore.New(db) directly over the caller's own *gorm.DB.
func init() {
	sqlstore.RegisterTransientClassifier("sqlite", isSQLiteBusy)
}

// Option configures the SQLite connection pool, GORM behavior, and optional
// schema setup.
type Option func(*config)

type config struct {
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
	autoMigrate     bool
	gormCfg         *gorm.Config
	// sqlstoreOpts is forwarded to sqlstore.New verbatim. It exists because
	// sqlitestore.Option configures the connection pool, a different concern
	// from sqlstore.Option, which configures the Provider itself (e.g. supply
	// encryption); sqlitestore.WithSupplyEncryption bridges the two.
	sqlstoreOpts []sqlstore.Option
}

func defaultConfig() *config {
	return &config{
		// SQLite allows many concurrent readers but exactly one writer, and a
		// second writer fails with SQLITE_BUSY rather than queueing. A
		// one-connection pool moves that serialization into database/sql, where
		// it is free and deterministic, instead of leaving it as busy-retry
		// latency on every write.
		maxOpenConns: 1,
		maxIdleConns: 1,
		// No lifetime rotation: SQLite has no server-side session to refresh.
		connMaxLifetime: 0,
		// TranslateError normalizes common SQLite constraint failures for
		// callers (gormlite ships a translator); audit outcome idempotency does
		// not depend on it, since it uses an explicit conflict-ignore insert and
		// RowsAffected instead.
		gormCfg: &gorm.Config{TranslateError: true},
	}
}

// WithMaxOpenConns sets the maximum number of open connections. Default 1.
//
// Raising this allows concurrent readers under WAL, but reintroduces
// SQLITE_BUSY between competing writers; the busy_timeout registered on every
// connection bounds that wait, and store.ErrTransient marks a surviving
// conflict as retryable. Values below 1 are ignored.
func WithMaxOpenConns(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxOpenConns = n
		}
	}
}

// WithMaxIdleConns sets the maximum number of idle connections. Default 1.
func WithMaxIdleConns(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxIdleConns = n
		}
	}
}

// WithConnMaxLifetime sets the maximum lifetime of a connection. Default 0
// (no limit). Rotating connections is unnecessary for SQLite, which has no
// server-side session to refresh.
func WithConnMaxLifetime(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.connMaxLifetime = d
		}
	}
}

// WithGormConfig overrides the GORM config (e.g. to set a custom logger).
// Only its settings are used: New opens on a fresh Config built from them, so
// the same Config can safely be shared with other connections. The default
// TranslateError: true is replaced along with everything else.
func WithGormConfig(cfg *gorm.Config) Option {
	return func(c *config) {
		if cfg != nil {
			c.gormCfg = cfg
		}
	}
}

// WithAutoMigrate creates or updates the shared sqlstore schema during New.
//
// SQLite ships no DDL of its own: db/xflow_schema.sql is MySQL DDL, so a
// fresh SQLite file has no tables until something creates them. This option
// runs AutoMigrate -- the single model list every backend shares -- right
// after the connection is established, which is the supported way to get a
// usable SQLite store. It is intended for development, tests, and local-first
// deployments; like AutoMigrate itself it only adds tables and columns and
// never drops them.
//
// Callers who prefer explicit migration can omit this option and call
// sqlitestore.AutoMigrate(provider.DB()) themselves once (or manage the
// schema out of band); both paths run the same code.
func WithAutoMigrate() Option {
	return func(c *config) {
		c.autoMigrate = true
	}
}

// AutoMigrate creates or updates the shared sqlstore schema on db.
//
// It is sqlstore.AutoMigrate plus the SQLite type translation the shared
// models need (see sqliteDataType); call it instead of sqlstore.AutoMigrate
// on a SQLite connection. New calls it for you when WithAutoMigrate is set.
// Use only for development/testing and local-first deployments: like every
// AutoMigrate it only adds tables and columns, and production schema remains
// the DBA-managed db/xflow_schema.sql.
func AutoMigrate(db *gorm.DB) error {
	return sqlstore.AutoMigrateWith(db, prepareSQLiteSchema)
}

// WithSupplyEncryption enables at-rest encryption of the supply content
// column. This is a passthrough: sqlitestore.Option configures the
// connection, which is a different type from sqlstore.Option, so this
// collects the underlying sqlstore.WithSupplyEncryption option and forwards
// it to sqlstore.New at the end of New.
func WithSupplyEncryption(encryption sqlstore.SupplyEncryption) Option {
	return func(c *config) {
		c.sqlstoreOpts = append(c.sqlstoreOpts, sqlstore.WithSupplyEncryption(encryption))
	}
}

// New opens a SQLite database at path and returns a ready-to-use Provider.
//
// path is a filesystem path, or ":memory:" for a private in-memory database
// (one per New call, shared by every connection of its pool, kept for the life
// of the process; see memdbDSN)
// (intended for tests). Every connection opened by New runs with WAL journal
// mode, foreign-key enforcement, and a 5s busy timeout, and the default pool
// holds a single connection (see defaultConfig for why).
//
// The database file is created if it does not exist, but its schema is not:
// pass WithAutoMigrate (or call sqlitestore.AutoMigrate afterwards) to create
// the shared tables on a fresh file.
func New(path string, opts ...Option) (*sqlstore.Provider, error) {
	if path == "" {
		return nil, errors.New("sqlitestore: database path is required")
	}
	c := defaultConfig()
	for _, o := range opts {
		o(c)
	}

	db, err := gorm.Open(gormlite.Open(sqliteDSN(path)), freshGormConfig(c.gormCfg))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
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
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	if c.autoMigrate {
		if err := AutoMigrate(db); err != nil {
			_ = sqlDB.Close()
			return nil, fmt.Errorf("migrate sqlite: %w", err)
		}
	}

	return sqlstore.New(db, c.sqlstoreOpts...), nil
}

// freshGormConfig copies the caller's settings onto a Config that has never
// been opened. gorm.Open caches parsed model schemas on the Config it is given
// and installs per-dialect state there (clause builders, plugins, the
// connection pool), and AutoMigrate rewrites cached schemas' column types for
// SQLite. A plain struct copy would carry an already-opened Config's schema
// cache and maps along, so the rewrite could leak into, say, a MySQL
// connection opened with the same Config; copying only the settings cannot.
func freshGormConfig(in *gorm.Config) *gorm.Config {
	return &gorm.Config{
		SkipDefaultTransaction:                   in.SkipDefaultTransaction,
		DefaultTransactionTimeout:                in.DefaultTransactionTimeout,
		DefaultContextTimeout:                    in.DefaultContextTimeout,
		NamingStrategy:                           in.NamingStrategy,
		FullSaveAssociations:                     in.FullSaveAssociations,
		Logger:                                   in.Logger,
		NowFunc:                                  in.NowFunc,
		DryRun:                                   in.DryRun,
		PrepareStmt:                              in.PrepareStmt,
		PrepareStmtMaxSize:                       in.PrepareStmtMaxSize,
		PrepareStmtTTL:                           in.PrepareStmtTTL,
		DisableAutomaticPing:                     in.DisableAutomaticPing,
		DisableForeignKeyConstraintWhenMigrating: in.DisableForeignKeyConstraintWhenMigrating,
		IgnoreRelationshipsWhenMigrating:         in.IgnoreRelationshipsWhenMigrating,
		DisableNestedTransaction:                 in.DisableNestedTransaction,
		AllowGlobalUpdate:                        in.AllowGlobalUpdate,
		QueryFields:                              in.QueryFields,
		CreateBatchSize:                          in.CreateBatchSize,
		TranslateError:                           in.TranslateError,
		PropagateUnscoped:                        in.PropagateUnscoped,
	}
}

// prepareSQLiteSchema adapts the shared sqlstore models' column types to
// SQLite's grammar. It is passed to sqlstore.AutoMigrateWith, so it only ever
// sees this connection's parsed schemas; the MySQL entry point keeps using
// the tags verbatim.
func prepareSQLiteSchema(s *schema.Schema) error {
	for _, field := range s.Fields {
		field.DataType = sqliteDataType(field.DataType)
	}
	return nil
}

// sqliteDataType translates one model column type into the SQLite type name
// that preserves its behaviour.
//
// MySQL allows a type to carry CHARACTER SET / COLLATE decorations
// (varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin, LONGTEXT
// COLLATE utf8mb4_bin); SQLite's grammar stops at the type name and treats the
// rest as a syntax error. The _bin collations these models ask for mean
// byte-wise comparison, which is exactly SQLite's default BINARY comparison,
// so dropping the decoration keeps the case sensitivity the MySQL schema
// relies on.
//
// JSON columns become TEXT. A literal "json" type name has NUMERIC affinity in
// SQLite, which would silently coerce a bare numeric JSON document ("42") to
// an integer and break scanning back into jsonBytes. TEXT affinity stores the
// document byte-for-byte. The MySQL TEXT family maps to TEXT for the same
// reason it already is TEXT there. Everything else (varchar(N), binary(32),
// text, the schema.String/Int/Time kinds) is left alone; SQLite accepts those
// names as-is.
func sqliteDataType(declared schema.DataType) schema.DataType {
	t := string(declared)
	upper := strings.ToUpper(t)
	if i := strings.Index(upper, " CHARACTER SET "); i >= 0 {
		t, upper = t[:i], upper[:i]
	}
	if i := strings.Index(upper, " COLLATE "); i >= 0 {
		t = t[:i]
	}
	t = strings.TrimSpace(t)
	if t == "" {
		return declared
	}
	switch strings.ToUpper(t) {
	case "JSON", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return schema.String
	default:
		return schema.DataType(t)
	}
}

// sqliteDSN builds the URI-form DSN the driver needs for PRAGMA options.
// Escaping the path keeps names containing URI metacharacters ("?", "#", "%",
// spaces) intact: SQLite percent-decodes the URI path before opening the
// file.
func sqliteDSN(path string) string {
	query := url.Values{}
	// Order matters to SQLite: busy_timeout must precede journal_mode, which
	// may itself need the lock wait when switching to WAL.
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", defaultBusyTimeout.Milliseconds()))
	query.Add("_pragma", "journal_mode(wal)")
	query.Add("_pragma", "foreign_keys(1)")
	// In WAL mode NORMAL syncs at checkpoints rather than on every commit: a
	// power loss can drop the last few commits but never corrupts the file.
	// That suits a local-first metadata mirror, whose writes sit on the
	// engine's scheduling path, far better than FULL's fsync per write.
	query.Add("_pragma", "synchronous(normal)")
	// SQLite has no native time type: the driver stores time.Time as text and
	// the shared queries compare it with "created_at < ?". The driver default
	// (RFC 3339 in the value's own zone, variable fraction width) does not
	// sort lexically across zones or precisions, so write every time as
	// fixed-width UTC with millisecond precision -- the same precision the
	// MySQL schema's DATETIME(3) columns keep.
	query.Set("_timefmt", sqliteTimeFormat)

	if path == ":memory:" {
		// This driver build omits SQLite's shared cache, so a plain ":memory:"
		// database would be private to whichever pooled connection opened it.
		// The memdb VFS shares one in-memory database among every connection
		// that names it, and a random name per New call keeps separate stores
		// from seeing each other's data.
		return memdbDSN(query)
	}
	return "file:" + url.PathEscape(path) + "?" + query.Encode()
}

// memdbDSN creates a fresh memdb database and returns its DSN. The name must
// start with "/" for memdb to share it across connections; the random suffix
// makes each New call its own database.
//
// memdb discards a database when its last open reference closes, which
// database/sql may do at any time (an evicted idle or bad connection).
// Creating it up front holds a reference of its own, so the data lives for the
// rest of the process regardless of pool churn. That reference is never
// released: ":memory:" is meant for tests and short-lived tools, not for
// opening and discarding stores in a long-running process.
func memdbDSN(query url.Values) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	// memdb registers a database under its name without the leading "/", which
	// is what its VFS looks up when a connection opens "/<name>".
	name := "xflow-" + hex.EncodeToString(b[:]) + ".db"
	memdb.Create(name, nil)
	query.Set("vfs", "memdb")
	return "file:/" + name + "?" + query.Encode()
}
