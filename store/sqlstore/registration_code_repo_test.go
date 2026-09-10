package sqlstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/storecontract"
)

// enrollTestDSN mirrors store/sqlstore/mysqlstore/mysqlstore_registration_test.go's
// registrationTestDSN (same local test MySQL: test/env/docker-compose.yml,
// 127.0.0.1:3306, db "xflow", root password "xflow"). Duplicated rather than
// imported, following this repo's established convention for real-DB test
// DSN helpers (see that file's own doc comment, and
// node/internal/action/database_mysql_test.go's probeMySQLDSN): each copy
// lives in its own package for its own reasons and none of them import from
// another _test.go file.
func enrollTestDSN() string {
	if dsn := os.Getenv("XFLOW_TEST_MYSQL_DSN"); dsn != "" {
		return dsn
	}
	port := envOrDefault("MYSQL_PORT", "3306")
	pw := envOrDefault("MYSQL_ROOT_PASSWORD", "xflow")
	dbname := envOrDefault("MYSQL_DATABASE", "xflow")
	return fmt.Sprintf("root:%s@tcp(127.0.0.1:%s)/%s?parseTime=true&multiStatements=true", pw, port, dbname)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// enrollMySQLEndpoint describes where enrollTestDSN points, WITHOUT the
// credential. Every skip/fail message below prints this instead of the DSN,
// which embeds MYSQL_ROOT_PASSWORD — test output reaches CI artifacts and
// terminal scrollback, and test/integration/harness.go:88-92 already made this
// call for its own message.
//
// A caller-supplied XFLOW_TEST_MYSQL_DSN is named rather than parsed: pulling a
// host out of an arbitrary DSN would put that password one parsing bug away
// from the log.
func enrollMySQLEndpoint() string {
	if os.Getenv("XFLOW_TEST_MYSQL_DSN") != "" {
		return "the DSN in $XFLOW_TEST_MYSQL_DSN"
	}
	return "127.0.0.1:" + envOrDefault("MYSQL_PORT", "3306")
}

// skipOrFailEnrollMySQL skips, or fails under XFLOW_REQUIRE_MYSQL_INTEGRATION=1,
// mirroring test/integration/harness.go's requireMySQL. Without the escalation
// branch these contract runs are invisible to CI's required mode: MySQL goes
// down, they report skip, the suite reports ok, and the real-DB half of the
// registration-code / issued-identity contracts silently does not run.
// Callers must pass an endpoint, never a DSN.
func skipOrFailEnrollMySQL(t *testing.T, format string, args ...any) {
	t.Helper()
	if os.Getenv("XFLOW_REQUIRE_MYSQL_INTEGRATION") == "1" {
		t.Fatalf("XFLOW_REQUIRE_MYSQL_INTEGRATION=1: "+format, args...)
	}
	t.Skipf(format, args...)
}

// newEnrollTestDB opens a real local MySQL connection, runs AutoMigrate (safe
// to call repeatedly: additive only), truncates the three enroll tables so
// each contract subtest's factory(t) call gets a fresh, empty store, and
// skips with an explicit reason if MySQL is unreachable — never silently, and
// never echoing the DSN.
func newEnrollTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := enrollTestDSN()
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)})
	if err != nil {
		skipOrFailEnrollMySQL(t, "mysql unavailable at %s (run `make env-up` / podman test env for real-DB coverage): %v",
			enrollMySQLEndpoint(), err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		skipOrFailEnrollMySQL(t, "mysql unavailable at %s: resolve *sql.DB: %v", enrollMySQLEndpoint(), err)
	}
	if err := sqlDB.Ping(); err != nil {
		skipOrFailEnrollMySQL(t, "mysql unavailable at %s: ping: %v", enrollMySQLEndpoint(), err)
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	for _, table := range []string{"xflow_registration_codes", "xflow_enroll_audit", "xflow_issued_identities"} {
		if err := db.Exec("DELETE FROM " + table).Error; err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
	return db
}

func TestSQLRegistrationCodeStoreSatisfiesContract(t *testing.T) {
	storecontract.RunRegistrationCodeStoreContract(t, func(t *testing.T) store.RegistrationCodeStore {
		db := newEnrollTestDB(t)
		return NewRegistrationCodeStore(db)
	})
}

func TestSQLIssuedIdentityStoreSatisfiesContract(t *testing.T) {
	storecontract.RunIssuedIdentityStoreContract(t, func(t *testing.T) store.IssuedIdentityStore {
		db := newEnrollTestDB(t)
		return NewIssuedIdentityStore(db)
	})
}

// TestResolveByPlaintextRejectsCorruptedScope pins Ruling U: a registration
// code whose allowed_namespaces column has been damaged into unparseable JSON
// must fail closed, not fall back to an empty scope.
//
// Why this matters more than an ordinary "bad input" case: RunnerPolicy.
// AllowsNamespace treats an EMPTY AllowedNamespaces as "default namespace
// only" -- a WIDER grant than the "team-a"-only scope this code actually
// carries. So decodeList silently returning nil on a JSON error (the old
// behavior) does not fail closed at all on the namespace axis: it turns a
// damaged column into a code that grants default-namespace enroll it was
// never scoped for. This test writes corrupt JSON directly via raw SQL --
// bypassing encodeList entirely, the way a botched migration or manual row
// edit would -- and then proves both (a) ResolveByPlaintext refuses to hand
// back a usable code at all, and (b) if it ever regressed to handing one
// back, that code would carry exactly the widened grant described above.
func TestResolveByPlaintextRejectsCorruptedScope(t *testing.T) {
	db := newEnrollTestDB(t)
	st := NewRegistrationCodeStore(db)
	ctx := context.Background()

	id, plaintext, err := store.GenerateRegistrationCode()
	if err != nil {
		t.Fatalf("GenerateRegistrationCode: %v", err)
	}
	if err := st.Create(ctx, store.RegistrationCode{
		ID:                id,
		CodeHash:          store.HashSecret(plaintext),
		AllowedNamespaces: []string{"team-a"},
		AllowedNodeTypes:  []string{"*"},
		CreatedAt:         time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Corrupt the column with raw SQL. "{" is a syntactically incomplete JSON
	// value -- json.Unmarshal rejects it, unlike "" (the legitimate empty
	// state) or "null" (a legitimate empty list).
	if err := db.Exec(
		"UPDATE xflow_registration_codes SET allowed_namespaces = ? WHERE id = ?", "{", id,
	).Error; err != nil {
		t.Fatalf("corrupt allowed_namespaces: %v", err)
	}

	got, err := st.ResolveByPlaintext(ctx, plaintext)
	if err == nil {
		// This branch is exactly the regression Ruling U forbids: a
		// team-a-scoped code silently came back usable, and because its
		// AllowedNamespaces decoded to nil, it now also grants the default
		// namespace -- a namespace the original scope never listed.
		t.Fatalf(
			"ResolveByPlaintext returned no error for a corrupted allowed_namespaces column; "+
				"got = %+v, AllowsNamespace(default) = %v -- corrupted scope must not silently widen access",
			got, got.Policy().AllowsNamespace(namespace.Default),
		)
	}
	if !errors.Is(err, store.ErrEnrollScopeCorrupted) {
		t.Fatalf("err = %v, want errors.Is(err, store.ErrEnrollScopeCorrupted)", err)
	}

	// List() walks the same rowToRegistrationCode conversion and must refuse
	// the same way rather than silently omitting the damaged row.
	if _, err := st.List(ctx, store.OwnerScope{All: true}); !errors.Is(err, store.ErrEnrollScopeCorrupted) {
		t.Fatalf("List err = %v, want errors.Is(err, store.ErrEnrollScopeCorrupted)", err)
	}
}
