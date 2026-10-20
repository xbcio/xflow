package sqlstore

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/xbcio/xflow/service/crypto/supplyenc"
	"github.com/xbcio/xflow/store"
)

// newSupplyResealTestDB opens the package's real test MySQL. It reuses the
// gate in registration_code_repo_test.go rather than adding a second DSN
// convention: there is one local test MySQL (test/env/docker-compose.yml),
// not an enrollment-specific one.
func newSupplyResealTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(mysql.Open(enrollTestDSN()), &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)})
	if err != nil {
		skipOrFailEnrollMySQL(t, "mysql unavailable at %s (run `make env-up` for real-DB coverage): %v",
			enrollMySQLEndpoint(), err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		skipOrFailEnrollMySQL(t, "mysql unavailable at %s: resolve *sql.DB: %v", enrollMySQLEndpoint(), err)
	}
	if err := sqlDB.Ping(); err != nil {
		skipOrFailEnrollMySQL(t, "mysql unavailable at %s: ping: %v", enrollMySQLEndpoint(), err)
	}
	return db
}

// supplyUpdatedAtHasOnUpdate reports whether the connected database carries
// the production DDL for xflow_supplies.updated_at. AutoMigrate does not
// reproduce `ON UPDATE CURRENT_TIMESTAMP(3)` (GORM handles UpdatedAt in Go,
// not in DDL), so a database created by AutoMigrate alone cannot exercise the
// defect this test guards.
func supplyUpdatedAtHasOnUpdate(t *testing.T, db *gorm.DB) bool {
	t.Helper()
	var extra string
	if err := db.Raw(`SELECT COALESCE(EXTRA, '') FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'xflow_supplies' AND COLUMN_NAME = 'updated_at'`).
		Scan(&extra).Error; err != nil {
		t.Fatalf("read xflow_supplies.updated_at definition: %v", err)
	}
	return strings.Contains(strings.ToLower(extra), "on update current_timestamp")
}

// TestSupplyResealPreservesUpdatedAt pins the reseal contract that a key
// rotation must not look like a content change: the ciphertext is rewritten,
// so the row is updated, but updated_at keeps its value.
//
// This strictly needs MySQL. The bump comes from the column's
// ON UPDATE CURRENT_TIMESTAMP(3) DDL clause in db/xflow_schema.sql, not from
// GORM, so neither a DryRun statement assertion nor a sqlite-backed store can
// reproduce it; the test skips with an explicit reason when the production
// DDL is not applied rather than passing vacuously.
func TestSupplyResealPreservesUpdatedAt(t *testing.T) {
	db := newSupplyResealTestDB(t)
	if !supplyUpdatedAtHasOnUpdate(t, db) {
		t.Skip("xflow_supplies.updated_at has no ON UPDATE CURRENT_TIMESTAMP(3); " +
			"apply the production DDL (make env-up && make env-migrate) to exercise this invariant")
	}

	ctx := context.Background()
	namespace := fmt.Sprintf("reseal-updated-at-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if err := db.Exec("DELETE FROM xflow_supplies WHERE namespace = ?", namespace).Error; err != nil {
			t.Errorf("clean up supplies in %s: %v", namespace, err)
		}
	})

	var oldDEK, newDEK [32]byte
	for i := range oldDEK {
		oldDEK[i] = byte(i + 1)
		newDEK[i] = byte(i + 101)
	}

	const name = "pinned"
	before := New(db, WithSupplyEncryption(supplyenc.NewAtRest(oldDEK)))
	if _, err := before.PutSupply(ctx, &store.SupplyResource{
		Namespace: namespace, Name: name, Content: []byte(`{"rules":["v"]}`),
	}, nil); err != nil {
		t.Fatalf("PutSupply: %v", err)
	}

	// Backdate instead of sleeping: a bump is then unmistakable, and the value
	// survives the DATETIME(3) round trip exactly at millisecond precision.
	pinned := time.Date(2020, 1, 2, 3, 4, 5, 678_000_000, time.UTC)
	if err := db.Exec(
		"UPDATE xflow_supplies SET updated_at = ? WHERE namespace = ? AND name = ?", pinned, namespace, name,
	).Error; err != nil {
		t.Fatalf("backdate updated_at: %v", err)
	}
	var contentBefore string
	if err := db.Raw(
		"SELECT content FROM xflow_supplies WHERE namespace = ? AND name = ?", namespace, name,
	).Scan(&contentBefore).Error; err != nil {
		t.Fatalf("read content before reseal: %v", err)
	}

	rotating, err := supplyenc.NewAtRestWithPrevious(newDEK, oldDEK)
	if err != nil {
		t.Fatalf("NewAtRestWithPrevious: %v", err)
	}
	resealing := New(db, WithSupplyEncryption(rotating))
	rep, err := resealing.ResealSupplies(ctx, ResealOptions{Namespace: namespace})
	if err != nil {
		t.Fatalf("ResealSupplies: %v", err)
	}
	if rep.Resealed != 1 || rep.Failed != 0 {
		t.Fatalf("report = %+v, want exactly 1 resealed and 0 failed", rep)
	}

	// Prove the row was actually rewritten, so the updated_at assertion below
	// cannot pass just because nothing touched the row.
	var contentAfter string
	if err := db.Raw(
		"SELECT content FROM xflow_supplies WHERE namespace = ? AND name = ?", namespace, name,
	).Scan(&contentAfter).Error; err != nil {
		t.Fatalf("read content after reseal: %v", err)
	}
	if contentAfter == contentBefore {
		t.Fatal("reseal left the content column unchanged; the updated_at assertion would be vacuous")
	}

	var got time.Time
	if err := db.Raw(
		"SELECT updated_at FROM xflow_supplies WHERE namespace = ? AND name = ?", namespace, name,
	).Scan(&got).Error; err != nil {
		t.Fatalf("read updated_at after reseal: %v", err)
	}
	if !got.Equal(pinned) {
		t.Fatalf("updated_at = %s after reseal, want %s unchanged; the reseal UPDATE must assign updated_at to itself to suppress MySQL's ON UPDATE clause",
			got.UTC().Format(time.RFC3339Nano), pinned.Format(time.RFC3339Nano))
	}
}
