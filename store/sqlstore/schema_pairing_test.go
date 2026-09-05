package sqlstore

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// This test is the pairing guard task-7-addendum.md correction 4 requires:
// autoMigrateModels (provider.go) and db/xflow_schema.sql are two
// independently maintained lists that must name the exact same set of
// tables, in BOTH directions.
//
// Why both directions matter, and why "AutoMigrate ran clean" cannot stand
// in for this test: CI seeds a fresh database from db/xflow_schema.sql and
// then calls sqlstore.AutoMigrate(db) on top of it. GORM's AutoMigrate only
// ever adds tables/columns, never removes them, so a model added to
// autoMigrateModels without a matching CREATE TABLE in the schema file gets
// silently patched over in CI — the suite stays green. Production never runs
// AutoMigrate (see provider.go's AutoMigrate doc comment); it only ever sees
// db/xflow_schema.sql. So the exact same gap that CI cannot see is a table
// that flatly does not exist in production. The reverse direction (a
// CREATE TABLE with no backing model) is the mirror failure: AutoMigrate
// silently leaves that table unmanaged forever.
//
// As of this task both lists name exactly the same 10 tables — no
// legitimate one-directional exception was found (see the schema file's own
// CREATE TABLE count vs. len(autoMigrateModels)), so this test asserts exact
// set equality with no allowlist.

// schemaTableNames parses db/xflow_schema.sql for every
// `CREATE TABLE IF NOT EXISTS <name>` statement and returns the table names
// it declares.
func schemaTableNames(t *testing.T) map[string]bool {
	t.Helper()
	path := filepath.Join("..", "..", "db", "xflow_schema.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s (expected relative to store/sqlstore): %v", path, err)
	}
	re := regexp.MustCompile(`(?m)^CREATE TABLE IF NOT EXISTS (\w+)`)
	matches := re.FindAllStringSubmatch(string(data), -1)
	if len(matches) == 0 {
		t.Fatalf("no `CREATE TABLE IF NOT EXISTS` statements found in %s; the regex or the file's style likely changed", path)
	}
	out := make(map[string]bool, len(matches))
	for _, m := range matches {
		out[m[1]] = true
	}
	return out
}

// autoMigrateTableNames calls TableName() on every model in
// autoMigrateModels (provider.go), the same list AutoMigrate itself uses.
func autoMigrateTableNames(t *testing.T) map[string]bool {
	t.Helper()
	out := make(map[string]bool, len(autoMigrateModels))
	for _, m := range autoMigrateModels {
		named, ok := m.(interface{ TableName() string })
		if !ok {
			t.Fatalf("model %T in autoMigrateModels does not implement TableName()", m)
		}
		out[named.TableName()] = true
	}
	return out
}

func TestAutoMigrateModelsMatchSchemaFileExactly(t *testing.T) {
	schema := schemaTableNames(t)
	models := autoMigrateTableNames(t)

	for name := range models {
		if !schema[name] {
			t.Errorf("autoMigrateModels declares table %q but db/xflow_schema.sql has no `CREATE TABLE IF NOT EXISTS %s`; production (which only ever applies db/xflow_schema.sql, never AutoMigrate) would be missing this table entirely", name, name)
		}
	}
	for name := range schema {
		if !models[name] {
			t.Errorf("db/xflow_schema.sql creates table %q but no model in autoMigrateModels declares it via TableName(); AutoMigrate would silently leave it unmanaged forever", name)
		}
	}
}
