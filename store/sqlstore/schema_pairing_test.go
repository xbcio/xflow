package sqlstore

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"gorm.io/gorm/schema"
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

// schemaTableColumns parses db/xflow_schema.sql into table -> set of column
// names. It is deliberately a separate parse from schemaTableNames rather than
// an extension of it: that function's contract is "the set of tables", and a
// column-level bug must not be able to change a table-level verdict.
//
// The parser is intentionally dumb. It walks each CREATE TABLE body line by
// line and takes the first backtick-quoted or bare identifier of any line that
// does not open with a key/constraint keyword. A DDL construct it does not
// understand shows up as a spurious column and fails the test loudly, which is
// the correct direction to fail in: a guard that silently skips what it cannot
// parse guards nothing.
func schemaTableColumns(t *testing.T) map[string]map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "db", "xflow_schema.sql"))
	if err != nil {
		t.Fatalf("read schema file: %v", err)
	}
	out := map[string]map[string]bool{}
	var cur string
	skip := regexp.MustCompile(`^(PRIMARY\s+KEY|UNIQUE\s+(KEY|INDEX)|KEY|INDEX|CONSTRAINT|FOREIGN\s+KEY|FULLTEXT|SPATIAL)\b`)
	open := regexp.MustCompile(`^CREATE TABLE IF NOT EXISTS ` + "`?" + `(\w+)` + "`?")
	ident := regexp.MustCompile("^`?(\\w+)`?")
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if m := open.FindStringSubmatch(line); m != nil {
			cur = m[1]
			out[cur] = map[string]bool{}
			continue
		}
		if cur == "" {
			continue
		}
		if strings.HasPrefix(line, ")") {
			cur = ""
			continue
		}
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		// A backtick-quoted identifier is unambiguously a column reference,
		// never a key/constraint keyword: all eight skip keywords above
		// (KEY, INDEX, UNIQUE, PRIMARY, CONSTRAINT, FOREIGN, FULLTEXT,
		// SPATIAL) are MySQL reserved words, so a column actually named one
		// of them MUST be backtick-quoted to be used as an identifier at
		// all — an unquoted line can never legitimately start with one of
		// them as a column name. So a leading backtick bypasses the skip
		// regex entirely rather than being tested against it; without this,
		// a column named e.g. `key` or `unique` would match the skip regex
		// on its bare first token and vanish silently, which is exactly the
		// failure mode this parser's doc comment says it must not have.
		if strings.HasPrefix(line, "`") {
			if m := ident.FindStringSubmatch(line); m != nil {
				out[cur][m[1]] = true
			}
			continue
		}
		if skip.MatchString(strings.ToUpper(line)) {
			continue
		}
		if m := ident.FindStringSubmatch(line); m != nil {
			out[cur][m[1]] = true
		}
	}
	return out
}

// autoMigrateColumns reflects the same table -> column set out of the GORM
// models AutoMigrate is driven from.
func autoMigrateColumns(t *testing.T) map[string]map[string]bool {
	t.Helper()
	ns := schema.NamingStrategy{}
	out := map[string]map[string]bool{}
	for _, m := range autoMigrateModels {
		s, err := schema.Parse(m, &sync.Map{}, ns)
		if err != nil {
			t.Fatalf("parse model %T: %v", m, err)
		}
		cols := map[string]bool{}
		for _, f := range s.Fields {
			if f.DBName == "" || f.IgnoreMigration {
				continue
			}
			cols[f.DBName] = true
		}
		out[s.Table] = cols
	}
	return out
}

// TestAutoMigrateModelsMatchSchemaFileColumns is the column-level half of the
// pairing guard. Table-level equality already holds (see the sibling test);
// this catches the failure mode that one lets through: a field added to a
// model and not to db/xflow_schema.sql. CI seeds from the schema file and then
// runs AutoMigrate, which can add missing columns but never removes them — so
// the missing column is silently patched in CI while production, which only
// ever applies the schema file, does not have it at all.
func TestAutoMigrateModelsMatchSchemaFileColumns(t *testing.T) {
	fileCols := schemaTableColumns(t)
	modelCols := autoMigrateColumns(t)
	for table, want := range modelCols {
		got, ok := fileCols[table]
		if !ok {
			t.Errorf("table %q is in autoMigrateModels but not in db/xflow_schema.sql", table)
			continue
		}
		for c := range want {
			if !got[c] {
				t.Errorf("column %s.%s exists on the GORM model but not in db/xflow_schema.sql", table, c)
			}
		}
		for c := range got {
			if !want[c] {
				t.Errorf("column %s.%s exists in db/xflow_schema.sql but not on the GORM model", table, c)
			}
		}
	}
}

// schemaColumnDeclaration returns a column's declaration from one named CREATE
// TABLE body. Scoping the search to that body prevents same-named columns in
// other tables from satisfying a schema guard.
func schemaColumnDeclaration(t *testing.T, schemaText, table, column string) string {
	t.Helper()
	tablePattern := regexp.MustCompile(`(?ims)^CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+` + "`?" + regexp.QuoteMeta(table) + "`?" + `\s*\((.*?)^\)\s*ENGINE\s*=`)
	tableMatch := tablePattern.FindStringSubmatch(schemaText)
	if tableMatch == nil {
		t.Fatalf("schema has no CREATE TABLE definition for %q", table)
	}

	columnPattern := regexp.MustCompile(`(?im)^\s*` + "`?" + regexp.QuoteMeta(column) + "`?" + `\s+([^,\r\n]+)`)
	columnMatch := columnPattern.FindStringSubmatch(tableMatch[1])
	if columnMatch == nil {
		t.Fatalf("schema has no declaration for %s.%s", table, column)
	}
	return columnMatch[1]
}

func TestBase64ContentColumnsUseLongtextBinaryCollation(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "db", "xflow_schema.sql"))
	if err != nil {
		t.Fatalf("read schema file: %v", err)
	}

	collation := regexp.MustCompile(`(?i)\bCOLLATE\s+utf8mb4_bin\b`)
	notNull := regexp.MustCompile(`(?i)\bNOT\s+NULL\b`)
	for _, tc := range []struct {
		table string
	}{
		{table: "xflow_supplies"},
		{table: "xflow_artifact_blobs"},
	} {
		t.Run(tc.table, func(t *testing.T) {
			declaration := schemaColumnDeclaration(t, string(raw), tc.table, "content")
			fields := strings.Fields(declaration)
			if len(fields) == 0 || !strings.EqualFold(fields[0], "LONGTEXT") {
				t.Errorf("%s.content declaration %q does not start with LONGTEXT", tc.table, declaration)
			}
			if !collation.MatchString(declaration) {
				t.Errorf("%s.content declaration %q does not include COLLATE utf8mb4_bin", tc.table, declaration)
			}
			if !notNull.MatchString(declaration) {
				t.Errorf("%s.content declaration %q does not include NOT NULL", tc.table, declaration)
			}
		})
	}
}
