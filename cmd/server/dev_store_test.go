package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"gorm.io/gorm"
)

// TestParseDevStoreSpec pins the --dev-store grammar. The empty value is the
// dev default (SQLite), so "unset" and "sqlite" must resolve identically.
func TestParseDevStoreSpec(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want devStoreSpec
	}{
		{raw: "", want: devStoreSpec{kind: devStoreSQLite}},
		{raw: "sqlite", want: devStoreSpec{kind: devStoreSQLite}},
		{raw: "sqlite:/tmp/xflow-dev.sqlite", want: devStoreSpec{kind: devStoreSQLite, path: "/tmp/xflow-dev.sqlite"}},
		{raw: "memory", want: devStoreSpec{kind: devStoreMemory}},
		{raw: "none", want: devStoreSpec{kind: devStoreNone}},
	} {
		got, err := parseDevStoreSpec(tc.raw)
		if err != nil {
			t.Errorf("parseDevStoreSpec(%q): %v", tc.raw, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseDevStoreSpec(%q) = %+v, want %+v", tc.raw, got, tc.want)
		}
	}

	for _, raw := range []string{"sqlite:", "sqlite:   ", "mysql", "none:extra"} {
		if _, err := parseDevStoreSpec(raw); err == nil {
			t.Errorf("parseDevStoreSpec(%q) = nil error, want a refusal", raw)
		}
	}
}

// TestDevStoreFlagPreconditions pins where --dev-store is accepted. It is a
// dev-posture override, never a second production store path, and it cannot
// silently shadow an explicit --mysql-dsn.
func TestDevStoreFlagPreconditions(t *testing.T) {
	if _, err := parseServerConfig([]string{"--mode=production", "--dev-store=none"}); err == nil {
		t.Fatal("--dev-store with --mode=production must be refused, not ignored")
	}
	if _, err := parseServerConfig([]string{"--mode=dev", "--mysql-dsn=x", "--dev-store=sqlite"}); err == nil {
		t.Fatal("--dev-store together with --mysql-dsn must be refused as ambiguous")
	}
	if _, err := parseServerConfig([]string{"--mode=dev", "--dev-store=bogus"}); err == nil {
		t.Fatal("an unrecognized --dev-store value must fail startup")
	}
	cfg, err := parseServerConfig([]string{"--mode=dev", "--memory", "--dev-store=none"})
	if err != nil {
		t.Fatalf("parseServerConfig(--mode=dev --dev-store=none): %v", err)
	}
	if cfg.devStore != "none" {
		t.Fatalf("cfg.devStore = %q, want none", cfg.devStore)
	}
}

// TestDefaultDevStorePathIsOutsideTheCheckout pins the default file location:
// under the OS user cache directory, never in the repository working tree.
func TestDefaultDevStorePathIsOutsideTheCheckout(t *testing.T) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		t.Skipf("no user cache directory on this platform: %v", err)
	}
	got, err := defaultDevStorePath()
	if err != nil {
		t.Fatalf("defaultDevStorePath: %v", err)
	}
	want := filepath.Join(cacheDir, "xflow", "dev.sqlite")
	if got != want {
		t.Fatalf("defaultDevStorePath() = %q, want %q", got, want)
	}
	if !filepath.IsAbs(got) {
		t.Fatalf("defaultDevStorePath() = %q, want an absolute path", got)
	}
}

// TestOpenDevExecutionStoreNoneAndMemory pins the two non-SQLite overrides: none
// keeps the storeless posture (nil store, so the API answers 501), while memory
// yields a usable store.
func TestOpenDevExecutionStoreNoneAndMemory(t *testing.T) {
	st, err := openDevExecutionStore("none", nil)
	if err != nil {
		t.Fatalf("openDevExecutionStore(none): %v", err)
	}
	if st != nil {
		t.Fatalf("openDevExecutionStore(none) = %T, want nil", st)
	}

	st, err = openDevExecutionStore("memory", nil)
	if err != nil {
		t.Fatalf("openDevExecutionStore(memory): %v", err)
	}
	if st == nil {
		t.Fatal("openDevExecutionStore(memory) = nil, want an in-memory store")
	}
	if _, ok := st.(store.Executions); !ok {
		t.Fatalf("openDevExecutionStore(memory) = %T, want an execution store", st)
	}
}

// TestOpenDevExecutionStoreSQLitePermissions pins the file and directory
// posture of the dev SQLite store: the containing directory is created 0700,
// the database file is 0600, and the WAL/SHM sidecars the driver creates are
// tightened to 0600 as well — whether the driver is about to create the
// database or it already exists with wider permissions. This store holds
// execution rows, supplies and audit data, so a group/world-readable file is
// not acceptable even in --mode=dev.
func TestOpenDevExecutionStoreSQLitePermissions(t *testing.T) {
	closeStore := func(t *testing.T, st store.Store) {
		t.Helper()
		if provider, ok := st.(interface{ DB() *gorm.DB }); ok {
			if sqlDB, err := provider.DB().DB(); err == nil {
				_ = sqlDB.Close()
			}
		}
	}

	t.Run("fresh file and directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "nested")
		path := filepath.Join(dir, "dev.sqlite")
		st, err := openDevExecutionStore("sqlite:"+path, nil)
		if err != nil {
			t.Fatalf("openDevExecutionStore(sqlite:%s): %v", path, err)
		}
		t.Cleanup(func() { closeStore(t, st) })

		dirInfo, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat dev store directory: %v", err)
		}
		if perm := dirInfo.Mode().Perm(); perm&0o077 != 0 {
			t.Errorf("dev store directory mode = %04o, want no group/other access (0700)", perm)
		}
		fileInfo, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat dev store file: %v", err)
		}
		if perm := fileInfo.Mode().Perm(); perm != 0o600 {
			t.Errorf("dev store file mode = %04o, want 0600", perm)
		}
	})

	t.Run("existing wider file is tightened", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dev.sqlite")
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatalf("seed existing dev store file: %v", err)
		}
		// WriteFile honours the umask; set the wider mode explicitly so the
		// fixture is the condition under test on every host.
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatalf("widen seeded dev store file: %v", err)
		}
		st, err := openDevExecutionStore("sqlite:"+path, nil)
		if err != nil {
			t.Fatalf("openDevExecutionStore(sqlite:%s): %v", path, err)
		}
		t.Cleanup(func() { closeStore(t, st) })

		fileInfo, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat dev store file: %v", err)
		}
		if perm := fileInfo.Mode().Perm(); perm != 0o600 {
			t.Errorf("pre-existing dev store file mode = %04o, want it tightened to 0600", perm)
		}
	})

	t.Run("WAL and SHM sidecars are owner-only", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dev.sqlite")
		st, err := openDevExecutionStore("sqlite:"+path, nil)
		if err != nil {
			t.Fatalf("openDevExecutionStore(sqlite:%s): %v", path, err)
		}
		t.Cleanup(func() { closeStore(t, st) })

		// WAL mode creates both sidecars when the database is opened: the WAL
		// carries committed rows, so it must be as private as the database.
		for _, sidecar := range []string{path + "-wal", path + "-shm"} {
			info, err := os.Stat(sidecar)
			if err != nil {
				t.Fatalf("stat %s: %v", filepath.Base(sidecar), err)
			}
			if perm := info.Mode().Perm(); perm != 0o600 {
				t.Errorf("%s mode = %04o, want 0600", filepath.Base(sidecar), perm)
			}
		}
	})
}

// TestOpenDevExecutionStoreSQLite migrates a fresh file and answers a scoped
// execution read. This is the real sqlstore path, not a mock: it is what makes
// the dev default more than a flag that parses.
func TestOpenDevExecutionStoreSQLite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dev.sqlite")
	st, err := openDevExecutionStore("sqlite:"+path, nil)
	if err != nil {
		t.Fatalf("openDevExecutionStore(sqlite:%s): %v", path, err)
	}
	t.Cleanup(func() {
		// sqlstore.Provider exposes its *gorm.DB rather than a Close; closing
		// the underlying pool keeps the temp file from outliving the test.
		if provider, ok := st.(interface{ DB() *gorm.DB }); ok {
			if sqlDB, err := provider.DB().DB(); err == nil {
				_ = sqlDB.Close()
			}
		}
	})
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("SQLite file was not created at %s: %v", path, err)
	}
	execs, ok := st.(store.Executions)
	if !ok {
		t.Fatalf("dev SQLite store = %T, want an execution store", st)
	}
	recs, err := execs.ListExecutions(context.Background(), namespace.Namespace("dev-store-test"),
		store.ExecutionFilter{}, store.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("ListExecutions on a migrated dev SQLite store: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("fresh dev SQLite store returned %d executions, want 0", len(recs))
	}
}
