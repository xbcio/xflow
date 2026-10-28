package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/sqlstore/sqlitestore"
	"gorm.io/gorm"
)

// blobTestDSN is never dialled: parseServerConfig only checks that a DSN was
// given, and every test here that needs a database uses SQLite instead.
const blobTestDSN = "xflow@tcp(127.0.0.1:1)/x?parseTime=true"

// blobArgs is a Redis backend, a MySQL DSN and a blob directory: the smallest
// valid blob selection. Callers append the flag under test.
func blobArgs(dir string, extra ...string) []string {
	args := []string{"-redis", "127.0.0.1:1", "-mysql-dsn", blobTestDSN, "-blob-dir", dir}
	return append(args, extra...)
}

// newBlobTestDB opens a migrated SQLite database, the blob index's stand-in for
// MySQL.
func newBlobTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	p, err := sqlitestore.New(filepath.Join(t.TempDir(), "xflow.db"), sqlitestore.WithAutoMigrate())
	if err != nil {
		t.Fatalf("sqlitestore.New: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, err := p.DB().DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return p.DB()
}

// TestBlobFlagsDefaultPathChangesNothing pins the default: without any --blob-*
// flag the blob layer is not requested, no configuration is built and nothing
// is created on disk.
func TestBlobFlagsDefaultPathChangesNothing(t *testing.T) {
	work := t.TempDir()
	t.Chdir(work)
	for _, args := range [][]string{{"-memory"}, {"-redis", "127.0.0.1:1"}, {"-redis", "127.0.0.1:1", "-mysql-dsn", blobTestDSN}} {
		cfg, err := parseServerConfig(args)
		if err != nil {
			t.Fatalf("parseServerConfig(%v): %v", args, err)
		}
		if cfg.blobRequested() {
			t.Fatalf("parseServerConfig(%v): blobRequested() = true with no --blob-* flag", args)
		}
		bc, err := buildBlobConfig(cfg, nil)
		if bc != nil || err != nil {
			t.Fatalf("buildBlobConfig(%v) = %+v, %v; want nil, nil", args, bc, err)
		}
	}
	entries, err := os.ReadDir(work)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("the default path created %d file(s) in the working directory", len(entries))
	}
}

// blobSecretDir is a directory whose path carries a marker that must never
// appear in an error or a log line.
func blobSecretDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "secretuser", "blobs")
}

// TestBlobDirRequiresMySQL: blob metadata lives in the database, so a blob
// directory without --mysql-dsn is refused at parse time.
func TestBlobDirRequiresMySQL(t *testing.T) {
	dir := blobSecretDir(t)
	_, err := parseServerConfig([]string{"-redis", "127.0.0.1:1", "-blob-dir", dir})
	if err == nil {
		t.Fatal("parseServerConfig accepted --blob-dir without --mysql-dsn")
	}
	if !strings.Contains(err.Error(), "--mysql-dsn") {
		t.Fatalf("error = %q, want it to name --mysql-dsn", err)
	}
	if strings.Contains(err.Error(), "secretuser") {
		t.Fatalf("error leaks the blob directory: %q", err)
	}
	if _, err := parseServerConfig(blobArgs(dir)); err != nil {
		t.Fatalf("parseServerConfig(valid blob selection): %v", err)
	}
}

// TestBlobFlagsRequireBlobDir: any --blob-* flag that differs from its default
// needs --blob-dir, otherwise the setting would be silently ignored.
func TestBlobFlagsRequireBlobDir(t *testing.T) {
	base := []string{"-redis", "127.0.0.1:1", "-mysql-dsn", blobTestDSN}
	for _, extra := range [][]string{
		{"-blob-inline-max", "1024"},
		{"-blob-enable-ref-writes"},
		{"-blob-gc-grace", "2h"},
		{"-blob-gc-batch", "50"},
		{"-blob-gc-interval", "1m"},
		{"-blob-compress"},
	} {
		_, err := parseServerConfig(append(append([]string{}, base...), extra...))
		if err == nil {
			t.Fatalf("parseServerConfig(%v) accepted a --blob-* flag without --blob-dir", extra)
		}
		if !strings.Contains(err.Error(), "--blob-dir") {
			t.Fatalf("error = %q, want it to name --blob-dir", err)
		}
	}
}

// TestBlobDirRejectedWithMemory: the blob layer is part of the Redis-backed
// backend, so --memory or no Redis at all refuses it.
func TestBlobDirRejectedWithMemory(t *testing.T) {
	dir := blobSecretDir(t)
	for _, args := range [][]string{
		{"-memory", "-mysql-dsn", blobTestDSN, "-blob-dir", dir},
		{"-mysql-dsn", blobTestDSN, "-blob-dir", dir},
	} {
		_, err := parseServerConfig(args)
		if err == nil {
			t.Fatalf("parseServerConfig accepted --blob-dir without a Redis backend (%v)", args[:2])
		}
		if !strings.Contains(err.Error(), "Redis backend") || strings.Contains(err.Error(), "secretuser") {
			t.Fatalf("error = %q, want a Redis backend message without the directory", err)
		}
	}
}

// TestBlobFlagsRejectBadNumbers: numbers the blob layer cannot honour fail at
// parse time, and the message names the flag, never the directory.
func TestBlobFlagsRejectBadNumbers(t *testing.T) {
	dir := blobSecretDir(t)
	cases := []struct {
		name  string
		extra []string
		want  string
	}{
		{"negative inline max", []string{"-blob-inline-max", "-1"}, "--blob-inline-max"},
		{"zero grace", []string{"-blob-gc-grace", "0"}, "--blob-gc-grace"},
		{"negative grace", []string{"-blob-gc-grace", "-1h"}, "--blob-gc-grace"},
		{"zero batch", []string{"-blob-gc-batch", "0"}, "--blob-gc-batch"},
		{"negative batch", []string{"-blob-gc-batch", "-5"}, "--blob-gc-batch"},
		{"negative interval", []string{"-blob-gc-interval", "-1s"}, "--blob-gc-interval"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseServerConfig(blobArgs(dir, c.extra...))
			if err == nil {
				t.Fatalf("parseServerConfig accepted %v", c.extra)
			}
			if !strings.Contains(err.Error(), c.want) || strings.Contains(err.Error(), "secretuser") {
				t.Fatalf("error = %q, want it to name %s and not the directory", err, c.want)
			}
		})
	}
}

// TestBuildBlobConfigMapsFlags: every flag lands in its BlobConfig field, and
// ref writes stay off unless asked for.
func TestBuildBlobConfigMapsFlags(t *testing.T) {
	db := newBlobTestDB(t)
	dir := t.TempDir()

	cfg, err := parseServerConfig(blobArgs(dir))
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	bc, err := buildBlobConfig(cfg, db)
	if err != nil || bc == nil {
		t.Fatalf("buildBlobConfig(defaults) = %+v, %v", bc, err)
	}
	if bc.Store == nil || bc.Index == nil {
		t.Fatal("Store and Index must both be set")
	}
	if bc.Enabled || bc.InlineMax != 0 || bc.Grace != time.Hour || bc.Batch != 200 || bc.GCInterval != 0 {
		t.Fatalf("defaults = enabled:%v inline:%d grace:%v batch:%d interval:%v; want false/0/1h/200/0",
			bc.Enabled, bc.InlineMax, bc.Grace, bc.Batch, bc.GCInterval)
	}
	if bc.Observer != nil {
		t.Fatal("Observer must stay nil; the server fills it from metrics")
	}
	if got := putCompressible(t, bc.Store, 'a'); got != "" {
		t.Fatalf("codec without --blob-compress = %q, want none", got)
	}

	cfg, err = parseServerConfig(blobArgs(dir, "-blob-enable-ref-writes", "-blob-inline-max", "4096",
		"-blob-gc-grace", "90m", "-blob-gc-batch", "17", "-blob-gc-interval", "45s", "-blob-compress"))
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	if bc, err = buildBlobConfig(cfg, db); err != nil || bc == nil {
		t.Fatalf("buildBlobConfig(flags) = %+v, %v", bc, err)
	}
	if !bc.Enabled || bc.InlineMax != 4096 || bc.Grace != 90*time.Minute || bc.Batch != 17 || bc.GCInterval != 45*time.Second {
		t.Fatalf("mapped = enabled:%v inline:%d grace:%v batch:%d interval:%v", bc.Enabled, bc.InlineMax, bc.Grace, bc.Batch, bc.GCInterval)
	}
	if got := putCompressible(t, bc.Store, 'b'); got != blob.CodecZstd {
		t.Fatalf("codec with --blob-compress = %q, want %q", got, blob.CodecZstd)
	}
	if _, err := buildBlobConfig(cfg, nil); err == nil || strings.Contains(err.Error(), dir) {
		t.Fatalf("buildBlobConfig(nil db) error = %v, want a path-free error", err)
	}
}

// putCompressible stores a highly compressible payload and returns the codec
// the store chose for it. fill varies the content: the index keeps the first
// writer's codec for a digest, so a second Put of the same bytes would report
// the first Put's choice.
func putCompressible(t *testing.T, s blob.Store, fill byte) string {
	t.Helper()
	ref, err := s.Put(context.Background(), namespace.Namespace("blobcfg"), bytes.Repeat([]byte{fill}, 8192))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	return ref.Codec
}

// captureLog redirects the standard logger into a buffer for the rest of the
// test and restores it afterwards.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prevW, prevF := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prevW); log.SetFlags(prevF) })
	return &buf
}

// probeLeftovers lists what a probe left in dir: probe objects and the temp
// files a write in flight uses.
func probeLeftovers(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var left []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".probe") || strings.HasPrefix(e.Name(), ".tmp-") {
			left = append(left, e.Name())
		}
	}
	return left
}

// TestProbeCleansUp: the startup probe removes its object whether it succeeds
// or fails, and a failure never reports the directory's path.
func TestProbeCleansUp(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "secretuser", "blobs")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := probeBlobDir(dir); err != nil {
		t.Fatalf("probeBlobDir(writable): %v", err)
	}
	if left := probeLeftovers(t, dir); len(left) != 0 {
		t.Fatalf("a successful probe left %v behind", left)
	}

	missing := filepath.Join(t.TempDir(), "secretuser", "missing")
	err := probeBlobDir(missing)
	if err == nil {
		t.Fatal("probeBlobDir accepted a directory that does not exist")
	}
	if strings.Contains(err.Error(), "secretuser") {
		t.Fatalf("error leaks the path: %q", err)
	}
	if _, statErr := os.Stat(missing); statErr == nil {
		t.Fatal("the probe created the missing directory instead of refusing it")
	}
}

// TestProbeFailureCleansUpAndHidesPath covers the read-only directory case.
func TestProbeFailureCleansUpAndHidesPath(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permission bits, so a read-only directory cannot make the probe fail")
	}
	dir := filepath.Join(t.TempDir(), "secretuser", "blobs")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	err := probeBlobDir(dir)
	if err == nil {
		t.Fatal("probeBlobDir succeeded on a read-only directory")
	}
	if strings.Contains(err.Error(), "secretuser") || strings.Contains(err.Error(), dir) {
		t.Fatalf("error leaks the path: %q", err)
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("error = %q, want the underlying cause kept", err)
	}
	if left := probeLeftovers(t, dir); len(left) != 0 {
		t.Fatalf("a failed probe left %v behind", left)
	}
}

// TestBlobDirNeverLogged: neither the startup log line nor any error the blob
// wiring returns may carry the directory, which can contain a user name.
func TestBlobDirNeverLogged(t *testing.T) {
	db := newBlobTestDB(t)
	dir := blobSecretDir(t)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	logged := captureLog(t)
	var errs []string
	note := func(err error) {
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	cfg, err := parseServerConfig(blobArgs(dir, "-blob-enable-ref-writes", "-blob-compress"))
	note(err)
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	opts, err := appendBlobOption(nil, cfg, db)
	note(err)
	if err != nil || len(opts) != 1 {
		t.Fatalf("appendBlobOption = %d option(s), %v; want exactly the blob option", len(opts), err)
	}
	if !strings.Contains(logged.String(), "blob store enabled (ref_writes=true inline_max=0 compress=true)") {
		t.Fatalf("startup log = %q, want the blob store line", logged.String())
	}

	// The failure paths: no database, a directory that does not exist, a
	// directory that is a file, and the parse-time rejections.
	_, err = appendBlobOption(nil, cfg, nil)
	note(err)
	for _, bad := range []string{filepath.Join(t.TempDir(), "secretuser", "gone"), filepath.Join(dir, ".marker")} {
		if bad == filepath.Join(dir, ".marker") {
			if werr := os.WriteFile(bad, []byte("x"), 0o600); werr != nil {
				t.Fatalf("WriteFile: %v", werr)
			}
		}
		badCfg := cfg
		badCfg.blobDir = bad
		_, err = appendBlobOption(nil, badCfg, db)
		note(err)
		if err == nil {
			t.Fatalf("appendBlobOption accepted an unusable directory")
		}
	}
	for _, args := range [][]string{
		{"-redis", "127.0.0.1:1", "-blob-dir", dir},
		{"-memory", "-mysql-dsn", blobTestDSN, "-blob-dir", dir},
		blobArgs(dir, "-blob-gc-batch", "-1"),
		blobArgs(dir, "-blob-gc-grace", "0"),
	} {
		_, err = parseServerConfig(args)
		note(err)
	}

	if len(errs) < 6 {
		t.Fatalf("collected %d error(s), want every failure path to have produced one", len(errs))
	}
	for _, text := range append(errs, logged.String()) {
		if strings.Contains(text, "secretuser") || strings.Contains(text, dir) {
			t.Fatalf("output leaks the blob directory: %q", text)
		}
	}
}
