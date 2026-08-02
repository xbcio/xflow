//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/store/sqlstore"
)

// newArtifactDB opens a *gorm.DB for artifact tests, auto-migrates the two
// artifact tables, and returns both the raw DB (for SQL-log assertions) and a
// Provider (for the repo accessors).
func newArtifactDB(t *testing.T) (*gorm.DB, *sqlstore.Provider) {
	t.Helper()
	dsn := requireMySQL(t)

	db, err := gorm.Open(gormmysql.Open(dsn), &gorm.Config{TranslateError: true})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	if err := sqlstore.AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	return db, sqlstore.New(db)
}

// newCapturingArtifactDB is newArtifactDB with a SQL-capturing GORM logger, so
// a test can assert on the statements a code path actually issues. Statements
// append to *captured; the caller resets that slice before the operation under
// test.
func newCapturingArtifactDB(t *testing.T, dsn string, captured *[]string) (*gorm.DB, *sqlstore.Provider) {
	t.Helper()
	logger := gormlogger.New(
		log.New(io.Discard, "", 0),
		gormlogger.Config{LogLevel: gormlogger.Info, Colorful: false},
	)
	db, err := gorm.Open(gormmysql.Open(dsn), &gorm.Config{
		TranslateError: true,
		Logger:         &sqlCapture{Interface: logger, stmts: captured},
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	if err := sqlstore.AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	return db, sqlstore.New(db)
}

// uniqueNS returns a namespace unique to this test run to avoid row collisions.
func uniqueNS(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("art-%s-%d", suffix, time.Now().UnixNano())
}

// uniqueContent returns bytes whose sha256 is unique to this test run.
//
// A unique namespace alone is NOT enough for any assertion on
// CountReferences: by contract it counts identity rows across ALL namespaces
// (it answers "is this blob still referenced by anyone"), so fixed content
// yields a fixed digest whose count grows by one on every run of the suite
// against a persistent database. That accumulation is invisible on a fresh
// schema and shows up as an off-by-N on the second run — so the content, not
// just the namespace, has to carry the run's identity.
func uniqueContent(t *testing.T, body string) []byte {
	t.Helper()
	return []byte(fmt.Sprintf("%s|run=%d", body, time.Now().UnixNano()))
}

// TestArtifactDedup verifies that uploading identical content twice produces
// only one blob row.
func TestArtifactDedup(t *testing.T) {
	db, p := newArtifactDB(t)
	ctx := context.Background()

	content := uniqueContent(t, "hello world dedup test")
	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	ns := uniqueNS(t, "dedup")

	ref1, err := as.Put(ctx, content, store.ArtifactMeta{
		Filename:  "file.wasm",
		Namespace: ns,
	})
	if err != nil {
		t.Fatalf("Put #1: %v", err)
	}

	ref2, err := as.Put(ctx, content, store.ArtifactMeta{
		Filename:  "file.wasm",
		Namespace: ns,
	})
	if err != nil {
		t.Fatalf("Put #2: %v", err)
	}

	if ref1.Digest != ref2.Digest {
		t.Fatalf("digests differ: %q vs %q", ref1.Digest, ref2.Digest)
	}

	// The headline assertion: one blob row for two Puts of identical bytes. The
	// identity count below is a different fact and cannot stand in for this one —
	// deduplication happens in the byte layer.
	var blobRows int64
	if err := db.WithContext(ctx).
		Table("xflow_artifact_blobs").
		Where("content_hash = ?", ref1.Digest).
		Count(&blobRows).Error; err != nil {
		t.Fatalf("count blob rows: %v", err)
	}
	if blobRows != 1 {
		t.Fatalf("blob rows = %d, want 1 (identical content must not be stored twice)", blobRows)
	}

	// Same namespace + filename + version (the default, itself derived from the
	// digest) means the identity bind was idempotent, so exactly one identity
	// row references this digest.
	count, err := p.ArtifactIndex().CountReferences(ctx, ref1.Digest)
	if err != nil {
		t.Fatalf("CountReferences: %v", err)
	}
	if count != 1 {
		t.Fatalf("CountReferences = %d, want 1", count)
	}
}

// TestArtifactIdempotentBind verifies that binding the same (ns, filename,
// version) + same digest twice succeeds without creating a second identity row.
func TestArtifactIdempotentBind(t *testing.T) {
	_, p := newArtifactDB(t)
	ctx := context.Background()

	content := uniqueContent(t, "idempotent bind content")
	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	ns := uniqueNS(t, "idem")

	ref1, err := as.Put(ctx, content, store.ArtifactMeta{
		Filename:  "module.wasm",
		Version:   "v1.0.0",
		Namespace: ns,
	})
	if err != nil {
		t.Fatalf("Put #1: %v", err)
	}

	ref2, err := as.Put(ctx, content, store.ArtifactMeta{
		Filename:  "module.wasm",
		Version:   "v1.0.0",
		Namespace: ns,
	})
	if err != nil {
		t.Fatalf("Put #2 (idempotent): %v", err)
	}

	if ref1.Digest != ref2.Digest {
		t.Fatalf("digests differ")
	}

	count, err := p.ArtifactIndex().CountReferences(ctx, ref1.Digest)
	if err != nil {
		t.Fatalf("CountReferences: %v", err)
	}
	if count != 1 {
		t.Fatalf("CountReferences = %d, want 1 (idempotent bind must not duplicate)", count)
	}
}

// TestArtifactVersionConflict verifies that rebinding the same triple to
// different content returns ErrVersionConflict (409).
func TestArtifactVersionConflict(t *testing.T) {
	_, p := newArtifactDB(t)
	ctx := context.Background()

	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	ns := uniqueNS(t, "conflict")

	_, err := as.Put(ctx, []byte("content A"), store.ArtifactMeta{
		Filename:  "rules.wasm",
		Version:   "v2.0.0",
		Namespace: ns,
	})
	if err != nil {
		t.Fatalf("Put content A: %v", err)
	}

	_, err = as.Put(ctx, []byte("content B (different)"), store.ArtifactMeta{
		Filename:  "rules.wasm",
		Version:   "v2.0.0",
		Namespace: ns,
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("Put content B: got %v, want ErrVersionConflict", err)
	}
}

// TestArtifactCrossNamespace verifies that different namespaces uploading the
// same bytes produce two identity rows but only one blob.
func TestArtifactCrossNamespace(t *testing.T) {
	db, p := newArtifactDB(t)
	ctx := context.Background()

	content := uniqueContent(t, "shared cross-namespace content")
	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	nsA := uniqueNS(t, "nsA")
	nsB := uniqueNS(t, "nsB")

	refA, err := as.Put(ctx, content, store.ArtifactMeta{
		Filename:  "shared.wasm",
		Namespace: nsA,
	})
	if err != nil {
		t.Fatalf("Put nsA: %v", err)
	}

	refB, err := as.Put(ctx, content, store.ArtifactMeta{
		Filename:  "shared.wasm",
		Namespace: nsB,
	})
	if err != nil {
		t.Fatalf("Put nsB: %v", err)
	}

	if refA.Digest != refB.Digest {
		t.Fatalf("digests differ across namespaces")
	}

	// Two identity rows, one blob. This pair is the whole point of the dual-table
	// split: tenant-scoped identity on top of globally deduplicated bytes.
	count, err := p.ArtifactIndex().CountReferences(ctx, refA.Digest)
	if err != nil {
		t.Fatalf("CountReferences: %v", err)
	}
	if count != 2 {
		t.Fatalf("CountReferences = %d, want 2", count)
	}
	var blobRows int64
	if err := db.WithContext(ctx).
		Table("xflow_artifact_blobs").
		Where("content_hash = ?", refA.Digest).
		Count(&blobRows).Error; err != nil {
		t.Fatalf("count blob rows: %v", err)
	}
	if blobRows != 1 {
		t.Fatalf("blob rows = %d, want 1 (dedup must cross namespaces)", blobRows)
	}

	// Each namespace sees its own reference.
	hasA, err := p.ArtifactIndex().HasReference(ctx, nsA, refA.Digest)
	if err != nil {
		t.Fatalf("HasReference nsA: %v", err)
	}
	hasB, err := p.ArtifactIndex().HasReference(ctx, nsB, refB.Digest)
	if err != nil {
		t.Fatalf("HasReference nsB: %v", err)
	}
	if !hasA || !hasB {
		t.Fatalf("HasReference: nsA=%v nsB=%v, want both true", hasA, hasB)
	}
}

// TestArtifactHeadObjectNoContent verifies that HeadObject does not SELECT the
// content column. We attach a GORM logger, capture the SQL, and assert the
// column list neither names a bare "content" (being careful to avoid false
// positives from "content_hash" or "content_type") nor uses a star.
//
// The star check is the one that matters: dropping the .Select(...) clause makes
// GORM emit `SELECT *`, which pulls the 16 MiB BLOB while containing no
// substring a "does it mention content" test would match. A column-name check
// alone therefore passes on exactly the regression it exists to catch.
func TestArtifactHeadObjectNoContent(t *testing.T) {
	dsn := requireMySQL(t)

	var captured []string
	_, p := newCapturingArtifactDB(t, dsn, &captured)

	ctx := context.Background()
	content := uniqueContent(t, "head object no content test payload")
	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	ns := uniqueNS(t, "head")

	ref, err := as.Put(ctx, content, store.ArtifactMeta{
		Filename:  "head.wasm",
		Namespace: ns,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Clear captured SQL before HeadObject.
	captured = nil

	_, err = as.Stat(ctx, ref.Digest)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	assertNoBlobContentSelected(t, captured)
}

// assertNoBlobContentSelected fails if any captured SELECT against
// xflow_artifact_blobs could return the content column — either by naming it or
// by using a star.
//
// Both checks are load-bearing and neither subsumes the other: naming the column
// is the obvious regression, while `SELECT *` (what GORM emits the moment the
// .Select(...) clause is dropped) pulls the same 16 MiB while containing no
// "content" substring at all.
func assertNoBlobContentSelected(t *testing.T, captured []string) {
	t.Helper()
	found := false
	for _, sql := range captured {
		if !strings.Contains(sql, "xflow_artifact_blobs") {
			continue
		}
		upper := strings.ToUpper(sql)
		if !strings.Contains(upper, "SELECT") {
			continue
		}
		found = true
		// A star selects every column, content included.
		if strings.Contains(upper, "SELECT *") || strings.Contains(upper, "SELECT `XFLOW_ARTIFACT_BLOBS`.*") {
			t.Fatalf("HeadObject SQL uses a star and therefore selects the content column:\n%s", sql)
		}
		// Named bare "content" (content_hash / content_type are fine).
		sanitized := strings.ReplaceAll(strings.ToLower(sql), "content_hash", "")
		sanitized = strings.ReplaceAll(sanitized, "content_type", "")
		if strings.Contains(sanitized, "content") {
			t.Fatalf("HeadObject SQL names the content column:\n%s", sql)
		}
	}
	if !found {
		t.Fatalf("no SELECT against xflow_artifact_blobs captured; captured: %v", captured)
	}
}

// sqlCapture wraps a gorm logger to capture SQL statements.
type sqlCapture struct {
	gormlogger.Interface
	stmts *[]string
}

func (c *sqlCapture) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	sql, _ := fc()
	*c.stmts = append(*c.stmts, sql)
	c.Interface.Trace(ctx, begin, fc, err)
}

// TestArtifactNotFoundSentinelAcrossBackends pins the one property that lets
// objectstore.ReadThrough compose a filesystem cache in front of an authoritative
// backend: both must report a missing key as objectstore.ErrNotFound.
//
// It is worth a test because the MySQL repo lives under the store package, whose
// own store.ErrNotFound is what every OTHER repo there returns — returning that
// one here would read as consistent with its neighbours while silently breaking
// the interface it actually implements. ReadThrough would then treat a plain
// cache miss as a backend failure.
func TestArtifactNotFoundSentinelAcrossBackends(t *testing.T) {
	_, p := newArtifactDB(t)
	ctx := context.Background()

	// A well-formed digest that was never stored.
	absent := store.ContentHash(uniqueContent(t, "never stored anywhere"))
	key := store.ObjectKeyForDigest(absent)

	backends := map[string]objectstore.Store{
		"mysql": p.ArtifactObjects(),
		"fs":    objectstore.NewFSStore(t.TempDir()),
	}
	for name, b := range backends {
		if _, _, err := b.GetObject(ctx, key); !errors.Is(err, objectstore.ErrNotFound) {
			t.Errorf("%s GetObject(absent): got %v, want objectstore.ErrNotFound", name, err)
		}
		if _, err := b.HeadObject(ctx, key); !errors.Is(err, objectstore.ErrNotFound) {
			t.Errorf("%s HeadObject(absent): got %v, want objectstore.ErrNotFound", name, err)
		}
	}

	// The composition that depends on it: a miss in both layers must surface as
	// not-found, not as an opaque backend error.
	rt := objectstore.NewReadThrough(objectstore.NewFSStore(t.TempDir()), p.ArtifactObjects())
	if _, _, err := rt.GetObject(ctx, key); !errors.Is(err, objectstore.ErrNotFound) {
		t.Errorf("ReadThrough GetObject(absent): got %v, want objectstore.ErrNotFound", err)
	}
}

// TestArtifactReadThroughOverMySQL proves the runner-side shape end to end: a
// filesystem cache in front of the authoritative MySQL backend serves the second
// read without touching the database.
func TestArtifactReadThroughOverMySQL(t *testing.T) {
	_, p := newArtifactDB(t)
	ctx := context.Background()

	content := uniqueContent(t, "read-through over mysql")
	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	ref, err := as.Put(ctx, content, store.ArtifactMeta{
		Filename:  "cached.wasm",
		Namespace: uniqueNS(t, "rt"),
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	counted := &countingStore{inner: p.ArtifactObjects()}
	rt := objectstore.NewReadThrough(objectstore.NewFSStore(t.TempDir()), counted)
	key := store.ObjectKeyForDigest(ref.Digest)

	for i := 1; i <= 2; i++ {
		rc, _, err := rt.GetObject(ctx, key)
		if err != nil {
			t.Fatalf("GetObject #%d: %v", i, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, content) {
			t.Fatalf("GetObject #%d returned wrong content", i)
		}
	}

	// One origin fetch for two reads. Asserting the count (not merely that the
	// content was right) is what distinguishes a working cache from a cache that
	// silently re-fetches every time and happens to return correct bytes.
	if n := counted.gets.Load(); n != 1 {
		t.Fatalf("origin GetObject called %d times, want 1 (second read must be served from cache)", n)
	}
}

// countingStore counts origin fetches so a cache hit is provable rather than
// inferred from the content being correct.
type countingStore struct {
	inner objectstore.Store
	gets  atomic.Int64
}

func (c *countingStore) PutObject(ctx context.Context, key string, body io.Reader, size int64, opts objectstore.PutOptions) (*objectstore.Object, error) {
	return c.inner.PutObject(ctx, key, body, size, opts)
}

func (c *countingStore) GetObject(ctx context.Context, key string) (io.ReadCloser, *objectstore.Object, error) {
	c.gets.Add(1)
	return c.inner.GetObject(ctx, key)
}

func (c *countingStore) HeadObject(ctx context.Context, key string) (*objectstore.Object, error) {
	return c.inner.HeadObject(ctx, key)
}

// TestArtifactFilenameValidation verifies that invalid filenames are rejected.
func TestArtifactFilenameValidation(t *testing.T) {
	_, p := newArtifactDB(t)
	ctx := context.Background()
	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	ns := uniqueNS(t, "fname")

	cases := []struct {
		name string
	}{
		{"../etc/passwd"},
		{"."},
		{".."},
		{"has\x00nul.wasm"},
	}
	for _, tc := range cases {
		_, err := as.Put(ctx, []byte("x"), store.ArtifactMeta{
			Filename:  tc.name,
			Namespace: ns,
		})
		if !errors.Is(err, store.ErrInvalidFilename) {
			t.Errorf("Put(%q): got %v, want ErrInvalidFilename", tc.name, err)
		}
	}
}

// TestArtifactHasReferenceTenantIsolation verifies that HasReference is scoped
// to the queried namespace: namespace A cannot see B's references.
func TestArtifactHasReferenceTenantIsolation(t *testing.T) {
	_, p := newArtifactDB(t)
	ctx := context.Background()
	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	nsA := uniqueNS(t, "isoA")
	nsB := uniqueNS(t, "isoB")

	content := uniqueContent(t, "tenant isolation content")
	ref, err := as.Put(ctx, content, store.ArtifactMeta{
		Filename:  "isolated.wasm",
		Namespace: nsB,
	})
	if err != nil {
		t.Fatalf("Put nsB: %v", err)
	}

	// nsA has no reference to this digest.
	has, err := p.ArtifactIndex().HasReference(ctx, nsA, ref.Digest)
	if err != nil {
		t.Fatalf("HasReference nsA: %v", err)
	}
	if has {
		t.Fatalf("HasReference(nsA, %q) = true, want false (tenant isolation)", ref.Digest)
	}

	// nsB does have a reference.
	has, err = p.ArtifactIndex().HasReference(ctx, nsB, ref.Digest)
	if err != nil {
		t.Fatalf("HasReference nsB: %v", err)
	}
	if !has {
		t.Fatalf("HasReference(nsB, %q) = false, want true", ref.Digest)
	}
}

// TestArtifactReadPathPurity verifies that Get, Head, and HasReference do not
// modify the database. This is design §2.3 premise 1: under multiple server
// replicas, a write on the retrieval path (a last_used_at bump, say) turns every
// runner fetch into row-lock contention. The property is structural here — there
// is no such column — and this test is what keeps it that way.
func TestArtifactReadPathPurity(t *testing.T) {
	db, p := newArtifactDB(t)
	ctx := context.Background()
	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	ns := uniqueNS(t, "pure")

	content := uniqueContent(t, "read path purity content")
	ref, err := as.Put(ctx, content, store.ArtifactMeta{
		Filename:  "pure.wasm",
		Namespace: ns,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	blobBefore := readBlobRow(t, db, ref.Digest)
	identityBefore := readIdentityRows(t, db, ref.Digest)

	rc, _, err := as.Open(ctx, ref.Digest)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, content) {
		t.Fatalf("Open returned wrong content")
	}

	if _, err = as.Stat(ctx, ref.Digest); err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if _, err = p.ArtifactIndex().HasReference(ctx, ns, ref.Digest); err != nil {
		t.Fatalf("HasReference: %v", err)
	}
	if _, err = p.ArtifactIndex().CountReferences(ctx, ref.Digest); err != nil {
		t.Fatalf("CountReferences: %v", err)
	}

	if blobAfter := readBlobRow(t, db, ref.Digest); blobBefore != blobAfter {
		t.Fatalf("blob row modified by read operations:\nbefore=%s\nafter =%s", blobBefore, blobAfter)
	}
	if identityAfter := readIdentityRows(t, db, ref.Digest); identityBefore != identityAfter {
		t.Fatalf("identity rows modified by read operations:\nbefore=%s\nafter =%s", identityBefore, identityAfter)
	}
}

// readBlobRow snapshots EVERY column of the blob row, including created_at.
//
// Covering all columns is the point: a read path that "touches" a row would do
// it by bumping a timestamp, not by rewriting the content. A snapshot built from
// GetObject's return value alone would omit exactly the column most likely to
// change and the test would pass while the property it names is broken.
func readBlobRow(t *testing.T, db *gorm.DB, digest string) string {
	t.Helper()
	var row struct {
		ContentHash string
		Content     []byte
		SizeBytes   uint64
		CreatedAt   time.Time
	}
	if err := db.WithContext(context.Background()).
		Table("xflow_artifact_blobs").
		Select("content_hash", "content", "size_bytes", "created_at").
		Where("content_hash = ?", digest).
		Take(&row).Error; err != nil {
		t.Fatalf("readBlobRow: %v", err)
	}
	return fmt.Sprintf("%s|%d|%d|%x", row.ContentHash, row.SizeBytes, row.CreatedAt.UnixNano(), row.Content)
}

// readIdentityRows snapshots every identity row referencing digest, ordered so
// the comparison is stable.
func readIdentityRows(t *testing.T, db *gorm.DB, digest string) string {
	t.Helper()
	var rows []struct {
		Namespace   string
		Filename    string
		Version     string
		ContentHash string
		ContentType string
		CreatedAt   time.Time
	}
	if err := db.WithContext(context.Background()).
		Table("xflow_artifacts").
		Select("namespace", "filename", "version", "content_hash", "content_type", "created_at").
		Where("content_hash = ?", digest).
		Order("namespace, filename, version").
		Find(&rows).Error; err != nil {
		t.Fatalf("readIdentityRows: %v", err)
	}
	var b strings.Builder
	for _, r := range rows {
		fmt.Fprintf(&b, "%s|%s|%s|%s|%s|%d\n", r.Namespace, r.Filename, r.Version, r.ContentHash, r.ContentType, r.CreatedAt.UnixNano())
	}
	return b.String()
}
