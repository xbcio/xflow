package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/xbcio/xflow/store/objectstore"
)

// The two guards covered here — Put's MaxArtifactBytes ceiling and
// ValidateFilename's backslash rejection — were both reachable only through
// code that nothing asserted on.
//
//   - ErrArtifactTooLarge has exactly three occurrences in the whole repo
//     (its declaration, the fmt.Errorf that wraps it, and nothing else); no
//     test anywhere names it, so the 16 MiB ceiling could be deleted outright
//     and every package stays green.
//   - ErrInvalidFilename is named by exactly one test,
//     test/integration/sqlstore_artifact_test.go's
//     TestArtifactFilenameValidation, which needs a live MySQL and whose case
//     list is {"../etc/passwd", ".", "..", "has\x00nul.wasm"} — every one of
//     which is caught by the filepath.Base or directory-reference checks
//     further down. The backslash clause is the one clause in that function
//     with a written justification ("filepath.Base on Linux does not treat it
//     as a separator") and the one clause no case reaches.
//
// Both tests live in package store rather than store_test so they exercise
// Put through the same in-package path production uses, with a stub object
// store: the assertions are about the guards, and a real backend would only
// add ways for the test to fail for unrelated reasons.

// countingObjects is an objectstore.Store that records how many bytes reached
// it. Put's size check runs before any object write, so "did anything reach
// the backend at all" is the sharpest available evidence that the ceiling
// rejected the content rather than storing it and erroring afterwards.
type countingObjects struct {
	puts  int
	bytes int64
	blobs map[string][]byte
}

func newCountingObjects() *countingObjects {
	return &countingObjects{blobs: map[string][]byte{}}
}

func (c *countingObjects) PutObject(_ context.Context, key string, body io.Reader, size int64, _ objectstore.PutOptions) (*objectstore.Object, error) {
	var buf bytes.Buffer
	n, err := io.Copy(&buf, body)
	if err != nil {
		return nil, err
	}
	c.puts++
	c.bytes += n
	c.blobs[key] = buf.Bytes()
	return &objectstore.Object{Key: key, Size: size}, nil
}

func (c *countingObjects) GetObject(_ context.Context, key string) (io.ReadCloser, *objectstore.Object, error) {
	b, ok := c.blobs[key]
	if !ok {
		return nil, nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), &objectstore.Object{Key: key, Size: int64(len(b))}, nil
}

func (c *countingObjects) HeadObject(_ context.Context, key string) (*objectstore.Object, error) {
	b, ok := c.blobs[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return &objectstore.Object{Key: key, Size: int64(len(b))}, nil
}

// TestPutEnforcesMaxArtifactBytesAtTheBoundary pins the 16 MiB ceiling from
// both sides. One side alone is not enough:
//
//   - Only asserting that oversize content is rejected leaves `>` free to
//     become `>=`, which silently makes the real limit 16 MiB minus one byte —
//     a cap that rejects exactly the largest artifact allowed by
//     MaxArtifactBytes.
//   - Only asserting that at-cap content is accepted leaves the whole check
//     deletable.
//
// What breaks without the ceiling: Put takes []byte precisely so the digest is
// computed here rather than trusted from the caller, and MaxArtifactBytes is
// the only thing bounding that decision's memory cost. Removing it means an
// artifact upload sizes the server's heap, and the failure surfaces as an OOM
// in an unrelated request rather than as a rejected Put. The upload can then
// reach object storage before a backend-specific size or transport limit
// surfaces — the accept/reject decision must happen before any of that.
func TestPutEnforcesMaxArtifactBytesAtTheBoundary(t *testing.T) {
	ctx := context.Background()

	t.Run("exactly at the cap is accepted", func(t *testing.T) {
		objects := newCountingObjects()
		as := NewArtifactStore(objects, nil)

		ref, err := as.Put(ctx, make([]byte, MaxArtifactBytes), ArtifactMeta{Filename: "atcap.wasm"})
		if err != nil {
			t.Fatalf("Put of exactly MaxArtifactBytes (%d) failed: %v; the ceiling is "+
				"off by one and rejects an artifact exactly at MaxArtifactBytes",
				int64(MaxArtifactBytes), err)
		}
		if ref.Size != MaxArtifactBytes {
			t.Fatalf("ref.Size = %d, want %d", ref.Size, int64(MaxArtifactBytes))
		}
	})

	t.Run("one byte over the cap is rejected before anything is stored", func(t *testing.T) {
		objects := newCountingObjects()
		as := NewArtifactStore(objects, nil)

		_, err := as.Put(ctx, make([]byte, MaxArtifactBytes+1), ArtifactMeta{Filename: "over.wasm"})
		if !errors.Is(err, ErrArtifactTooLarge) {
			t.Fatalf("Put of MaxArtifactBytes+1 returned err = %v, want ErrArtifactTooLarge: "+
				"the size ceiling is not enforced, so artifact size bounds server heap", err)
		}
		if objects.puts != 0 {
			t.Fatalf("object store received %d Put(s) totalling %d bytes for a rejected "+
				"artifact, want 0: the ceiling must reject before hashing and storing",
				objects.puts, objects.bytes)
		}
	})
}

// TestValidateFilenameRejectsBackslashSeparatedPaths pins the one clause in
// ValidateFilename that no existing case reaches. The function's own comment
// states why it is there: filepath.Base on Linux does not treat "\" as a
// separator, so "build\\tagger.wasm" survives the `filepath.Base(name) != name`
// check below it and would be accepted as one long, legal-looking base name.
//
// The clause covers NUL too, and the sub-tests below separate them: a single
// strings.ContainsAny call means dropping either character from the cutset is
// one edit, and the existing integration case list contains a NUL case but no
// backslash case, so only the backslash half is genuinely unpinned. Asserting
// them apart keeps that asymmetry visible instead of letting one case stand in
// for both.
//
// What breaks without it: a Windows-style path becomes a stored artifact
// filename, which is then handed to the index as an identity component and,
// on any consumer that does treat "\" as a separator, resolves outside the
// directory the bare-base-name rule exists to confine it to.
func TestValidateFilenameRejectsBackslashSeparatedPaths(t *testing.T) {
	t.Run("backslash", func(t *testing.T) {
		for _, name := range []string{
			`build\tagger.wasm`,
			`..\..\windows\system32\evil.wasm`,
			`\absolute\rooted.wasm`,
		} {
			got, err := ValidateFilename(name)
			if !errors.Is(err, ErrInvalidFilename) {
				t.Errorf("ValidateFilename(%q) = (%q, %v), want ErrInvalidFilename: "+
					"filepath.Base does not split on backslash, so this passes through "+
					"as a single base name", name, got, err)
				continue
			}
			// ValidateFilename quotes the name with %q, which escapes the
			// backslashes — so the literal `name` is not a substring of the
			// message and comparing against it directly would always fail.
			if !strings.Contains(err.Error(), fmt.Sprintf("%q", name)) {
				t.Errorf("ValidateFilename(%q) error = %v, want it to quote the rejected "+
					"name so the caller learns what was wrong", name, err)
			}
		}
	})

	t.Run("NUL", func(t *testing.T) {
		if _, err := ValidateFilename("has\x00nul.wasm"); !errors.Is(err, ErrInvalidFilename) {
			t.Errorf("ValidateFilename with an embedded NUL returned %v, want ErrInvalidFilename", err)
		}
	})

	t.Run("a bare base name still passes", func(t *testing.T) {
		got, err := ValidateFilename("tagger.wasm")
		if err != nil {
			t.Fatalf("ValidateFilename(\"tagger.wasm\") error = %v, want it accepted", err)
		}
		if got != "tagger.wasm" {
			t.Fatalf("ValidateFilename returned %q, want it unchanged", got)
		}
	})
}

// TestValidateVersionRules pins the version charset and the reserved prefix.
//
// The version is the only carrier of "which source built these bytes" — the
// digest answers "which bytes" and cross-compilation is not reproducible, so
// nothing can recover a source revision from content. That is why '+' is
// admitted: it is semver's build-metadata separator and the commit lands after
// it.
func TestValidateVersionRules(t *testing.T) {
	t.Run("accepts", func(t *testing.T) {
		for _, v := range []string{
			"v1.4.0+g8f3a2c1", // the shape this whole design exists to carry
			"v1",
			"1.0.0-rc.1",
			"build_2026.08.29",
			strings.Repeat("v", MaxVersionBytes), // exactly at the column width
		} {
			got, err := ValidateVersion(v)
			if err != nil {
				t.Errorf("ValidateVersion(%q) rejected a legal version: %v", v, err)
			}
			if got != v {
				t.Errorf("ValidateVersion(%q) returned %q; it must return the value unchanged, "+
					"never a sanitized one — a caller whose version was rewritten would bind an "+
					"identity it never asked for", v, got)
			}
		}
	})

	t.Run("rejects", func(t *testing.T) {
		for _, tc := range []struct {
			v   string
			why string
		}{
			{"", "empty: Put's own defaulting handles the empty case before this is reached"},
			{strings.Repeat("v", MaxVersionBytes+1), "over the VARCHAR(64) column width: MySQL would truncate it into a different identity"},
			{"v1 ", "trailing space"},
			{" v1", "leading space"},
			{"v 1", "interior space"},
			{"v1/../x", "path separators have no business in a version"},
			{"<script>alert(1)</script>", "rendered next to the filename on the ops page"},
			{"v1:2", "colon: reserved by the digest form sha256:<hex>"},
			{"sha256-deadbeef1234", "impersonates a DefaultVersion output"},
			{"SHA256-deadbeef1234", "impersonates a DefaultVersion output; the deception is visual, so the check is case-insensitive"},
		} {
			if _, err := ValidateVersion(tc.v); err == nil {
				t.Errorf("ValidateVersion(%q) accepted an illegal version (%s)", tc.v, tc.why)
			} else if !errors.Is(err, ErrInvalidVersion) {
				t.Errorf("ValidateVersion(%q) must return ErrInvalidVersion so callers can match "+
					"the sentinel; got %v", tc.v, err)
			}
		}
	})
}

// TestPutDoesNotValidateItsOwnDefault is the regression guard for the ordering
// trap: DefaultVersion deliberately emits the "sha256-" prefix that
// ValidateVersion rejects, so validating after defaulting would fail every Put
// that omits a version — which today is every non-test caller
// (sdk/xflow/artifact_resolve.go:117-120 never sets one).
func TestPutDoesNotValidateItsOwnDefault(t *testing.T) {
	ctx := context.Background()
	idx := &recordingIndex{}
	as := NewArtifactStore(newCountingObjects(), idx)

	content := []byte("wasm-ish bytes")
	ref, err := as.Put(ctx, content, ArtifactMeta{Filename: "d.wasm", Namespace: "ns"})
	if err != nil {
		t.Fatalf("Put with an empty version must succeed and fall back to DefaultVersion; got %v", err)
	}
	want := DefaultVersion(ref.Digest)
	if len(idx.bound) != 1 {
		t.Fatalf("want exactly 1 identity bound, got %d", len(idx.bound))
	}
	if idx.bound[0].Version != want {
		t.Errorf("empty version must bind DefaultVersion(%s) = %q; got %q",
			ref.Digest, want, idx.bound[0].Version)
	}
}

// TestPutRejectsBadVersionBeforeWritingBytes pins that validation happens on the
// way in, not on the way to the index. A version rejected only at Bind time
// would leave the blob already written to the object store — an orphan the
// caller cannot see and nothing ever collects (store/objectstore has no delete
// path by design, objectstore.go:79-83).
func TestPutRejectsBadVersionBeforeWritingBytes(t *testing.T) {
	ctx := context.Background()
	objects := newCountingObjects()
	idx := &recordingIndex{}
	as := NewArtifactStore(objects, idx)

	_, err := as.Put(ctx, []byte("bytes"), ArtifactMeta{
		Filename:  "bad.wasm",
		Namespace: "ns",
		Version:   "sha256-cafebabe0000",
	})
	if !errors.Is(err, ErrInvalidVersion) {
		t.Fatalf("Put must reject a reserved-prefix version with ErrInvalidVersion; got %v", err)
	}
	// countingObjects.puts is a plain field on the fake defined earlier in this
	// same file (store/artifact_guards_test.go:41-49), not a method.
	if objects.puts != 0 {
		t.Errorf("a rejected Put must not write bytes; PutObject was called %d time(s)", objects.puts)
	}
	if objects.bytes != 0 {
		t.Errorf("a rejected Put must not write bytes; %d byte(s) reached the object store", objects.bytes)
	}
	if len(idx.bound) != 0 {
		t.Errorf("a rejected Put must not bind an identity; %d binding(s) recorded", len(idx.bound))
	}
}

// recordingIndex is a minimal ArtifactIndex that remembers what was bound.
type recordingIndex struct {
	bound []ArtifactIdentity
}

func (r *recordingIndex) Bind(_ context.Context, id ArtifactIdentity) error {
	r.bound = append(r.bound, id)
	return nil
}

func (r *recordingIndex) HasReference(context.Context, string, string) (bool, error) {
	return false, nil
}

func (r *recordingIndex) CountReferences(context.Context, string) (int64, error) {
	return 0, nil
}

// ListLatestVersions is unused by these tests: they exercise the Put/Bind
// guards, not the ops-page read path, which is covered against real MySQL in
// test/integration/sqlstore_artifact_test.go.
func (r *recordingIndex) ListLatestVersions(context.Context, string, ListOptions) ([]*ArtifactVersion, error) {
	return nil, nil
}

// ListVersions is unused by these tests; see ListLatestVersions above.
func (r *recordingIndex) ListVersions(context.Context, string, string, ListOptions) ([]*ArtifactVersion, error) {
	return nil, nil
}
