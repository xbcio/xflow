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
//     a cap that rejects exactly the artifact a caller sized to fit the
//     MEDIUMBLOB column the constant is documented to match.
//   - Only asserting that at-cap content is accepted leaves the whole check
//     deletable.
//
// What breaks without the ceiling: Put takes []byte precisely so the digest is
// computed here rather than trusted from the caller, and MaxArtifactBytes is
// the only thing bounding that decision's memory cost. Removing it means an
// artifact upload sizes the server's heap, and the failure surfaces as an OOM
// in an unrelated request rather than as a rejected Put. The blob then also
// exceeds the MEDIUMBLOB column and fails at the SQL layer with a driver
// error, after the bytes have already been hashed and written to the object
// store — the accept/reject decision must happen before any of that.
func TestPutEnforcesMaxArtifactBytesAtTheBoundary(t *testing.T) {
	ctx := context.Background()

	t.Run("exactly at the cap is accepted", func(t *testing.T) {
		objects := newCountingObjects()
		as := NewArtifactStore(objects, nil)

		ref, err := as.Put(ctx, make([]byte, MaxArtifactBytes), ArtifactMeta{Filename: "atcap.wasm"})
		if err != nil {
			t.Fatalf("Put of exactly MaxArtifactBytes (%d) failed: %v; the ceiling is "+
				"off by one and rejects the largest artifact the MEDIUMBLOB column holds",
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
