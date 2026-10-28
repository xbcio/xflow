package blob_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/objectstore"
)

// secretMarker stands for the part of a path that must never reach an error
// message: a directory name that can hold a user name or a tenant.
const secretMarker = "SECRETPATH"

// leakyPathError returns an error shaped like what the file system hands back:
// an *fs.PathError whose text holds the full path.
func leakyPathError(op string, errno syscall.Errno) error {
	return &fs.PathError{Op: op, Path: "/var/lib/" + secretMarker + "/blobs/ab/cd/abcdef", Err: errno}
}

// assertNoLeak fails when err's text holds the marker, a slash-separated path, or
// when the original *fs.PathError can still be reached through the chain.
func assertNoLeak(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: error = nil, want an error", what)
	}
	text := err.Error()
	if strings.Contains(text, secretMarker) || strings.Contains(text, "/var/lib") || strings.Contains(text, "blobs/") {
		t.Errorf("%s: error text %q leaks a path", what, text)
	}
	var pe *fs.PathError
	if errors.As(err, &pe) {
		t.Errorf("%s: errors.As reaches the original *fs.PathError %v", what, pe)
	}
}

func TestResolveClassifiesErrors(t *testing.T) {
	permanent := []error{
		blob.ErrNotFound, blob.ErrCorrupt, blob.ErrNamespaceMismatch,
		blob.ErrInvalidRef, blob.ErrTooLarge,
	}
	cases := []struct {
		name string
		got  error
		want error // the one permanent class the result must match; nil for transient
	}{
		{"NotFound", fmt.Errorf("%w: %s/obj", blob.ErrNotFound, secretMarker), blob.ErrNotFound},
		{"Corrupt", fmt.Errorf("%w (object %s)", blob.ErrCorrupt, secretMarker), blob.ErrCorrupt},
		{"NamespaceMismatch", fmt.Errorf("%w: ref %q", blob.ErrNamespaceMismatch, "other"), blob.ErrNamespaceMismatch},
		{"InvalidRef", fmt.Errorf("%w: bad", blob.ErrInvalidRef), blob.ErrInvalidRef},
		{"TooLarge", fmt.Errorf("%w: 99 > 16", blob.ErrTooLarge), blob.ErrTooLarge},
		{"Transient", errors.New("connection reset"), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newPolicyRigWith(t, 8, false, func(s blob.Store) blob.Store { return &faultStore{Store: s, getErr: tc.got} })
			ref, ok, err := rig.externalize(t, payload(100))
			if err != nil || !ok {
				t.Fatalf("setup Externalize = (%v, %v)", ok, err)
			}
			_, err = rig.policy.Resolve(context.Background(), policyNS, ref)
			if err == nil {
				t.Fatal("Resolve error = nil, want an error")
			}
			for _, p := range permanent {
				if got, want := errors.Is(err, p), p == tc.want; got != want {
					t.Errorf("errors.Is(err, %v) = %v, want %v (err = %q)", p, got, want, err)
				}
			}
			if got, want := errors.Is(err, blob.ErrTransient), tc.want == nil; got != want {
				t.Errorf("errors.Is(err, ErrTransient) = %v, want %v (err = %q)", got, want, err)
			}
			if tc.want == nil && !errors.Is(err, tc.got) {
				t.Errorf("a transient error no longer matches its cause: %q", err)
			}
		})
	}
}

// TestResolveDistinguishesRealStoreFailures runs the three permanent conditions
// through a real ObjectBacked store instead of a scripted error.
func TestResolveDistinguishesRealStoreFailures(t *testing.T) {
	rig := newPolicyRig(t, 8)
	data := payload(100)
	ref, ok, err := rig.externalize(t, data)
	if err != nil || !ok {
		t.Fatalf("setup Externalize = (%v, %v)", ok, err)
	}
	ctx := context.Background()

	if _, err := rig.policy.Resolve(ctx, "another-ns", ref); !errors.Is(err, blob.ErrNamespaceMismatch) {
		t.Errorf("Resolve in another namespace error = %v, want ErrNamespaceMismatch", err)
	}
	missing := ref
	missing.Digest = strings.Repeat("b", 64)
	if _, err := rig.policy.Resolve(ctx, policyNS, missing); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Resolve of an unknown digest error = %v, want ErrNotFound", err)
	}
	// Damage the stored object in place: same key, different bytes.
	var damaged int
	err = filepath.WalkDir(rig.dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ref.Digest) {
			damaged++
			return os.WriteFile(path, append([]byte{0}, bytes.Repeat([]byte("y"), len(data))...), 0o600)
		}
		return err
	})
	if err != nil || damaged != 1 {
		t.Fatalf("damaging the object: walk error = %v, objects changed = %d, want nil and 1", err, damaged)
	}
	if _, err := rig.policy.Resolve(ctx, policyNS, ref); !errors.Is(err, blob.ErrCorrupt) || errors.Is(err, blob.ErrTransient) {
		t.Errorf("Resolve of damaged bytes error = %v, want ErrCorrupt and not transient", err)
	}
}

// pathStore is a Store whose every call fails with an error that carries a path.
type pathStore struct{ err error }

func (s pathStore) Put(context.Context, namespace.Namespace, []byte) (blob.Ref, error) {
	return blob.Ref{}, s.err
}

func (s pathStore) Get(context.Context, namespace.Namespace, blob.Ref) ([]byte, error) {
	return nil, s.err
}

func (s pathStore) Delete(context.Context, namespace.Namespace, blob.Ref) error { return s.err }

// pathIndex is an Index whose Link fails with an error that carries a path.
type pathIndex struct {
	blob.Index
	err error
}

func (x pathIndex) Link(context.Context, string, blob.Ref, time.Time) error { return x.err }

func TestPolicyErrorsNeverLeakPaths(t *testing.T) {
	ctx := context.Background()
	exp := time.Now().Add(time.Hour)
	ref := blob.Ref{Namespace: string(policyNS), Digest: strings.Repeat("c", 64), Size: 10, Stored: 10}
	for _, errno := range []syscall.Errno{syscall.EACCES, syscall.EIO, syscall.ENOSPC} {
		cause := leakyPathError("open", errno)
		t.Run(errno.Error(), func(t *testing.T) {
			policy, err := blob.NewPolicy(pathStore{err: cause}, blob.NewMemoryIndex(nil), blob.PolicyOptions{InlineMax: 4})
			if err != nil {
				t.Fatalf("NewPolicy error = %v", err)
			}
			_, _, err = policy.Externalize(ctx, policyNS, policyExec, payload(10), exp)
			assertNoLeak(t, "Put failure", err)
			if !errors.Is(err, blob.ErrTransient) || !errors.Is(err, errno) {
				t.Errorf("Put failure %q: want ErrTransient that still matches %v", err, errno)
			}

			_, err = policy.Resolve(ctx, policyNS, ref)
			assertNoLeak(t, "Get failure", err)
			if !errors.Is(err, blob.ErrTransient) || !errors.Is(err, errno) {
				t.Errorf("Get failure %q: want ErrTransient that still matches %v", err, errno)
			}

			mem := blob.NewMemoryIndex(nil)
			backed := blob.NewObjectBacked(objectstore.NewFSStore(t.TempDir()), blob.ObjectBackedOptions{Index: mem})
			policy, err = blob.NewPolicy(backed, pathIndex{Index: mem, err: cause}, blob.PolicyOptions{InlineMax: 4})
			if err != nil {
				t.Fatalf("NewPolicy error = %v", err)
			}
			_, _, err = policy.Externalize(ctx, policyNS, policyExec, payload(10), exp)
			assertNoLeak(t, "Link failure", err)
			if !errors.Is(err, blob.ErrTransient) {
				t.Errorf("Link failure %q: want ErrTransient", err)
			}
		})
	}
}

// TestPolicyErrorsFromRealFSStoreHidePath fails a real FSStore whose root holds
// the marker and checks every way an error can be rendered.
func TestPolicyErrorsFromRealFSStoreHidePath(t *testing.T) {
	ctx := context.Background()
	// The root is a regular file, so creating or opening anything below it fails
	// with ENOTDIR and an *fs.PathError that names the full path. That works the
	// same for a user with or without permission checks.
	root := filepath.Join(t.TempDir(), secretMarker)
	if err := os.WriteFile(root, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}
	mem := blob.NewMemoryIndex(nil)
	backed := blob.NewObjectBacked(objectstore.NewFSStore(root), blob.ObjectBackedOptions{Index: mem})
	policy, err := blob.NewPolicy(backed, mem, blob.PolicyOptions{InlineMax: 4})
	if err != nil {
		t.Fatalf("NewPolicy error = %v", err)
	}

	// Prove the setup does leak at the layer below, or the checks mean nothing.
	_, rawErr := backed.Put(ctx, policyNS, payload(10))
	if rawErr == nil || !strings.Contains(rawErr.Error(), secretMarker) {
		t.Fatalf("setup: ObjectBacked.Put error = %v, want one that contains the marker", rawErr)
	}

	_, _, putErr := policy.Externalize(ctx, policyNS, policyExec, payload(10), time.Now().Add(time.Hour))
	ref := blob.Ref{Namespace: string(policyNS), Digest: strings.Repeat("d", 64), Size: 10, Stored: 10}
	_, getErr := policy.Resolve(ctx, policyNS, ref)
	for what, err := range map[string]error{"Put": putErr, "Get": getErr} {
		assertNoLeak(t, what, err)
		if !errors.Is(err, blob.ErrTransient) {
			t.Errorf("%s: error %q is not transient", what, err)
		}
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
			if out := fmt.Sprintf(verb, err); strings.Contains(out, secretMarker) {
				t.Errorf("%s: %s renders as %q, which leaks the path", what, verb, out)
			}
		}
	}
}

func TestPolicyErrorsDoNotEchoHostileRef(t *testing.T) {
	rig := newPolicyRig(t, 8)
	hostile := blob.Ref{Namespace: string(policyNS), Digest: "../../etc/passwd\nforged log line", Size: 1, Stored: 1}
	_, err := rig.policy.Resolve(context.Background(), policyNS, hostile)
	if !errors.Is(err, blob.ErrInvalidRef) {
		t.Fatalf("Resolve(hostile ref) error = %v, want ErrInvalidRef", err)
	}
	if text := err.Error(); strings.Contains(text, "passwd") || strings.ContainsAny(text, "\n\r") {
		t.Errorf("error text %q echoes the hostile digest", text)
	}
	odd := blob.Ref{Namespace: "Bad/../Namespace", Digest: strings.Repeat("e", 64), Size: 1, Stored: 1}
	_, err = rig.policy.Resolve(context.Background(), "Bad/../Namespace", odd)
	if text := fmt.Sprint(err); err == nil || strings.Contains(text, "Namespace") || strings.Contains(text, "..") {
		t.Errorf("Resolve(hostile namespace) error = %q, want one without the namespace text", text)
	}
}
