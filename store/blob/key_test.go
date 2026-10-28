package blob

import (
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/namespace"
)

func TestValidateNamespace(t *testing.T) {
	tests := []struct {
		name    string
		ns      namespace.Namespace
		wantErr bool
	}{
		{"simple", "default", false},
		{"with dash and underscore", "team-a_1", false},
		{"with single dot inside", "a.b", false},
		{"max length", namespace.Namespace(strings.Repeat("a", namespace.MaxNameLen)), false},
		{"empty", "", true},
		{"slash", "a/b", true},
		{"leading slash", "/etc", true},
		{"parent segment", "..", true},
		{"embedded parent", "a..b", true},
		{"traversal", "../x", true},
		{"single dot", ".", true},
		{"colon", "a:b", true},
		{"too long", namespace.Namespace(strings.Repeat("a", namespace.MaxNameLen+1)), true},
		{"space", "a b", true},
		{"tab", "a\tb", true},
		{"newline", "a\nb", true},
		{"NUL", "a\x00b", true},
		{"DEL", "a\x7fb", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNamespace(tt.ns)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateNamespace(%q) error = %v, wantErr %t", string(tt.ns), err, tt.wantErr)
			}
		})
	}
}

func TestObjectKey(t *testing.T) {
	const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	t.Run("format", func(t *testing.T) {
		got, err := objectKey("team-a", digest)
		if err != nil {
			t.Fatalf("objectKey error = %v", err)
		}
		// "team-a" hex-encoded.
		want := "blobs/7465616d2d61/sha256/01/23/" + digest
		if got != want {
			t.Fatalf("objectKey(team-a, %s) = %q, want %q", digest, got, want)
		}
	})

	t.Run("only lowercase hex and slashes", func(t *testing.T) {
		for _, ns := range []namespace.Namespace{"Tenant", "team-a_1", "a.b", "é", "名字"} {
			got, err := objectKey(ns, digest)
			if err != nil {
				t.Fatalf("objectKey(%q) error = %v", string(ns), err)
			}
			for _, c := range got {
				if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '/' {
					t.Fatalf("objectKey(%q) = %q contains %q, want only [a-z0-9/]", string(ns), got, c)
				}
			}
		}
	})

	t.Run("distinct under case and normalization folding", func(t *testing.T) {
		pairs := []struct {
			name string
			a, b namespace.Namespace
		}{
			{"case", "Tenant", "tenant"},
			{"NFC vs NFD", "\u00e9", "e\u0301"},
		}
		for _, p := range pairs {
			ka, err := objectKey(p.a, digest)
			if err != nil {
				t.Fatalf("%s: objectKey(%q) error = %v", p.name, string(p.a), err)
			}
			kb, err := objectKey(p.b, digest)
			if err != nil {
				t.Fatalf("%s: objectKey(%q) error = %v", p.name, string(p.b), err)
			}
			if ka == kb {
				t.Fatalf("%s: objectKey(%q) == objectKey(%q) = %q", p.name, string(p.a), string(p.b), ka)
			}
			// Compare case-insensitively, as a case-folding filesystem would.
			// EqualFold must report them as different: that is the assertion.
			if strings.EqualFold(ka, kb) {
				t.Fatalf("%s: keys %q and %q are equal when case is ignored", p.name, ka, kb)
			}
		}
	})

	badDigests := []struct {
		name   string
		digest string
	}{
		{"empty", ""},
		{"too short", digest[:63]},
		{"too long", digest + "0"},
		{"uppercase", strings.ToUpper(digest)},
		{"not hex", "zz" + digest[2:]},
		{"path in digest", "../" + digest[3:]},
	}
	for _, tt := range badDigests {
		t.Run("bad digest "+tt.name, func(t *testing.T) {
			got, err := objectKey("team-a", tt.digest)
			if !errors.Is(err, ErrInvalidRef) {
				t.Fatalf("objectKey(team-a, %q) = (%q, %v), want ErrInvalidRef", tt.digest, got, err)
			}
		})
	}

	t.Run("traversal namespace", func(t *testing.T) {
		for _, ns := range []namespace.Namespace{"../x", "a/b", "..", "", "a b"} {
			got, err := objectKey(ns, digest)
			if !errors.Is(err, ErrInvalidRef) {
				t.Fatalf("objectKey(%q, ...) = (%q, %v), want ErrInvalidRef", string(ns), got, err)
			}
		}
	})
}
