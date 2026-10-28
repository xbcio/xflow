package blob

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func validRef() Ref {
	return Ref{
		Namespace: "default",
		Digest:    strings.Repeat("ab", 32),
		Size:      10,
		Stored:    6,
		Codec:     CodecZstd,
	}
}

func TestRefValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(r *Ref)
		wantErr bool
	}{
		{"valid zstd", func(r *Ref) {}, false},
		{"valid uncompressed", func(r *Ref) { r.Codec = ""; r.Stored = r.Size }, false},
		{"valid empty blob", func(r *Ref) { r.Size = 0; r.Stored = 0; r.Codec = "" }, false},
		{"valid zstd larger than original", func(r *Ref) { r.Stored = r.Size + 20 }, false},
		{"valid zstd empty payload", func(r *Ref) { r.Stored = 0 }, false},
		{"no codec stored smaller", func(r *Ref) { r.Codec = ""; r.Stored = r.Size - 1 }, true},
		{"no codec stored larger", func(r *Ref) { r.Codec = ""; r.Stored = r.Size + 1 }, true},
		{"empty namespace", func(r *Ref) { r.Namespace = "" }, true},
		{"namespace with slash", func(r *Ref) { r.Namespace = "a/b" }, true},
		{"namespace traversal", func(r *Ref) { r.Namespace = "../x" }, true},
		{"digest empty", func(r *Ref) { r.Digest = "" }, true},
		{"digest short", func(r *Ref) { r.Digest = r.Digest[:63] }, true},
		{"digest long", func(r *Ref) { r.Digest += "0" }, true},
		{"digest uppercase", func(r *Ref) { r.Digest = strings.ToUpper(r.Digest) }, true},
		{"digest not hex", func(r *Ref) { r.Digest = "g" + r.Digest[1:] }, true},
		{"size negative", func(r *Ref) { r.Size = -1 }, true},
		{"stored negative", func(r *Ref) { r.Stored = -1 }, true},
		{"codec unknown", func(r *Ref) { r.Codec = "gzip" }, true},
		{"codec uppercase", func(r *Ref) { r.Codec = "ZSTD" }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validRef()
			tt.mutate(&r)
			err := r.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate(%+v) error = %v, wantErr %t", r, err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, ErrInvalidRef) {
				t.Fatalf("Validate(%+v) error = %v, want it to wrap ErrInvalidRef", r, err)
			}
		})
	}
}

func TestRef_JSON(t *testing.T) {
	t.Run("round trip", func(t *testing.T) {
		in := validRef()
		raw, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("Marshal(%+v) error = %v", in, err)
		}
		var out Ref
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("Unmarshal(%s) error = %v", raw, err)
		}
		if out != in {
			t.Fatalf("round trip = %+v, want %+v", out, in)
		}
	})

	t.Run("field names", func(t *testing.T) {
		raw, err := json.Marshal(validRef())
		if err != nil {
			t.Fatalf("Marshal error = %v", err)
		}
		var fields map[string]any
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatalf("Unmarshal(%s) error = %v", raw, err)
		}
		for _, name := range []string{"ns", "sha256", "size", "stored", "codec"} {
			if _, ok := fields[name]; !ok {
				t.Errorf("JSON %s has no field %q", raw, name)
			}
		}
		if len(fields) != 5 {
			t.Errorf("JSON %s has %d fields, want 5", raw, len(fields))
		}
	})

	t.Run("empty codec is omitted", func(t *testing.T) {
		r := validRef()
		r.Codec = ""
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("Marshal(%+v) error = %v", r, err)
		}
		if strings.Contains(string(raw), "codec") {
			t.Fatalf("Marshal(%+v) = %s, want no codec field", r, raw)
		}
	})
}
