package engine

import "testing"

func TestDefaultHelpers_Base64RoundTrip(t *testing.T) {
	h := DefaultHelpers()
	enc := h.Base64Encode("hello")
	if enc != "aGVsbG8=" {
		t.Fatalf("encode = %q, want aGVsbG8=", enc)
	}
	dec, err := h.Base64Decode(enc)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if dec != "hello" {
		t.Fatalf("decode = %q, want hello", dec)
	}
}

func TestDefaultHelpers_Base64DecodeInvalid(t *testing.T) {
	if _, err := DefaultHelpers().Base64Decode("!!not-base64!!"); err == nil {
		t.Fatal("expected error for invalid base64")
	}
}
