package types

import (
	"errors"
	"testing"
)

// TestNodeTimeoutErrorCodeIsWireStable pins the literal on purpose: the code is
// persisted and sent over the Runner Protocol, so renaming the constant's value
// is a wire break and must fail here.
func TestNodeTimeoutErrorCodeIsWireStable(t *testing.T) {
	if NodeTimeoutErrorCode != "node.timeout" {
		t.Fatalf("NodeTimeoutErrorCode = %q, want %q", NodeTimeoutErrorCode, "node.timeout")
	}

	var ce *ClassifiedError
	err := error(NewPermanentError(NodeTimeoutErrorCode, "x"))
	if !errors.As(err, &ce) {
		t.Fatalf("errors.As(%v, *ClassifiedError) = false, want true", err)
	}
	if ce.Code != "node.timeout" {
		t.Fatalf("ClassifiedError.Code = %q, want %q", ce.Code, "node.timeout")
	}
	if !IsPermanent(err) {
		t.Fatalf("IsPermanent(%v) = false, want true", err)
	}
}
