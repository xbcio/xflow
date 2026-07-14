package engine

import "testing"

func TestMapResult_Object(t *testing.T) {
	out := MapResult(map[string]any{"status": "ok", "n": 1.0})
	if out["status"] != "ok" || out["n"] != 1.0 {
		t.Fatalf("object passthrough wrong: %v", out)
	}
}

func TestMapResult_Scalar(t *testing.T) {
	out := MapResult(42.0)
	if out["result"] != 42.0 {
		t.Fatalf("scalar should wrap as result, got %v", out)
	}
}

func TestMapResult_Nil(t *testing.T) {
	out := MapResult(nil)
	if len(out) != 0 {
		t.Fatalf("nil should map to empty object, got %v", out)
	}
}
