package script

import (
	"context"
	"testing"
)

type fakeEngine struct{ name string }

func (e *fakeEngine) Name() string { return e.name }
func (e *fakeEngine) Execute(_ context.Context, _ string, _ map[string]any, _ Helpers) (any, error) {
	return map[string]any{"ok": true}, nil
}

func TestRegisterAndLookup(t *testing.T) {
	Register("js", "fake", func() Engine { return &fakeEngine{name: "js/fake"} })

	e, ok := Lookup("js", "fake")
	if !ok {
		t.Fatal("expected fake engine registered")
	}
	if e.Name() != "js/fake" {
		t.Fatalf("name = %q, want js/fake", e.Name())
	}
}

func TestLookupUnknown(t *testing.T) {
	if _, ok := Lookup("nope", "nope"); ok {
		t.Fatal("expected lookup miss for unknown engine")
	}
}
