package engine

import (
	"context"
	"testing"
)

type fakeEngine struct{ name string }

func (e *fakeEngine) Name() string { return e.name }
func (e *fakeEngine) Execute(_ context.Context, _ Source, _ map[string]any, _ Helpers) (any, error) {
	return map[string]any{"ok": true}, nil
}

// TestRegisterAndLookup covers that BOTH halves of the registry key
// participate.
//
// It used to register one engine and look up that same pair, which proved
// neither. The key is registryKey{language, runtime}; collapse it to
// {language, ""} and Register/Lookup still agree, so the round trip succeeds
// and the name matches. Collapse it to {"", runtime} and the same thing
// happens. TestLookupUnknown does not rescue it either — ("nope", "nope")
// differs in both components, so it misses under any of the three keyings.
//
// Two runtimes under one language and two languages sharing a runtime name
// pin down both.
func TestRegisterAndLookup(t *testing.T) {
	Register("js", "fakeA", func() Engine { return &fakeEngine{name: "js/fakeA"} })
	Register("js", "fakeB", func() Engine { return &fakeEngine{name: "js/fakeB"} })
	Register("wasm", "fakeA", func() Engine { return &fakeEngine{name: "wasm/fakeA"} })

	for _, c := range []struct{ language, runtime, want string }{
		{"js", "fakeA", "js/fakeA"},
		{"js", "fakeB", "js/fakeB"},
		{"wasm", "fakeA", "wasm/fakeA"},
	} {
		e, ok := Lookup(c.language, c.runtime)
		if !ok {
			t.Fatalf("Lookup(%q, %q): not registered", c.language, c.runtime)
		}
		if got := e.Name(); got != c.want {
			t.Errorf("Lookup(%q, %q).Name() = %q, want %q; the registry key is not "+
				"discriminating on both language and runtime",
				c.language, c.runtime, got, c.want)
		}
	}
}

// TestLookupUnknown covers a miss where NEITHER component is registered, and
// the two one-component-off misses that a collapsed key would turn into hits.
func TestLookupUnknown(t *testing.T) {
	Register("js", "known", func() Engine { return &fakeEngine{name: "js/known"} })

	for _, c := range []struct{ language, runtime, why string }{
		{"nope", "nope", "neither component registered"},
		{"js", "nope", "language registered, runtime is not"},
		{"nope", "known", "runtime registered, language is not"},
	} {
		if e, ok := Lookup(c.language, c.runtime); ok {
			t.Errorf("Lookup(%q, %q) = %q, want a miss (%s)",
				c.language, c.runtime, e.Name(), c.why)
		}
	}
}
