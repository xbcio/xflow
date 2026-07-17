package conv

import (
	"testing"
	"time"
)

func TestPositiveInt(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		fallback int
		want     int
	}{
		{"int positive", 5, 1, 5},
		{"int zero uses fallback", 0, 7, 7},
		{"int negative uses fallback", -3, 7, 7},
		{"float positive", 4.0, 1, 4},
		{"string numeric", "12", 1, 12},
		{"string non-numeric uses fallback", "abc", 9, 9},
		{"nil uses fallback", nil, 3, 3},
		// cast.ToIntE(true) -> 1, which is positive, so it is returned.
		{"bool true converts to 1", true, 3, 1},
		{"bool false uses fallback", false, 3, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := PositiveInt(tc.input, tc.fallback); got != tc.want {
				t.Fatalf("PositiveInt(%v, %d) = %d, want %d", tc.input, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestNonEmptyStringSlice(t *testing.T) {
	t.Run("[]any with empties", func(t *testing.T) {
		got := NonEmptyStringSlice([]any{"a", "", "b", ""})
		want := []string{"a", "b"}
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	})
	t.Run("[]string", func(t *testing.T) {
		got := NonEmptyStringSlice([]string{"x", "", "y"})
		if len(got) != 2 || got[0] != "x" || got[1] != "y" {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("unsupported type returns nil", func(t *testing.T) {
		// cast coerces scalars to string slices; only a truly un-coercible
		// type yields nil. A struct value cannot be coerced.
		type s struct{ X int }
		if got := NonEmptyStringSlice(s{}); got != nil {
			t.Fatalf("expected nil, got %v", got)
		}
	})
	t.Run("nil returns nil", func(t *testing.T) {
		if got := NonEmptyStringSlice(nil); got != nil {
			t.Fatalf("expected nil, got %v", got)
		}
	})
}

func TestPositiveDuration(t *testing.T) {
	t.Run("valid string", func(t *testing.T) {
		d, err := PositiveDuration("5s")
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if d != 5*time.Second {
			t.Fatalf("got %v", d)
		}
	})
	t.Run("duration value", func(t *testing.T) {
		d, err := PositiveDuration(10 * time.Second)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if d != 10*time.Second {
			t.Fatalf("got %v", d)
		}
	})
	t.Run("nil is error", func(t *testing.T) {
		if _, err := PositiveDuration(nil); err == nil {
			t.Fatal("expected error for nil")
		}
	})
	t.Run("empty is error", func(t *testing.T) {
		if _, err := PositiveDuration(""); err == nil {
			t.Fatal("expected error for empty")
		}
	})
	t.Run("non-positive is error", func(t *testing.T) {
		if _, err := PositiveDuration("-5s"); err == nil {
			t.Fatal("expected error for negative")
		}
		if _, err := PositiveDuration(0); err == nil {
			t.Fatal("expected error for zero")
		}
	})
	t.Run("unparseable is error", func(t *testing.T) {
		if _, err := PositiveDuration("abc"); err == nil {
			t.Fatal("expected error for unparseable")
		}
	})
}

func TestToSlice(t *testing.T) {
	t.Run("[]any passthrough", func(t *testing.T) {
		in := []any{"a", 1}
		out, err := ToSlice(in)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(out) != 2 || out[0] != "a" || out[1] != 1 {
			t.Fatalf("got %v", out)
		}
	})
	t.Run("[]string", func(t *testing.T) {
		out, err := ToSlice([]string{"a", "b"})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(out) != 2 || out[0] != "a" || out[1] != "b" {
			t.Fatalf("got %v", out)
		}
	})
	t.Run("[]int", func(t *testing.T) {
		out, err := ToSlice([]int{1, 2, 3})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(out) != 3 || out[0] != 1 || out[2] != 3 {
			t.Fatalf("got %v", out)
		}
	})
	t.Run("[]float64", func(t *testing.T) {
		out, err := ToSlice([]float64{1.5, 2.5})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(out) != 2 || out[0] != 1.5 {
			t.Fatalf("got %v", out)
		}
	})
	t.Run("[]map[string]any", func(t *testing.T) {
		out, err := ToSlice([]map[string]any{{"k": 1}})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(out) != 1 {
			t.Fatalf("got %v", out)
		}
		m, ok := out[0].(map[string]any)
		if !ok || m["k"] != 1 {
			t.Fatalf("got %v", out)
		}
	})
	t.Run("unsupported type errors", func(t *testing.T) {
		if _, err := ToSlice(42); err == nil {
			t.Fatal("expected error for int")
		}
		if _, err := ToSlice("string"); err == nil {
			t.Fatal("expected error for string")
		}
	})
}
