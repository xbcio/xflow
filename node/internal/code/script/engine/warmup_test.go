package engine

import (
	"context"
	"errors"
	"testing"
)

func TestWarmup_NoEnginesRegistered(t *testing.T) {
	// Swap out the global list so this test stays isolated; restore on exit.
	saved := warmers
	warmers = nil
	defer func() { warmers = saved }()

	if err := Warmup(context.Background()); err != nil {
		t.Fatalf("Warmup with no warmers should be a no-op, got %v", err)
	}
}

func TestWarmup_RunsAllRegistered(t *testing.T) {
	saved := warmers
	warmers = nil
	defer func() { warmers = saved }()

	var calls int
	RegisterWarmer(func(context.Context) error { calls++; return nil })
	RegisterWarmer(func(context.Context) error { calls++; return nil })

	if err := Warmup(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 warmer calls, got %d", calls)
	}
}

func TestWarmup_PropagatesFirstError(t *testing.T) {
	saved := warmers
	warmers = nil
	defer func() { warmers = saved }()

	boom := errors.New("boom")
	var secondCalled bool
	RegisterWarmer(func(context.Context) error { return boom })
	RegisterWarmer(func(context.Context) error { secondCalled = true; return nil })

	err := Warmup(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
	if secondCalled {
		t.Fatal("Warmup should stop at the first error")
	}
}
