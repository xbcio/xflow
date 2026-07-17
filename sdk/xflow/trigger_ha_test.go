package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/local"
)

func TestTriggerDedupSharedPrimitivesAllowsOnlyOneInstance(t *testing.T) {
	primitives := local.New().TriggerPrimitives()
	ctx := context.Background()

	first, err := primitives.Dedup(ctx, "event:1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := primitives.Dedup(ctx, "event:1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !first || second {
		t.Fatalf("dedup results = %v/%v, want true/false", first, second)
	}
}

func TestCronSingletonSharedPrimitivesAllowsOnlyOneInstance(t *testing.T) {
	primitives := local.New().TriggerPrimitives()
	ctx := context.Background()

	lock, ok, err := primitives.TryLock(ctx, "cron:wf:node", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("first lock was not acquired")
	}
	t.Cleanup(func() { _ = lock.Release(context.Background()) })

	_, ok, err = primitives.TryLock(ctx, "cron:wf:node", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("second lock was acquired; want singleton behavior")
	}
}
