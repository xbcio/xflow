package subgraph

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
)

func TestItemDeadline(t *testing.T) {
	early := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	late := early.Add(time.Minute)
	cases := []struct {
		name string
		a, b time.Time
		want time.Time
	}{
		{name: "neither", want: time.Time{}},
		{name: "constructor only", a: early, want: early},
		{name: "request only", b: early, want: early},
		{name: "request tighter", a: late, b: early, want: early},
		{name: "constructor tighter", a: early, b: late, want: early},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := itemDeadline(tc.a, tc.b); !got.Equal(tc.want) {
				t.Errorf("itemDeadline = %v, want %v", got, tc.want)
			}
		})
	}
}

// A batch whose body runs past the request's deadline must be stopped there,
// not left to finish: the deadline is the map node's own timeout, and a body
// that outlives it is precisely the gap this bound closes.
func TestExecuteBatchBodyStopsABodyPastTheRequestDeadline(t *testing.T) {
	handler := &sleepHandler{duration: 5 * time.Second}
	ex := deadlineTestExecutor(t, handler)
	pkg := buildSingleMemberPackage(t, 0)
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}
	// Constructor-time deadline deliberately zero: the long-lived callers build
	// their executor that way, so the bound has to come from the request.
	bodies := NewMapBodyExecutor(ex, true, time.Time{})

	start := time.Now()
	results, err := bodies.ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
		Body:     pkg,
		BodyHash: hash,
		Items:    []any{"only"},
		Deadline: time.Now().Add(200 * time.Millisecond),
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ExecuteBatchBody: %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("batch took %v, want it stopped near the 200ms deadline", elapsed)
	}
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("results = %+v, want the one item failed by the deadline", results)
	}
	if handler.completed.Load() > 0 {
		t.Fatal("body member ran to completion past the batch deadline")
	}
}
