package exprx

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/types"
)

// BuildExprEnv must not get slower or allocate more as supply content grows:
// the env holds the registry's already-decoded shared map, never a copy.
func BenchmarkBuildExprEnvWithLargeSupply(b *testing.B) {
	reg := supply.NewRegistry()
	// ~1 MiB of JSON object content.
	big := make([]byte, 0, 1<<20)
	big = append(big, `{"rules":[`...)
	for i := 0; i < 20000; i++ {
		if i > 0 {
			big = append(big, ',')
		}
		big = append(big, `{"k":"vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"}`...)
	}
	big = append(big, `]}`...)
	if err := reg.Apply(context.Background(), supply.Snapshot{
		Name: "rules", Content: big, Hash: "h", Revision: 1, FetchedAt: time.Now(),
	}); err != nil {
		b.Fatalf("apply: %v", err)
	}
	extra := SuppliesEnv(reg)
	input := &types.Input{Data: map[string]any{"k": "v"}}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = BuildExprEnv(input, extra)
	}
}

// Baseline with no supply at all, to compare against.
func BenchmarkBuildExprEnvNoSupply(b *testing.B) {
	extra := SuppliesEnv(supply.NewRegistry())
	input := &types.Input{Data: map[string]any{"k": "v"}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = BuildExprEnv(input, extra)
	}
}
