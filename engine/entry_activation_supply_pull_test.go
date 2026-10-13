package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// The Supplies list is part of the activation directive's wire format, and it
// is written by one control-plane version and read by runners that may lag
// behind it. These tests pin both directions of that compatibility claim: an
// older store-backed requirement must not grow a pull_source key, and a pull
// requirement must survive the trip with its source intact.
func TestSupplyRequirementPullSourceWire(t *testing.T) {
	t.Run("store-backed requirement is unchanged", func(t *testing.T) {
		raw, err := json.Marshal(SupplyRequirement{Node: "rules", Resource: "shared", RequireReady: true})
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		if _, ok := m["pull_source"]; ok {
			t.Fatalf("a nil PullSource marshals a pull_source key: %s", raw)
		}
	})

	t.Run("pull requirement round-trips", func(t *testing.T) {
		want := SupplyRequirement{
			Node:         "rules",
			Resource:     "rules",
			RequireReady: true,
			PullSource: &types.SupplyPullSource{
				URL:            "https://config.example.com/rules.json",
				Authentication: "rules_token",
				PollInterval:   30 * time.Second,
			},
		}
		raw, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var got SupplyRequirement
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got.PullSource == nil {
			t.Fatalf("decoded requirement lost its pull source: %s", raw)
		}
		if *got.PullSource != *want.PullSource {
			t.Fatalf("pull source = %+v, want %+v", *got.PullSource, *want.PullSource)
		}
	})
}
