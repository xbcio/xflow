package engine

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// These tests cover the ingest-side projection for U-9. NodeSnapshot.ErrorDetails
// reaches an operator-facing API, and its content is chosen by the node rather
// than by the engine, so the conversion from an arbitrary runner-supplied map to
// something servable is the security-relevant step. It is asserted here, at the
// boundary, rather than only end-to-end.

func TestBoundedErrorDetailsKeepsScalarsAndDropsStructuredPayload(t *testing.T) {
	got := boundedErrorDetails(map[string]any{
		"source":       "endpoints",
		"phase":        "connect",
		"attempt":      3,
		"retryable":    false,
		"score":        1.5,
		"nothing":      nil,
		"nested":       map[string]any{"leaked": "payload"},
		"list":         []any{"a", "b"},
		"structs":      []string{"x"},
		"complex":      complex(1, 2),
		"func":         func() {},
		"nested_extra": map[string]any{"deep": map[string]any{"deeper": "value"}},
	})

	for _, key := range []string{"nested", "list", "structs", "complex", "func", "nested_extra"} {
		if _, ok := got[key]; ok {
			t.Errorf("boundedErrorDetails kept non-scalar %q = %#v; a nested value is the "+
				"shape a payload dump takes", key, got[key])
		}
	}
	for key, want := range map[string]any{
		"source":    "endpoints",
		"phase":     "connect",
		"attempt":   3,
		"retryable": false,
		"score":     1.5,
	} {
		if got[key] != want {
			t.Errorf("boundedErrorDetails[%q] = %#v, want %#v", key, got[key], want)
		}
	}
	if v, ok := got["nothing"]; !ok || v != nil {
		t.Errorf("boundedErrorDetails[nothing] = %#v (present %v), want an explicit nil", v, ok)
	}
}

func TestBoundedErrorDetailsCapsKeyCountDeterministically(t *testing.T) {
	// More than the cap, inserted in an order that says nothing about which
	// keys should survive — Go map iteration is randomized, so survival must
	// not depend on it.
	source := make(map[string]any, 40)
	for i := 0; i < 40; i++ {
		source[fmt.Sprintf("key-%02d", i)] = i
	}

	first := boundedErrorDetails(source)
	if len(first) != maxErrorDetailKeys {
		t.Fatalf("boundedErrorDetails kept %d keys, want the cap %d", len(first), maxErrorDetailKeys)
	}

	// The cap must be applied to a stable order, or two reads of the same
	// immutable failure could return different subsets.
	want := make([]string, 0, maxErrorDetailKeys)
	for i := 0; i < maxErrorDetailKeys; i++ {
		want = append(want, fmt.Sprintf("key-%02d", i))
	}
	got := make([]string, 0, len(first))
	for k := range first {
		got = append(got, k)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("boundedErrorDetails kept %v, want the lexicographically first %d: %v", got, maxErrorDetailKeys, want)
	}

	for i := 0; i < 20; i++ {
		again := boundedErrorDetails(source)
		if len(again) != len(first) {
			t.Fatalf("call %d returned %d keys, want %d — the cap is not deterministic", i, len(again), len(first))
		}
		for k := range first {
			if _, ok := again[k]; !ok {
				t.Fatalf("call %d dropped %q that an earlier call kept — the cap is not deterministic", i, k)
			}
		}
	}
}

func TestBoundedErrorDetailsTruncatesLongValuesAndDropsOddKeys(t *testing.T) {
	long := strings.Repeat("a", maxErrorDetailValueSize*2)
	got := boundedErrorDetails(map[string]any{
		"long": long,
		"":     "empty key",
		strings.Repeat("k", maxErrorDetailKeyBytes+1): "over-long key",
		"ok": "kept",
	})

	if v, _ := got["long"].(string); len(v) != maxErrorDetailValueSize {
		t.Fatalf("boundedErrorDetails[long] length = %d, want the cap %d", len(v), maxErrorDetailValueSize)
	}
	if _, ok := got[""]; ok {
		t.Error("boundedErrorDetails kept an empty key")
	}
	if _, ok := got[strings.Repeat("k", maxErrorDetailKeyBytes+1)]; ok {
		t.Error("boundedErrorDetails kept an over-long key")
	}
	if got["ok"] != "kept" {
		t.Errorf("boundedErrorDetails[ok] = %#v, want \"kept\"", got["ok"])
	}
}

func TestBoundedErrorDetailsReturnsNilForEmptyInput(t *testing.T) {
	// nil, not an empty map: the omitempty JSON tag is what keeps a
	// detail-free error byte-identical to the pre-change wire form.
	if got := boundedErrorDetails(nil); got != nil {
		t.Errorf("boundedErrorDetails(nil) = %#v, want nil", got)
	}
	if got := boundedErrorDetails(map[string]any{}); got != nil {
		t.Errorf("boundedErrorDetails({}) = %#v, want nil", got)
	}
	if got := boundedErrorDetails(map[string]any{"nested": map[string]any{"a": 1}}); got != nil {
		t.Errorf("boundedErrorDetails(all-dropped) = %#v, want nil so no empty object is served", got)
	}
}

// TestBuildEffectiveClassificationCarriesBoundedDetails is the link the rest of
// the plumbing hangs from: if this struct does not carry Details, every backend
// field downstream stores nil and the whole change is inert. That was exactly
// the pre-U-9 behaviour, which is why the assertion is here and not only at the
// far end of the wire.
func TestBuildEffectiveClassificationCarriesBoundedDetails(t *testing.T) {
	classified := &types.ClassifiedError{
		Kind:      types.ErrorKindPermanent,
		Code:      "browser.host_denied",
		Message:   "browser destination is denied by policy",
		Permanent: true,
		Details: map[string]any{
			"source":       "navigation_policy",
			"rejected_url": "https://denied.test/path",
			"nested":       map[string]any{"leaked": "payload"},
		},
	}

	cls := buildEffectiveClassification(classified, nil, false)
	if !cls.Classified {
		t.Fatal("buildEffectiveClassification() Classified = false, want true")
	}
	if got, want := cls.Details["source"], "navigation_policy"; got != want {
		t.Fatalf("cls.Details[source] = %#v, want %#v", got, want)
	}
	if got, want := cls.Details["rejected_url"], "https://denied.test/path"; got != want {
		t.Errorf("cls.Details[rejected_url] = %#v, want %#v", got, want)
	}
	if _, ok := cls.Details["nested"]; ok {
		t.Error("cls.Details kept a nested map: the projection must be applied before the value is persisted")
	}

	// The non-ClassifiedError branches must not invent a detail, and a
	// permanent error stamped without the DTO has none to carry.
	plainPermanent := errors.Join(types.ErrPermanent, errors.New("stamped permanent"))
	if got := buildEffectiveClassification(plainPermanent, nil, false); got.Details != nil {
		t.Errorf("stamped-permanent cls.Details = %#v, want nil", got.Details)
	}
	if got := buildEffectiveClassification(nil, nil, false); got.Details != nil {
		t.Errorf("no-error cls.Details = %#v, want nil", got.Details)
	}
	if got := buildEffectiveClassification(nil, &types.Error{Message: "business"}, false); got.Details != nil {
		t.Errorf("business-error cls.Details = %#v, want nil", got.Details)
	}
	if got := buildEffectiveClassification(classified, nil, true); got.Details != nil {
		t.Errorf("error-port cls.Details = %#v, want nil", got.Details)
	}
}
