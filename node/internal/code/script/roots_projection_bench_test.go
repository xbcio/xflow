package script

import (
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/types"
)

// BenchmarkRootsProjectionPayload measures what a Roots() declaration removes
// from the bytes a wasm guest must re-parse inside its sandbox.
//
// It is the before/after for the SAS traffic pipeline's two nodes, at the live
// topic's measured mean record size:
//
//	decode  reads $item   — its input is the raw Kafka record
//	clean   reads $input  — its input is decode's output plus the same $item
//
// Read the B/op column, not ns/op. The marshal this benchmark times is ~1% of
// the real cost; the other 99% is the guest rebuilding these objects inside the
// sandbox, where the same bytes decode ~32x slower than natively (see
// wasm.stripItems). B/op is therefore the term that predicts eval time, and the
// ratio between declared and undeclared is the saving.
func BenchmarkRootsProjectionPayload(b *testing.B) {
	const recordSize = 6845 // live topic mean

	record := syntheticRecord(recordSize)
	// Decode's output: the parsed traffic record. Measured at 1.84x its input,
	// because a JSON body is stored both parsed (Body) and verbatim (BodyText).
	decoded := syntheticRecord(recordSize * 184 / 100)

	cases := []struct {
		name  string
		data  map[string]any
		roots []string
	}{
		// The decode node: input.Data holds only the map-body scope.
		{"decode/undeclared", map[string]any{"$item": record, "$index": 1}, nil},
		{"decode/roots=$item", map[string]any{"$item": record, "$index": 1}, []string{"$item"}},

		// The clean node: input.Data is decode's output WITH the map-body scope
		// merged in, which is why the raw record rides along a second time.
		{"clean/undeclared", cleanData(decoded, record), nil},
		{"clean/roots=$input", cleanData(decoded, record), []string{"$input"}},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			input := &types.Input{Data: tc.data, Params: map[string]any{}}
			if tc.roots != nil {
				input.Params["roots"] = tc.roots
			}
			env := buildScriptGlobals(input, nil, nil)
			b.ReportMetric(float64(len(marshalB(b, env))), "payload_bytes")
			b.ResetTimer()
			for range b.N {
				marshalB(b, buildScriptGlobals(input, nil, nil))
			}
		})
	}
}

// cleanData is the clean node's input.Data: decode's output flattened in, plus
// the map-body scope that applyExecutionScope merges on top of it.
func cleanData(decoded map[string]any, record map[string]any) map[string]any {
	data := make(map[string]any, len(decoded)+2)
	for k, v := range decoded {
		data[k] = v
	}
	data["$item"] = record
	data["$index"] = 1
	return data
}

// syntheticRecord builds a traffic-record-shaped map of roughly size bytes when
// marshalled — a small request and a JSON response body holding the weight,
// which is the live topic's shape.
func syntheticRecord(size int) map[string]any {
	items := make([]any, 0, size/56)
	for n := 0; n < size; n += 56 {
		items = append(items, map[string]any{
			"id": 12345, "name": "item-abcdefghijklmnop", "tag": "xyz",
		})
	}
	return map[string]any{
		"method":  "GET",
		"uri":     "/api/v1/users",
		"status":  200,
		"headers": map[string]any{"host": "api.example.com", "content-type": "application/json"},
		"body":    map[string]any{"items": items},
	}
}

func marshalB(b *testing.B, v any) []byte {
	b.Helper()
	out, err := json.Marshal(v)
	if err != nil {
		b.Fatalf("marshal: %v", err)
	}
	return out
}
