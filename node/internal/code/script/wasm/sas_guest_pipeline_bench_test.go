package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	sasBenchDecodeWasmEnv = "XFLOW_BENCH_SAS_DECODE_WASM"
	sasBenchCleanWasmEnv  = "XFLOW_BENCH_SAS_CLEAN_WASM"
	sasBenchConfigEnv     = "XFLOW_BENCH_SAS_GUEST_CONFIG"
	sasBenchMessageSize   = "XFLOW_BENCH_SAS_MESSAGE_BYTES"
)

// BenchmarkSASGuestPipeline_Paired measures the real SAS decode -> clean guest
// pair under two otherwise identical wazero runtimes. It is opt-in because the
// 6-7 MiB business modules and their live-shaped rule config belong to SAS, not
// to this repository:
//
//	XFLOW_BENCH_SAS_DECODE_WASM=/path/to/guest-decode.wasm
//	XFLOW_BENCH_SAS_CLEAN_WASM=/path/to/guest-clean.wasm
//	XFLOW_BENCH_SAS_GUEST_CONFIG=/path/to/config.json
//	go test ./node/internal/code/script/wasm \
//	  -run '^$' -bench '^BenchmarkSASGuestPipeline_Paired$' -benchtime=100x
//
// The config must be the single combined supply payload consumed by both
// modules ({revision, pre_analysis, post_decode}). The benchmark reports only
// its rule count, never its contents. The input is generated locally and holds
// no captured traffic or credentials. XFLOW_BENCH_SAS_MESSAGE_BYTES selects its
// raw Kafka value size and defaults to the measured live mean, 11,665 bytes.
//
// This is deliberately a LOWER BOUND for the complete product path. It includes
// both sandbox evals and the host JSON hand-off between them, but excludes Kafka,
// Redis, group scheduling, map backend construction, and sink work. Therefore,
// if closeOnDone=false still exceeds the whole-system CPU budget, changing the
// framework scheduler cannot make the current guest pair meet that budget.
//
// The false arm is diagnostic only. It is safe here because these two known
// guests are bounded for the generated input. Production must retain cancellable
// execution: without CloseOnContextDone, a spinning guest permanently occupies
// its pool slot because wazero v1.9 has no independent fuel/epoch interrupt.
func BenchmarkSASGuestPipeline_Paired(b *testing.B) {
	decodePath := strings.TrimSpace(os.Getenv(sasBenchDecodeWasmEnv))
	cleanPath := strings.TrimSpace(os.Getenv(sasBenchCleanWasmEnv))
	configPath := strings.TrimSpace(os.Getenv(sasBenchConfigEnv))
	if decodePath == "" && cleanPath == "" && configPath == "" {
		b.Skipf("set %s, %s, and %s to run the real SAS guest benchmark",
			sasBenchDecodeWasmEnv, sasBenchCleanWasmEnv, sasBenchConfigEnv)
	}
	if decodePath == "" || cleanPath == "" || configPath == "" {
		b.Fatalf("%s, %s, and %s must all be set",
			sasBenchDecodeWasmEnv, sasBenchCleanWasmEnv, sasBenchConfigEnv)
	}

	decodeWasm := readSASBenchFile(b, decodePath, "decode wasm")
	cleanWasm := readSASBenchFile(b, cleanPath, "clean wasm")
	config := readSASBenchFile(b, configPath, "guest config")
	if rules := ruleCount(config); rules < 0 {
		b.Fatalf("guest config does not contain rules, pre_analysis, or post_decode")
	} else {
		b.Logf("fixture: decode=%d B clean=%d B config_rules=%d",
			len(decodeWasm), len(cleanWasm), rules)
	}

	targetBytes := 11_665
	if raw := strings.TrimSpace(os.Getenv(sasBenchMessageSize)); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			b.Fatalf("%s=%q must be a positive integer", sasBenchMessageSize, raw)
		}
		targetBytes = v
	}
	item, rawBytes := sasBenchKafkaItem(b, targetBytes)

	cases := []struct {
		name        string
		closeOnDone bool
	}{
		{name: "closeOnDone=true", closeOnDone: true},
		{name: "closeOnDone=false-diagnostic-only", closeOnDone: false},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.StopTimer()
			// The deadline keeps wazero's production Call monitor active. Its
			// duration does not affect generated code; it is long only so module
			// compilation is not charged against the script's 30-second budget.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			host := hostWithContextDone(ctx, b, tc.closeOnDone)
			de, err := host.engineFor(ctx, decodeWasm)
			if err != nil {
				b.Fatalf("compile decode guest: %v", err)
			}
			ce, err := host.engineFor(ctx, cleanWasm)
			if err != nil {
				b.Fatalf("compile clean guest: %v", err)
			}
			// Width one removes pool contention and makes this an optimistic
			// per-message CPU floor, not a concurrency benchmark.
			if err := de.swapConfig(ctx, config, 1, 1); err != nil {
				b.Fatalf("configure decode guest: %v", err)
			}
			if err := ce.swapConfig(ctx, config, 1, 1); err != nil {
				b.Fatalf("configure clean guest: %v", err)
			}
			facade := &reactorFacade{host: host}

			shapes := []struct {
				name  string
				shape sasBenchCleanShape
			}{
				{name: "legacy-clean-input-with-map-scope", shape: sasBenchCleanLegacyWithMapScope},
				// Diagnostic only: quantifies the cost of Roots("$input") carrying
				// the original Kafka $item through decode and into clean.
				{name: "legacy-clean-input-decoded-only", shape: sasBenchCleanLegacyDecodedOnly},
				// The optimized SAS workflow uses Roots("req", "resp"), so clean sees
				// these fields at the top level instead of below $input.
				{name: "projected-clean-req-resp", shape: sasBenchCleanProjectedReqResp},
			}
			for _, shape := range shapes {
				b.Run(shape.name, func(b *testing.B) {
					out, err := evalSASGuestPair(ctx, facade, de, ce, item, shape.shape)
					if err != nil {
						b.Fatalf("warm guest pair: %v", err)
					}
					if !sasBenchSurvived(out) {
						b.Fatalf("warm guest pair returned a filtered/invalid result of type %T", out)
					}

					b.ResetTimer()
					b.StartTimer()
					cpuStart := cpuSeconds()
					for b.Loop() {
						if _, err := evalSASGuestPair(ctx, facade, de, ce, item, shape.shape); err != nil {
							b.Fatalf("eval guest pair: %v", err)
						}
					}
					cpu := cpuSeconds() - cpuStart
					b.StopTimer()

					cpuMSPerMessage := cpu / float64(b.N) * 1_000
					b.ReportMetric(float64(rawBytes), "raw_B/message")
					b.ReportMetric(cpuMSPerMessage, "cpu_ms/message")
					// One message always executes exactly decode + clean. This metric
					// apportions the complete pair cost over those two evals; it includes
					// their host JSON hand-off rather than pretending it is free.
					b.ReportMetric(cpuMSPerMessage/2, "cpu_ms/eval")
					if cpuMSPerMessage > 0 {
						b.ReportMetric(1_000/cpuMSPerMessage, "msg/s/core")
						b.ReportMetric(8_000/cpuMSPerMessage, "msg/s/8cpu")
					}
				})
			}
		})
	}
}

func readSASBenchFile(b *testing.B, path, label string) []byte {
	b.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		b.Fatalf("read %s %q: %v", label, path, err)
	}
	if len(content) == 0 {
		b.Fatalf("%s %q is empty", label, path)
	}
	return content
}

type sasBenchCleanShape int

const (
	sasBenchCleanLegacyWithMapScope sasBenchCleanShape = iota
	sasBenchCleanLegacyDecodedOnly
	sasBenchCleanProjectedReqResp
)

// evalSASGuestPair mirrors the payload shapes produced by Roots("$item") on
// decode and the historical/optimized clean projections.
func evalSASGuestPair(
	ctx context.Context,
	facade *reactorFacade,
	decode, clean *reactorEngine,
	item map[string]any,
	cleanShape sasBenchCleanShape,
) (any, error) {
	decodeInput, err := encodeStdin(map[string]any{"$item": item})
	if err != nil {
		return nil, fmt.Errorf("encode decode input: %w", err)
	}
	decoded, err := facade.evalFromPool(ctx, decode, decodeInput)
	if err != nil {
		return nil, fmt.Errorf("decode guest: %w", err)
	}
	decodedMap, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("decode guest returned %T, want object", decoded)
	}

	var cleanPayload map[string]any
	switch cleanShape {
	case sasBenchCleanLegacyWithMapScope, sasBenchCleanLegacyDecodedOnly:
		extra := 0
		if cleanShape == sasBenchCleanLegacyWithMapScope {
			extra = 3
		}
		cleanData := make(map[string]any, len(decodedMap)+extra)
		for k, v := range decodedMap {
			cleanData[k] = v
		}
		if cleanShape == sasBenchCleanLegacyWithMapScope {
			cleanData["$item"] = item
			cleanData["$index"] = 0
			cleanData["$items"] = []any{item}
		}
		cleanPayload = map[string]any{"$input": cleanData}
	case sasBenchCleanProjectedReqResp:
		cleanPayload = make(map[string]any, 2)
		if req, ok := decodedMap["req"]; ok {
			cleanPayload["req"] = req
		}
		if resp, ok := decodedMap["resp"]; ok {
			cleanPayload["resp"] = resp
		}
	default:
		return nil, fmt.Errorf("unknown clean payload shape %d", cleanShape)
	}
	cleanInput, err := encodeStdin(cleanPayload)
	if err != nil {
		return nil, fmt.Errorf("encode clean input: %w", err)
	}
	cleaned, err := facade.evalFromPool(ctx, clean, cleanInput)
	if err != nil {
		return nil, fmt.Errorf("clean guest: %w", err)
	}
	return cleaned, nil
}

func sasBenchSurvived(v any) bool {
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}
	_, hasReq := m["req"]
	_, hasResp := m["resp"]
	return hasReq && hasResp
}

// sasBenchKafkaItem creates a credential-free access log whose raw Kafka value
// is exactly targetBytes. Padding lives in a JSON response body so decode must
// parse and materialise it, instead of merely skipping an unknown filler field.
func sasBenchKafkaItem(b *testing.B, targetBytes int) (map[string]any, int) {
	b.Helper()
	padding := targetBytes
	var raw []byte
	for range 4 {
		msg := map[string]any{
			"service_id": "xflow-benchmark",
			"route_id":   "route-benchmark",
			"request": map[string]any{
				"method": "POST",
				"url":    "https://benchmark.invalid/api/v1/resource",
				"size":   25,
				"headers": map[string]string{
					"host":         "benchmark.invalid",
					"content-type": "application/json",
					"user-agent":   "xflow-local-benchmark/1.0",
				},
				"querystring": map[string]any{"page": "1"},
				"body":        `{"request":"synthetic"}`,
			},
			"response": map[string]any{
				"status":  200,
				"size":    padding + len(`{"payload":""}`),
				"headers": map[string]any{"content-type": "application/json"},
				"body":    `{"payload":"` + strings.Repeat("x", max(padding, 0)) + `"}`,
			},
		}
		var err error
		raw, err = json.Marshal(msg)
		if err != nil {
			b.Fatalf("marshal synthetic access log: %v", err)
		}
		delta := targetBytes - len(raw)
		if delta == 0 {
			break
		}
		padding += delta
		if padding < 0 {
			b.Fatalf("requested message size %d B is below fixture minimum %d B",
				targetBytes, len(raw)-padding)
		}
	}
	if len(raw) != targetBytes {
		b.Fatalf("synthetic access log is %d B, want exactly %d B", len(raw), targetBytes)
	}

	return map[string]any{
		"topic":     "xflow-synthetic-benchmark",
		"partition": 0,
		"offset":    int64(1),
		"key":       "benchmark",
		"value":     string(raw),
		"headers":   map[string]string{},
		"time":      time.Unix(0, 0).UTC(),
	}, len(raw)
}
