//go:build perf

package perf

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// wasmStaleOutputMaxBytes mirrors
// node/internal/code/script/engine.DefaultMaxOutputBytes (1 MiB). That
// constant lives under node/internal, which this package cannot import, so
// the value is duplicated here; both sides are pinned by the wasm package's
// own tests (see stale_output_real_load_test.go) so drift would be caught
// there first.
const wasmStaleOutputMaxBytes = 1 << 20

// buildReactorBigWasm compiles testdata/reactorbig/main.go — a byte-for-byte
// copy of the production reference reactor guest at
// node/internal/code/script/wasm/testdata/reactor/main.go (see that file's
// header for why the copy exists and why it must stay unmodified) — into a
// wasip1 reactor module and returns its bytes.
func buildReactorBigWasm(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	out := filepath.Join(dir, "reactorbig.wasm")
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out, "./testdata/reactorbig/main.go")
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build reactorbig guest: %v\n%s", err, b)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read reactorbig guest: %v", err)
	}
	return b
}

// wasmStaleOutputFillerRules returns enough identical-shape filler rules
// (each matching only "big == true") that, once every one of them matches, the
// guest's `{"matched":[...]}` encoding already exceeds wasmStaleOutputMaxBytes
// — mirroring buildOversizeFillerRules in
// node/internal/code/script/wasm/stale_output_real_load_test.go so both
// fixtures are sized by the same method instead of a guessed rule count.
func wasmStaleOutputFillerRules(t *testing.T) []map[string]string {
	t.Helper()
	const nameWidth = 200_000
	var rules []map[string]string
	var names []string
	for {
		b, err := json.Marshal(map[string]any{"matched": names})
		if err != nil {
			t.Fatalf("marshal output probe: %v", err)
		}
		if len(b) > wasmStaleOutputMaxBytes {
			return rules
		}
		name := fmt.Sprintf("filler-%06d-%s", len(names), strings.Repeat("x", nameWidth))
		names = append(names, name)
		rules = append(rules, map[string]string{"name": name, "expr": "big == true"})
	}
}

// staleOutputRecord is the JSON shape produced onto the Kafka topic and
// consumed back off it.
type staleOutputRecord struct {
	Marker string `json:"marker"`
	Big    bool   `json:"big"`
}

// TestWasmReactorStaleOutputRealKafka drives the production reactor engine
// ("wasm"/"wazero-reactor", registered by
// node/internal/code/script/wasm/reactor.go) with real Kafka records so that
// a guest's oversized OUTPUT — not merely an oversized input message — trips
// the host's ERR_OUTPUT (-5) path for real.
//
// Trigger condition under test (see
// node/internal/code/script/wasm/pool.go pooledInstance.evalOnce and
// node/internal/code/script/wasm/evaldetail.go decodeEvalDetail): when a
// reactor guest returns a negative code without first calling its own
// writeErr, out_len() still describes the PREVIOUS successful eval's output,
// so the host's readOut(ctx, -1) copies those stale bytes into
// reactorEvalError.detail. Before commit b75342f, decodeEvalDetail logged
// those stale bytes verbatim as if they explained THIS call's failure. The
// fix restricts decodeEvalDetail to the strict {"error":"..."} shape, so a
// bare stale-output return decodes to "" and is never logged.
//
// The reference reactor guest (testdata/reactorbig/main.go, an unmodified
// copy of the production one) hits exactly this shape: its ERR_OUTPUT branch
// is a bare `return errOutput`, no writeErr call.
//
// To reach ERR_OUTPUT for real, the guest's OUTPUT — not its input — must
// exceed engine.DefaultMaxOutputBytes (1 MiB). The guest reports, per eval,
// which of its configured rules matched; wasmStaleOutputFillerRules supplies
// enough identically-shaped filler rules (all keyed on "big == true") that a
// record with "big":true produces a matched-name array whose JSON encoding
// alone is already >1 MiB, deterministically forcing ERR_OUTPUT without
// relying on any single oversized input record.
func TestWasmReactorStaleOutputRealKafka(t *testing.T) {
	brokers := realKafkaBrokers(t)
	topic := fmt.Sprintf("xflow-perf-wasm-stale-%d", time.Now().UnixNano())
	// A single partition makes producer write order and consumer read order
	// identical, which is what lets the canary/poison/background sequence
	// below be asserted deterministically instead of merely probabilistically.
	createTopic(t, brokers[0], topic, 1)

	guestBytes := buildReactorBigWasm(t)
	code := base64.StdEncoding.EncodeToString(guestBytes)

	// The canary rule's name is the "secret" that must never leak into a log
	// line attributed to a later, unrelated call.
	const canaryMarkerRuleName = "canary-output-leak-must-not-be-logged"
	rules := append([]map[string]string{
		{"name": canaryMarkerRuleName, "expr": `marker == "canary"`},
	}, wasmStaleOutputFillerRules(t)...)
	cfg := map[string]any{"rules": rules}

	// node.Script's Language/Runtime setters populate struct fields consumed
	// only by RawParams() for DSL compilation; ScriptNode.Execute reads
	// engine selection and code from input.Params directly (see script.go),
	// so they are set here purely for readability, not because Execute needs
	// them — the Params map below is what actually drives execution.
	scriptNode := node.Script(code).Language("wasm").Runtime("wazero-reactor")

	const pairCount = 15
	const bgPerPair = 4
	type ordered struct {
		kind string // "canary" | "poison" | "bg"
		rec  staleOutputRecord
	}
	var sequence []ordered
	for i := 0; i < pairCount; i++ {
		sequence = append(sequence, ordered{kind: "canary", rec: staleOutputRecord{Marker: "canary", Big: false}})
		sequence = append(sequence, ordered{kind: "poison", rec: staleOutputRecord{Marker: "poison", Big: true}})
		for j := 0; j < bgPerPair; j++ {
			sequence = append(sequence, ordered{kind: "bg", rec: staleOutputRecord{Marker: fmt.Sprintf("bg-%d-%d", i, j), Big: false}})
		}
	}

	writer := &kafka.Writer{
		Addr:        kafka.TCP(brokers...),
		Topic:       topic,
		Balancer:    &kafka.RoundRobin{},
		MaxAttempts: 10,
		BatchSize:   50,
	}
	defer writer.Close()

	writeCtx, writeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer writeCancel()
	msgs := make([]kafka.Message, 0, len(sequence))
	for _, item := range sequence {
		b, err := json.Marshal(item.rec)
		if err != nil {
			t.Fatalf("marshal record %+v: %v", item.rec, err)
		}
		msgs = append(msgs, kafka.Message{Value: b})
	}
	if err := writer.WriteMessages(writeCtx, msgs...); err != nil {
		t.Fatalf("write %d records to topic %s: %v", len(msgs), topic, err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   0,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	// Capture every slog line emitted while these records are processed, so a
	// leaked stale-output detail can be asserted absent rather than merely
	// "not observed by chance".
	var logBuf bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	defer slog.SetDefault(prevLogger)

	// defaultPoolSize() (node/internal/code/script/wasm/pool.go) is computed
	// once, from runtime.GOMAXPROCS(0), the first time this guest's config is
	// installed, and then cached for that config's lifetime. Pinning it to 1
	// for that first call guarantees a single pooled instance serves every
	// record below, so a poison call's stale bytes are provably the
	// immediately preceding canary's real output rather than some other
	// concurrent instance's.
	prevGOMAXPROCS := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prevGOMAXPROCS)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var canaryCount, poisonCount, bgCount int
	for i := 0; i < len(sequence); i++ {
		want := sequence[i]

		readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
		m, err := reader.ReadMessage(readCtx)
		readCancel()
		if err != nil {
			t.Fatalf("read record %d/%d (want kind=%s): %v", i+1, len(sequence), want.kind, err)
		}
		var rec staleOutputRecord
		if err := json.Unmarshal(m.Value, &rec); err != nil {
			t.Fatalf("decode record %d: %v", i, err)
		}
		if rec != want.rec {
			t.Fatalf("record %d out of order: got %+v, want %+v (topic order must equal write order)", i, rec, want.rec)
		}

		input := &types.Input{
			NodeName:     "wasm-stale-output",
			WorkflowName: "wasm-stale-output-real-kafka",
			ExecutionID:  fmt.Sprintf("exec-%s-%d", topic, i),
			Params: map[string]any{
				"language": "wasm",
				"runtime":  "wazero-reactor",
				"code":     code,
			},
			Data:   map[string]any{"marker": rec.Marker, "big": rec.Big},
			Config: cfg,
		}

		execCtx, execCancel := context.WithTimeout(ctx, 10*time.Second)
		out, err := scriptNode.Execute(execCtx, input)
		execCancel()

		switch want.kind {
		case "poison":
			// This is the assertion that ERR_OUTPUT actually fired and was
			// handled per the documented contract: a clean, empty-data
			// success on the "main" port (script.go's
			// engine.IsRecordSkippable branch) — never a returned error, and
			// never data that looks like a match result (which would mean
			// the stale bytes were mistaken for this call's real output).
			if err != nil {
				t.Fatalf("poison record %d: Execute returned error %v, want nil (ERR_OUTPUT must be absorbed as a skip)", i, err)
			}
			if out.Port != "main" {
				t.Fatalf("poison record %d: Port = %q, want %q", i, out.Port, "main")
			}
			if len(out.Data) != 0 {
				t.Fatalf("poison record %d: Data = %v, want empty (stale/oversized output must not leak through as call data)", i, out.Data)
			}
			poisonCount++
		case "canary":
			if err != nil {
				t.Fatalf("canary record %d: unexpected error: %v", i, err)
			}
			if out.Port != "main" {
				t.Fatalf("canary record %d: Port = %q, want %q", i, out.Port, "main")
			}
			matched, _ := out.Data["matched"].([]any)
			if !containsAny(matched, canaryMarkerRuleName) {
				t.Fatalf("canary record %d: matched = %v, want it to contain %q; either the instance did not "+
					"recover cleanly after a preceding poison call, or the canary rule did not fire", i, matched, canaryMarkerRuleName)
			}
			canaryCount++
		case "bg":
			if err != nil {
				t.Fatalf("background record %d: unexpected error: %v", i, err)
			}
			matched, _ := out.Data["matched"].([]any)
			if len(matched) != 0 {
				t.Fatalf("background record %d: matched = %v, want empty", i, matched)
			}
			bgCount++
		}
	}

	if poisonCount != pairCount {
		t.Fatalf("poisonCount = %d, want %d; not every oversized record reached ERR_OUTPUT", poisonCount, pairCount)
	}
	if canaryCount != pairCount {
		t.Fatalf("canaryCount = %d, want %d", canaryCount, pairCount)
	}
	if bgCount != pairCount*bgPerPair {
		t.Fatalf("bgCount = %d, want %d", bgCount, pairCount*bgPerPair)
	}

	if got := logBuf.String(); strings.Contains(got, canaryMarkerRuleName) {
		t.Fatalf("a canary's real output leaked into the log as an ERR_OUTPUT failure reason "+
			"(pre-b75342f regression): %s", got)
	}
}

func containsAny(items []any, want string) bool {
	for _, item := range items {
		if s, ok := item.(string); ok && s == want {
			return true
		}
	}
	return false
}
