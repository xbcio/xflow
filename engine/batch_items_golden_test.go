package engine

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
)

// updateBatchGolden rewrites the golden file instead of comparing against it.
// It is off by default: the golden pins the bytes of a batch task as built when
// no item store is involved, and a rewrite is a deliberate act.
var updateBatchGolden = flag.Bool("update", false, "rewrite engine/testdata golden files instead of comparing")

const batchTaskDefaultGolden = "testdata/batch_task_default.golden.json"

// batchGoldenCase records the task plus the scheduling fields Task keeps out of
// its own JSON (ActivationID, AutoDepth), so the golden also pins them.
type batchGoldenCase struct {
	Name         string `json:"name"`
	ActivationID int    `json:"activation_id"`
	AutoDepth    int    `json:"auto_depth"`
	Task         Task   `json:"task"`
}

func newBatchGoldenCase(name string, task Task) batchGoldenCase {
	return batchGoldenCase{Name: name, ActivationID: task.ActivationID, AutoDepth: task.AutoDepth, Task: task}
}

// batchGoldenCases drives expansionBatchTask over the input axes that change its
// output: batch_size present / absent / ignored, a namespace on the parent lease,
// a zero versus non-zero IssuedAt, and the map node's own items array being
// present or absent (which decides all_items).
func batchGoldenCases() []batchGoldenCase {
	issuedAt := time.Date(2026, 10, 26, 8, 30, 15, 123456789, time.FixedZone("CST", 8*3600))
	parent := func(ns namespace.Namespace, at time.Time) *TaskLease {
		return &TaskLease{
			LeaseID:    "lease-1",
			LeaseToken: "token-1",
			Attempt:    2,
			Namespace:  ns,
			IssuedAt:   at,
			Task: Task{
				ExecutionID:  "exec-1",
				NodeName:     "map1",
				NodeIdx:      3,
				ActivationID: 4,
				AutoDepth:    5,
			},
		}
	}
	items := []any{"a", float64(2), map[string]any{"k": "v"}, nil}
	whole := []any{"a", float64(2), map[string]any{"k": "v"}, nil, "e"}
	return []batchGoldenCase{
		newBatchGoldenCase("bare: no batch_size, no namespace, zero IssuedAt, no all_items",
			expansionBatchTask(parent("", time.Time{}), "exec-1/sub/map1/lease-1/0", 0, items, nil)),
		newBatchGoldenCase("batch_size int with all_items",
			expansionBatchTask(parent("", time.Time{}), "exec-1/sub/map1/lease-1/1", 1, items,
				map[string]any{"batch_size": 2, "items": whole})),
		newBatchGoldenCase("batch_size float64 (JSON round trip)",
			expansionBatchTask(parent("", time.Time{}), "exec-1/sub/map1/lease-1/2", 2, items,
				map[string]any{"batch_size": float64(2), "items": whole})),
		newBatchGoldenCase("batch_size zero is dropped",
			expansionBatchTask(parent("", time.Time{}), "exec-1/sub/map1/lease-1/3", 3, items,
				map[string]any{"batch_size": 0, "items": whole})),
		newBatchGoldenCase("batch_size of the wrong type is dropped",
			expansionBatchTask(parent("", time.Time{}), "exec-1/sub/map1/lease-1/4", 4, items,
				map[string]any{"batch_size": "2", "items": whole})),
		newBatchGoldenCase("namespace set, IssuedAt zero",
			expansionBatchTask(parent("tenant-b", time.Time{}), "exec-1/sub/map1/lease-1/5", 5, items,
				map[string]any{"batch_size": 2, "items": whole})),
		newBatchGoldenCase("namespace empty, IssuedAt set",
			expansionBatchTask(parent("", issuedAt), "exec-1/sub/map1/lease-1/6", 6, items,
				map[string]any{"batch_size": 2, "items": whole})),
		newBatchGoldenCase("namespace and IssuedAt both set",
			expansionBatchTask(parent("tenant-b", issuedAt), "exec-1/sub/map1/lease-1/7", 7, items,
				map[string]any{"batch_size": 2, "items": whole})),
		newBatchGoldenCase("all_items absent when data items is not []any",
			expansionBatchTask(parent("tenant-b", issuedAt), "exec-1/sub/map1/lease-1/8", 8, items,
				map[string]any{"batch_size": 2, "items": []string{"a", "b"}})),
		newBatchGoldenCase("nil batch items, empty data",
			expansionBatchTask(parent("", time.Time{}), "exec-1/sub/map1/lease-1/9", 9, nil, map[string]any{})),
	}
}

func buildBatchGolden(t *testing.T) []byte {
	t.Helper()
	cases := batchGoldenCases()
	for i := range cases {
		if _, err := json.Marshal(cases[i].Task); err != nil {
			t.Fatalf("marshal case %q: %v", cases[i].Name, err)
		}
	}
	raw, err := json.Marshal(cases)
	if err != nil {
		t.Fatalf("marshal golden: %v", err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		t.Fatalf("indent golden: %v", err)
	}
	out.WriteByte('\n')
	return out.Bytes()
}

// TestBatchTaskDefaultMatchesGolden pins the exact bytes of every batch task the
// engine builds when no item store is involved. Without -update it only
// compares; a diff means the default (inline) batch format changed.
func TestBatchTaskDefaultMatchesGolden(t *testing.T) {
	got := buildBatchGolden(t)
	if *updateBatchGolden {
		if err := os.MkdirAll(filepath.Dir(batchTaskDefaultGolden), 0o755); err != nil {
			t.Fatalf("create testdata dir: %v", err)
		}
		if err := os.WriteFile(batchTaskDefaultGolden, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(batchTaskDefaultGolden)
	if err != nil {
		t.Fatalf("read golden (record it with -update): %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("default batch task bytes differ from %s;\ngot:\n%s\nwant:\n%s", batchTaskDefaultGolden, got, want)
	}
}
