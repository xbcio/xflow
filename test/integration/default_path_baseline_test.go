//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// dpUpdate rewrites the baseline files from the current run. Without it the
// test only compares, so an engine change that alters a stored or queued byte
// fails here instead of being absorbed.
var dpUpdate = flag.Bool("update", false, "rewrite the default-path baseline files instead of comparing against them")

const (
	dpGoldenDir    = "testdata/default_path"
	dpBodiesGolden = "outbox_bodies.golden.json"
	dpQueueGolden  = "queue_payloads.golden.json"
)

// dpNormalizeAll masks every captured document and puts them in a fixed order.
// The order is by normalized content: which of two concurrent deliveries
// reached Redis or the broker first is not something the baseline may depend
// on, while the bytes inside each document are.
func dpNormalizeAll(t *testing.T, raw [][]byte) [][]byte {
	t.Helper()
	out := make([][]byte, 0, len(raw))
	for _, doc := range raw {
		n, err := dpNormalize(doc)
		if err != nil {
			t.Fatalf("normalize %.100s: %v", doc, err)
		}
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i], out[j]) < 0 })
	return out
}

// dpRender lays the documents out as a JSON array with one document per line,
// so a difference is a one-line difference in a diff.
func dpRender(docs [][]byte) []byte {
	var b bytes.Buffer
	b.WriteString("[\n")
	for i, d := range docs {
		if i > 0 {
			b.WriteString(",\n")
		}
		b.Write(d)
	}
	b.WriteString("\n]\n")
	return b.Bytes()
}

// dpFirstDiff describes the first line where want and got differ.
func dpFirstDiff(want, got []byte) string {
	w, g := strings.Split(string(want), "\n"), strings.Split(string(got), "\n")
	cut := func(s string) string {
		if len(s) > 240 {
			return s[:240] + "..."
		}
		return s
	}
	for i := 0; i < len(w) || i < len(g); i++ {
		var wl, gl string
		if i < len(w) {
			wl = w[i]
		}
		if i < len(g) {
			gl = g[i]
		}
		if wl != gl {
			return fmt.Sprintf("line %d (baseline has %d lines, this run %d)\n  baseline: %s\n  this run: %s",
				i+1, len(w), len(g), cut(wl), cut(gl))
		}
	}
	return "no differing line"
}

// dpCheckGolden writes the file under -update and otherwise compares to it.
func dpCheckGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join(dpGoldenDir, name)
	if *dpUpdate {
		if err := os.MkdirAll(dpGoldenDir, 0o755); err != nil {
			t.Fatalf("create %s: %v", dpGoldenDir, err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		t.Logf("recorded %s (%d bytes)", path, len(got))
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read baseline %s: %v (record it with -update)", path, err)
	}
	if !bytes.Equal(want, got) {
		t.Errorf("%s differs from this run: %s", path, dpFirstDiff(want, got))
	}
}

// dpRequireCoverage fails when the run did not exercise what the baseline is
// meant to pin. A workflow that silently stopped suspending, batching or
// retrying would still produce a self-consistent file, so the file's meaning is
// asserted separately: the outbox must hold a batch, a resume and a delayed
// retry, and the queue must carry batch and resume payloads on both queues.
func dpRequireCoverage(t *testing.T, bodies, queued [][]byte) {
	t.Helper()
	count := func(docs [][]byte, sub string) int {
		n := 0
		for _, d := range docs {
			if strings.Contains(string(d), sub) {
				n++
			}
		}
		return n
	}
	batches := dpItems / dpBatchSize
	checks := []struct {
		what string
		got  int
		want int
	}{
		{"outbox bodies of batch tasks (\"type\":4)", count(bodies, `"type":4`), batches},
		{"outbox bodies of resume tasks (\"type\":1)", count(bodies, `"type":1`), 1},
		{"outbox bodies of the retry (retry/ entry)", count(bodies, `"id":"retry/`), 1},
		{"queue payloads of batch tasks (\"type\":4)", count(queued, `"type":4`), batches},
		{"queue payloads of resume tasks (\"type\":1)", count(queued, `"type":1`), 1},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: %d, want %d", c.what, c.got, c.want)
		}
	}
	if len(bodies) == 0 || len(queued) == 0 {
		t.Fatalf("captured %d outbox bodies and %d queue payloads, a baseline needs both", len(bodies), len(queued))
	}
}

// dpValidJSON fails when a captured document is not JSON, which would make the
// rendered file unparseable.
func dpValidJSON(t *testing.T, docs [][]byte) {
	t.Helper()
	for _, d := range docs {
		if !json.Valid(d) {
			t.Fatalf("captured document is not valid JSON: %.120s", d)
		}
	}
}

// TestDefaultPathBaseline runs the default-path workflow (map in several
// batches, a suspend resumed by a signal, one retry) against real Redis and
// compares every outbox:body and every queue payload it produced, after
// normalization, with the recorded files. Run with -update to record them.
func TestDefaultPathBaseline(t *testing.T) {
	addr := requireRedis(t)
	r := newDPRun(t, addr)
	r.serve(t)
	r.drive(t)

	bodies, queued, errs := r.cap.snapshot()
	for _, err := range errs {
		t.Errorf("capture: %v", err)
	}
	dpRequireCoverage(t, bodies, queued)
	gotBodies := dpNormalizeAll(t, bodies)
	gotQueued := dpNormalizeAll(t, queued)
	dpValidJSON(t, gotBodies)
	dpValidJSON(t, gotQueued)
	dpCheckGolden(t, dpBodiesGolden, dpRender(gotBodies))
	dpCheckGolden(t, dpQueueGolden, dpRender(gotQueued))
}
