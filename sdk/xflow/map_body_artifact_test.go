package xflow

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/types"
)

// writeBodyScriptArtifact writes the js the map body's script node runs and
// returns its path. js, not wasm: ScriptNode.Execute reads Input.ArtifactCode
// BEFORE it dispatches on language, so the resolver chain a js artifact
// exercises is the same one a wasm guest takes -- without the ~10s build.
//
// The doubling is what proves the code RAN. A resolver that returned empty
// bytes, or a body that echoed its item through, would satisfy a "no error"
// assertion just as well.
func writeBodyScriptArtifact(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "double.js")
	if err := os.WriteFile(path, []byte(`({doubled: $item * 2})`), 0o600); err != nil {
		t.Fatalf("write script artifact: %v", err)
	}
	return path
}

// runMapBodyScriptWorkflow builds and runs the workflow both tests below share:
// a map node whose body is a single artifact-backed script node. Returns the
// per-item results the map node aggregated.
//
// The engine is built through NewLocal with a real ArtifactStore, so the
// resolver reaches the body executor only if newBatchBodyExecutor was actually
// given one -- not because the test handed it to anything directly.
func runMapBodyScriptWorkflow(t *testing.T, opts ...Option) (map[string]any, error) {
	t.Helper()
	eng, err := NewLocal(opts...)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	defer eng.Stop()

	body := Workflow("double-body")
	body.Node("s", node.ScriptFile(writeBodyScriptArtifact(t)).Language("js").Runtime("goja"))

	wf := Workflow("map-body-artifact")
	start := wf.Node("start", node.Start())
	mapNode := wf.Node("m", node.Map("$input.ids", 1))
	mapNode.Body(body)
	wf.Connect(start, mapNode)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wfID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		return nil, err
	}
	execID, err := eng.Invoke(ctx, wfID, Start(), map[string]any{"ids": []any{3, 4}})
	if err != nil {
		return nil, err
	}
	res, err := eng.Wait(ctx, execID)
	if err != nil {
		return nil, err
	}
	if res.Status != types.ExecutionStatusSuccess {
		return nil, &mapBodyFailure{status: string(res.Status), err: res.Error}
	}
	out, _ := res.Output["m"].(map[string]any)
	return out, nil
}

// mapBodyFailure carries a failed execution's status and error as one value so
// the callers below can assert on the message the body produced.
type mapBodyFailure struct {
	status string
	err    string
}

func (f *mapBodyFailure) Error() string {
	return "execution " + f.status + ": " + f.err
}

// TestLocalEngineMapBodyResolvesScriptArtifact is the SDK-side criterion for the
// artifact resolver reaching a map body.
//
// A map body runs on a FRESH backend that newBatchBodyExecutor builds, not on
// the engine's own, so the resolver NewLocal installed on its backend does not
// reach a body member on its own. Before newBatchBodyExecutor took the resolver
// explicitly, a ScriptFile node inside a map body read a nil resolver and failed
// permanently with script.artifact_unavailable -- while the identical node at
// the TOP level resolved fine, which is why no existing test caught it.
func TestLocalEngineMapBodyResolvesScriptArtifact(t *testing.T) {
	artifacts := store.NewArtifactStore(objectstore.NewFSStore(t.TempDir()), nil)

	out, err := runMapBodyScriptWorkflow(t, WithArtifactStore(artifacts), WithNodes(node.Script("")))
	if err != nil {
		t.Fatalf("map body with an artifact-backed script failed: %v\n"+
			"the resolver did not reach the body executor's per-item backend", err)
	}

	// The map node aggregates its batches' per-item rows under "results".
	items, _ := out["results"].([]any)
	if len(items) != 2 {
		t.Fatalf("map node reported %#v, want two per-item results", out)
	}
	got := map[float64]bool{}
	for _, it := range items {
		row, _ := it.(map[string]any)
		got[asFloatForMapBodyTest(row["doubled"])] = true
	}
	if !got[6] || !got[8] {
		t.Fatalf("map body produced doubled values %v, want 6 and 8 -- the artifact "+
			"code did not run for every item (items: %#v)", got, items)
	}
}

// TestLocalEngineMapBodyWithoutArtifactStore pins the failure the fix removes,
// so a regression is recognizable rather than merely red: with no artifact store
// the body member must fail naming the missing resolver.
//
// Registration itself still succeeds — resolveArtifacts is a no-op without a
// store, leaving the node with neither code nor digest — so the failure has to
// surface at execution, which is where a silent pass would be most costly.
func TestLocalEngineMapBodyWithoutArtifactStore(t *testing.T) {
	_, err := runMapBodyScriptWorkflow(t, WithNodes(node.Script("")))
	if err == nil {
		t.Fatal("map body succeeded with no artifact store configured; the script " +
			"node had no code to run, so succeeding means it ran something else")
	}
	if !strings.Contains(err.Error(), "script.") {
		t.Fatalf("error = %q, want a script node config failure naming the missing "+
			"code or resolver", err)
	}
}

// asFloatForMapBodyTest normalizes the numeric type a js engine exports: goja
// yields int64 for integer-valued numbers and a JSON round-trip yields float64,
// so pinning one Go type would make the assertion depend on the transport.
func asFloatForMapBodyTest(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	default:
		return 0
	}
}
