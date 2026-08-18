package xflow

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	xnode "github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// artifactCodeResolverFor adapts an ArtifactStore to the digest -> bytes
// resolver signature the backends and the map-body executor take. Returns nil
// for a nil store, which every caller treats as "no resolver configured".
//
// Shared by NewLocal, NewCluster and newBatchBodyExecutor so all three agree on
// what resolving an artifact means; a map body previously got no resolver at
// all, which surfaced as script.artifact_unavailable one level down.
func artifactCodeResolverFor(as *store.ArtifactStore) func(ctx context.Context, digest string) ([]byte, error) {
	if as == nil {
		return nil
	}
	return func(ctx context.Context, digest string) ([]byte, error) {
		rc, _, err := as.Open(ctx, digest)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	}
}

// resolveArtifacts scans the workflow definition for script nodes that carry a
// file-path marker (__artifact_file_path) set by ScriptFile, reads the file,
// stores it in the ArtifactStore, and rewrites the node's parameters to use
// artifact_digest instead. This runs between preCheckHandlerVersions and
// graph.Compile in AddWorkflow, so the compiled graph never sees file paths.
//
// The scan recurses into node bodies. A map or split body is not in def.Nodes —
// it is opaque JSON under the parent's "body" parameter — so a top-level-only
// scan left a body's ScriptFile carrying its marker into the compiled graph,
// where Execute saw neither code nor artifact_digest and failed with
// script.code_required. That is exactly where a wasm guest belongs in a
// per-item pipeline.
//
// For wasm modules, it also pre-compiles the engine so supply consumers
// registered via RegisterWasmSupplyConsumerByDigest can configure the pool
// immediately when content arrives, rather than waiting for the first Execute.
func resolveArtifacts(ctx context.Context, def *types.WorkflowDef, artifacts *store.ArtifactStore) error {
	for i := range def.Nodes {
		nd := &def.Nodes[i]
		if err := resolveNodeArtifacts(ctx, nd.Name, nd.Type, nd.Parameters, def.Namespace, artifacts); err != nil {
			return err
		}
	}
	return nil
}

// resolveNodeArtifacts resolves one node's own artifact marker, then descends
// into its body. Parameters are mutated in place; a body's members are the
// map[string]any values the JSON round-trip in subgraphBodyParam produced, so
// rewriting them here is what the compiler subsequently reads.
func resolveNodeArtifacts(ctx context.Context, name, nodeType string, params map[string]any, ns string, artifacts *store.ArtifactStore) error {
	if params == nil {
		return nil
	}
	if nodeType == "xflow.script" {
		if err := rewriteScriptArtifact(ctx, name, params, ns, artifacts); err != nil {
			return err
		}
	}
	// A body is {type: xflow.subgraph, parameters: {nodes: [...], ...}}. Every
	// level is optional: a node without a body, or a body without members, simply
	// has nothing to descend into.
	body, _ := params["body"].(map[string]any)
	bodyParams, _ := body["parameters"].(map[string]any)
	members, _ := bodyParams["nodes"].([]any)
	for _, raw := range members {
		member, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		memberName, _ := member["name"].(string)
		memberType, _ := member["type"].(string)
		memberParams, _ := member["parameters"].(map[string]any)
		// Qualify the name so an error names the path, not just a leaf that may
		// share its name with a member of some other body.
		if err := resolveNodeArtifacts(ctx, name+"/"+memberName, memberType, memberParams, ns, artifacts); err != nil {
			return err
		}
	}
	return nil
}

// rewriteScriptArtifact turns one script node's __artifact_file_path marker into
// a stored artifact plus an artifact_digest parameter. A node without the marker
// is left alone.
func rewriteScriptArtifact(ctx context.Context, name string, params map[string]any, ns string, artifacts *store.ArtifactStore) error {
	filePath, _ := params["__artifact_file_path"].(string)
	if filePath == "" {
		return nil
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("resolveArtifacts: node %q: read %s: %w", name, filePath, err)
	}

	// Namespace is what the artifact endpoint's authorization reads: it answers
	// GET /v1/artifacts/{digest} only when HasReference(callerNamespace, digest)
	// finds an identity row. An unnamespaced Put lands a row with namespace "",
	// which no caller's namespace ever matches, so every fetch 404s — the runner
	// then cannot get the code it was told to run.
	ref, err := artifacts.Put(ctx, content, store.ArtifactMeta{
		Filename:  filepath.Base(filePath),
		Namespace: ns,
	})
	if err != nil {
		return fmt.Errorf("resolveArtifacts: node %q: put artifact: %w", name, err)
	}

	delete(params, "__artifact_file_path")
	delete(params, "code")
	params["artifact_digest"] = ref.Digest

	// Pre-compile wasm modules so supply consumers can configure the pool
	// immediately. The engine key is the sha256 of the raw bytes — the same
	// key the digest path in Execute would resolve to. Without this, a supply
	// consumer registered before AddWorkflow cannot find the engine and
	// content delivery is deferred until the first Execute creates it.
	if lang, _ := params["language"].(string); lang == "wasm" {
		if err := xnode.CompileWasmModuleBytes(ctx, content); err != nil {
			return fmt.Errorf("resolveArtifacts: node %q: compile wasm: %w", name, err)
		}
	}
	return nil
}
