package xflow

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"

	xnode "github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// resolveArtifacts scans the workflow definition for script nodes that carry a
// file-path marker (__artifact_file_path) set by ScriptFile, reads the file,
// stores it in the ArtifactStore, and rewrites the node's parameters to use
// artifact_digest instead. This runs between preCheckHandlerVersions and
// graph.Compile in AddWorkflow, so the compiled graph never sees file paths.
//
// For wasm modules, it also pre-compiles the engine so supply consumers
// registered via RegisterWasmSupplyConsumerByDigest can configure the pool
// immediately when content arrives, rather than waiting for the first Execute.
func resolveArtifacts(ctx context.Context, def *types.WorkflowDef, artifacts *store.ArtifactStore) error {
	for i := range def.Nodes {
		nd := &def.Nodes[i]
		if nd.Type != "xflow.script" {
			continue
		}
		if nd.Parameters == nil {
			continue
		}
		filePath, _ := nd.Parameters["__artifact_file_path"].(string)
		if filePath == "" {
			continue
		}

		content, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("resolveArtifacts: node %q: read %s: %w", nd.Name, filePath, err)
		}

		ref, err := artifacts.Put(ctx, content, store.ArtifactMeta{
			Filename: filepath.Base(filePath),
		})
		if err != nil {
			return fmt.Errorf("resolveArtifacts: node %q: put artifact: %w", nd.Name, err)
		}

		delete(nd.Parameters, "__artifact_file_path")
		delete(nd.Parameters, "code")
		nd.Parameters["artifact_digest"] = ref.Digest

		// Pre-compile wasm modules so supply consumers can configure the pool
		// immediately. The engine key is the sha256 of the raw bytes — the same
		// key the digest path in Execute would resolve to. Without this, a supply
		// consumer registered before AddWorkflow cannot find the engine and
		// content delivery is deferred until the first Execute creates it.
		lang, _ := nd.Parameters["language"].(string)
		if lang == "wasm" {
			code := base64.StdEncoding.EncodeToString(content)
			if err := xnode.CompileWasmModule(ctx, code); err != nil {
				return fmt.Errorf("resolveArtifacts: node %q: compile wasm: %w", nd.Name, err)
			}
		}
	}
	return nil
}
