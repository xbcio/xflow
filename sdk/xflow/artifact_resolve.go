package xflow

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// resolveArtifacts scans the workflow definition for script nodes that carry a
// file-path marker (__artifact_file_path) set by ScriptFile, reads the file,
// stores it in the ArtifactStore, and rewrites the node's parameters to use
// artifact_digest instead. This runs between preCheckHandlerVersions and
// graph.Compile in AddWorkflow, so the compiled graph never sees file paths.
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
	}
	return nil
}
