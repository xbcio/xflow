import * as React from "react";
import { useCallback, useMemo } from "react";
import { ReactFlowProvider } from "@xyflow/react";
import { WorkflowCanvas } from "@xflow/workflow-renderer";
import type { ExecutionSnapshot as RendererExecutionSnapshot } from "@xflow/workflow-renderer";
import { useEditor } from "../../context/EditorContext";

const overlayStyle = {
  "--xf-overlay-selection-top": "var(--editor-toolbar-height)",
  "--xf-overlay-exec-top": "var(--editor-toolbar-height)",
  "--xf-overlay-diag-top": "auto",
  "--xf-overlay-diag-bottom": "12px",
} as React.CSSProperties;

export function CanvasContainer() {
  const {
    definition,
    diagnostics,
    executionSnapshot,
    selectedNodeIds,
    toggleNodeSelected,
  } = useEditor();

  const handleSelectionChange = useCallback(
    (nodeIds: string[]) => {
      const current = new Set(selectedNodeIds);
      const next = new Set(nodeIds);

      for (const id of current) {
        if (!next.has(id)) {
          toggleNodeSelected(id);
        }
      }
      for (const id of next) {
        if (!current.has(id)) {
          toggleNodeSelected(id);
        }
      }
    },
    [selectedNodeIds, toggleNodeSelected],
  );

  const emptyState = useMemo(
    () => (
      <div className="canvas-container__empty" data-testid="canvas-empty-state">
        No workflow definition loaded.
      </div>
    ),
    [],
  );

  return (
    <div className="canvas-container" style={overlayStyle} data-testid="canvas-container">
      <ReactFlowProvider>
        {definition ? (
          <WorkflowCanvas
            definition={definition}
            diagnostics={diagnostics}
            executionSnapshot={executionSnapshot as RendererExecutionSnapshot | undefined}
            readOnly={false}
            selectable
            selectedNodeIds={selectedNodeIds}
            onSelectionChange={handleSelectionChange}
          />
        ) : (
          emptyState
        )}
      </ReactFlowProvider>
    </div>
  );
}
