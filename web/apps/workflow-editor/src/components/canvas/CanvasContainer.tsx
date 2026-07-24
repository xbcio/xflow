import * as React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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

const ORIGIN_SIZE = 24;

function renderRulerTicks(
  container: HTMLElement,
  direction: "x" | "y",
) {
  const existing = container.querySelectorAll(".ruler-tick, .ruler-label");
  existing.forEach((el) => el.remove());

  const size = direction === "x" ? container.clientWidth : container.clientHeight;
  const available = Math.max(0, size - ORIGIN_SIZE);

  for (let pos = 0; pos < available; pos += 10) {
    const tick = document.createElement("div");
    const isMajor = pos % 100 === 0;
    const isMid = pos % 50 === 0;
    tick.className = `ruler-tick absolute bg-editor-ruler-tick ${
      direction === "x" ? "bottom-0 w-px" : "right-0 h-px"
    } ${isMajor ? "major bg-editor-ruler-tick-major" : ""}`;

    if (direction === "x") {
      tick.style.left = `${ORIGIN_SIZE + pos}px`;
      tick.style.height = isMajor ? "10px" : isMid ? "7px" : "4px";
      if (isMajor) {
        const label = document.createElement("span");
        label.className = "ruler-label absolute bottom-0.5 text-[9px] leading-none text-editor-ruler-text font-medium";
        label.textContent = String(pos);
        label.style.left = `${ORIGIN_SIZE + pos + 2}px`;
        container.appendChild(label);
      }
    } else {
      tick.style.top = `${ORIGIN_SIZE + pos}px`;
      tick.style.width = isMajor ? "10px" : isMid ? "7px" : "4px";
      if (isMajor) {
        const label = document.createElement("span");
        label.className = "ruler-label absolute right-0.5 text-[9px] leading-none text-editor-ruler-text font-medium";
        label.textContent = String(pos);
        label.style.top = `${ORIGIN_SIZE + pos + 2}px`;
        container.appendChild(label);
      }
    }

    container.appendChild(tick);
  }
}

export function CanvasContainer() {
  const {
    definition,
    diagnostics,
    executionSnapshot,
    selectedNodeIds,
    toggleNodeSelected,
  } = useEditor();

  const rulerXRef = useRef<HTMLDivElement>(null);
  const rulerYRef = useRef<HTMLDivElement>(null);
  const [tickKey, setTickKey] = useState(0);

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
      <div className="flex items-center justify-center h-full text-editor-text-secondary" data-testid="canvas-empty-state">
        没有加载工作流定义
      </div>
    ),
    [],
  );

  useEffect(() => {
    function draw() {
      if (rulerXRef.current) {
        renderRulerTicks(rulerXRef.current, "x");
      }
      if (rulerYRef.current) {
        renderRulerTicks(rulerYRef.current, "y");
      }
    }

    draw();
    setTickKey((k) => k + 1);

    window.addEventListener("resize", draw);
    return () => window.removeEventListener("resize", draw);
  }, []);

  return (
    <div className="w-full h-full relative" style={overlayStyle} data-testid="canvas-container">
      <div className="absolute inset-0 flex flex-col">
        <div
          ref={rulerXRef}
          className="relative h-6 bg-editor-ruler-bg border-b border-editor-border select-none shadow-[inset_0_-1px_1px_rgba(0,0,0,0.08),0_1px_2px_rgba(0,0,0,0.04)]"
          data-testid="ruler-x"
        >
          <div className="absolute left-0 top-0 w-6 h-6 bg-editor-ruler-bg z-[2]" />
        </div>
        <div className="flex flex-1 min-h-0">
          <div
            ref={rulerYRef}
            className="relative w-6 bg-editor-ruler-bg border-r border-editor-border select-none shadow-[inset_0_-1px_1px_rgba(0,0,0,0.08),0_1px_2px_rgba(0,0,0,0.04)]"
            data-testid="ruler-y"
          />
          <div
            className="flex-1 relative overflow-hidden bg-editor-canvas bg-[linear-gradient(to_right,var(--editor-grid-line)_1px,transparent_1px),linear-gradient(to_bottom,var(--editor-grid-line)_1px,transparent_1px)] bg-[length:20px_20px]"
            data-testid="canvas-area"
            key={tickKey}
          >
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
        </div>
      </div>
    </div>
  );
}
