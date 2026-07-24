import { useEditor } from "../../context/EditorContext";
import { NodeInspector } from "../inspector/NodeInspector";

export function RightSidebar() {
  const { definition, selectedNodeIds, panels, toggleRightSidebar } = useEditor();

  if (!panels.rightSidebarOpen) {
    return null;
  }

  const selectedNodes =
    definition?.nodes?.filter((node) => node.id && selectedNodeIds.includes(node.id)) ?? [];

  return (
    <aside
      className="flex flex-col w-right-sidebar h-full bg-editor-panel border-l border-editor-border shrink-0"
      data-testid="right-sidebar"
    >
      <div className="flex items-center justify-between px-3 py-2 border-b border-editor-border">
        <div className="flex items-center gap-2">
          <svg className="w-4 h-4 text-editor-text-secondary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4" />
          </svg>
          <h2 className="font-medium text-sm text-editor-text">属性</h2>
        </div>
        <button
          type="button"
          className="px-2 py-1 text-lg leading-none text-editor-muted hover:text-editor-text transition-colors"
          onClick={toggleRightSidebar}
          aria-label="折叠右侧面板"
          data-testid="right-sidebar-collapse"
        >
          ›
        </button>
      </div>
      <div className="flex-1 min-h-0 overflow-auto">
        <NodeInspector selectedNodes={selectedNodes} />
      </div>
    </aside>
  );
}
