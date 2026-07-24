import { Panel, PanelGroup } from "react-resizable-panels";
import { useEditor } from "../../context/EditorContext";
import { NodeCatalog } from "../catalog/NodeCatalog";
import { NodeOutline } from "../outline/NodeOutline";

const collapseButtonClass =
  "px-2 py-1 text-lg leading-none text-editor-muted hover:text-editor-text transition-colors";

export function LeftSidebar() {
  const {
    definition,
    selectedNodeIds,
    panels,
    selectNode,
    toggleLeftSidebar,
  } = useEditor();

  const isOpen = panels.leftSidebarOpen;

  return (
    <aside
      className={`flex flex-col h-full bg-editor-panel border-r border-editor-border shrink-0 ${
        isOpen ? "w-left-sidebar" : "w-auto collapsed left-sidebar"
      }`}
      data-testid="left-sidebar"
      aria-expanded={isOpen}
    >
      {isOpen ? (
        <PanelGroup direction="vertical" className="flex-1 min-h-0">
          <Panel defaultSize={55} minSize={10} maxSize={90}>
            <NodeCatalog />
          </Panel>
          <Panel defaultSize={45} minSize={10} maxSize={90}>
            <div className="flex flex-col h-full border-t border-editor-border">
              <div className="flex items-center justify-between px-3 py-2 border-b border-editor-border">
                <div className="flex items-center gap-2">
                  <svg className="w-4 h-4 text-editor-text-secondary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h7M20 18l-3-3m0 6l3-3" />
                  </svg>
                  <span className="text-sm font-medium text-editor-text">大纲</span>
                </div>
                <button
                  type="button"
                  className={collapseButtonClass}
                  data-testid="left-sidebar-collapse-button"
                  aria-label="折叠左侧面板"
                  onClick={toggleLeftSidebar}
                >
                  ‹
                </button>
              </div>
              <div className="flex-1 min-h-0 overflow-hidden">
                <NodeOutline
                  definition={definition}
                  selectedNodeIds={selectedNodeIds}
                  onSelectNode={selectNode}
                />
              </div>
            </div>
          </Panel>
        </PanelGroup>
      ) : (
        <div className="flex flex-col h-full">
          <div className="flex items-center justify-between px-3 py-2 border-b border-editor-border">
            <button
              type="button"
              className={collapseButtonClass}
              data-testid="left-sidebar-collapse-button"
              aria-label="展开左侧面板"
              onClick={toggleLeftSidebar}
            >
              ›
            </button>
          </div>
        </div>
      )}
    </aside>
  );
}
