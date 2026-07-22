import { useEditor } from "../../context/EditorContext";
import { NodeOutline } from "../outline/NodeOutline";

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
      className={`left-sidebar${isOpen ? "" : " collapsed"}`}
      data-testid="left-sidebar"
      aria-expanded={isOpen}
    >
      <div className="left-sidebar-header">
        <span className="left-sidebar-title">Outline</span>
        <button
          type="button"
          className="left-sidebar-collapse-button"
          data-testid="left-sidebar-collapse-button"
          aria-label={isOpen ? "Collapse outline" : "Expand outline"}
          onClick={toggleLeftSidebar}
        >
          {isOpen ? "‹" : "›"}
        </button>
      </div>
      {isOpen && (
        <div className="left-sidebar-content">
          <NodeOutline
            definition={definition}
            selectedNodeIds={selectedNodeIds}
            onSelectNode={selectNode}
          />
        </div>
      )}
    </aside>
  );
}
