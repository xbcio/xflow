import { Panel, PanelGroup } from "react-resizable-panels";
import { useEditor } from "../../context/EditorContext";
import { NodeCatalog } from "../catalog/NodeCatalog";
import { NodeInspector } from "../inspector/NodeInspector";

export function RightSidebar() {
  const { definition, selectedNodeIds, panels, toggleRightSidebar } = useEditor();

  if (!panels.rightSidebarOpen) {
    return null;
  }

  const selectedNodes =
    definition?.nodes?.filter((node) => node.id && selectedNodeIds.includes(node.id)) ?? [];

  return (
    <aside className="right-sidebar" data-testid="right-sidebar">
      <div className="right-sidebar__header">
        <h2 className="right-sidebar__title">Properties</h2>
        <button
          type="button"
          className="right-sidebar__collapse"
          onClick={toggleRightSidebar}
          aria-label="Collapse right sidebar"
          data-testid="right-sidebar-collapse"
        >
          &gt;
        </button>
      </div>
      <PanelGroup direction="vertical" className="right-sidebar__panels">
        <Panel defaultSize={50} minSize={10} maxSize={90}>
          <NodeCatalog />
        </Panel>
        <Panel defaultSize={50} minSize={10} maxSize={90}>
          <NodeInspector selectedNodes={selectedNodes} />
        </Panel>
      </PanelGroup>
    </aside>
  );
}
