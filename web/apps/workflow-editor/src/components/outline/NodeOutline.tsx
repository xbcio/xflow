import type { WorkflowDef } from "@xflow/workflow-core";

export interface NodeOutlineProps {
  definition: WorkflowDef | null;
  selectedNodeIds: string[];
  onSelectNode: (nodeId: string) => void;
}

export function NodeOutline({
  definition,
  selectedNodeIds,
  onSelectNode,
}: NodeOutlineProps) {
  const nodes = definition?.nodes ?? [];
  const selectedSet = new Set(selectedNodeIds);

  return (
    <div className="node-outline" data-testid="node-outline">
      <ul className="node-outline-list" role="listbox" aria-label="Node outline">
        {nodes.length === 0 && (
          <li className="node-outline-empty" role="option" aria-disabled="true">
            No nodes
          </li>
        )}
        {nodes.map((node) => {
          const nodeId = node.id ?? "";
          const isSelected = selectedSet.has(nodeId);

          return (
            <li
              key={nodeId || `${node.name}-${node.type}`}
              className={`node-outline-item${isSelected ? " selected" : ""}`}
              role="option"
              aria-selected={isSelected}
              data-testid={`node-outline-item-${nodeId}`}
              onClick={() => onSelectNode(nodeId)}
            >
              <span className="node-outline-name">{node.name}</span>
              <span className="node-outline-type">{node.type}</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
