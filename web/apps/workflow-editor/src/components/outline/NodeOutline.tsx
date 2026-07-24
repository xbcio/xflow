import type { WorkflowDef } from "@xflow/workflow-core";

export interface NodeOutlineProps {
  definition: WorkflowDef | null;
  selectedNodeIds: string[];
  onSelectNode: (nodeId: string) => void;
}

const typeColorMap: Record<string, string> = {
  start: "bg-blue-500",
  end: "bg-red-500",
  http: "bg-cyan-500",
  grpc: "bg-cyan-600",
  function: "bg-pink-500",
  database: "bg-orange-500",
  if: "bg-blue-500",
  switch: "bg-purple-500",
  merge: "bg-yellow-500",
  wait: "bg-amber-500",
  approval: "bg-indigo-500",
};

function getNodeColor(type?: string): string {
  return type ? typeColorMap[type] ?? "bg-gray-500" : "bg-gray-500";
}

export function NodeOutline({
  definition,
  selectedNodeIds,
  onSelectNode,
}: NodeOutlineProps) {
  const nodes = definition?.nodes ?? [];
  const selectedSet = new Set(selectedNodeIds);

  return (
    <div className="h-full overflow-auto p-2" data-testid="node-outline">
      <ul className="list-none p-0 m-0 space-y-0.5" role="listbox" aria-label="节点大纲">
        {nodes.length === 0 && (
          <li className="px-2 py-1.5 text-sm text-editor-text-secondary" role="option" aria-disabled="true">
            暂无节点
          </li>
        )}
        {nodes.map((node, index) => {
          const nodeId = node.id ?? "";
          const isSelected = selectedSet.has(nodeId);
          const depth = 0; // TODO: compute nesting depth from connections if needed
          const seq = String(index + 1);

          return (
            <li
              key={nodeId || `${node.name}-${node.type}`}
              className={`outline-item flex items-center gap-2 py-1.5 px-2 rounded cursor-pointer text-editor-text transition-colors hover:bg-editor-hover ${
                isSelected
                  ? "selected bg-editor-selected-bg text-editor-selected-text"
                  : ""
              } ${isSelected ? "[&>.outline-item__seq]:text-inherit [&>.outline-item__seq]:opacity-70 [&>.outline-item__type]:text-inherit [&>.outline-item__type]:opacity-70" : ""}`}
              style={{ paddingLeft: `${0.5 + depth * 1}rem` }}
              role="option"
              aria-selected={isSelected}
              data-testid={`node-outline-item-${nodeId}`}
              onClick={() => onSelectNode(nodeId)}
            >
              <span className="outline-item__seq w-7 text-[10px] text-editor-muted text-right">{seq}</span>
              <span className={`w-1.5 h-1.5 rounded-full ${getNodeColor(node.type)}`} />
              <span className="flex-1 truncate text-sm">{node.name ?? "未命名"}</span>
              <span className="outline-item__type text-[10px] uppercase text-editor-muted">{node.type ?? "unknown"}</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
