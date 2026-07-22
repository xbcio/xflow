import type { NodeDef } from "@xflow/workflow-core";

export interface NodeInspectorProps {
  selectedNodes: NodeDef[];
}

export function NodeInspector({ selectedNodes }: NodeInspectorProps) {
  if (selectedNodes.length === 0) {
    return (
      <div className="node-inspector" data-testid="node-inspector">
        <div className="node-inspector__header">
          <h3>Node Inspector</h3>
        </div>
        <div className="node-inspector__empty" data-testid="inspector-empty">
          Select a node to view its details.
        </div>
      </div>
    );
  }

  if (selectedNodes.length > 1) {
    return (
      <div className="node-inspector" data-testid="node-inspector">
        <div className="node-inspector__header">
          <h3>Node Inspector</h3>
        </div>
        <div className="node-inspector__empty" data-testid="inspector-multi">
          {selectedNodes.length} nodes selected. Select a single node to inspect.
        </div>
      </div>
    );
  }

  const node = selectedNodes[0];
  const inputs = node.inputs ?? [];
  const parameters = node.parameters ? Object.entries(node.parameters) : [];

  return (
    <div className="node-inspector" data-testid="node-inspector">
      <div className="node-inspector__header">
        <h3>Node Inspector</h3>
      </div>
      <div className="node-inspector__content" data-testid="inspector-content">
        <dl className="node-inspector__properties">
          <div className="node-inspector__property">
            <dt>Name</dt>
            <dd data-testid="inspector-name">{node.name ?? <em>unnamed</em>}</dd>
          </div>
          <div className="node-inspector__property">
            <dt>Type</dt>
            <dd data-testid="inspector-type">{node.type ?? <em>unknown</em>}</dd>
          </div>
          <div className="node-inspector__property">
            <dt>Kind</dt>
            <dd data-testid="inspector-kind">{node.kind ?? <em>unknown</em>}</dd>
          </div>
          <div className="node-inspector__property">
            <dt>Version</dt>
            <dd data-testid="inspector-version">{node.version ?? <em>none</em>}</dd>
          </div>
        </dl>

        <div className="node-inspector__section">
          <h4>Inputs</h4>
          {inputs.length === 0 ? (
            <p className="node-inspector__none">No inputs defined.</p>
          ) : (
            <ul className="node-inspector__list">
              {inputs.map((input, index) => (
                <li key={index} data-testid="inspector-input">
                  {input.name ?? `input-${index}`}
                  {input.required ? <span className="node-inspector__required"> *</span> : null}
                </li>
              ))}
            </ul>
          )}
        </div>

        <div className="node-inspector__section">
          <h4>Parameters</h4>
          {parameters.length === 0 ? (
            <p className="node-inspector__none">No parameters defined.</p>
          ) : (
            <dl className="node-inspector__parameters">
              {parameters.map(([key, value]) => (
                <div key={key} className="node-inspector__parameter" data-testid="inspector-parameter">
                  <dt>{key}</dt>
                  <dd>{typeof value === "object" ? JSON.stringify(value) : String(value)}</dd>
                </div>
              ))}
            </dl>
          )}
        </div>
      </div>
    </div>
  );
}
