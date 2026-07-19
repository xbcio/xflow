import type { RuntimeConfig } from "../config/runtime";
import { workflowFixture } from "../mocks/fixtures";

interface HealthPageProps {
  config: RuntimeConfig;
}

export function HealthPage({ config }: HealthPageProps) {
  return (
    <div className="xflow-root health-page" data-testid="health-page">
      <h1>{config.appName}</h1>
      <p className="meta">
        Version: {config.appVersion} · Environment: {config.environment} · Mock:{" "}
        {config.mockEnabled ? "enabled" : "disabled"}
      </p>

      <section className="fixture">
        <h2>Workflow Fixture</h2>
        <p>
          <strong>{workflowFixture.name}</strong> ({workflowFixture.namespace} /{" "}
          {workflowFixture.version})
        </p>
        <p>{workflowFixture.description}</p>

        <h3>Nodes</h3>
        <ul data-testid="fixture-nodes">
          {workflowFixture.nodes.map((node) => (
            <li key={node.id}>
              {node.name} ({node.type}) — {node.kind}
            </li>
          ))}
        </ul>

        <h3>Connections</h3>
        <ul data-testid="fixture-connections">
          {Object.entries(workflowFixture.connections).map(([source, ports]) =>
            Object.entries(ports).map(([port, targets]) =>
              targets.map((target) => (
                <li key={`${source}-${port}-${target.node}`}>
                  {source}:{port} → {target.node}:{target.input}
                </li>
              ))
            )
          )}
        </ul>
      </section>
    </div>
  );
}
