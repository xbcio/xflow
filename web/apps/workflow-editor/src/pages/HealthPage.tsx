import { useEffect, useState } from "react";
import type { RuntimeConfig } from "../config/runtime";
import type { MockWorkflow } from "../mocks";

interface HealthPageProps {
  config: RuntimeConfig;
}

export function HealthPage({ config }: HealthPageProps) {
  const [fixture, setFixture] = useState<MockWorkflow | null>(null);

  useEffect(() => {
    if (import.meta.env.DEV && config.mockEnabled) {
      import("../mocks")
        .then((m) => m.loadMockWorkflow())
        .then(setFixture);
    }
  }, [config.mockEnabled]);

  return (
    <div className="xflow-root health-page" data-testid="health-page">
      <h1>{config.appName}</h1>
      <p className="meta">
        Version: {config.appVersion} · Environment: {config.environment} · Mock:{" "}
        {config.mockEnabled ? "enabled" : "disabled"}
      </p>

      {fixture && (
        <section className="fixture">
          <h2>Workflow Fixture</h2>
          <p>
            <strong>{fixture.name}</strong> ({fixture.namespace} /{" "}
            {fixture.version})
          </p>
          <p>{fixture.description}</p>

          <h3>Nodes</h3>
          <ul data-testid="fixture-nodes">
            {fixture.nodes.map((node) => (
              <li key={node.id}>
                {node.name} ({node.type}) — {node.kind}
              </li>
            ))}
          </ul>

          <h3>Connections</h3>
          <ul data-testid="fixture-connections">
            {Object.entries(fixture.connections).map(([source, ports]) =>
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
      )}
    </div>
  );
}
