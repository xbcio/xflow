import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { WorkflowViewer } from "@xflow/workflow-viewer";
import type { WorkflowDef } from "@xflow/workflow-core";
import type { RuntimeConfig } from "../config/runtime";

interface ViewerPageProps {
  config: RuntimeConfig;
}

export function ViewerPage({ config }: ViewerPageProps) {
  const { workflowId } = useParams();
  const [definition, setDefinition] = useState<WorkflowDef | null>(null);

  useEffect(() => {
    if (import.meta.env.DEV && config.mockEnabled) {
      import("../mocks")
        .then((m) => m.loadMockWorkflow())
        .then(setDefinition);
    }
  }, [config.mockEnabled]);

  if (!definition) {
    return (
      <div
        className="xflow-root viewer-page"
        data-testid="viewer-page"
        style={{ width: "100vw", height: "100vh", position: "relative" }}
      >
        <h1>Viewer: {workflowId}</h1>
        <p data-testid="empty-state">
          No workflow loaded. Configure the API provider, or run in development
          with mock data enabled.
        </p>
      </div>
    );
  }

  return (
    <div
      className="viewer-page"
      data-testid="viewer-page"
      style={{ width: "100vw", height: "100vh", position: "relative" }}
    >
      <h1
        style={{
          position: "absolute",
          top: 12,
          left: 300,
          zIndex: 20,
          margin: 0,
          fontSize: 18,
          pointerEvents: "none",
        }}
      >
        Viewer: {workflowId}
      </h1>
      <WorkflowViewer definition={definition} />
    </div>
  );
}
