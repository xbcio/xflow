import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import type { RuntimeConfig } from "../config/runtime";

interface EditorPageProps {
  config: RuntimeConfig;
}

export function EditorPage({ config }: EditorPageProps) {
  const { workflowId } = useParams();
  const [fixtureName, setFixtureName] = useState<string | null>(null);

  useEffect(() => {
    if (import.meta.env.DEV && config.mockEnabled) {
      import("../mocks")
        .then((m) => m.loadMockWorkflow())
        .then((f) => setFixtureName(f.name));
    }
  }, [config.mockEnabled]);

  return (
    <div className="xflow-root editor-page" data-testid="editor-page">
      <h1>Editor: {workflowId}</h1>
      {config.mockEnabled && fixtureName ? (
        <p data-testid="fixture-summary">
          Editor mock loaded: {fixtureName}
        </p>
      ) : (
        <p data-testid="empty-state">
          Editor not configured. Connect a workflow provider to begin editing.
        </p>
      )}
    </div>
  );
}
