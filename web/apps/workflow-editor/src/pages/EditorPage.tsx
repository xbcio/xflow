import { useEffect } from "react";
import { useParams } from "react-router-dom";
import type { RuntimeConfig } from "../config/runtime";
import { EditorProvider, useEditor } from "../context/EditorContext";
import { TopToolbar } from "../components/layout/TopToolbar";
import { LeftSidebar } from "../components/layout/LeftSidebar";
import { CanvasContainer } from "../components/canvas/CanvasContainer";
import { RightSidebar } from "../components/layout/RightSidebar";
import { BottomPanel } from "../components/layout/BottomPanel";

interface EditorPageProps {
  config: RuntimeConfig;
}

export function EditorPage({ config }: EditorPageProps) {
  const { workflowId } = useParams();

  return (
    <EditorProvider>
      <EditorPageInner workflowId={workflowId} config={config} />
    </EditorProvider>
  );
}

interface EditorPageInnerProps {
  workflowId?: string;
  config: RuntimeConfig;
}

function EditorPageInner({ workflowId, config }: EditorPageInnerProps) {
  const { setDefinition } = useEditor();

  useEffect(() => {
    if (import.meta.env.DEV && config.mockEnabled) {
      let cancelled = false;
      import("../mocks")
        .then((m) => m.loadMockWorkflow())
        .then((definition) => {
          if (!cancelled) {
            setDefinition(definition);
          }
        });
      return () => {
        cancelled = true;
      };
    }
    return undefined;
  }, [config.mockEnabled, setDefinition]);

  return (
    <div className="xflow-root editor-page" data-testid="editor-page">
      <TopToolbar workflowId={workflowId} />
      <div className="editor-page__workspace">
        <LeftSidebar />
        <main className="editor-page__canvas">
          <CanvasContainer />
        </main>
        <RightSidebar />
      </div>
      <BottomPanel />
    </div>
  );
}
