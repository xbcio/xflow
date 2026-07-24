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
  const { setDefinition, theme } = useEditor();

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
    <div
      className={`xflow-root flex flex-col w-screen h-screen overflow-hidden bg-editor-bg text-editor-text ${theme === "dark" ? "dark" : ""}`}
      data-testid="editor-page"
    >
      <TopToolbar workflowId={workflowId} />
      <div className="flex flex-row flex-1 min-h-0 overflow-hidden">
        <LeftSidebar />
        <main className="relative flex-1 min-w-0 min-h-0 p-0 overflow-hidden">
          <CanvasContainer />
        </main>
        <RightSidebar />
      </div>
      <BottomPanel />
    </div>
  );
}
