import { useParams } from "react-router-dom";
import { WorkflowViewer } from "@xflow/workflow-viewer";
import { workflowFixture } from "../mocks/fixtures";

export function ViewerPage() {
  const { workflowId } = useParams();

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
      <WorkflowViewer definition={workflowFixture} />
    </div>
  );
}
