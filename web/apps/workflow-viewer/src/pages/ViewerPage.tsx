import { useParams } from "react-router-dom";
import { workflowFixture } from "../mocks/fixtures";

export function ViewerPage() {
  const { workflowId } = useParams();

  return (
    <div className="xflow-root viewer-page" data-testid="viewer-page">
      <h1>Viewer: {workflowId}</h1>
      <p>This is a read-only viewer page backed by a static fixture.</p>
      <pre data-testid="fixture-json">{JSON.stringify(workflowFixture, null, 2)}</pre>
    </div>
  );
}
