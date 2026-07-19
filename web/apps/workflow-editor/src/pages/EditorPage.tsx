import { useParams } from "react-router-dom";
import { workflowFixture } from "../mocks/fixtures";

export function EditorPage() {
  const { workflowId } = useParams();

  return (
    <div className="xflow-root editor-page" data-testid="editor-page">
      <h1>Editor: {workflowId}</h1>
      <p>This is a read-only placeholder editor page backed by a static fixture.</p>
      <pre data-testid="fixture-json">{JSON.stringify(workflowFixture, null, 2)}</pre>
    </div>
  );
}
