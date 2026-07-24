// @vitest-environment jsdom
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { createRuntimeConfig } from "../config/runtime";
import { ViewerPage } from "./ViewerPage";

vi.mock("@xflow/workflow-viewer", () => ({
  WorkflowViewer: ({ definition }: { definition: { id: string; name: string } }) => (
    <div data-testid="workflow-viewer" data-definition-id={definition.id}>
      <div className="react-flow__node">{definition.name}</div>
    </div>
  ),
}));

vi.mock("../mocks", () => ({
  loadMockWorkflow: vi.fn().mockResolvedValue({
    id: "wf-test",
    namespace: "default",
    name: "test-workflow",
    version: "1.0.0",
    description: "Test fixture",
    spec: "1.0",
    nodes: [
      {
        id: "a",
        name: "A",
        type: "xflow.start",
        kind: "trigger",
        version: 1,
        position: { x: 0, y: 0 },
      },
      {
        id: "b",
        name: "B",
        type: "xflow.end",
        kind: "action",
        version: 1,
        position: { x: 100, y: 0 },
      },
    ],
    connections: {},
  }),
}));

describe("ViewerPage", () => {
  it("renders empty state when mock is disabled", () => {
    const config = createRuntimeConfig("Workflow Viewer", "0.1.0");
    config.mockEnabled = false;
    render(<ViewerPage config={config} />);

    expect(screen.getByTestId("empty-state")).toBeDefined();
    expect(document.querySelector(".react-flow__node")).toBeNull();
  });

  it("renders workflow canvas when mock is enabled", async () => {
    const config = createRuntimeConfig("Workflow Viewer", "0.1.0");
    config.mockEnabled = true;
    render(<ViewerPage config={config} />);

    await waitFor(() => {
      expect(screen.queryByTestId("empty-state")).toBeNull();
    });
    await waitFor(() => {
      expect(document.querySelector(".react-flow__node")).not.toBeNull();
    });
    expect(screen.getByTestId("workflow-viewer")).toBeDefined();
  });
});
