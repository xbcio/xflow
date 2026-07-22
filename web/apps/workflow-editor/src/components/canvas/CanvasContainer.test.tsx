// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { EditorProvider } from "../../context/EditorContext";
import { CanvasContainer } from "./CanvasContainer";
import type { ReactNode } from "react";

vi.mock("@xflow/workflow-renderer", () => ({
  WorkflowCanvas: (props: { readOnly?: boolean; selectable?: boolean; definition: { name?: string }; selectedNodeIds?: string[] }) => (
    <div
      data-testid="workflow-canvas"
      data-readonly={String(props.readOnly)}
      data-selectable={String(props.selectable)}
      data-definition={props.definition.name}
      data-selected={JSON.stringify(props.selectedNodeIds)}
    >
      WorkflowCanvas
    </div>
  ),
}));

vi.mock("@xyflow/react", () => ({
  ReactFlowProvider: ({ children }: { children: ReactNode }) => <>{children}</>,
}));

afterEach(cleanup);

const definition = {
  id: "wf-test",
  namespace: "default",
  name: "test-workflow",
  version: "1.0.0",
  nodes: [{ id: "node-1", name: "Start", type: "start" }],
  connections: {},
};

describe("CanvasContainer", () => {
  it("renders WorkflowCanvas with editable props", () => {
    render(
      <EditorProvider initialDefinition={definition}>
        <CanvasContainer />
      </EditorProvider>,
    );

    const canvas = screen.getByTestId("workflow-canvas");
    expect(canvas).toBeDefined();
    expect(canvas.getAttribute("data-readonly")).toBe("false");
    expect(canvas.getAttribute("data-selectable")).toBe("true");
    expect(canvas.getAttribute("data-definition")).toBe("test-workflow");
  });

  it("binds selectedNodeIds from EditorContext", () => {
    render(
      <EditorProvider initialDefinition={definition}>
        <CanvasContainer />
      </EditorProvider>,
    );

    const canvas = screen.getByTestId("workflow-canvas");
    expect(canvas.getAttribute("data-selected")).toBe("[]");
  });

  it("shows empty state when no definition is loaded", () => {
    render(
      <EditorProvider>
        <CanvasContainer />
      </EditorProvider>,
    );

    expect(screen.getByTestId("canvas-empty-state")).toBeDefined();
    expect(screen.queryByTestId("workflow-canvas")).toBeNull();
  });
});
