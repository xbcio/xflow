// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import type { ReactNode } from "react";
import { createRuntimeConfig } from "../config/runtime";
import { EditorPage } from "./EditorPage";

afterEach(cleanup);

vi.mock("../mocks", () => ({
  loadMockWorkflow: vi.fn().mockResolvedValue({
    id: "wf-test",
    namespace: "default",
    name: "test-workflow",
    version: "1.0.0",
    description: "Test fixture",
    spec: "1.0",
    nodes: [],
    connections: {},
  }),
}));

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

const config = createRuntimeConfig("Workflow Editor", "0.1.0");
config.mockEnabled = false;

describe("EditorPage", () => {
  it("renders the editor page shell", () => {
    render(
      <MemoryRouter>
        <EditorPage config={config} />
      </MemoryRouter>,
    );
    expect(screen.getByTestId("editor-page")).toBeDefined();
  });

  it("renders the top toolbar", () => {
    render(
      <MemoryRouter>
        <EditorPage config={config} />
      </MemoryRouter>,
    );
    expect(screen.getByRole("toolbar")).toBeDefined();
  });

  it("renders the left sidebar", () => {
    render(
      <MemoryRouter>
        <EditorPage config={config} />
      </MemoryRouter>,
    );
    expect(screen.getByTestId("left-sidebar")).toBeDefined();
  });

  it("renders the canvas container", () => {
    render(
      <MemoryRouter>
        <EditorPage config={config} />
      </MemoryRouter>,
    );
    expect(screen.getByTestId("canvas-container")).toBeDefined();
  });

  it("renders the right sidebar", () => {
    render(
      <MemoryRouter>
        <EditorPage config={config} />
      </MemoryRouter>,
    );
    expect(screen.getByTestId("right-sidebar")).toBeDefined();
  });

  it("renders the bottom panel", () => {
    render(
      <MemoryRouter>
        <EditorPage config={config} />
      </MemoryRouter>,
    );
    expect(screen.getByTestId("bottom-panel")).toBeDefined();
  });
});
