// @vitest-environment jsdom
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import type { ReactNode } from "react";
import { createRuntimeConfig } from "../config/runtime";
import { EditorPage } from "./EditorPage";
import { loadMockWorkflow } from "../mocks";
import { workflowFixture } from "../mocks/fixtures";

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

  it("loads the mock fixture and renders all fixture nodes in the outline", async () => {
    vi.mocked(loadMockWorkflow).mockResolvedValue(workflowFixture);

    const mockConfig = createRuntimeConfig("Workflow Editor", "0.1.0");
    mockConfig.mockEnabled = true;

    render(
      <MemoryRouter>
        <EditorPage config={mockConfig} />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("node-outline")).toBeDefined();
    });

    expect(screen.getByTestId("node-outline-item-start")).toBeDefined();
    expect(screen.getByTestId("node-outline-item-http")).toBeDefined();
    expect(screen.getByTestId("node-outline-item-if")).toBeDefined();
    expect(screen.getByTestId("node-outline-item-database")).toBeDefined();
    expect(screen.getByTestId("node-outline-item-function")).toBeDefined();
    expect(screen.getByTestId("node-outline-item-merge")).toBeDefined();
    expect(screen.getByTestId("node-outline-item-end")).toBeDefined();
  });
});
