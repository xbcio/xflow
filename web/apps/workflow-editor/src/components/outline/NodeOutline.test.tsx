// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { WorkflowDef } from "@xflow/workflow-core";
import { NodeOutline } from "./NodeOutline";

const mockDefinition: WorkflowDef = {
  id: "wf-test",
  name: "test-workflow",
  nodes: [
    { id: "start", name: "Start", type: "start" },
    { id: "http", name: "HTTP Request", type: "http" },
    { id: "end", name: "End", type: "end" },
  ],
};

afterEach(cleanup);

describe("NodeOutline", () => {
  it("renders a row for each node", () => {
    render(
      <NodeOutline
        definition={mockDefinition}
        selectedNodeIds={[]}
        onSelectNode={vi.fn()}
      />,
    );

    expect(screen.getByTestId("node-outline")).toBeDefined();
    expect(screen.getByText("Start")).toBeDefined();
    expect(screen.getByText("HTTP Request")).toBeDefined();
    expect(screen.getByText("End")).toBeDefined();
    expect(screen.getByText("http")).toBeDefined();
  });

  it("renders empty state when definition has no nodes", () => {
    render(
      <NodeOutline
        definition={{ id: "wf-empty", name: "empty-workflow", nodes: [] }}
        selectedNodeIds={[]}
        onSelectNode={vi.fn()}
      />,
    );

    expect(screen.getByText("暂无节点")).toBeDefined();
  });

  it("renders empty state when definition is null", () => {
    render(
      <NodeOutline
        definition={null}
        selectedNodeIds={[]}
        onSelectNode={vi.fn()}
      />,
    );

    expect(screen.getByText("暂无节点")).toBeDefined();
  });

  it("highlights selected nodes", () => {
    const { container } = render(
      <NodeOutline
        definition={mockDefinition}
        selectedNodeIds={["http"]}
        onSelectNode={vi.fn()}
      />,
    );

    const selectedItem = container.querySelector(".outline-item.selected");
    expect(selectedItem).not.toBeNull();
    expect(selectedItem?.getAttribute("data-testid")).toBe(
      "node-outline-item-http",
    );
  });

  it("calls onSelectNode with node id when a row is clicked", () => {
    const handleSelect = vi.fn();
    render(
      <NodeOutline
        definition={mockDefinition}
        selectedNodeIds={[]}
        onSelectNode={handleSelect}
      />,
    );

    fireEvent.click(screen.getByText("HTTP Request"));
    expect(handleSelect).toHaveBeenCalledTimes(1);
    expect(handleSelect).toHaveBeenCalledWith("http");
  });
});
