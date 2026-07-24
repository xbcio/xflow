// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import type { NodeDef } from "@xflow/workflow-core";
import { NodeInspector } from "./NodeInspector";

afterEach(cleanup);

describe("NodeInspector", () => {
  it("shows empty state when no node is selected", () => {
    render(<NodeInspector selectedNodes={[]} />);

    expect(screen.getByTestId("node-inspector")).toBeDefined();
    expect(screen.getByText("请在画布中选择一个节点")).toBeDefined();
    expect(screen.queryByTestId("inspector-content")).toBeNull();
  });

  it("shows multiple selection state when more than one node is selected", () => {
    const nodes: NodeDef[] = [
      { id: "a", name: "First", type: "start" },
      { id: "b", name: "Second", type: "end" },
    ];

    render(<NodeInspector selectedNodes={nodes} />);

    expect(screen.getByTestId("inspector-multi")).toBeDefined();
    expect(screen.queryByTestId("inspector-content")).toBeNull();
  });

  it("renders node details for a single selected node", () => {
    const node: NodeDef = {
      id: "node-1",
      name: "Fetch Data",
      type: "http",
      kind: "action",
      version: 1,
      inputs: [{ name: "url", required: true }, { name: "method" }],
      parameters: {
        timeout: 30,
        headers: { Accept: "application/json" },
      },
    };

    render(<NodeInspector selectedNodes={[node]} />);

    expect(screen.getByTestId("inspector-content")).toBeDefined();
    expect(screen.getByTestId("inspector-type").textContent).toBe("http");
    expect(screen.getByTestId("inspector-kind").textContent).toBe("action");

    const inputs = screen.getAllByTestId("inspector-input");
    expect(inputs.length).toBe(2);
    expect(inputs[0].textContent).toContain("url");

    const parameters = screen.getAllByTestId("inspector-parameter");
    expect(parameters.length).toBe(2);
    expect(parameters[0].textContent).toContain("timeout");
  });

  it("renders placeholder values when optional fields are missing", () => {
    const node: NodeDef = { id: "node-2" };

    render(<NodeInspector selectedNodes={[node]} />);

    expect(screen.getByTestId("inspector-type").textContent).toContain("未知");
    expect(screen.getByTestId("inspector-kind").textContent).toContain("未知");
  });
});
