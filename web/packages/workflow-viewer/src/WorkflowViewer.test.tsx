// @vitest-environment jsdom

import * as React from "react";
import { describe, expect, it, beforeAll } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import type { Diagnostic, WorkflowDef } from "@xflow/workflow-core";
import { WorkflowViewer } from "./WorkflowViewer";

class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}

beforeAll(() => {
  (globalThis as unknown as { ResizeObserver: typeof ResizeObserverStub }).ResizeObserver =
    ResizeObserverStub;
});

const basicDef: WorkflowDef = {
  name: "basic",
  nodes: [
    { name: "trigger", type: "xflow.start", position: { x: 0, y: 0 } },
    {
      name: "hello",
      type: "xflow.log",
      position: { x: 200, y: 0 },
      inputs: [{ name: "message", required: true }],
      parameters: { level: "info" },
    },
  ],
  connections: {
    trigger: {
      default: [{ node: "hello", input: "message" }],
    },
  },
};

const diagnostics: Diagnostic[] = [
  {
    code: "PORT_UNKNOWN_INPUT",
    severity: "error",
    message: "No input port message",
    nodeId: "hello",
    connectionRef: { node: "hello", input: "message" },
  },
];

function renderViewer(element: React.ReactElement) {
  return render(
    <div style={{ width: 400, height: 300 }}>{element}</div>
  );
}

describe("WorkflowViewer", () => {
  it("renders the React Flow canvas for a basic workflow", () => {
    const { container } = renderViewer(
      <WorkflowViewer definition={basicDef} />
    );
    expect(container.querySelector(".xflow-root")).not.toBeNull();
    expect(container.querySelector('[data-testid="node-trigger"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="node-hello"]')).not.toBeNull();
  });

  it("remains read-only without mutation controls", () => {
    const { container } = renderViewer(<WorkflowViewer definition={basicDef} />);
    expect(container.querySelector(".react-flow__controls")).toBeNull();
  });

  it("passes diagnostics to the canvas and renders node markers", () => {
    const { container } = renderViewer(
      <WorkflowViewer definition={basicDef} diagnostics={diagnostics} />
    );
    const badge = container.querySelector('[data-testid="node-diagnostics-hello"]');
    expect(badge).not.toBeNull();
    expect(badge?.textContent).toBe("1");
  });

  it("overlays execution snapshot status on nodes", () => {
    const snapshot = { nodeStatuses: { hello: "running" as const } };
    const { container } = renderViewer(
      <WorkflowViewer definition={basicDef} executionSnapshot={snapshot} />
    );
    const status = container.querySelector('[data-testid="node-status-hello"]');
    expect(status?.textContent).toBe("running");
  });

  it("filters nodes by search keyword and selects a result", () => {
    renderViewer(<WorkflowViewer definition={basicDef} />);

    const input = screen.getByLabelText("Search nodes");
    fireEvent.change(input, { target: { value: "hello" } });

    const result = screen.getByRole("option", { name: "hello" });
    expect(result).toBeDefined();

    fireEvent.click(screen.getByRole("button", { name: "hello" }));

    const panel = screen.getByTestId("node-detail-panel");
    expect(panel).toBeDefined();
    expect(screen.getByText("Inputs")).toBeDefined();
    expect(screen.getByText("message *")).toBeDefined();
  });

  it("shows node parameters in the detail panel", () => {
    renderViewer(<WorkflowViewer definition={basicDef} />);

    fireEvent.change(screen.getByLabelText("Search nodes"), {
      target: { value: "hello" },
    });
    fireEvent.click(screen.getByRole("button", { name: "hello" }));

    expect(screen.getByText("Parameters")).toBeDefined();
    expect(screen.getByText(/"level"/)).toBeDefined();
  });
});
