// @vitest-environment jsdom

import * as React from "react";
import { describe, expect, it, beforeAll } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import type { WorkflowDef } from "@xflow/workflow-core";
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

const approvalDef: WorkflowDef = {
  name: "approval-flow",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    {
      name: "request",
      type: "approval.request",
      kind: "action",
      inputs: [{ name: "approver", required: true }],
    },
    { name: "approve", type: "approval.human", kind: "action" },
  ],
  connections: {
    start: {
      default: [{ node: "request", input: "approver" }],
    },
    request: {
      default: [{ node: "approve", input: "decision" }],
    },
  },
};

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

  it("renders all approval workflow nodes", () => {
    const { container } = renderViewer(
      <WorkflowViewer definition={approvalDef} />
    );
    expect(container.querySelector('[data-testid="node-start"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="node-request"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="node-approve"]')).not.toBeNull();
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
