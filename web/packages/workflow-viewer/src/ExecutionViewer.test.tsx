// @vitest-environment jsdom

import { describe, expect, it, beforeAll } from "vitest";
import { render, screen } from "@testing-library/react";
import type { WorkflowDef } from "@xflow/workflow-core";
import { ExecutionViewer } from "./ExecutionViewer";

class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}

beforeAll(() => {
  (globalThis as unknown as { ResizeObserver: typeof ResizeObserverStub }).ResizeObserver =
    ResizeObserverStub;
});

const definition: WorkflowDef = {
  name: "status-demo",
  nodes: [
    { name: "a", type: "xflow.start" },
    { name: "b", type: "xflow.log" },
    { name: "c", type: "xflow.log" },
    { name: "d", type: "xflow.log" },
  ],
  connections: {
    a: { default: [{ node: "b" }] },
  },
};

describe("ExecutionViewer", () => {
  it("renders execution status summary counts", () => {
    const snapshot = {
      nodeStatuses: {
        a: "completed" as const,
        b: "running" as const,
        c: "failed" as const,
        d: "suspended" as const,
      },
    };

    render(
      <div style={{ width: 400, height: 300 }}>
        <ExecutionViewer definition={definition} executionSnapshot={snapshot} />
      </div>
    );

    const summary = screen.getByTestId("execution-summary");
    expect(summary.textContent).toContain("Running: 1");
    expect(summary.textContent).toContain("Completed: 1");
    expect(summary.textContent).toContain("Failed: 1");
    expect(summary.textContent).toContain("Suspended: 1");
  });

  it("renders zero counts when no statuses are provided", () => {
    render(
      <div style={{ width: 400, height: 300 }}>
        <ExecutionViewer definition={definition} executionSnapshot={{}} />
      </div>
    );

    const summary = screen.getByTestId("execution-summary");
    expect(summary.textContent).toContain("Running: 0");
    expect(summary.textContent).toContain("Completed: 0");
    expect(summary.textContent).toContain("Failed: 0");
    expect(summary.textContent).toContain("Suspended: 0");
  });
});
