// @vitest-environment jsdom

import * as React from "react";
import { describe, expect, it } from "vitest";
import { render } from "@testing-library/react";
import type { WorkflowDef } from "@xflow/workflow-core";
import { WorkflowCanvas } from "./WorkflowCanvas";

// jsdom does not implement ResizeObserver; provide a minimal stub.
class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}
(globalThis as unknown as { ResizeObserver: typeof ResizeObserverStub }).ResizeObserver =
  ResizeObserverStub;

const basicDef: WorkflowDef = {
  name: "basic",
  nodes: [
    { name: "trigger", type: "xflow.start", position: { x: 0, y: 0 } },
    { name: "hello", type: "xflow.log", position: { x: 200, y: 0 } },
  ],
  connections: {
    trigger: {
      default: [{ node: "hello", input: "message" }],
    },
  },
};

const unknownDef: WorkflowDef = {
  name: "unknown",
  nodes: [{ name: "weird", type: "acme.unknown" }],
};

const danglingDef: WorkflowDef = {
  name: "dangling",
  nodes: [{ name: "a", type: "xflow.start" }],
  connections: {
    a: {
      default: [{ node: "ghost" }],
    },
  },
};

describe("WorkflowCanvas", () => {
  it("renders basic workflow without crashing", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas definition={basicDef} />
      </div>
    );
    expect(container.querySelector(".xflow-root")).not.toBeNull();
    expect(container.querySelector('[data-testid="node-trigger"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="node-hello"]')).not.toBeNull();
  });

  it("renders empty graph without crashing", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas definition={{ name: "empty" }} />
      </div>
    );
    expect(container.querySelector(".xflow-root")).not.toBeNull();
  });

  it("renders unknown node without crashing", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas definition={unknownDef} />
      </div>
    );
    const node = container.querySelector('[data-testid="node-weird"]');
    expect(node).not.toBeNull();
    expect(node?.getAttribute("data-node-kind")).toBe("unknown");
  });

  it("renders dangling edge target without crashing", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas definition={danglingDef} />
      </div>
    );
    expect(container.querySelector(".xflow-root")).not.toBeNull();
    expect(container.querySelector('[data-testid="node-a"]')).not.toBeNull();
  });

  it("overlays execution snapshot status on nodes", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas
          definition={basicDef}
          executionSnapshot={{ nodeStatuses: { hello: "running" } }}
        />
      </div>
    );
    const status = container.querySelector('[data-testid="node-status-hello"]');
    expect(status?.textContent).toBe("running");
  });

  it("applies custom className", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas definition={basicDef} className="my-canvas" />
      </div>
    );
    expect(container.querySelector(".xflow-root.my-canvas")).not.toBeNull();
  });
});
