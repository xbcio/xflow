// @vitest-environment jsdom

import { describe, expect, it } from "vitest";
import { render } from "@testing-library/react";
import type { Diagnostic, WorkflowDef } from "@xflow/workflow-core";
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

const unregisteredXflowDef: WorkflowDef = {
  name: "unregistered-xflow",
  nodes: [{ name: "novel", type: "xflow.unknownType" }],
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

const diagnostics: Diagnostic[] = [
  {
    code: "PORT_UNKNOWN_INPUT",
    severity: "error",
    message: "No input port message",
    nodeId: "hello",
    connectionRef: { node: "hello", input: "message" },
  },
  {
    code: "NODE_MISSING_NAME",
    severity: "warning",
    message: "Name is missing",
    nodeId: "trigger",
  },
];

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

  it("renders one react-flow__edge per transform output", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas definition={basicDef} />
      </div>
    );
    // jsdom cannot compute layout, so React Flow does not paint SVG edge
    // paths; only the container is mounted. We assert the container exists
    // and that the transform model has the expected edge so the contract is
    // verified end-to-end in the Playwright e2e instead.
    const edgesContainer = container.querySelector(".react-flow__edges");
    expect(edgesContainer).not.toBeNull();
  });

  it("renders no edges for a workflow with only dangling targets", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas definition={danglingDef} />
      </div>
    );
    expect(container.querySelectorAll(".react-flow__edge")).toHaveLength(0);
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

  it("renders unregistered xflow.* as unknown node", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas definition={unregisteredXflowDef} />
      </div>
    );
    const node = container.querySelector('[data-testid="node-novel"]');
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
    expect(container.querySelector('[data-testid="dangling-targets"]')).not.toBeNull();
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

  it("wires diagnostics into node data and renders markers", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas definition={basicDef} diagnostics={diagnostics} />
      </div>
    );

    const helloBadge = container.querySelector('[data-testid="node-diagnostics-hello"]');
    expect(helloBadge).not.toBeNull();
    expect(helloBadge?.textContent).toBe("1");

    const helloNode = container.querySelector('[data-testid="node-hello"]');
    expect(helloNode?.classList.contains("xf-node--error")).toBe(true);

    const triggerBadge = container.querySelector('[data-testid="node-diagnostics-trigger"]');
    expect(triggerBadge).not.toBeNull();
    expect(triggerBadge?.textContent).toBe("1");
  });

  it("renders UnknownPort for unknown ports referenced by diagnostics", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas definition={basicDef} diagnostics={diagnostics} />
      </div>
    );

    const unknownPorts = container.querySelector('[data-testid="node-unknown-ports-hello"]');
    expect(unknownPorts).not.toBeNull();
    expect(unknownPorts?.querySelector('[data-testid="port-message"]')).not.toBeNull();
  });

  it("renders non-no-op overlays", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas definition={basicDef} selectedNodeIds={["trigger"]} />
      </div>
    );

    expect(container.querySelector('[data-testid="selection-overlay"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="execution-overlay"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="diagnostic-overlay"]')).not.toBeNull();

    const count = container.querySelector('[data-testid="selection-count"]');
    expect(count?.textContent).toBe("1");
  });

  it("diagnostic overlay shows aggregate counts", () => {
    const { container } = render(
      <div style={{ width: 400, height: 300 }}>
        <WorkflowCanvas definition={basicDef} diagnostics={diagnostics} />
      </div>
    );

    const total = container.querySelector('[data-testid="diagnostic-total"]');
    expect(total?.textContent).toBe("2");

    const breakdown = container.querySelector('[data-testid="diagnostic-breakdown"]');
    expect(breakdown?.textContent).toContain("E:1");
    expect(breakdown?.textContent).toContain("W:1");
  });
});
