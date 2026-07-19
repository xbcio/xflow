import { describe, expect, it } from "vitest";
import type { WorkflowDef } from "@xflow/workflow-core";
import { resolveNodeType, workflowToFlow } from "./transform";

const basicDef: WorkflowDef = {
  name: "basic-demo",
  nodes: [
    { name: "trigger", type: "xflow.start" },
    { name: "hello", type: "xflow.log", inputs: [{ name: "message", required: true }] },
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
    { name: "start", type: "xflow.start" },
    { name: "request", type: "approval.request", inputs: [{ name: "approver", required: true }] },
    { name: "approve", type: "approval.human", inputs: [{ name: "decision", required: true }] },
    { name: "notify", type: "notify.email", inputs: [{ name: "to", required: true }] },
  ],
  connections: {
    start: { default: [{ node: "request", input: "approver" }] },
    request: { default: [{ node: "approve", input: "decision" }] },
    approve: { approved: [{ node: "notify", input: "to" }] },
  },
};

const cyclicDef: WorkflowDef = {
  name: "cyclic-demo",
  options: { allow_cycles: true },
  nodes: [
    { name: "loop", type: "xloop.unknown" },
    { name: "check", type: "xflow.if" },
  ],
  connections: {
    loop: { default: [{ node: "check" }] },
    check: { yes: [{ node: "loop" }] },
  },
};

describe("resolveNodeType", () => {
  it("maps known xflow types", () => {
    expect(resolveNodeType({ type: "xflow.start" })).toBe("start");
    expect(resolveNodeType({ type: "xflow.if" })).toBe("if");
    expect(resolveNodeType({ type: "xflow.http" })).toBe("http");
  });

  it("maps approval types", () => {
    expect(resolveNodeType({ type: "approval.request" })).toBe("approval");
    expect(resolveNodeType({ type: "approval.human" })).toBe("approval");
  });

  it("returns unknown for unrecognized types", () => {
    expect(resolveNodeType({ type: "notify.email" })).toBe("unknown");
    expect(resolveNodeType({ type: "acme.custom" })).toBe("unknown");
    expect(resolveNodeType({})).toBe("unknown");
  });
});

describe("workflowToFlow", () => {
  it("converts basic workflow", () => {
    const flow = workflowToFlow(basicDef);
    expect(flow.nodes).toHaveLength(2);
    expect(flow.nodes[0].id).toBe("trigger");
    expect(flow.nodes[0].type).toBe("start");
    expect(flow.nodes[1].id).toBe("hello");
    expect(flow.nodes[1].type).toBe("generic");

    expect(flow.edges).toHaveLength(1);
    expect(flow.edges[0].source).toBe("trigger");
    expect(flow.edges[0].target).toBe("hello");
    expect(flow.edges[0].targetHandle).toBe("message");
    expect(flow.edges[0].sourceHandle).toBeUndefined();
  });

  it("keeps explicit positions", () => {
    const def: WorkflowDef = {
      name: "pos-demo",
      nodes: [{ name: "a", type: "xflow.start", position: { x: 42, y: 99 } }],
    };
    const flow = workflowToFlow(def);
    expect(flow.nodes[0].position).toEqual({ x: 42, y: 99 });
  });

  it("auto-layouts missing positions for approval DAG", () => {
    const flow = workflowToFlow(approvalDef);
    const positions = flow.nodes.map((n) => n.position);
    expect(positions.every((p) => p.x != null && p.y != null)).toBe(true);
    const startNode = flow.nodes.find((n) => n.id === "start")!;
    const notifyNode = flow.nodes.find((n) => n.id === "notify")!;
    expect(startNode.position.x).toBeLessThan(notifyNode.position.x);
  });

  it("auto-layouts cyclic graph without throwing", () => {
    const flow = workflowToFlow(cyclicDef);
    expect(flow.nodes).toHaveLength(2);
    expect(flow.nodes.every((n) => n.position.x != null)).toBe(true);
  });

  it("handles empty graph", () => {
    const flow = workflowToFlow({ name: "empty" });
    expect(flow.nodes).toHaveLength(0);
    expect(flow.edges).toHaveLength(0);
  });

  it("skips edges with missing target node", () => {
    const def: WorkflowDef = {
      name: "dangling",
      nodes: [{ name: "a", type: "xflow.start" }],
      connections: {
        a: {
          default: [{ node: "" }],
        },
      },
    };
    const flow = workflowToFlow(def);
    expect(flow.edges).toHaveLength(0);
  });

  it("preserves named source ports", () => {
    const flow = workflowToFlow(approvalDef);
    const approvedEdge = flow.edges.find((e) => e.source === "approve")!;
    expect(approvedEdge.sourceHandle).toBe("approved");
  });
});
