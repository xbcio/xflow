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

  it("returns unknown for unregistered xflow.* types", () => {
    expect(resolveNodeType({ type: "xflow.unknownType" })).toBe("unknown");
    expect(resolveNodeType({ type: "xflow.custom" })).toBe("unknown");
  });

  it("preserves explicit generic type", () => {
    expect(resolveNodeType({ type: "xflow.generic" })).toBe("generic");
  });
});

describe("workflowToFlow", () => {
  it("converts basic workflow", () => {
    const flow = workflowToFlow(basicDef);
    expect(flow.nodes).toHaveLength(2);
    expect(flow.nodes[0].id).toBe("trigger");
    expect(flow.nodes[0].type).toBe("start");
    expect(flow.nodes[1].id).toBe("hello");
    expect(flow.nodes[1].type).toBe("unknown");

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
    expect(flow.danglingTargets).toHaveLength(0);
  });

  it("collects dangling targets", () => {
    const def: WorkflowDef = {
      name: "dangling",
      nodes: [{ name: "a", type: "xflow.start" }],
      connections: {
        a: {
          default: [{ node: "ghost", input: "in" }],
        },
      },
    };
    const flow = workflowToFlow(def);
    expect(flow.edges).toHaveLength(0);
    expect(flow.danglingTargets).toHaveLength(1);
    expect(flow.danglingTargets[0]).toEqual({
      source: "a",
      port: "default",
      target: "ghost",
      input: "in",
    });
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

  it("emits named source ports as sourceHandle", () => {
    const flow = workflowToFlow(approvalDef);
    const approvedEdge = flow.edges.find((e) => e.source === "approve")!;
    // Named source ports must match a real <Handle id="..."> on the source
    // node so React Flow can connect the edge without warnings.
    expect(approvedEdge.sourceHandle).toBe("approved");
    // The named input port is still preserved on the target side.
    expect(approvedEdge.targetHandle).toBe("to");
  });

  it("normalizes main/default/empty target input to undefined", () => {
    const def: WorkflowDef = {
      name: "port-normalization",
      nodes: [
        { name: "a", type: "xflow.start" },
        { name: "b", type: "xflow.end" },
        { name: "c", type: "xflow.end" },
        { name: "d", type: "xflow.end" },
      ],
      connections: {
        a: {
          default: [
            { node: "b", input: "main" },
            { node: "c", input: "default" },
            { node: "d", input: "" },
          ],
        },
      },
    };
    const flow = workflowToFlow(def);
    expect(flow.edges).toHaveLength(3);
    for (const edge of flow.edges) {
      expect(edge.targetHandle).toBeUndefined();
      expect(edge.sourceHandle).toBeUndefined();
    }
  });

  it("keeps named input port as targetHandle", () => {
    const def: WorkflowDef = {
      name: "named-input",
      nodes: [
        { name: "src", type: "xflow.start" },
        { name: "dst", type: "xflow.log", inputs: [{ name: "message", required: true }] },
      ],
      connections: {
        src: { default: [{ node: "dst", input: "message" }] },
      },
    };
    const flow = workflowToFlow(def);
    expect(flow.edges).toHaveLength(1);
    expect(flow.edges[0].targetHandle).toBe("message");
    expect(flow.edges[0].sourceHandle).toBeUndefined();
  });

  it("sets sourcePorts on node data for named output ports", () => {
    const flow = workflowToFlow(approvalDef);
    const approveNode = flow.nodes.find((n) => n.id === "approve")!;
    expect(approveNode.data.sourcePorts).toEqual(["approved"]);

    const startNode = flow.nodes.find((n) => n.id === "start")!;
    expect(startNode.data.sourcePorts).toBeUndefined();
  });

  it("normalizes main/default/empty source ports to undefined", () => {
    const def: WorkflowDef = {
      name: "source-normalization",
      nodes: [
        { name: "a", type: "xflow.start" },
        { name: "b", type: "xflow.end" },
        { name: "c", type: "xflow.end" },
        { name: "d", type: "xflow.end" },
      ],
      connections: {
        a: {
          main: [{ node: "b" }],
          default: [{ node: "c" }],
          "": [{ node: "d" }],
        },
      },
    };
    const flow = workflowToFlow(def);
    expect(flow.edges).toHaveLength(3);
    for (const edge of flow.edges) {
      expect(edge.sourceHandle).toBeUndefined();
    }
    const aNode = flow.nodes.find((n) => n.id === "a")!;
    expect(aNode.data.sourcePorts).toBeUndefined();
    expect(aNode.data.hasDefaultSourcePort).toBe(true);
  });

  it("marks mixed named + default source ports on node data", () => {
    const def: WorkflowDef = {
      name: "mixed-source",
      nodes: [
        { name: "a", type: "xflow.if" },
        { name: "b", type: "xflow.end" },
        { name: "c", type: "xflow.end" },
      ],
      connections: {
        a: {
          approved: [{ node: "b" }],
          default: [{ node: "c" }],
        },
      },
    };
    const flow = workflowToFlow(def);
    const aNode = flow.nodes.find((n) => n.id === "a")!;
    expect(aNode.data.sourcePorts).toEqual(["approved"]);
    expect(aNode.data.hasDefaultSourcePort).toBe(true);

    const namedEdge = flow.edges.find((e) => e.sourceHandle === "approved")!;
    expect(namedEdge.target).toBe("b");
    const defaultEdge = flow.edges.find((e) => e.sourceHandle === undefined)!;
    expect(defaultEdge.target).toBe("c");
  });

  it("records missing source nodes without emitting dangling edges", () => {
    const def: WorkflowDef = {
      name: "missing-source",
      nodes: [{ name: "b", type: "xflow.end" }],
      connections: {
        ghost: {
          default: [{ node: "b", input: "in" }],
        },
      },
    };
    const flow = workflowToFlow(def);
    expect(flow.edges).toHaveLength(0);
    expect(flow.missingSources).toHaveLength(1);
    expect(flow.missingSources[0]).toEqual({
      source: "ghost",
      port: "default",
      target: "b",
      input: "in",
    });
  });

  it("missing source and dangling target are tracked separately", () => {
    const def: WorkflowDef = {
      name: "both-missing",
      nodes: [{ name: "real", type: "xflow.start" }],
      connections: {
        ghost: { default: [{ node: "real" }] },
        real: { default: [{ node: "phantom" }] },
      },
    };
    const flow = workflowToFlow(def);
    expect(flow.edges).toHaveLength(0);
    expect(flow.missingSources).toHaveLength(1);
    expect(flow.missingSources[0].source).toBe("ghost");
    expect(flow.danglingTargets).toHaveLength(1);
    expect(flow.danglingTargets[0].target).toBe("phantom");
  });
});
