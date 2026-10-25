import { describe, expect, it } from "vitest";
import { autoLayoutPositions, declaredOutputPorts, toGraphModel, type WorkflowDef } from "./index";

describe("declaredOutputPorts", () => {
  it("replaces the single default output with the branches a type declares", () => {
    expect(declaredOutputPorts({ type: "xflow.if" })).toEqual({
      ports: ["true", "false"],
      hasMain: false
    });
    expect(declaredOutputPorts({ type: "xflow.approval" })).toEqual({
      ports: ["approved", "rejected", "returned", "timeout"],
      hasMain: false
    });
  });

  it("keeps main alongside the extra ports a type adds", () => {
    expect(declaredOutputPorts({ type: "xflow.http" })).toEqual({
      ports: ["error"],
      hasMain: true
    });
    expect(declaredOutputPorts({ type: "xflow.wait" })).toEqual({
      ports: ["timeout", "error"],
      hasMain: true
    });
    expect(declaredOutputPorts({ type: "xflow.start" })).toEqual({ ports: [], hasMain: true });
  });

  it("treats an end node as having no output at all", () => {
    expect(declaredOutputPorts({ type: "xflow.end" })).toEqual({ ports: [], hasMain: false });
  });

  it("puts ports named by the node's own outputs parameter ahead of the type's", () => {
    expect(declaredOutputPorts({ type: "xflow.switch", parameters: { outputs: ["paid", "unpaid", ""] } })).toEqual({
      ports: ["paid", "unpaid"],
      hasMain: true
    });
  });
});

describe("toGraphModel", () => {
  it("exposes the ports each node declares so handles exist before any wiring", () => {
    const graph = toGraphModel({
      nodes: [
        { name: "branch", type: "xflow.if" },
        { name: "call", type: "xflow.http" },
        { name: "done", type: "xflow.end" }
      ]
    });

    expect(graph.nodes.map((node) => [node.name, node.outputs])).toEqual([
      ["branch", ["true", "false"]],
      ["call", ["main"]],
      ["done", []]
    ]);
  });

  // §6.2 / §6.1: the red error outlet is drawn only where failures are routed to
  // it. A node-level policy wins over the workflow default, and the wait node
  // keeps its timeout outlet either way.
  it("declares the error outlet only under an effective error_output policy", () => {
    const outputs = (workflow: WorkflowDef) =>
      Object.fromEntries(toGraphModel(workflow).nodes.map((node) => [node.name, node.outputs]));

    expect(
      outputs({
        nodes: [
          { name: "own", type: "xflow.http", on_error: "error_output" },
          { name: "inherits", type: "xflow.http" },
          { name: "overrides", type: "xflow.http", on_error: "stop" },
          { name: "waits", type: "xflow.wait" }
        ],
        settings: { on_error: "error_output" }
      })
    ).toEqual({
      own: ["main", "error"],
      inherits: ["main", "error"],
      overrides: ["main"],
      waits: ["main", "timeout", "error"]
    });

    expect(
      outputs({
        nodes: [
          { name: "plain", type: "xflow.http" },
          { name: "waits", type: "xflow.wait" },
          { name: "main_out", type: "xflow.http", on_error: "main_output" }
        ]
      })
    ).toEqual({ plain: ["main"], waits: ["main", "timeout"], main_out: ["main"] });
  });

  it("normalizes workflow nodes and connections into a stable graph model", () => {
    const workflow: WorkflowDef = {
      id: "wf-order",
      name: "Order workflow",
      nodes: [
        { id: "start-id", name: "start", type: "xflow.start", position: { x: 24, y: 48 } },
        {
          name: "charge",
          type: "xflow.http",
          kind: "action",
          notes: "Calls payment gateway",
          inputs: [{ name: "main", required: true }]
        }
      ],
      connections: {
        start: {
          main: [{ node: "charge", input: "main" }]
        }
      }
    };

    const graph = toGraphModel(workflow);

    expect(graph.workflowId).toBe("wf-order");
    expect(graph.nodes).toEqual([
      {
        id: "start-id",
        name: "start",
        type: "xflow.start",
        label: "start",
        position: { x: 24, y: 48 },
        disabled: false,
        kind: "action",
        notes: undefined,
        inputs: [],
        outputs: ["main"]
      },
      {
        id: "charge",
        name: "charge",
        type: "xflow.http",
        label: "charge",
        position: { x: 360, y: 0 },
        disabled: false,
        kind: "action",
        notes: "Calls payment gateway",
        inputs: [{ name: "main", required: true }],
        outputs: ["main"]
      }
    ]);
    expect(graph.edges).toEqual([
      {
        id: "start:main->charge:main",
        source: "start-id",
        sourceName: "start",
        sourcePort: "main",
        target: "charge",
        targetName: "charge",
        targetPort: "main"
      }
    ]);
  });

  it("keeps edges readable when a connection references an unknown target node", () => {
    const workflow: WorkflowDef = {
      nodes: [{ name: "start", type: "xflow.start" }],
      connections: {
        start: {
          error: [{ node: "missing" }]
        }
      }
    };

    expect(toGraphModel(workflow).edges[0]).toMatchObject({
      source: "start",
      target: "missing",
      targetName: "missing",
      targetPort: "main"
    });
  });

  it("uses ui.label as the display label without changing node identity", () => {
    const workflow: WorkflowDef = {
      nodes: [{ name: "route_by_amount", type: "xflow.switch", ui: { label: "金额路由" } }]
    };

    expect(toGraphModel(workflow).nodes[0]).toMatchObject({
      id: "route_by_amount",
      name: "route_by_amount",
      label: "金额路由"
    });
  });

  it("normalizes object-form data connections without an explicit type", () => {
    const workflow: WorkflowDef = {
      nodes: [
        { name: "start", type: "xflow.start" },
        { name: "charge", type: "xflow.http" }
      ],
      connections: {
        start: {
          main: {
            targets: [{ node: "charge", input: "main" }]
          }
        }
      }
    };

    expect(toGraphModel(workflow).edges).toEqual([
      {
        id: "start:main->charge:main",
        source: "start",
        sourceName: "start",
        sourcePort: "main",
        target: "charge",
        targetName: "charge",
        targetPort: "main"
      }
    ]);
  });

  it("keeps repeated identical targets as distinct edges with unique ids", () => {
    const workflow: WorkflowDef = {
      nodes: [
        { name: "start", type: "xflow.start" },
        { name: "charge", type: "xflow.http" }
      ],
      connections: {
        start: {
          main: [{ node: "charge" }, { node: "charge" }, { node: "charge" }]
        }
      }
    };

    expect(toGraphModel(workflow).edges.map((edge) => edge.id)).toEqual([
      "start:main->charge:main",
      "start:main->charge:main#2",
      "start:main->charge:main#3"
    ]);
  });

  it("does not turn unknown typed connections into graph edges", () => {
    const workflow = {
      nodes: [
        { name: "start", type: "xflow.start" },
        { name: "charge", type: "xflow.http" }
      ],
      connections: {
        start: {
          main: {
            type: "unknown",
            targets: [{ node: "charge", input: "main" }]
          }
        }
      }
    } as unknown as WorkflowDef;

    expect(toGraphModel(workflow).edges).toEqual([]);
  });

  it("keeps supply dependencies out of the dataflow topology", () => {
    const workflow: WorkflowDef = {
      options: {
        allow_cycles: true,
        max_auto_depth: 4,
        experimental_node_group: true,
        transient: true,
        transient_ttl: 60_000_000_000,
        transient_completion_ttl: 10_000_000_000
      },
      groups: [
        {
          name: "worker-group",
          members: ["worker"],
          on_error: "continue",
          timeout: 30_000_000_000,
          mode: "transient",
          activation_replicas: 2
        }
      ],
      nodes: [
        {
          name: "rules",
          type: "xflow.supply",
          kind: "supply",
          output: { private: true },
          timeout: 5_000_000_000,
          activation_replicas: 2
        },
        { name: "worker", type: "xflow.function" }
      ],
      connections: {
        rules: {
          supply: {
            type: "dependency",
            targets: [{ node: "worker" }]
          }
        }
      },
      dependency_edges: [{ node: "worker", supply: "rules" }]
    };

    const graph = toGraphModel(workflow);

    expect(graph.edges).toEqual([]);
    expect(graph.nodes).toEqual([
      expect.objectContaining({
        name: "rules",
        kind: "supply",
        position: { x: 0, y: 0 }
      }),
      expect.objectContaining({
        name: "worker",
        kind: "action",
        position: { x: 0, y: 192 }
      })
    ]);
  });

  it("places nodes in stable columns when positions are not provided", () => {
    const workflow: WorkflowDef = {
      nodes: [
        { name: "notify", type: "xflow.http" },
        { name: "start", type: "xflow.start" },
        { name: "review", type: "xflow.wait" },
        { name: "charge", type: "xflow.http" }
      ],
      connections: {
        start: {
          main: [{ node: "charge" }, { node: "review" }]
        },
        charge: {
          main: [{ node: "notify" }]
        }
      }
    };

    expect(toGraphModel(workflow).nodes.map((node) => [node.name, node.position])).toEqual([
      ["notify", { x: 720, y: 0 }],
      ["start", { x: 0, y: 0 }],
      ["review", { x: 360, y: 192 }],
      ["charge", { x: 360, y: 0 }]
    ]);
  });
});

describe("autoLayoutPositions", () => {
  const stack: WorkflowDef = {
    nodes: [
      { id: "start-id", name: "start", type: "xflow.start" },
      { id: "a-id", name: "a", type: "xflow.http" },
      { id: "b-id", name: "b", type: "xflow.http" },
      { id: "c-id", name: "c", type: "xflow.http" }
    ],
    connections: {
      start: { main: [{ node: "a" }, { node: "b" }] },
      a: { main: [{ node: "c" }] },
      b: { main: [{ node: "c" }] }
    }
  };

  it("spaces same-column rows by the measured height plus the 32px gap", () => {
    const positions = autoLayoutPositions(stack, { boxOf: () => ({ width: 264, height: 159 }) });
    const a = positions.get("a-id")!;
    const b = positions.get("b-id")!;
    expect(a.x).toBe(b.x);
    expect(b.y - a.y).toBe(159 + 32);
    // A taller card moves the row below it instead of overlapping it.
    const tall = autoLayoutPositions(stack, { boxOf: () => ({ width: 264, height: 178 }) });
    expect(tall.get("b-id")!.y - tall.get("a-id")!.y).toBe(178 + 32);
  });

  it("measures each node on its own instead of assuming one card height", () => {
    const heights = new Map([["a-id", 120], ["b-id", 200]]);
    const positions = autoLayoutPositions(stack, {
      boxOf: (id) => ({ width: 264, height: heights.get(id) ?? 160 })
    });
    expect(positions.get("b-id")!.y - positions.get("a-id")!.y).toBe(120 + 32);
  });

  it("returns only the coordinate-less nodes when asked, keeping pinned ones untouched", () => {
    const pinned: WorkflowDef = {
      ...stack,
      nodes: stack.nodes!.map((node) => (node.id === "b-id" ? { ...node, position: { x: 900, y: 40 } } : node))
    };
    const positions = autoLayoutPositions(pinned, { onlyUnpositioned: true });
    expect(positions.has("b-id")).toBe(false);
    expect(positions.get("a-id")).toEqual({ x: 360, y: 0 });
    // The pinned node still owns its row, so later nodes keep their slots.
    expect(positions.get("c-id")!.x).toBe(720);
  });
});
