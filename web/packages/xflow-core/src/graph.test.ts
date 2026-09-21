import { describe, expect, it } from "vitest";
import { toGraphModel, type WorkflowDef } from "./index";

describe("toGraphModel", () => {
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
        inputs: []
      },
      {
        id: "charge",
        name: "charge",
        type: "xflow.http",
        label: "charge",
        position: { x: 280, y: 0 },
        disabled: false,
        kind: "action",
        notes: "Calls payment gateway",
        inputs: [{ name: "main", required: true }]
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
        position: { x: 0, y: 120 }
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
      ["notify", { x: 560, y: 0 }],
      ["start", { x: 0, y: 0 }],
      ["review", { x: 280, y: 120 }],
      ["charge", { x: 280, y: 0 }]
    ]);
  });
});
