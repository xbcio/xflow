import { describe, expect, it } from "vitest";
import type { WorkflowDef, WorkflowNode } from "@xflow/core";

import {
  addConnection,
  canLabelInputs,
  listConnectionReferences,
  relabelConnectionInput,
  validateInputLabel
} from "./workflowOps";

const merge: WorkflowNode = { name: "join", type: "xflow.merge" };

const workflow: WorkflowDef = {
  id: "wf-merge",
  name: "merge labels",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "stock", type: "xflow.function" },
    { name: "price", type: "xflow.function" },
    merge,
    { name: "after", type: "xflow.function" }
  ],
  connections: {
    start: { main: [{ node: "stock" }, { node: "price" }] },
    stock: { main: [{ node: "join" }] },
    price: { main: [{ node: "join", input: "total" }] },
    join: { main: [{ node: "after" }] }
  }
};

const edge = (sourceName: string, targetPort: string) => ({
  sourceName,
  sourcePort: "main",
  targetName: "join",
  targetPort
});

describe("canLabelInputs", () => {
  it("is true only for a merge that declares no ports", () => {
    expect(canLabelInputs(merge)).toBe(true);
    expect(canLabelInputs({ ...merge, inputs: [] })).toBe(true);
    expect(canLabelInputs({ ...merge, inputs: [{ name: "a" }] })).toBe(false);
    expect(canLabelInputs({ name: "f", type: "xflow.function" })).toBe(false);
    expect(canLabelInputs(undefined)).toBe(false);
  });
});

describe("validateInputLabel", () => {
  it.each(["inventory", "price_2", "A", "a1_b"])("accepts %j", (label) => {
    expect(validateInputLabel(label)).toBeUndefined();
  });

  it.each(["", "2fast", "_hidden", "with space", "a-b", "a.b", "价格", "$x"])("rejects %j", (label) => {
    expect(validateInputLabel(label)).toBeTruthy();
  });
});

describe("relabelConnectionInput", () => {
  it("writes the new label on exactly that edge and nothing else", () => {
    const { workflow: next, error } = relabelConnectionInput(workflow, edge("stock", "main"), "inventory");

    expect(error).toBeUndefined();
    expect(next.connections?.stock).toEqual({ main: [{ node: "join", input: "inventory" }] });
    expect(next.connections?.price).toEqual(workflow.connections?.price);
    expect(next.connections?.start).toBe(workflow.connections?.start);
    // The input was not mutated.
    expect(workflow.connections?.stock).toEqual({ main: [{ node: "join" }] });
  });

  it("round-trips through the wire shape: the label is the connection's `input`", () => {
    const { workflow: next } = relabelConnectionInput(workflow, edge("stock", "main"), "inventory");
    const wire = JSON.parse(JSON.stringify(next)) as WorkflowDef;

    expect(listConnectionReferences(wire).filter((ref) => ref.targetName === "join")).toEqual([
      expect.objectContaining({ sourceName: "stock", targetPort: "inventory" }),
      expect.objectContaining({ sourceName: "price", targetPort: "total" })
    ]);
  });

  it("writes main as no `input` at all, like addConnection does", () => {
    const { workflow: next } = relabelConnectionInput(workflow, edge("price", "total"), "main");

    expect(next.connections?.price).toEqual({ main: [{ node: "join" }] });
    expect(Object.keys(next.connections?.price?.main?.[0] as object)).toEqual(["node"]);
  });

  it("trims the label", () => {
    const { workflow: next, error } = relabelConnectionInput(workflow, edge("stock", "main"), "  inventory ");
    expect(error).toBeUndefined();
    expect(next.connections?.stock?.main).toEqual([{ node: "join", input: "inventory" }]);
  });

  it("keeps the object form of a typed port", () => {
    const typed: WorkflowDef = {
      ...workflow,
      connections: { ...workflow.connections, stock: { main: { type: "data", targets: [{ node: "join" }] } } }
    };
    const { workflow: next } = relabelConnectionInput(typed, edge("stock", "main"), "inventory");

    expect(next.connections?.stock?.main).toEqual({ type: "data", targets: [{ node: "join", input: "inventory" }] });
  });

  it("returns the same workflow when the label is unchanged", () => {
    const result = relabelConnectionInput(workflow, edge("price", "total"), "total");
    expect(result.error).toBeUndefined();
    expect(result.workflow).toBe(workflow);
  });

  it("refuses a malformed label without touching the workflow", () => {
    for (const label of ["", "a b", "1x", "_x"]) {
      const result = relabelConnectionInput(workflow, edge("stock", "main"), label);
      expect(result.error).toBeTruthy();
      expect(result.workflow).toBe(workflow);
    }
  });

  it("refuses a second edge from the same source port under the same label", () => {
    const twin: WorkflowDef = {
      ...workflow,
      connections: { ...workflow.connections, stock: { main: [{ node: "join" }, { node: "join", input: "x" }] } }
    };
    const result = relabelConnectionInput(twin, edge("stock", "main"), "x");

    expect(result.error).toBeTruthy();
    expect(result.workflow).toBe(twin);
  });

  it("lets two different sources share a label (a deliberate shared port)", () => {
    const { workflow: next, error } = relabelConnectionInput(workflow, edge("stock", "main"), "total");
    expect(error).toBeUndefined();
    expect(next.connections?.stock?.main).toEqual([{ node: "join", input: "total" }]);
  });

  it("refuses a target that declares its ports or is not a merge", () => {
    const declared: WorkflowDef = {
      ...workflow,
      nodes: workflow.nodes?.map((node) => (node.name === "join" ? { ...node, inputs: [{ name: "main" }] } : node))
    };
    expect(relabelConnectionInput(declared, edge("stock", "main"), "inventory").error).toBeTruthy();

    const plain: WorkflowDef = {
      ...workflow,
      connections: { ...workflow.connections, start: { main: [{ node: "stock" }] } }
    };
    const toFunction = relabelConnectionInput(plain, { ...edge("start", "main"), targetName: "stock" }, "x");
    expect(toFunction.error).toBeTruthy();
    expect(toFunction.workflow).toBe(plain);
  });

  it("reports a vanished edge instead of inventing one", () => {
    const result = relabelConnectionInput(workflow, edge("stock", "ghost"), "inventory");
    expect(result.error).toBeTruthy();
    expect(result.workflow).toBe(workflow);
  });

  it("never rewrites a dependency port", () => {
    const dependency: WorkflowDef = {
      ...workflow,
      connections: { ...workflow.connections, stock: { main: { type: "dependency", targets: [{ node: "join" }] } } }
    };
    const result = relabelConnectionInput(dependency, edge("stock", "main"), "inventory");
    expect(result.error).toBeTruthy();
    expect(result.workflow).toBe(dependency);
  });
});

describe("addConnection into a label-free merge", () => {
  const source = workflow.nodes?.find((node) => node.name === "stock");

  it("accepts a well-formed label the canvas offers as a handle", () => {
    const next = addConnection(workflow, source, "join", "main", "inventory");
    expect(next.connections?.stock?.main).toEqual([{ node: "join" }, { node: "join", input: "inventory" }]);
  });

  it("still refuses a malformed label", () => {
    expect(addConnection(workflow, source, "join", "main", "bad label")).toBe(workflow);
  });

  it("still refuses an undeclared label on an ordinary node", () => {
    expect(addConnection(workflow, source, "after", "main", "inventory")).toBe(workflow);
  });
});
