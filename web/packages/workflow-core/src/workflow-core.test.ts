import { describe, expect, it } from "vitest";
import {
  type WorkflowDef,
  buildAdjacency,
  cloneWorkflow,
  detectCycle,
  downstream,
  mergeEditorMetadata,
  normalizeWorkflow,
  parseWorkflow,
  reachable,
  renameNode,
  serializeWorkflow,
  splitEditorMetadata,
  topologicalSort,
  upstream,
  validatePorts,
  validateWorkflow,
} from "./index";

function loadFixture(name: string): string {
  return new URL(`./__tests__/golden/${name}.json`, import.meta.url).pathname;
}

async function readFixture(name: string): Promise<WorkflowDef> {
  const { readFile } = await import("node:fs/promises");
  const raw = await readFile(loadFixture(name), "utf-8");
  return parseWorkflow(raw);
}

describe("golden fixtures round-trip", () => {
  it("basic", async () => {
    const raw = await readFixture("basic");
    const serialized = serializeWorkflow(raw);
    const reparsed = parseWorkflow(serialized);
    expect(normalizeWorkflow(reparsed)).toEqual(normalizeWorkflow(raw));
  });

  it("approval", async () => {
    const raw = await readFixture("approval");
    const serialized = serializeWorkflow(raw);
    const reparsed = parseWorkflow(serialized);
    expect(normalizeWorkflow(reparsed)).toEqual(normalizeWorkflow(raw));
  });

  it("cyclic", async () => {
    const raw = await readFixture("cyclic");
    const serialized = serializeWorkflow(raw);
    const reparsed = parseWorkflow(serialized);
    expect(normalizeWorkflow(reparsed)).toEqual(normalizeWorkflow(raw));
  });

  it("with-editor-metadata", async () => {
    const raw = await readFixture("with-editor-metadata");
    const serialized = serializeWorkflow(raw);
    const reparsed = parseWorkflow(serialized);
    expect(normalizeWorkflow(reparsed)).toEqual(normalizeWorkflow(raw));
  });
});

describe("serialize utilities", () => {
  it("preserves unknown fields during parse", () => {
    const json = JSON.stringify({ name: "demo", unknownField: 42 });
    const parsed = parseWorkflow(json);
    expect((parsed as Record<string, unknown>).unknownField).toBe(42);
  });

  it("cloneWorkflow creates a deep copy", () => {
    const def: WorkflowDef = {
      name: "demo",
      nodes: [{ name: "a", parameters: { x: 1 } }],
    };
    const cloned = cloneWorkflow(def);
    cloned.nodes![0].parameters!.x = 2;
    expect(def.nodes![0].parameters!.x).toBe(1);
  });

  it("cloneWorkflow handles undefined", () => {
    expect(cloneWorkflow(undefined)).toBeUndefined();
  });

  it("normalizeWorkflow sorts object keys and removes undefined", () => {
    const def: WorkflowDef = {
      name: "demo",
      nodes: [{ name: "node-a", disabled: false }],
    };
    const normalized = normalizeWorkflow(def);
    expect(Object.keys(normalized)).toEqual(["name", "nodes"]);
    expect(Object.keys(normalized.nodes![0])).toEqual(["disabled", "name"]);
    // `disabled: false` is preserved (distinct from undefined).
    expect(normalized.nodes![0].disabled).toBe(false);
  });
});

describe("graph algorithms", () => {
  it("buildAdjacency builds predecessor/successor sets", async () => {
    const def = await readFixture("basic");
    const adj = buildAdjacency(def);
    expect(Array.from(adj.successors["trigger"] ?? [])).toContain("hello");
    expect(Array.from(adj.predecessors["hello"] ?? [])).toContain("trigger");
  });

  it("detectCycle returns null for DAG", async () => {
    const def = await readFixture("basic");
    expect(detectCycle(def)).toBeNull();
  });

  it("detectCycle returns path for cyclic workflow", async () => {
    const def = await readFixture("cyclic");
    // Override allow_cycles so cycle detection is not skipped.
    const defWithCycleCheck = { ...def, options: { ...def.options, allow_cycles: false } };
    const cycle = detectCycle(defWithCycleCheck);
    expect(cycle).not.toBeNull();
    expect(cycle).toContain("loop");
    expect(cycle).toContain("check");
  });

  it("detectCycle is skipped when allow_cycles is true", async () => {
    const def = await readFixture("cyclic");
    expect(def.options?.allow_cycles).toBe(true);
    expect(detectCycle(def)).toBeNull();
  });

  it("topologicalSort returns DAG order", async () => {
    const def = await readFixture("basic");
    const order = topologicalSort(def);
    expect(order.indexOf("trigger")).toBeLessThan(order.indexOf("hello"));
  });

  it("topologicalSort throws on cycle", async () => {
    const def = await readFixture("cyclic");
    expect(() => topologicalSort(def)).toThrow();
  });

  it("upstream/downstream", async () => {
    const def = await readFixture("approval");
    expect(upstream(def, "approve")).toEqual(["request"]);
    expect(downstream(def, "start")).toEqual(["request"]);
  });

  it("reachable", async () => {
    const def = await readFixture("approval");
    expect(reachable(def, "start").sort()).toEqual([
      "approve",
      "notify",
      "request",
    ]);
  });

  it("validatePorts detects dangling references", () => {
    const def: WorkflowDef = {
      name: "bad",
      nodes: [{ name: "a" }],
      connections: {
        missing: {
          out: [{ node: "a" }],
        },
        a: {
          out: [{ node: "ghost" }],
        },
      },
    };
    const diagnostics = validatePorts(def);
    const codes = diagnostics.map((d) => d.code);
    expect(codes).toContain("PORT_DANGLING_SOURCE");
    expect(codes).toContain("PORT_DANGLING_TARGET");
  });

  it("validateWorkflow reports errors", () => {
    const def: WorkflowDef = {
      nodes: [{ name: "a" }, { name: "a" }],
      connections: {
        a: {
          out: [{ node: "a" }],
        },
      },
    };
    const diagnostics = validateWorkflow(def);
    const codes = diagnostics.map((d) => d.code);
    expect(codes).toContain("WORKFLOW_MISSING_NAME");
    expect(codes).toContain("NODE_DUPLICATE_NAME");
    expect(codes).toContain("WORKFLOW_CYCLE");
  });
});

describe("renameNode", () => {
  it("renames node name and connection references immutably", async () => {
    const def = await readFixture("basic");
    const originalName = def.name;
    const renamed = renameNode(def, "hello", "greet");

    expect(renamed).not.toBe(def);
    expect(def.name).toBe(originalName);

    expect(renamed.nodes?.some((n) => n.name === "greet")).toBe(true);
    expect(renamed.nodes?.some((n) => n.name === "hello")).toBe(false);
    expect(renamed.connections?.trigger?.default?.[0].node).toBe("greet");
  });

  it("keeps unknown expression references untouched", async () => {
    const def = await readFixture("basic");
    const renamed = renameNode(def, "hello", "greet");
    // output value expression is still the old reference; expected boundary.
    expect(renamed.outputs?.greeting?.value).toBe("nodes.hello.output.message");
  });
});

describe("editor metadata split/merge", () => {
  it("splitEditorMetadata strips editor fields from def", async () => {
    const def = await readFixture("with-editor-metadata");
    const { def: stripped, metadata } = splitEditorMetadata(def);

    for (const node of stripped.nodes ?? []) {
      expect(node.position).toBeUndefined();
      expect(node.ui).toBeUndefined();
      expect(node.notes).toBeUndefined();
    }
    expect(stripped.pin_data).toBeUndefined();

    expect(metadata.positions?.trigger).toEqual({ x: 50, y: 50 });
    expect(metadata.ui?.action).toEqual({ color: "#0000ff", icon: "bug" });
    expect(metadata.notes?.trigger).toBe("Start here");
    expect(metadata.pinData?.trigger).toEqual({ event: "demo" });
  });

  it("mergeEditorMetadata restores editor fields", async () => {
    const def = await readFixture("with-editor-metadata");
    const { def: stripped, metadata } = splitEditorMetadata(def);
    const merged = mergeEditorMetadata(stripped, metadata);
    expect(normalizeWorkflow(merged)).toEqual(normalizeWorkflow(def));
  });
});
