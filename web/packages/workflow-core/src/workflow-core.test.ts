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

describe("editor metadata split/merge (F0-A2)", () => {
  it("splitEditorMetadata strips editor-only fields from def but preserves pin_data", async () => {
    const def = await readFixture("with-editor-metadata");
    const { def: stripped, metadata } = splitEditorMetadata(def);

    for (const node of stripped.nodes ?? []) {
      expect(node.position).toBeUndefined();
      expect(node.ui).toBeUndefined();
      expect(node.notes).toBeUndefined();
    }
    // F0-A2: pin_data is runtime-semantic, MUST remain in stripped def.
    expect(stripped.pin_data).toEqual(def.pin_data);
    expect(stripped.pin_data?.trigger).toEqual({ event: "demo" });

    // Metadata is keyed by node.id when present (node-trigger / node-action).
    expect(metadata.positions?.["node-trigger"]).toEqual({ x: 50, y: 50 });
    expect(metadata.ui?.["node-action"]).toEqual({ color: "#0000ff", icon: "bug" });
    expect(metadata.notes?.["node-trigger"]).toBe("Start here");
    // pinData in metadata is a read-only derived cache of def.pin_data.
    expect(metadata.pinData).toEqual(def.pin_data);
  });

  it("metadata.pinData is a reference copy of def.pin_data (read-only cache)", async () => {
    const def = await readFixture("with-editor-metadata");
    const { metadata } = splitEditorMetadata(def);
    expect(metadata.pinData).toEqual(def.pin_data);
  });

  it("merge round-trip preserves pin_data from the def (not from metadata)", async () => {
    const def = await readFixture("with-editor-metadata");
    const { def: stripped, metadata } = splitEditorMetadata(def);
    const merged = mergeEditorMetadata(stripped, metadata);
    expect(normalizeWorkflow(merged)).toEqual(normalizeWorkflow(def));
    // pin_data came from the def, not from metadata.pinData.
    expect(merged.pin_data).toEqual(def.pin_data);
  });

  it("merge does not overwrite def.pin_data with metadata.pinData (def wins)", async () => {
    const def = await readFixture("with-editor-metadata");
    const { def: stripped, metadata } = splitEditorMetadata(def);
    // Tamper with metadata.pinData; def.pin_data is canonical and must win.
    const tamperedMetadata = {
      ...metadata,
      pinData: { trigger: { event: "TAMPERED" } },
    };
    const merged = mergeEditorMetadata(stripped, tamperedMetadata);
    expect(merged.pin_data).toEqual(stripped.pin_data);
    expect(merged.pin_data?.trigger).toEqual({ event: "demo" });
  });

  it("positions/ui/notes indexed by node.id when id is present", async () => {
    const def: WorkflowDef = {
      name: "id-indexed",
      nodes: [
        {
          id: "n1",
          name: "alpha",
          position: { x: 1, y: 1 },
          ui: { color: "red" },
          notes: "first",
        },
        {
          id: "n2",
          name: "beta",
          position: { x: 2, y: 2 },
          ui: { color: "blue" },
          notes: "second",
        },
      ],
    };
    const { def: stripped, metadata } = splitEditorMetadata(def);
    expect(metadata.positions?.n1).toEqual({ x: 1, y: 1 });
    expect(metadata.positions?.n2).toEqual({ x: 2, y: 2 });
    expect(metadata.ui?.n1).toEqual({ color: "red" });
    expect(metadata.notes?.n2).toBe("second");
    for (const node of stripped.nodes ?? []) {
      expect(node.position).toBeUndefined();
    }
  });

  it("falls back to node.name when id is absent", () => {
    const def: WorkflowDef = {
      name: "name-fallback",
      nodes: [
        {
          name: "legacy-a",
          position: { x: 10, y: 10 },
          notes: "legacy note",
        },
      ],
    };
    const { metadata } = splitEditorMetadata(def);
    expect(metadata.positions?.["legacy-a"]).toEqual({ x: 10, y: 10 });
    expect(metadata.notes?.["legacy-a"]).toBe("legacy note");
  });

  it("two nodes with same name but different ids do not cross metadata", () => {
    const def: WorkflowDef = {
      name: "id-disambiguates",
      nodes: [
        {
          id: "id-a",
          name: "dup",
          position: { x: 100, y: 100 },
          notes: "A",
        },
        {
          id: "id-b",
          name: "dup",
          position: { x: 200, y: 200 },
          notes: "B",
        },
      ],
    };
    const { metadata, def: stripped } = splitEditorMetadata(def);
    expect(metadata.positions?.["id-a"]).toEqual({ x: 100, y: 100 });
    expect(metadata.positions?.["id-b"]).toEqual({ x: 200, y: 200 });
    expect(metadata.notes?.["id-a"]).toBe("A");
    expect(metadata.notes?.["id-b"]).toBe("B");

    // Round-trip restores the correct per-node metadata via id.
    const merged = mergeEditorMetadata(stripped, metadata);
    const a = merged.nodes?.find((n) => n.id === "id-a");
    const b = merged.nodes?.find((n) => n.id === "id-b");
    expect(a?.position).toEqual({ x: 100, y: 100 });
    expect(a?.notes).toBe("A");
    expect(b?.position).toEqual({ x: 200, y: 200 });
    expect(b?.notes).toBe("B");
  });
});
