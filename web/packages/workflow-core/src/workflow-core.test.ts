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
    expect(normalizeWorkflow(merged.def)).toEqual(normalizeWorkflow(def));
    // pin_data came from the def, not from metadata.pinData.
    expect(merged.def.pin_data).toEqual(def.pin_data);
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
    expect(merged.def.pin_data).toEqual(stripped.pin_data);
    expect(merged.def.pin_data?.trigger).toEqual({ event: "demo" });
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

  it("migrates a missing node id to a deterministic stable id", () => {
    const def: WorkflowDef = {
      name: "id-migrated",
      nodes: [
        {
          name: "legacy-a",
          position: { x: 10, y: 10 },
          notes: "legacy note",
        },
      ],
    };
    const { def: migrated, metadata, diagnostics } = splitEditorMetadata(def);

    // The migrated runtime node carries the new stable id and has editor-only
    // fields stripped (as split always does).
    expect(migrated.nodes?.[0]).toEqual({
      id: "node-0-legacy-a",
      name: "legacy-a",
    });

    // Metadata is keyed by the new stable id, not by name.
    expect(metadata.positions?.["node-0-legacy-a"]).toEqual({ x: 10, y: 10 });
    expect(metadata.notes?.["node-0-legacy-a"]).toBe("legacy note");
    expect(metadata.positions?.["legacy-a"]).toBeUndefined();

    // A NODE_METADATA_ID_MIGRATED diagnostic is emitted once per migrated node.
    const migratedDiags = diagnostics.filter(
      (d) => d.code === "NODE_METADATA_ID_MIGRATED"
    );
    expect(migratedDiags).toHaveLength(1);
    expect(migratedDiags[0].severity).toBe("warning");
    expect(migratedDiags[0].message).toContain("legacy-a");
    expect(migratedDiags[0].message).toContain("node-0-legacy-a");
    expect(migratedDiags[0].path).toBe("nodes[0]");
    expect(migratedDiags[0].nodeId).toBe("node-0-legacy-a");

    // The old fallback diagnostic is no longer emitted.
    expect(
      diagnostics.filter((d) => d.code === "NODE_METADATA_KEYED_BY_NAME")
    ).toHaveLength(0);
  });

  it("does NOT emit migration diagnostic when all ids are present", async () => {
    // Reuses the with-editor-metadata fixture whose nodes carry `id`.
    const def = await readFixture("with-editor-metadata");
    const { diagnostics } = splitEditorMetadata(def);
    const fallbackDiags = diagnostics.filter(
      (d) => d.code === "NODE_METADATA_KEYED_BY_NAME"
    );
    expect(fallbackDiags).toHaveLength(0);
  });

  it("emits one migration diagnostic per node with a missing id", () => {
    const def: WorkflowDef = {
      name: "multi-missing-id",
      nodes: [
        { name: "a", position: { x: 1, y: 1 }, notes: "n-a", ui: { k: 1 } },
        { name: "b", position: { x: 2, y: 2 } },
      ],
    };
    const { def: migrated, diagnostics } = splitEditorMetadata(def);

    const migratedDiags = diagnostics.filter(
      (d) => d.code === "NODE_METADATA_ID_MIGRATED"
    );
    expect(migratedDiags).toHaveLength(2);
    expect(migratedDiags[0].path).toBe("nodes[0]");
    expect(migratedDiags[1].path).toBe("nodes[1]");

    // Each node received a deterministic id based on its index and name.
    expect(migrated.nodes?.[0].id).toBe("node-0-a");
    expect(migrated.nodes?.[1].id).toBe("node-1-b");

    // Metadata is keyed by the new stable ids.
    expect(migratedDiags[0].nodeId).toBe("node-0-a");
    expect(migratedDiags[1].nodeId).toBe("node-1-b");
  });

  it("mergeEditorMetadata emits migration diagnostic symmetrically", () => {
    const def: WorkflowDef = {
      name: "merge-migrated",
      nodes: [{ name: "legacy-a", position: { x: 1, y: 1 } }],
    };
    const { metadata, diagnostics: splitDiags } = splitEditorMetadata(def);
    expect(
      splitDiags.filter((d) => d.code === "NODE_METADATA_ID_MIGRATED")
    ).toHaveLength(1);

    // Build a fresh def without going through split; merge should still
    // migrate ids and report the migration.
    const fresh: WorkflowDef = {
      name: "merge-migrated",
      nodes: [{ name: "legacy-a" }],
    };
    const { def: mergedDef, diagnostics: mergeDiags } = mergeEditorMetadata(
      fresh,
      metadata
    );
    const migratedMergeDiags = mergeDiags.filter(
      (d) => d.code === "NODE_METADATA_ID_MIGRATED"
    );
    expect(migratedMergeDiags).toHaveLength(1);
    expect(migratedMergeDiags[0].nodeId).toBe("node-0-legacy-a");
    expect(mergedDef.nodes?.[0].id).toBe("node-0-legacy-a");

    // The metadata keyed by the stable id is restored.
    expect(mergedDef.nodes?.[0].position).toEqual({ x: 1, y: 1 });
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
    const a = merged.def.nodes?.find((n) => n.id === "id-a");
    const b = merged.def.nodes?.find((n) => n.id === "id-b");
    expect(a?.position).toEqual({ x: 100, y: 100 });
    expect(a?.notes).toBe("A");
    expect(b?.position).toEqual({ x: 200, y: 200 });
    expect(b?.notes).toBe("B");
  });

  it("migrates duplicate node ids to distinct deterministic ids", () => {
    const def: WorkflowDef = {
      name: "duplicate-ids",
      nodes: [
        {
          id: "dup",
          name: "alpha",
          position: { x: 10, y: 10 },
          notes: "first",
        },
        {
          id: "dup",
          name: "beta",
          position: { x: 20, y: 20 },
          notes: "second",
        },
      ],
    };
    const { def: migrated, metadata, diagnostics } = splitEditorMetadata(def);

    // Both colliding nodes are migrated to different stable ids.
    const ids = migrated.nodes?.map((n) => n.id);
    expect(ids).toContain("node-0-alpha");
    expect(ids).toContain("node-1-beta");
    expect(ids?.[0]).not.toBe(ids?.[1]);

    // Metadata is keyed separately under the new ids.
    expect(metadata.positions?.["node-0-alpha"]).toEqual({ x: 10, y: 10 });
    expect(metadata.positions?.["node-1-beta"]).toEqual({ x: 20, y: 20 });
    expect(metadata.notes?.["node-0-alpha"]).toBe("first");
    expect(metadata.notes?.["node-1-beta"]).toBe("second");

    // Duplicate-id diagnostics are emitted per migrated node.
    const duplicateDiags = diagnostics.filter(
      (d) => d.code === "NODE_METADATA_DUPLICATE_IDS_MIGRATED"
    );
    expect(duplicateDiags).toHaveLength(2);
    expect(duplicateDiags[0].path).toBe("nodes[0]");
    expect(duplicateDiags[1].path).toBe("nodes[1]");
    expect(duplicateDiags[0].message).toContain("dup");
    expect(duplicateDiags[1].message).toContain("dup");
  });

  it("renameNode does not change node id or metadata binding", () => {
    const def: WorkflowDef = {
      name: "rename-stable",
      nodes: [
        {
          id: "n1",
          name: "alpha",
          position: { x: 1, y: 1 },
          ui: { color: "red" },
          notes: "note-alpha",
        },
      ],
    };
    const renamed = renameNode(def, "alpha", "beta");

    // renameNode only changes name; id is untouched.
    expect(renamed.nodes?.[0].id).toBe("n1");
    expect(renamed.nodes?.[0].name).toBe("beta");

    // Split still binds metadata via the original stable id.
    const { def: stripped, metadata } = splitEditorMetadata(renamed);
    expect(metadata.positions?.n1).toEqual({ x: 1, y: 1 });
    expect(metadata.ui?.n1).toEqual({ color: "red" });
    expect(metadata.notes?.n1).toBe("note-alpha");

    // Round-trip preserves the binding.
    const merged = mergeEditorMetadata(stripped, metadata);
    expect(merged.def.nodes?.[0].id).toBe("n1");
    expect(merged.def.nodes?.[0].position).toEqual({ x: 1, y: 1 });
    expect(merged.def.nodes?.[0].ui).toEqual({ color: "red" });
  });

  it("split/merge round-trips positions/ui/notes/pin_data after migration", () => {
    const def: WorkflowDef = {
      name: "round-trip-migration",
      nodes: [
        {
          name: "first",
          position: { x: 1, y: 2 },
          ui: { color: "red" },
          notes: "note-first",
        },
        {
          id: "dup",
          name: "second",
          position: { x: 3, y: 4 },
          ui: { color: "blue" },
          notes: "note-second",
        },
        {
          id: "dup",
          name: "third",
          position: { x: 5, y: 6 },
          ui: { color: "green" },
          notes: "note-third",
        },
      ],
      pin_data: {
        first: { value: 1 },
        second: { value: 2 },
      },
    };

    const { def: stripped, metadata, diagnostics } = splitEditorMetadata(def);

    // All problematic ids were migrated.
    expect(diagnostics.map((d) => d.code).sort()).toEqual([
      "NODE_METADATA_DUPLICATE_IDS_MIGRATED",
      "NODE_METADATA_DUPLICATE_IDS_MIGRATED",
      "NODE_METADATA_ID_MIGRATED",
    ]);

    // Editor-only fields are stripped from the runtime def.
    for (const node of stripped.nodes ?? []) {
      expect(node.position).toBeUndefined();
      expect(node.ui).toBeUndefined();
      expect(node.notes).toBeUndefined();
    }

    // pin_data stays on the runtime def.
    expect(stripped.pin_data).toEqual(def.pin_data);

    // Metadata is keyed by the new stable ids.
    expect(metadata.positions?.["node-0-first"]).toEqual({ x: 1, y: 2 });
    expect(metadata.positions?.["node-1-second"]).toEqual({ x: 3, y: 4 });
    expect(metadata.positions?.["node-2-third"]).toEqual({ x: 5, y: 6 });
    expect(metadata.ui?.["node-0-first"]).toEqual({ color: "red" });
    expect(metadata.notes?.["node-2-third"]).toBe("note-third");

    // Merge restores editor-only fields on the migrated nodes. Because the
    // stripped def already carries stable ids, merge does not re-emit
    // migration diagnostics.
    const { def: merged, diagnostics: mergeDiags } = mergeEditorMetadata(
      stripped,
      metadata
    );
    expect(mergeDiags).toHaveLength(0);

    const first = merged.nodes?.find((n) => n.name === "first");
    const second = merged.nodes?.find((n) => n.name === "second");
    const third = merged.nodes?.find((n) => n.name === "third");

    expect(first?.id).toBe("node-0-first");
    expect(first?.position).toEqual({ x: 1, y: 2 });
    expect(first?.ui).toEqual({ color: "red" });
    expect(first?.notes).toBe("note-first");

    expect(second?.id).toBe("node-1-second");
    expect(second?.position).toEqual({ x: 3, y: 4 });
    expect(second?.ui).toEqual({ color: "blue" });
    expect(second?.notes).toBe("note-second");

    expect(third?.id).toBe("node-2-third");
    expect(third?.position).toEqual({ x: 5, y: 6 });
    expect(third?.ui).toEqual({ color: "green" });
    expect(third?.notes).toBe("note-third");

    // pin_data is preserved from the runtime def, not overwritten.
    expect(merged.pin_data).toEqual(def.pin_data);
  });
});
