import { describe, expect, it } from "vitest";
import {
  mergeEditorMetadata,
  NODE_METADATA_KEYED_BY_NAME,
  splitEditorMetadata,
  type WorkflowDef,
  type WorkflowEditorMetadata,
  type WorkflowEditorSection,
  type WireWorkflowDef
} from "./index";

/**
 * Mirrors types/editor_metadata_validate_test.go's cases 1:1 so the Go and TS
 * key-resolution/diagnostic behavior cannot silently drift apart (ADR-D4 §2.4).
 */
describe("mergeEditorMetadata", () => {
  it("merges with no diagnostics when metadata is absent", () => {
    const wire: WireWorkflowDef = { nodes: [{ id: "node-1", name: "a" }] };
    const { def, diagnostics } = mergeEditorMetadata(wire, undefined);
    expect(diagnostics).toEqual([]);
    expect(def.nodes?.[0]).toEqual({ id: "node-1", name: "a" });
  });

  it("keeps a key resolved by node id with no diagnostic", () => {
    const wire: WireWorkflowDef = { nodes: [{ id: "node-1", name: "a" }] };
    const metadata: WorkflowEditorMetadata = {
      positions: { "node-1": { x: 1, y: 2 } },
      notes: { "node-1": "note" }
    };
    const { def, diagnostics } = mergeEditorMetadata(wire, metadata);
    expect(diagnostics).toEqual([]);
    expect(def.nodes?.[0].position).toEqual({ x: 1, y: 2 });
    expect(def.nodes?.[0].notes).toBe("note");
  });

  it("keeps a key resolved by name and warns NODE_METADATA_KEYED_BY_NAME", () => {
    const wire: WireWorkflowDef = { nodes: [{ name: "a" }] }; // no id
    const metadata: WorkflowEditorMetadata = { positions: { a: { x: 1 } } };
    const { def, diagnostics } = mergeEditorMetadata(wire, metadata);
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0].startsWith(NODE_METADATA_KEYED_BY_NAME)).toBe(true);
    expect(def.nodes?.[0].position).toEqual({ x: 1 });
  });

  it("drops an unmatched key silently", () => {
    const wire: WireWorkflowDef = { nodes: [{ id: "node-1", name: "a" }] };
    const metadata: WorkflowEditorMetadata = {
      positions: { "node-1": { x: 1 }, ghost: { x: 99 } },
      ui: { ghost: "x" },
      notes: { ghost: "x" }
    };
    const { def, diagnostics } = mergeEditorMetadata(wire, metadata);
    expect(diagnostics).toEqual([]);
    expect(def.nodes?.[0].position).toEqual({ x: 1 });
    expect(def.nodes?.[0].ui).toBeUndefined();
    expect(def.nodes?.[0].notes).toBeUndefined();
  });

  it("drops a name key when the node has a stable id", () => {
    const wire: WireWorkflowDef = { nodes: [{ id: "node-1", name: "a" }] };
    const metadata: WorkflowEditorMetadata = { positions: { a: { x: 1 } } };
    const { def, diagnostics } = mergeEditorMetadata(wire, metadata);
    expect(diagnostics).toEqual([]);
    expect(def.nodes?.[0].position).toBeUndefined();
  });

  it("passes the viewport through unconditionally", () => {
    const wire: WireWorkflowDef = { nodes: [] };
    const metadata: WorkflowEditorMetadata = { viewport: { x: 1, y: 2, zoom: 3 } };
    // Viewport is workflow-level, not node-keyed, so it has no merge target on
    // nodes; callers read it directly off the split result (see below).
    const { diagnostics } = mergeEditorMetadata(wire, metadata);
    expect(diagnostics).toEqual([]);
    expect(metadata.viewport).toEqual({ x: 1, y: 2, zoom: 3 });
  });
});

describe("splitEditorMetadata", () => {
  it("strips position/ui/notes from the wire def and keys them by id", () => {
    const def: WorkflowDef = {
      name: "wf",
      nodes: [
        { id: "node-1", name: "a", position: { x: 1, y: 2 }, notes: "hi", ui: { color: "blue" } },
        { id: "node-2", name: "b" }
      ]
    };
    const { def: wire, metadata, diagnostics } = splitEditorMetadata(def);
    expect(diagnostics).toEqual([]);
    expect(wire.nodes?.[0]).not.toHaveProperty("position");
    expect(wire.nodes?.[0]).not.toHaveProperty("notes");
    expect(wire.nodes?.[0]).not.toHaveProperty("ui");
    expect(wire.nodes?.[0]).toEqual({ id: "node-1", name: "a" });
    expect(metadata.positions).toEqual({ "node-1": { x: 1, y: 2 } });
    expect(metadata.notes).toEqual({ "node-1": "hi" });
    expect(metadata.ui).toEqual({ "node-1": { color: "blue" } });
  });

  it("leaves pin_data and other runtime-semantic fields untouched", () => {
    const def: WorkflowDef = {
      name: "wf",
      pin_data: { a: { foo: "bar" } },
      nodes: [{ name: "a", position: { x: 1 } }]
    };
    const { def: wire } = splitEditorMetadata(def);
    expect(wire.pin_data).toEqual({ a: { foo: "bar" } });
  });

  it("keys by name and warns when a node with metadata has no id", () => {
    const def: WorkflowDef = {
      nodes: [{ name: "a", position: { x: 1 } }]
    };
    const { metadata, diagnostics } = splitEditorMetadata(def);
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0].startsWith(NODE_METADATA_KEYED_BY_NAME)).toBe(true);
    expect(metadata.positions).toEqual({ a: { x: 1 } });
  });

  it("round-trips through split then merge back to the original merged shape", () => {
    const def: WorkflowDef = {
      name: "wf",
      nodes: [
        { id: "node-1", name: "a", position: { x: 10, y: 20 }, notes: "n", ui: { label: "A" } },
        { id: "node-2", name: "b" }
      ]
    };
    const { def: wire, metadata } = splitEditorMetadata(def);
    const { def: merged, diagnostics } = mergeEditorMetadata(wire, metadata);
    expect(diagnostics).toEqual([]);
    expect(merged).toEqual(def);
  });

  it("omits metadata.viewport when the def carries no viewport (caller owns it)", () => {
    const def: WorkflowDef = { nodes: [] };
    const { metadata } = splitEditorMetadata(def);
    expect(metadata.viewport).toBeUndefined();
  });
});

/**
 * `WorkflowEditorMetadata` doubles as the `editor_metadata` wire shape, so
 * these pin the JSON encoding against Go's `types.WorkflowEditorMetadata`
 * tags (`types/editor_metadata.go`). A renamed property here would not fail
 * typecheck -- it would be dropped by the server's `json.Unmarshal` as an
 * unknown key, silently losing editor state.
 */
describe("WorkflowEditorMetadata serialization", () => {
  const sections: WorkflowEditorSection[] = [
    {
      id: "s-approval",
      title: "VIP approval",
      node_keys: ["node-approve", "node-notify"],
      x: 40,
      y: 80,
      width: 360,
      height: 200
    },
    { id: "s-plain", node_keys: ["node-plain"] }
  ];

  it("round-trips sections through JSON unchanged", () => {
    const metadata: WorkflowEditorMetadata = {
      positions: { "node-approve": { x: 10, y: 20 } },
      viewport: { x: 0, y: 0, zoom: 1 },
      notes: { "node-approve": "ask before approving" },
      sections
    };

    const encoded = JSON.stringify(metadata);

    // The section keys use Go's wire names: `node_keys`, never `nodeKeys`.
    expect(encoded).toContain('"node_keys":["node-approve","node-notify"]');
    expect(JSON.parse(encoded)).toEqual(metadata);
  });

  it("encodes byte-identically to the pre-sections shape when sections is absent", () => {
    const metadata: WorkflowEditorMetadata = { positions: { "node-1": { x: 1, y: 2 } } };

    // The new optional field adds no key when unset, so a save that carries no
    // sections produces exactly the bytes older clients sent.
    expect(JSON.stringify(metadata)).toBe('{"positions":{"node-1":{"x":1,"y":2}}}');
    expect(JSON.stringify(splitEditorMetadata({ nodes: [{ id: "node-1", position: { x: 1, y: 2 } }] }).metadata)).toBe(
      '{"positions":{"node-1":{"x":1,"y":2}}}'
    );
  });

  it("keeps an explicit empty sections array when plain-serialized", () => {
    // `omitempty` is a Go encoder rule, not a JavaScript one: plain
    // `JSON.stringify` emits `"sections":[]`. The save path in `@xflow/api`
    // drops the empty array explicitly so the wire matches the Go encoding.
    const metadata: WorkflowEditorMetadata = { sections: [] };
    expect(JSON.stringify(metadata)).toBe('{"sections":[]}');
  });
});
