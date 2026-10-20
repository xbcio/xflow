import { describe, expect, it } from "vitest";
import type { WorkflowDef } from "@xflow/core";
import { buildOutline, type OutlineRow } from "./outline";

const node = (name: string, type = "xflow.http", extra: Record<string, unknown> = {}) => ({ name, type, ...extra });

/** Compact form of the rows: sections as `# label n`, ports as `@port`, items as `name:indent`. */
const shape = (rows: OutlineRow[]): string[] =>
  rows.map((row) => {
    if (row.kind === "section") return `# ${row.label} ${row.count}`;
    if (row.kind === "port") return `@${row.port}`;
    return `${row.name}:${row.indent}`;
  });

describe("buildOutline", () => {
  it("returns no rows for an empty graph", () => {
    expect(buildOutline({ nodes: [] })).toEqual([]);
    expect(buildOutline({})).toEqual([]);
  });

  it("lists a chain in execution order under 入口 / 流程 / 结束, flush left", () => {
    const workflow: WorkflowDef = {
      // Array order is deliberately not execution order.
      nodes: [node("finish", "xflow.end"), node("fetch"), node("start", "xflow.start", { kind: "trigger" }), node("map", "xflow.function")],
      connections: {
        start: { main: [{ node: "fetch" }] },
        fetch: { main: [{ node: "map" }] },
        map: { main: [{ node: "finish" }] }
      }
    };

    expect(shape(buildOutline(workflow))).toEqual([
      "# 入口 1",
      "start:0",
      "# 流程 2",
      "fetch:0",
      "map:0",
      "# 结束 1",
      "finish:0"
    ]);
  });

  it("indents a branch one level, names the ports and steps back out at the merge", () => {
    const workflow: WorkflowDef = {
      nodes: [
        node("start", "xflow.start", { kind: "trigger" }),
        node("check", "xflow.if"),
        node("vip"),
        node("vip2"),
        node("normal"),
        node("join", "xflow.merge"),
        node("done", "xflow.end")
      ],
      connections: {
        start: { main: [{ node: "check" }] },
        check: { true: [{ node: "vip" }], false: [{ node: "normal" }] },
        vip: { main: [{ node: "vip2" }] },
        vip2: { main: [{ node: "join" }] },
        normal: { main: [{ node: "join" }] },
        join: { main: [{ node: "done" }] }
      }
    };

    expect(shape(buildOutline(workflow))).toEqual([
      "# 入口 1",
      "start:0",
      "# 流程 5",
      "check:0",
      "@true",
      "vip:1",
      "vip2:1",
      "@false",
      "normal:1",
      "join:0",
      "# 结束 1",
      "done:0"
    ]);
  });

  it("places a merge only after its last predecessor, whatever order the branches were wired in", () => {
    const workflow: WorkflowDef = {
      nodes: [node("start", "xflow.start", { kind: "trigger" }), node("a"), node("b"), node("join", "xflow.merge"), node("tail", "xflow.end")],
      connections: {
        start: { main: [{ node: "a" }, { node: "join" }] },
        a: { main: [{ node: "b" }] },
        b: { main: [{ node: "join" }] },
        join: { main: [{ node: "tail" }] }
      }
    };

    const names = buildOutline(workflow).flatMap((row) => (row.kind === "item" ? [row.name] : []));
    expect(names).toEqual(["start", "a", "b", "join", "tail"]);
  });

  it("does not follow an edge that closes a cycle, and still lists every node once", () => {
    const workflow: WorkflowDef = {
      nodes: [node("start", "xflow.start", { kind: "trigger" }), node("a"), node("b")],
      connections: {
        start: { main: [{ node: "a" }] },
        a: { main: [{ node: "b" }] },
        b: { main: [{ node: "a" }] }
      }
    };

    const rows = buildOutline(workflow);
    const names = rows.flatMap((row) => (row.kind === "item" ? [row.name] : []));
    expect(names).toEqual(["start", "a", "b"]);
    expect(rows.filter((row) => row.kind === "item")).toHaveLength(3);
  });

  it("appends a cycle with no way in at the end, in array order", () => {
    const workflow: WorkflowDef = {
      nodes: [node("loopB"), node("start", "xflow.start", { kind: "trigger" }), node("loopA"), node("after"), node("tail", "xflow.end")],
      connections: {
        start: { main: [{ node: "after" }] },
        after: { main: [{ node: "tail" }] },
        loopA: { main: [{ node: "loopB" }] },
        loopB: { main: [{ node: "loopA" }] }
      }
    };

    // The walked node comes first in 流程; the pair nothing leads to follows it, as the array has them.
    expect(shape(buildOutline(workflow))).toEqual([
      "# 入口 1",
      "start:0",
      "# 流程 3",
      "after:0",
      "loopB:0",
      "loopA:0",
      "# 结束 1",
      "tail:0"
    ]);
  });

  it("puts an isolated node in 入口 (no incoming edge) and counts each section's rows", () => {
    const workflow: WorkflowDef = {
      nodes: [node("lonely"), node("start", "xflow.start", { kind: "trigger" }), node("end", "xflow.end")],
      connections: { start: { main: [{ node: "end" }] } }
    };

    const rows = buildOutline(workflow);
    expect(shape(rows)).toEqual(["# 入口 2", "lonely:0", "start:0", "# 结束 1", "end:0"]);
    const sections = rows.filter((row) => row.kind === "section");
    expect(sections.map((row) => row.count).reduce((sum, count) => sum + count, 0)).toBe(3);
  });

  it("keeps a trigger-category node in 入口 even when something feeds it", () => {
    const workflow: WorkflowDef = {
      nodes: [node("seed"), node("hook", "xflow.trigger.webhook", { kind: "trigger" }), node("sink", "xflow.end")],
      connections: { seed: { main: [{ node: "hook" }] }, hook: { main: [{ node: "sink" }] } }
    };

    expect(shape(buildOutline(workflow))).toEqual(["# 入口 2", "seed:0", "hook:0", "# 结束 1", "sink:0"]);
  });

  it("uses a type's declared category before the prefix map", () => {
    const workflow: WorkflowDef = {
      nodes: [node("start", "xflow.start", { kind: "trigger" }), node("review", "acme.review")]
    };
    const item = buildOutline(workflow, { "acme.review": "human" }).find(
      (row): row is Extract<OutlineRow, { kind: "item" }> => row.kind === "item" && row.name === "review"
    );
    expect(item).toMatchObject({ category: "human" });
  });

  it("keeps undeclared types unknown and still resolves a declared builtin by prefix fallback", () => {
    const workflow: WorkflowDef = {
      nodes: [node("mystery", "acme.mystery"), node("call", "xflow.http")]
    };
    const items = buildOutline(workflow, { "acme.mystery": "not-a-category" }).filter(
      (row): row is Extract<OutlineRow, { kind: "item" }> => row.kind === "item"
    );
    expect(items.map((row) => [row.name, row.category])).toEqual([
      ["mystery", "unknown"],
      ["call", "action"]
    ]);
  });

  it("takes each section's colour from its nodes' main category", () => {
    const workflow: WorkflowDef = {
      nodes: [node("hook", "xflow.trigger.webhook", { kind: "trigger" }), node("call"), node("ask", "xflow.approval"), node("stop", "xflow.end")],
      connections: {
        hook: { main: [{ node: "call" }] },
        call: { main: [{ node: "ask" }] },
        ask: { main: [{ node: "stop" }] }
      }
    };

    const sections = buildOutline(workflow).filter((row) => row.kind === "section");
    expect(sections.map((row) => (row.kind === "section" ? [row.section, row.category] : []))).toEqual([
      ["entry", "trigger"],
      ["middle", "action"],
      ["end", "control"]
    ]);
  });

  it("ignores dependency edges and edges to nodes that do not exist", () => {
    const workflow: WorkflowDef = {
      nodes: [node("start", "xflow.start", { kind: "trigger" }), node("supplier", "xflow.supply.value", { kind: "supply" }), node("use")],
      connections: {
        start: { main: [{ node: "use" }, { node: "ghost" }] },
        supplier: { value: { type: "dependency", targets: [{ node: "use" }] } },
        phantom: { main: [{ node: "use" }] }
      }
    };

    const rows = buildOutline(workflow);
    expect(shape(rows)).toEqual(["# 入口 2", "start:0", "supplier:0", "# 结束 1", "use:0"]);
  });

  it("orders a very long chain without exhausting the call stack", () => {
    const count = 20000;
    const nodes = Array.from({ length: count }, (_, index) => node(`n${index}`));
    const connections: NonNullable<WorkflowDef["connections"]> = {};
    for (let index = 0; index < count - 1; index += 1) connections[`n${index}`] = { main: [{ node: `n${index + 1}` }] };

    const items = buildOutline({ nodes, connections }).filter((row) => row.kind === "item");
    expect(items).toHaveLength(count);
    expect(items[0]).toMatchObject({ name: "n0" });
    expect(items.at(-1)).toMatchObject({ name: `n${count - 1}` });
  });

  it("carries the display fields a row needs to draw and select", () => {
    const workflow: WorkflowDef = {
      nodes: [{ id: "n-1", name: "start", type: "xflow.start", kind: "trigger", disabled: true, ui: { label: "开始" } }]
    };

    const item = buildOutline(workflow).find((row) => row.kind === "item");
    expect(item).toMatchObject({
      key: "n-1",
      name: "start",
      label: "开始",
      type: "xflow.start",
      category: "control",
      indent: 0,
      disabled: true
    });
  });
});
