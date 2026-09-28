import { describe, expect, it, vi } from "vitest";
import { z } from "zod";
import { createCheckRegistry, defineCheck } from "./checks";
import { createExpressionRegistry, defineExpression } from "./expressions";
import { zodProps, type PropsSchema } from "./props";
import { resolve, type ResolveInput } from "./resolve";
import type { ComposerSpec, ElementDef, ResolvedNode, ResolvedTree } from "./types";
import { applyPatches, write } from "./write";

const anyProps: PropsSchema<unknown> = { parse: (input) => ({ ok: true, value: input }) };
const schemas: Record<string, PropsSchema<unknown>> = {
  Form: anyProps,
  Group: anyProps,
  Input: zodProps(z.object({ label: z.string().optional(), placeholder: z.string().optional() })),
  Select: anyProps,
  Table: anyProps,
  Object: anyProps
};
const bindingKinds = { Table: { value: "array" as const }, Object: { value: "object" as const } };
const checks = createCheckRegistry();
const expressions = createExpressionRegistry();

function spec(elements: Record<string, ElementDef>, root = "form", minor = 0): ComposerSpec {
  return { spec: "composer/v1", minor, root, elements };
}

function run(s: ComposerSpec, value: object, extra: Partial<ResolveInput> = {}): ResolvedTree {
  return resolve({ spec: s, value, schemas, bindingKinds, checks, expressions, ...extra });
}

function find(tree: ResolvedTree, id: string): ResolvedNode | undefined {
  const walk = (node: ResolvedNode): ResolvedNode | undefined => {
    if (node.id === id) return node;
    for (const child of [...node.children, ...Object.values(node.slots).flat()]) {
      const hit = walk(child);
      if (hit) return hit;
    }
    return undefined;
  };
  return tree.root ? walk(tree.root) : undefined;
}

const ids = (nodes: ResolvedNode[]) => nodes.map((n) => n.id);

describe("resolve: props, bindings, slots", () => {
  const s = spec({
    form: { type: "Form", props: { layout: "horizontal" }, children: ["mode", "obj"], slots: { footer: ["note"] } },
    mode: {
      type: "Select",
      props: {
        label: "Mode",
        value: { $bindState: "/p/mode" },
        other: { $bindState: "/p/other" },
        defaultHint: "signal",
        hint: { $cond: { $state: "/p/mode", eq: "timer" }, then: { $state: "/$ctx/timerHint" }, else: "none" },
        options: [{ value: "a", label: { $state: "/$ctx/labelA" } }, { value: "b", label: "B" }]
      }
    },
    obj: { type: "Object", props: { value: { $bindState: "/p/headers" } } },
    note: { type: "Input", props: { label: "footer" } }
  });

  it("evaluates read expressions, extracts bindings and defaultHint", () => {
    const tree = run(s, { p: { mode: "timer", other: 3 } }, { context: { timerHint: "every", labelA: "A!" } });
    const mode = find(tree, "mode")!;
    expect(mode.props).toEqual({
      label: "Mode",
      hint: "every",
      options: [{ value: "a", label: "A!" }, { value: "b", label: "B" }]
    });
    expect(mode.defaultHint).toBe("signal");
    expect(mode.bindings).toEqual({
      value: { target: { path: "/p/mode" }, value: "timer", valueKind: "scalar" },
      other: { target: { path: "/p/other" }, value: 3, valueKind: "scalar" }
    });
    expect(find(tree, "obj")!.bindings.value.valueKind).toBe("object");
    expect(tree.objectTargets).toEqual([{ path: "/p/headers" }]);
    expect(ids(tree.root!.slots.footer)).toEqual(["note"]);
    expect(ids(tree.root!.children)).toEqual(["mode", "obj"]);
    expect(tree.specErrors).toEqual([]);
  });

  it("keeps literal sub-objects by reference", () => {
    const tree = run(s, { p: {} }, { context: { labelA: "x" } });
    const options = find(tree, "mode")!.props.options as unknown[];
    expect(options[1]).toBe((s.elements.mode.props!.options as unknown[])[1]);
  });

  it("supports host expressions via defineExpression", () => {
    const t = defineExpression("$t", { resolve: (args, ctx) => `${String(args.$t)}:${String(ctx.get("/$ctx/lang"))}` });
    const tree = run(spec({ form: { type: "Form", props: { title: { $t: "hello" } } } }), {}, {
      expressions: createExpressionRegistry([t]),
      context: { lang: "zh" }
    });
    expect(tree.root!.props.title).toBe("hello:zh");
  });
});

describe("resolve: visibility", () => {
  const s = spec({
    form: { type: "Form", children: ["mode", "signal", "bad"] },
    mode: { type: "Select", props: { value: { $bindState: "/mode" } } },
    signal: {
      type: "Input",
      props: { label: "s", value: { $bindState: "/signal" } },
      visible: { $state: "/mode", in: ["signal", null] },
      checks: [{ type: "required", message: "req" }]
    },
    bad: { type: "Input", props: { label: "b" }, visible: { $state: "/mode", gt: 1 } }
  });

  it("hides elements and skips their checks", () => {
    const tree = run(s, { mode: "timer" });
    expect(ids(tree.root!.children)).toEqual(["mode", "bad"]);
    expect(tree.issues).toEqual([]);
  });

  it("missing path counts as null", () => {
    const tree = run(s, {});
    expect(ids(tree.root!.children)).toContain("signal");
    expect(find(tree, "signal")!.issues.map((i) => i.message)).toEqual(["req"]);
  });

  it("condition errors keep the element visible with an error", () => {
    const bad = find(run(s, { mode: "x" }), "bad")!;
    expect(bad.error?.code).toBe("cond");
  });
});

describe("resolve: element-level isolation", () => {
  it("marks unknown types, props failures and expression errors on the element only", () => {
    const s = spec({
      form: { type: "Form", children: ["unknown", "badProps", "badExpr", "ok"] },
      unknown: { type: "Fancy", props: { label: "x" } },
      badProps: { type: "Input", props: { label: 42 } },
      badExpr: { type: "Input", props: { label: { $nope: 1 }, placeholder: { $item: "x" } } },
      ok: { type: "Input", props: { label: "fine" } }
    });
    const tree = run(s, {});
    expect(find(tree, "unknown")!.error?.code).toBe("unknown-type");
    const badProps = find(tree, "badProps")!;
    expect(badProps.error?.code).toBe("props");
    expect(badProps.error?.issues?.[0].path).toBe("/label");
    expect(find(tree, "badExpr")!.error?.code).toBe("expression");
    expect(find(tree, "ok")!.error).toBeUndefined();
    expect(tree.specErrors.length).toBeGreaterThan(0);
  });

  it("caches props validation per props object", () => {
    const parse = vi.fn((input: unknown) => ({ ok: true as const, value: input }));
    const counted = { ...schemas, Input: { parse } };
    const s = spec({ form: { type: "Form", children: ["a"] }, a: { type: "Input", props: { label: "x", value: { $bindState: "/a" } } } });
    let tree = run(s, { a: 1 }, { schemas: counted });
    tree = run(s, { a: 2 }, { schemas: counted, previous: tree });
    run(s, { a: 3 }, { schemas: counted, previous: tree });
    expect(parse).toHaveBeenCalledTimes(1);
  });

  it("refuses to render an unknown major or missing root", () => {
    expect(run({ ...spec({ form: { type: "Form" } }), spec: "composer/v9" as "composer/v1" }, {}).root).toBeNull();
    expect(run(spec({ form: { type: "Form" } }, "nope"), {}).root).toBeNull();
  });

  it("newer minor: unknown field / check / expression degrade to element errors, siblings render", () => {
    const s = spec(
      {
        form: { type: "Form", children: ["a", "b", "c", "ok"] },
        a: { type: "Input", tooltip: "x" } as ElementDef,
        b: { type: "Input", checks: [{ type: "future", message: "m" }] },
        c: { type: "Input", props: { label: { $future: 1 } } },
        ok: { type: "Input", props: { label: "fine" } }
      },
      "form",
      5
    );
    const tree = run(s, {});
    expect(tree.specErrors.every((e) => e.severity === "warning")).toBe(true);
    expect(find(tree, "a")!.error?.code).toBe("unsupported");
    expect(find(tree, "b")!.error?.code).toBe("check");
    expect(find(tree, "c")!.error?.code).toBe("expression");
    expect(find(tree, "ok")!.error).toBeUndefined();
  });

  it("guards cycles without recursing forever", () => {
    const tree = run(spec({ form: { type: "Form", children: ["a"] }, a: { type: "Group", children: ["form"] } }), {});
    expect(find(tree, "a")!.children[0].error?.code).toBe("cycle");
  });
});

describe("resolve: checks and issues", () => {
  it("evaluates when, targets, severity and expression messages", () => {
    const s = spec({
      form: {
        type: "Form",
        children: ["mode", "code", "digest"],
        checks: [
          {
            type: "oneOf",
            args: { paths: ["/code", "/digest"] },
            targets: ["/code", "/digest"],
            message: "exactly one"
          }
        ]
      },
      mode: { type: "Select", props: { value: { $bindState: "/mode" } } },
      code: {
        type: "Input",
        props: { value: { $bindState: "/code" } },
        checks: [
          { type: "required", when: { $state: "/mode", eq: "signal" }, message: { $state: "/$ctx/requiredMsg" } },
          { type: "minLength", args: { value: 5 }, severity: "warning", message: "short" }
        ]
      },
      digest: { type: "Input", props: { value: { $bindState: "/digest" } }, visible: { $state: "/mode", eq: "art" } }
    });
    const tree = run(s, { mode: "signal" }, { context: { requiredMsg: "必填" } });
    expect(find(tree, "code")!.issues.map((i) => `${i.severity}:${i.message}`)).toEqual(["error:exactly one", "error:必填"]);
    expect(tree.issues.map((i) => i.path)).toEqual(["/code", "/digest", "/code"]);
    // digest is hidden, so its oneOf issue has no bound holder and falls back
    // to the owning element (the form root); it is also in tree.issues.
    expect(tree.root!.issues.map((i) => i.path)).toEqual(["/digest"]);
    const shortTree = run(s, { mode: "x", code: "abc" }, { context: {} });
    expect(find(shortTree, "code")!.issues).toEqual([
      { message: "short", severity: "warning", check: "minLength", source: "code", path: "/code" }
    ]);
  });

  it("oneOf runs for hidden elements, other checks do not", () => {
    const s = spec({
      form: { type: "Form", children: ["group"] },
      group: { type: "Group", visible: false, children: ["field"] },
      field: {
        type: "Input",
        props: { value: { $bindState: "/a" } },
        checks: [
          { type: "required", message: "req" },
          { type: "oneOf", args: { paths: ["/a", "/b"] }, message: "one" }
        ]
      }
    });
    const tree = run(s, { a: 1, b: 2 });
    expect(tree.issues.map((i) => `${i.check}:${i.source}`)).toEqual(["oneOf:field"]);
  });

  it("custom checks see ctx.get / ctx.path / ctx.cond and may set their own path", () => {
    const fn = vi.fn((value: unknown, args: Record<string, unknown>, ctx: { get(p: string): unknown; path?: string; cond(c: unknown): { value: boolean } }) => {
      expect(ctx.path).toBe("/x");
      expect(ctx.get("/y")).toBe(2);
      expect(ctx.cond({ $state: "/y", eq: 2 }).value).toBe(true);
      return value === args.forbidden ? [{ path: "/y", message: "detail" }] : [];
    });
    const custom = defineCheck("notValue", fn as never);
    const s = spec({
      form: { type: "Form", children: ["x", "y"] },
      x: { type: "Input", props: { value: { $bindState: "/x" } }, checks: [{ type: "notValue", args: { forbidden: 1 }, message: "bad" }] },
      y: { type: "Input", props: { value: { $bindState: "/y" } } }
    });
    const tree = run(s, { x: 1, y: 2 }, { checks: createCheckRegistry([custom]) });
    expect(find(tree, "y")!.issues).toEqual([{ message: "bad", severity: "error", check: "notValue", source: "x", path: "/y", detail: "detail" }]);
    expect(find(tree, "x")!.issues).toEqual([]);
  });

  it("merges externalIssues by pointer (exact, nearest bound ancestor, unplaced kept in tree.issues)", () => {
    const s = spec({
      form: { type: "Form", children: ["a", "kv"] },
      a: { type: "Input", props: { value: { $bindState: "/p/a" } } },
      kv: { type: "Object", props: { value: { $bindState: "/p/headers" } } }
    });
    const tree = run(s, {}, {
      externalIssues: {
        "/p/a": [{ message: "server says no", severity: "error" }],
        "/p/headers/X-Key": [{ message: "bad header", severity: "warning" }],
        "/p/zzz": [{ message: "orphan", severity: "error" }]
      }
    });
    expect(find(tree, "a")!.issues).toEqual([{ message: "server says no", severity: "error", path: "/p/a" }]);
    expect(find(tree, "kv")!.issues.map((i) => i.message)).toEqual(["bad header"]);
    expect(tree.issues.map((i) => i.message)).toEqual(["server says no", "bad header", "orphan"]);
  });
});

describe("resolve: repeat", () => {
  const s = spec({
    form: { type: "Form", children: ["rules"] },
    rules: {
      type: "Table",
      props: { value: { $bindState: "/rules" } },
      repeat: { statePath: "/rules" },
      children: ["name", "tags"]
    },
    name: {
      type: "Input",
      props: { value: { $bindItem: "name" }, placeholder: { $cond: { $item: "on", truthy: true }, then: "on", else: "off" } },
      checks: [{ type: "required", message: "name required" }]
    },
    tags: { type: "Table", props: { value: { $bindItem: "tags" } }, repeat: { statePath: { $item: "tags" } }, children: ["tag"] },
    tag: { type: "Select", props: { value: { $bindItem: "" }, index: { $index: true } } }
  });

  it("expands rows into $row nodes with uid-based instance ids and row targets", () => {
    const rows = [{ name: "a", tags: ["x", "y"] }, { on: true }];
    const tree = run(s, { rules: rows });
    const container = find(tree, "rules")!;
    expect(container.children.map((n) => n.type)).toEqual(["$row", "$row"]);
    const [row0, row1] = container.children;
    expect(row0.id).toMatch(/^\$row@rules#r\d+$/);
    expect(row0.props).toEqual({ index: 0 });
    const uid0 = row0.id.split("#")[1];
    const name0 = row0.children[0];
    expect(name0.id).toBe(`name@rules#${uid0}`);
    expect(name0.bindings.value).toEqual({ target: { repeat: "rules", row: uid0, field: "name" }, value: "a", valueKind: "scalar" });
    expect(name0.props.placeholder).toBe("off");
    expect(row1.children[0].props.placeholder).toBe("on");
    // required issue on the second row lands on that row's field only
    expect(row1.children[0].issues.map((i) => i.path)).toEqual(["/rules/1/name"]);
    expect(name0.issues).toEqual([]);
    // nested scalar repeat: index fallback + $index
    const tags = row0.children[1];
    expect(tags.id).toBe(`tags@rules#${uid0}`);
    expect(ids(tags.children)).toEqual([`$row@tags@rules#${uid0}#i0`, `$row@tags@rules#${uid0}#i1`]);
    const tag1 = tags.children[1].children[0];
    expect(tag1.id).toBe(`tag@tags@rules#${uid0}#i1`);
    expect(tag1.props.index).toBe(1);
    expect(tag1.bindings.value.value).toBe("y");
    expect(tree.repeats[tags.id]).toEqual({ source: { repeat: "rules", row: uid0, field: "tags" } });
  });

  it("row ids stay stable across delete, insert and reorder (object identity)", () => {
    const a = { name: "a" };
    const b = { name: "b" };
    const c = { name: "c" };
    const rowIds = (rows: object[]) => ids(find(run(s, { rules: rows }), "rules")!.children);
    const [ia, ib, ic] = rowIds([a, b, c]);
    expect(rowIds([a, c])).toEqual([ia, ic]);
    const d = { name: "d" };
    const withInsert = rowIds([a, d, b, c]);
    expect([withInsert[0], withInsert[2], withInsert[3]]).toEqual([ia, ib, ic]);
    expect(new Set(withInsert).size).toBe(4);
    expect(rowIds([c, a, b])).toEqual([ic, ia, ib]);
  });

  it("row identity survives edits applied through write + applyPatches", () => {
    const value = { rules: [{ name: "a" }, { name: "b" }] };
    const tree = run(s, value);
    const rowB = find(tree, "rules")!.children[1];
    const nameB = rowB.children[0];
    const patches = write(nameB.bindings.value.target, "bb", "scalar", { value, tree });
    expect(patches).toEqual([{ op: "set", path: "/rules/1/name", value: "bb" }]);
    const next = applyPatches(value, patches);
    expect(next.rules[0]).toBe(value.rules[0]);
    const nextTree = run(s, next, { previous: tree });
    expect(find(nextTree, "rules")!.children[1].id).toBe(rowB.id);
    expect(find(nextTree, "rules")!.children[0]).toBe(find(tree, "rules")!.children[0]);
  });

  it("uses repeat.key for identity when present", () => {
    const keyed = spec({
      form: { type: "Form", children: ["rules"] },
      rules: { type: "Table", repeat: { statePath: "/rules", key: "id" }, children: ["name"] },
      name: { type: "Input", props: { value: { $bindItem: "name" } } }
    });
    const tree = run(keyed, { rules: [{ id: "x" }, { id: 7 }, { id: "x" }] });
    expect(ids(find(tree, "rules")!.children)).toEqual(["$row@rules#k:x", "$row@rules#k:7", "$row@rules#k:x~1"]);
    // copies with the same key keep their id
    const again = run(keyed, { rules: [{ id: 7 }, { id: "x" }] });
    expect(ids(find(again, "rules")!.children)).toEqual(["$row@rules#k:7", "$row@rules#k:x"]);
  });

  it("missing list renders no rows; non-array marks the container", () => {
    expect(find(run(s, {}), "rules")!.children).toEqual([]);
    const broken = find(run(s, { rules: "nope" }), "rules")!;
    expect(broken.error?.code).toBe("repeat");
    expect(broken.children).toEqual([]);
  });

  it("$bindItem on a scalar row is an element error", () => {
    const scalar = spec({
      form: { type: "Form", children: ["list"] },
      list: { type: "Table", repeat: { statePath: "/list" }, children: ["f"] },
      f: { type: "Input", props: { value: { $bindItem: "x" } } }
    });
    const tree = run(scalar, { list: ["a"] });
    expect(find(tree, "list")!.children[0].children[0].error?.code).toBe("expression");
  });
});

describe("resolve: structural sharing", () => {
  const s = spec({
    form: { type: "Form", children: ["g1", "g2"] },
    g1: { type: "Group", children: ["a", "b"] },
    g2: { type: "Group", children: ["c"] },
    a: { type: "Input", props: { label: "A", value: { $bindState: "/a" } }, checks: [{ type: "required", message: "r" }] },
    b: { type: "Input", props: { label: "B", value: { $bindState: "/b" } } },
    c: { type: "Input", props: { label: { $state: "/$ctx/c" }, value: { $bindState: "/c" } } }
  });

  it("reuses untouched nodes by reference after a single-field change", () => {
    const first = run(s, { a: "1", b: "2", c: "3" }, { context: { c: "C" } });
    const second = run(s, { a: "1", b: "changed", c: "3" }, { context: { c: "C" }, previous: first });
    expect(find(second, "a")).toBe(find(first, "a"));
    expect(find(second, "g2")).toBe(find(first, "g2"));
    expect(find(second, "c")).toBe(find(first, "c"));
    expect(find(second, "b")).not.toBe(find(first, "b"));
    expect(find(second, "b")!.props).toBe(find(first, "b")!.props);
    expect(find(second, "g1")).not.toBe(find(first, "g1"));
    expect(second.root).not.toBe(first.root);
  });

  it("returns the identical tree root when nothing changed", () => {
    const value = { a: "1" };
    const first = run(s, value, { context: { c: "C" } });
    const second = run(s, value, { context: { c: "C" }, previous: first });
    expect(second.root).toBe(first.root);
  });

  it("issue changes invalidate only the affected node", () => {
    const first = run(s, { a: "1" }, { context: { c: "C" } });
    const second = run(s, {}, { context: { c: "C" }, previous: first });
    expect(find(second, "a")).not.toBe(find(first, "a"));
    expect(find(second, "a")!.issues).toHaveLength(1);
    expect(find(second, "b")).toBe(find(first, "b"));
  });
});

describe("resolve: performance budget", () => {
  // Budget (Doc B §4): 500-element spec, median resolve <= 5ms. Measured
  // ~1ms locally, so the doc budget itself leaves ~5x headroom. On slow or
  // overloaded CI runners set COMPOSER_PERF_BUDGET_MS (e.g. 15) to widen it,
  // or COMPOSER_PERF_SKIP=1 to skip the timing assertion entirely.
  const skip = process.env.COMPOSER_PERF_SKIP === "1";
  const budget = Number(process.env.COMPOSER_PERF_BUDGET_MS ?? 5);

  function bigSpec(): { spec: ComposerSpec; value: Record<string, unknown> } {
    const elements: Record<string, ElementDef> = { form: { type: "Form", children: [] } };
    const value: Record<string, unknown> = { mode: "on" };
    for (let g = 0; g < 50; g++) {
      const group = `g${g}`;
      elements.form.children!.push(group);
      elements[group] = { type: "Group", props: { title: `Group ${g}` }, children: [] };
      for (let f = 0; f < 9; f++) {
        const id = `f${g}_${f}`;
        elements[group].children!.push(id);
        elements[id] = {
          type: "Input",
          props: { label: `Field ${id}`, value: { $bindState: `/v/${id}` } },
          visible: { $state: "/mode", in: ["on", null] },
          checks: [{ type: "required", message: "required" }, { type: "maxLength", args: { value: 20 }, message: "long" }]
        };
        (value.v ??= {} as Record<string, unknown>) as Record<string, unknown>;
        (value.v as Record<string, unknown>)[id] = f % 2 ? `value ${f}` : "";
      }
    }
    return { spec: spec(elements), value };
  }

  it.skipIf(skip)("500-element spec resolves within budget (median over 50 runs)", () => {
    const { spec: big, value } = bigSpec();
    expect(Object.keys(big.elements).length).toBe(501);
    let previous = run(big, value);
    const samples: number[] = [];
    for (let i = 0; i < 50; i++) {
      const next = { ...value, v: { ...(value.v as object), f0_0: `edit ${i}` } };
      const start = performance.now();
      previous = run(big, next, { previous });
      samples.push(performance.now() - start);
    }
    // Cold resolve (no previous) is measured too.
    const cold: number[] = [];
    for (let i = 0; i < 20; i++) {
      const start = performance.now();
      run(big, value);
      cold.push(performance.now() - start);
    }
    const median = (xs: number[]) => xs.slice().sort((x, y) => x - y)[Math.floor(xs.length / 2)];
    expect(median(samples)).toBeLessThanOrEqual(budget);
    expect(median(cold)).toBeLessThanOrEqual(budget);
  });
});
