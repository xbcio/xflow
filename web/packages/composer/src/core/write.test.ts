import { describe, expect, it } from "vitest";
import { createCheckRegistry } from "./checks";
import { createExpressionRegistry } from "./expressions";
import { formatPointer, getPointer, isPlainObject, parsePointer } from "./pointer";
import type { PropsSchema } from "./props";
import { resolve } from "./resolve";
import { objectUid, rowUids } from "./rows";
import type { BindTarget, ComposerSpec, Patch, ResolvedTree, ValueKind, Warning } from "./types";
import { applyPatches, createPatchBatch, isEmptyValue, mergePatches, write, type WriteState } from "./write";

const tree = (objectPaths: string[] = [], repeats: ResolvedTree["repeats"] = {}): WriteState["tree"] => ({
  repeats,
  objectTargets: objectPaths.map((path) => ({ path }))
});

function collect(state: Omit<WriteState, "onWarning">) {
  const warnings: Warning[] = [];
  return {
    warnings,
    write: (target: BindTarget, next: unknown, kind: ValueKind = "scalar", emptyAs?: "keep") =>
      write(target, next, kind, { ...state, emptyAs, onWarning: (w) => warnings.push(w) })
  };
}

describe("rule 1: clearing is unset", () => {
  const value = { p: { s: "x", list: [1], obj: { a: 1 } } };
  const { write: w } = collect({ value });

  it("scalar '' / undefined / null → unset", () => {
    for (const next of ["", undefined, null]) expect(w({ path: "/p/s" }, next)).toEqual([{ op: "unset", path: "/p/s" }]);
  });

  it("array [] and object {} → unset", () => {
    expect(w({ path: "/p/list" }, [], "array")).toEqual([{ op: "unset", path: "/p/list" }]);
    expect(w({ path: "/p/obj" }, {}, "object")).toEqual([{ op: "unset", path: "/p/obj" }]);
  });

  it("never emits set null, even for non-scalar kinds", () => {
    expect(w({ path: "/p/obj" }, null, "object")).toEqual([{ op: "unset", path: "/p/obj" }]);
    expect(w({ path: "/p/list" }, null, "array")).toEqual([{ op: "unset", path: "/p/list" }]);
  });

  it("emptyAs keep writes '' as a real value", () => {
    expect(w({ path: "/p/s" }, "", "scalar", "keep")).toEqual([{ op: "set", path: "/p/s", value: "" }]);
    expect(isEmptyValue(undefined, "scalar", "keep")).toBe(true);
  });

  it("non-empty values of another kind are set", () => {
    expect(w({ path: "/p/s" }, [], "scalar")).toEqual([{ op: "set", path: "/p/s", value: [] }]);
    expect(w({ path: "/p/s" }, 0)).toEqual([{ op: "set", path: "/p/s", value: 0 }]);
    expect(w({ path: "/p/s" }, false)).toEqual([{ op: "set", path: "/p/s", value: false }]);
  });
});

describe("rule 2: upward collapse", () => {
  it("collapses through object-bound ancestors that become {}", () => {
    const value = { parameters: { headers: { outer: { k: "v" } }, other: 1 } };
    const { write: w } = collect({ value, tree: tree(["/parameters/headers", "/parameters/headers/outer"]) });
    expect(w({ path: "/parameters/headers/outer/k" }, "")).toEqual([{ op: "unset", path: "/parameters/headers" }]);
  });

  it("stops at an unbound ancestor (e.g. /parameters)", () => {
    const value = { parameters: { headers: { k: "v" } } };
    const { write: w } = collect({ value, tree: tree(["/parameters/headers"]) });
    const patches = w({ path: "/parameters/headers/k" }, "");
    expect(patches).toEqual([{ op: "unset", path: "/parameters/headers" }]);
    expect(applyPatches(value, patches)).toEqual({ parameters: {} });
  });

  it("does not collapse an ancestor that still has other keys", () => {
    const value = { p: { h: { a: 1, b: 2 } } };
    const { write: w } = collect({ value, tree: tree(["/p/h"]) });
    expect(w({ path: "/p/h/a" }, undefined)).toEqual([{ op: "unset", path: "/p/h/a" }]);
  });

  it("does not collapse through an unbound intermediate", () => {
    const value = { p: { h: { mid: { a: 1 } } } };
    const { write: w } = collect({ value, tree: tree(["/p/h"]) });
    expect(w({ path: "/p/h/mid/a" }, "")).toEqual([{ op: "unset", path: "/p/h/mid/a" }]);
  });

  it("hidden object elements do not collapse (resolve excludes them from objectTargets)", () => {
    const s: ComposerSpec = {
      spec: "composer/v1",
      root: "form",
      elements: {
        form: { type: "Form", children: ["obj", "leaf"] },
        obj: { type: "Object", props: { value: { $bindState: "/p/h" } }, visible: false },
        leaf: { type: "Input", props: { value: { $bindState: "/p/h/a" } } }
      }
    };
    const any: PropsSchema<unknown> = { parse: (input) => ({ ok: true, value: input }) };
    const value = { p: { h: { a: "x" } } };
    const resolved = resolve({
      spec: s,
      value,
      schemas: { Form: any, Object: any, Input: any },
      bindingKinds: { Object: { value: "object" } },
      checks: createCheckRegistry(),
      expressions: createExpressionRegistry()
    });
    expect(resolved.objectTargets).toEqual([]);
    expect(write({ path: "/p/h/a" }, "", "scalar", { value, tree: resolved })).toEqual([{ op: "unset", path: "/p/h/a" }]);
  });

  it("never collapses across an array element: the row stays {}", () => {
    const value = { rows: [{ x: 1 }, { y: 2 }] };
    const { write: w } = collect({ value, tree: tree(["/rows/0", "/rows"]) });
    const patches = w({ path: "/rows/0/x" }, "");
    expect(patches).toEqual([{ op: "unset", path: "/rows/0/x" }]);
    expect(applyPatches(value, patches)).toEqual({ rows: [{}, { y: 2 }] });
  });
});

describe("rule 3: no unset on array indices", () => {
  it("rejects unset of an array element with a warning", () => {
    const value = { list: ["a", "b"] };
    const { write: w, warnings } = collect({ value });
    expect(w({ path: "/list/1" }, "")).toEqual([]);
    expect(warnings.map((x) => x.code)).toEqual(["unset-array-index"]);
    expect(w({ path: "/list/1" }, "c")).toEqual([{ op: "set", path: "/list/1", value: "c" }]);
  });

  it("applyPatches refuses array-index unset", () => {
    expect(() => applyPatches({ list: [1] }, [{ op: "unset", path: "/list/0" }])).toThrow(/array element/);
  });
});

describe("rule 4: core never writes defaults", () => {
  it("resolve does not mutate or patch the value; defaultHint is display-only", () => {
    const any: PropsSchema<unknown> = { parse: (input) => ({ ok: true, value: input }) };
    const value = Object.freeze({ p: Object.freeze({}) });
    const resolved = resolve({
      spec: {
        spec: "composer/v1",
        root: "f",
        elements: { f: { type: "Input", props: { value: { $bindState: "/p/mode" }, defaultHint: "signal" } } }
      },
      value,
      schemas: { Input: any },
      checks: createCheckRegistry(),
      expressions: createExpressionRegistry()
    });
    expect(resolved.root!.defaultHint).toBe("signal");
    expect(resolved.root!.bindings.value.value).toBeUndefined();
    expect(value).toEqual({ p: {} });
  });
});

describe("rule 5: only changed paths", () => {
  const value = { a: 1, o: { k: [1, 2] } };
  const { write: w } = collect({ value });
  it("equal set and absent unset produce no patch", () => {
    expect(w({ path: "/a" }, 1)).toEqual([]);
    expect(w({ path: "/a" }, 1.0)).toEqual([]);
    expect(w({ path: "/o" }, { k: [1, 2] }, "object")).toEqual([]);
    expect(w({ path: "/missing" }, "")).toEqual([]);
    expect(w({ path: "/missing" }, undefined)).toEqual([]);
  });
});

describe("rule 7: structural sharing in applyPatches", () => {
  it("copies only the changed path", () => {
    const value = { a: { x: 1 }, b: { y: [1, 2] }, c: [{ z: 1 }, { z: 2 }] };
    const next = applyPatches(value, [{ op: "set", path: "/c/1/z", value: 3 }]);
    expect(next).not.toBe(value);
    expect(next.a).toBe(value.a);
    expect(next.b).toBe(value.b);
    expect(next.c[0]).toBe(value.c[0]);
    expect(next.c[1]).toEqual({ z: 3 });
    expect(value.c[1]).toEqual({ z: 2 });
  });

  it("returns the same reference for no-op patches", () => {
    const value = { a: 1, o: { k: 1 } };
    expect(applyPatches(value, [{ op: "unset", path: "/missing/deep" }])).toBe(value);
    expect(applyPatches(value, [{ op: "set", path: "/a", value: 1 }])).toBe(value);
    expect(applyPatches(value, [])).toBe(value);
  });

  it("creates missing intermediate objects and appends with '-'", () => {
    expect(applyPatches({}, [{ op: "set", path: "/a/b/c", value: 1 }])).toEqual({ a: { b: { c: 1 } } });
    expect(applyPatches({ l: [1] }, [{ op: "set", path: "/l/-", value: 2 }])).toEqual({ l: [1, 2] });
    expect(() => applyPatches({ a: 1 }, [{ op: "set", path: "/a/b", value: 1 }])).toThrow(/not a container/);
  });

  it("copied rows keep their row identity", () => {
    const rows = [{ n: 1 }, { n: 2 }];
    const before = rowUids(rows);
    const next = applyPatches({ rows }, [{ op: "set", path: "/rows/1/n", value: 5 }]);
    expect(rowUids(next.rows)).toEqual(before);
    expect(objectUid(next.rows[1])).toBe(objectUid(rows[1]));
  });
});

describe("rule 8: /$ctx is read-only", () => {
  it("rejects writes with a warning and never emits a /$ctx patch", () => {
    const { write: w, warnings } = collect({ value: {} });
    expect(w({ path: "/$ctx/user" }, "x")).toEqual([]);
    expect(w({ path: "/$ctx" }, "", "scalar")).toEqual([]);
    expect(warnings.map((x) => x.code)).toEqual(["ctx-readonly", "ctx-readonly"]);
    expect(() => applyPatches({}, [{ op: "set", path: "/$ctx/a", value: 1 }])).toThrow(/read-only/);
  });
});

describe("row targets", () => {
  it("resolve the row by uid in the current value and drop writes to removed rows", () => {
    const a = { name: "a" };
    const b = { name: "b" };
    const [, uidB] = rowUids([a, b]);
    const repeats = { rules: { source: { path: "/rules" } } };
    const target = { repeat: "rules", row: uidB, field: "name" };
    expect(write(target, "B", "scalar", { value: { rules: [a, b] }, tree: tree([], repeats) })).toEqual([
      { op: "set", path: "/rules/1/name", value: "B" }
    ]);
    // b moved to the front: the write follows it.
    expect(write(target, "B", "scalar", { value: { rules: [b, a] }, tree: tree([], repeats) })).toEqual([
      { op: "set", path: "/rules/0/name", value: "B" }
    ]);
    // b deleted: the (blur) write is dropped instead of landing on a neighbour.
    const warnings: Warning[] = [];
    expect(
      write(target, "B", "scalar", { value: { rules: [a] }, tree: tree([], repeats), onWarning: (w) => warnings.push(w) })
    ).toEqual([]);
    expect(warnings[0].code).toBe("row-gone");
  });
});

describe("batching", () => {
  it("merges same-path writes, last wins, order preserved", () => {
    const patches: Patch[] = [
      { op: "set", path: "/a", value: 1 },
      { op: "set", path: "/b", value: 1 },
      { op: "set", path: "/a", value: 2 },
      { op: "set", path: "/c/x", value: 1 },
      { op: "unset", path: "/c" },
      { op: "set", path: "/c/y", value: 2 }
    ];
    expect(mergePatches(patches)).toEqual([
      { op: "set", path: "/b", value: 1 },
      { op: "set", path: "/a", value: 2 },
      { op: "unset", path: "/c" },
      { op: "set", path: "/c/y", value: 2 }
    ]);
    const value = { a: 0, c: { z: 1 } };
    expect(applyPatches(value, mergePatches(patches))).toEqual(applyPatches(value, patches));
  });

  it("createPatchBatch accumulates and flushes", () => {
    const batch = createPatchBatch();
    batch.push({ op: "set", path: "/a", value: 1 });
    batch.push({ op: "set", path: "/a", value: 2 }, { op: "unset", path: "/b" });
    expect(batch.size).toBe(3);
    expect(batch.flush()).toEqual([
      { op: "set", path: "/a", value: 2 },
      { op: "unset", path: "/b" }
    ]);
    expect(batch.size).toBe(0);
  });
});

// ---------------------------------------------------------------- property

/** mulberry32: small deterministic PRNG so failures reproduce from the seed. */
function prng(seed: number) {
  let a = seed >>> 0;
  const next = () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return {
    next,
    int: (n: number) => Math.floor(next() * n),
    pick<T>(xs: readonly T[]): T {
      return xs[Math.floor(next() * xs.length)];
    },
    chance: (p: number) => next() < p
  };
}
type Rng = ReturnType<typeof prng>;

const KEYS = ["a", "b", "c", "d"];

/** Random JSON value without nulls or empty containers. */
function randomValue(rng: Rng, depth: number): unknown {
  const roll = rng.int(depth <= 0 ? 3 : 5);
  if (roll === 0) return rng.pick(["x", "y", "0"]);
  if (roll === 1) return rng.pick([0, 1, 2.5, -1]);
  if (roll === 2) return rng.chance(0.5);
  if (roll === 3) {
    const out: Record<string, unknown> = {};
    const n = 1 + rng.int(3);
    for (let i = 0; i < n; i++) out[rng.pick(KEYS)] = randomValue(rng, depth - 1);
    return out;
  }
  return Array.from({ length: 1 + rng.int(3) }, () => randomValue(rng, depth - 1));
}

function allPaths(value: unknown, tokens: string[] = [], out: string[][] = []): string[][] {
  out.push(tokens);
  if (Array.isArray(value)) value.forEach((child, i) => allPaths(child, [...tokens, String(i)], out));
  else if (isPlainObject(value)) for (const [k, child] of Object.entries(value)) allPaths(child, [...tokens, k], out);
  return out;
}

function containsNull(value: unknown): boolean {
  if (value === null) return true;
  if (Array.isArray(value)) return value.some(containsNull);
  if (isPlainObject(value)) return Object.values(value).some(containsNull);
  return false;
}

function parentIsArray(value: unknown, pointer: string): boolean {
  return Array.isArray(getPointer(value, formatPointer(parsePointer(pointer).slice(0, -1))));
}

describe("write property test (seeded)", () => {
  const SEEDS = 300;
  const STEPS = 40;

  it(`holds the §6 invariants over ${SEEDS} random value trees × ${STEPS} writes`, () => {
    for (let seed = 1; seed <= SEEDS; seed++) {
      const rng = prng(seed);
      let value = { p: randomValue(rng, 3) } as Record<string, unknown>;
      // Bound object paths: a random subset of the object nodes (depth >= 2).
      const objectPaths = new Set<string>();
      for (const tokens of allPaths(value)) {
        if (tokens.length >= 2 && isPlainObject(getPointer(value, formatPointer(tokens))) && rng.chance(0.6)) {
          objectPaths.add(formatPointer(tokens));
        }
      }
      const writeTree = tree([...objectPaths]);
      const context = (msg: string) => `seed ${seed}: ${msg}`;

      for (let step = 0; step < STEPS; step++) {
        const existing = allPaths(value).filter((t) => t.length > 0);
        if (existing.length === 0) existing.push(["p"]);
        let tokens: string[];
        const mode = rng.int(10);
        if (mode === 0) tokens = ["$ctx", rng.pick(KEYS)];
        else if (mode === 1) tokens = [...rng.pick(existing), rng.pick(KEYS)];
        else tokens = rng.pick(existing);
        const path = formatPointer(tokens);
        const kind: ValueKind = objectPaths.has(path) ? "object" : rng.pick(["scalar", "array", "scalar"] as const);
        const next = rng.pick([undefined, null, "", [], {}, "v", 7, randomValue(rng, 2), randomValue(rng, 1)]);

        const before = value;
        const patches = write({ path }, next, kind, { value, tree: writeTree });
        for (const patch of patches) {
          expect(patch.path.startsWith("/$ctx"), context(`ctx patch ${patch.path}`)).toBe(false);
          if (patch.op === "set") {
            expect(containsNull(patch.value), context(`null written at ${patch.path}`)).toBe(false);
            expect(patch.value, context("undefined set")).not.toBeUndefined();
          } else {
            expect(parentIsArray(before, patch.path), context(`unset of array index ${patch.path}`)).toBe(false);
          }
        }
        value = applyPatches(value, patches);
        if (patches.length === 0) expect(value).toBe(before);

        // No empty object left at a bound object path (outside array rows).
        for (const bound of objectPaths) {
          if (parentIsArray(value, bound)) continue;
          const at = getPointer(value, bound);
          expect(isPlainObject(at) && Object.keys(at).length === 0, context(`empty container left at ${bound}`)).toBe(false);
        }
        // Siblings of the written path keep their references.
        for (const patch of patches) {
          const patchTokens = parsePointer(patch.path);
          if (patchTokens[0] !== "p" || patchTokens.length < 2 || patches.length > 1) continue;
          const top = patchTokens[1];
          const parentBefore = getPointer(before, "/p");
          const parentAfter = getPointer(value, "/p");
          if (isPlainObject(parentBefore) && isPlainObject(parentAfter)) {
            for (const key of Object.keys(parentBefore)) {
              if (key !== top && key in parentAfter) expect(parentAfter[key], context("sibling copied")).toBe(parentBefore[key]);
            }
          }
        }
      }
    }
  });
});
