import { describe, expect, it } from "vitest";
import { condShapeErrors, evalCond } from "./cond";
import truthTable from "./testdata/cond-truth-table.json";
import { getPointer } from "./pointer";
import type { Cond } from "./types";

interface TruthCase {
  name: string;
  cond: Cond;
  value: unknown;
  expected: boolean;
  error?: boolean;
}

const table = truthTable as { cases: TruthCase[] };

describe("condition truth table (shared with Go)", () => {
  it("has unique case names", () => {
    const names = table.cases.map((c) => c.name);
    expect(new Set(names).size).toBe(names.length);
  });

  it.each(table.cases.map((c) => [c.name, c] as const))("%s", (_name, c) => {
    const result = evalCond(c.cond, { get: (pointer) => getPointer(c.value, pointer) });
    expect(result.value).toBe(c.expected);
    expect(result.error !== undefined).toBe(c.error === true);
  });
});

describe("evalCond", () => {
  it("treats undefined as visible", () => {
    expect(evalCond(undefined, { get: () => undefined })).toEqual({ value: true });
  });

  it("reads $item from the row scope", () => {
    const scope = { get: () => undefined, item: (field: string) => ({ kind: "a" })[field] };
    expect(evalCond({ $item: "kind", eq: "a" }, scope).value).toBe(true);
    expect(evalCond({ $item: "missing", eq: null }, scope).value).toBe(true);
  });

  it("reports $item outside a repeat as an error (condition true)", () => {
    const result = evalCond({ $item: "kind", eq: "a" }, { get: () => undefined });
    expect(result.value).toBe(true);
    expect(result.error).toMatch(/outside a repeat/);
  });
});

describe("condShapeErrors", () => {
  it("accepts well-formed conditions", () => {
    const cond = { $and: [{ $state: "/a", in: [1, null] }, { $not: { $state: "/b", truthy: true } }, [true]] };
    expect(condShapeErrors(cond, "/visible", { inRepeat: false })).toEqual([]);
  });

  it("points at malformed leaves", () => {
    const errors = condShapeErrors({ $or: [{ $state: "a", eq: 1 }, { $state: "/b", gt: "1" }, { $state: "/c", like: 1 }] }, "/v", {
      inRepeat: false
    });
    expect(errors.map((e) => e.path)).toEqual(["/v/$or/0/$state", "/v/$or/1/gt", "/v/$or/2/like", "/v/$or/2"]);
  });

  it("rejects $item outside a repeat", () => {
    expect(condShapeErrors({ $item: "x", eq: 1 }, "", { inRepeat: false })).toHaveLength(1);
    expect(condShapeErrors({ $item: "x", eq: 1 }, "", { inRepeat: true })).toHaveLength(0);
  });
});
