import { describe, expect, it } from "vitest";
import { categoryOf, nodeCategoryMeta, type NodeCategory } from "./categoryMeta";
import { semanticColors } from "./tokens";

describe("nodeCategoryMeta", () => {
  it("has the seven categories of spec §5", () => {
    expect(Object.keys(nodeCategoryMeta).sort()).toEqual(
      ["action", "code", "control", "human", "transform", "trigger", "unknown"],
    );
  });

  it("draws every category in a colour that has a derived family", () => {
    for (const [category, meta] of Object.entries(nodeCategoryMeta)) {
      expect(semanticColors, category).toContain(meta.color);
    }
  });

  it("gives unknown the neutral colour and a plain grid, not a question mark", () => {
    expect(nodeCategoryMeta.unknown.color).toBe("neutral");
    expect(nodeCategoryMeta.unknown.icon).toBe("grid");
  });
});

describe("categoryOf", () => {
  const cases: Array<[string, NodeCategory]> = [
    ["xflow.trigger.webhook", "trigger"],
    ["xflow.trigger.cron", "trigger"],
    ["xflow.trigger.kafka", "trigger"],
    ["xflow.trigger.redis", "trigger"],
    ["xflow.trigger.timer", "trigger"],
    ["xflow.if", "control"],
    ["xflow.switch", "control"],
    ["xflow.merge", "control"],
    ["xflow.map", "control"],
    ["xflow.wait", "control"],
    ["xflow.start", "control"],
    ["xflow.end", "control"],
    ["xflow.http", "action"],
    ["xflow.grpc", "action"],
    ["xflow.database", "action"],
    ["xflow.function", "code"],
    ["xflow.script", "code"],
    ["xflow.transform.filter", "transform"],
    ["xflow.transform.set", "transform"],
    ["xflow.approval", "human"],
  ];

  for (const [type, category] of cases) {
    it(`maps ${type} to ${category}`, () => {
      expect(categoryOf(type)).toBe(category);
    });
  }

  it("lets the descriptor's declared category win over the prefix", () => {
    expect(categoryOf("xflow.http", "code")).toBe("code");
    expect(categoryOf("acme.custom", "human")).toBe("human");
  });

  it("ignores a declared category it does not know", () => {
    expect(categoryOf("xflow.http", "bogus")).toBe("action");
    expect(categoryOf("acme.custom", "bogus")).toBe("unknown");
  });

  it("falls back to unknown for custom, empty and missing types without throwing", () => {
    expect(categoryOf("acme.custom")).toBe("unknown");
    expect(categoryOf("")).toBe("unknown");
    expect(categoryOf(undefined)).toBe("unknown");
    expect(categoryOf(undefined, undefined)).toBe("unknown");
  });

  it("matches whole dotted segments, not string prefixes", () => {
    expect(categoryOf("xflow.iffy")).toBe("unknown");
    expect(categoryOf("xflow.httpx")).toBe("unknown");
    expect(categoryOf("xflow.triggers.foo")).toBe("unknown");
  });

  it("always returns a key present in the table", () => {
    for (const type of ["", "x", "xflow.", "xflow.if", "xflow.trigger.", "a.b.c"]) {
      expect(nodeCategoryMeta[categoryOf(type)], type).toBeDefined();
    }
  });
});
