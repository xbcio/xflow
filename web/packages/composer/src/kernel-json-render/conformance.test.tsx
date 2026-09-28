import { describe, expect, it } from "vitest";
import { resolve, createCheckRegistry, createExpressionRegistry } from "../core";
import { kernelConformance } from "../testing";
import { createConformanceRegistry } from "../testing/components";
import { rowsSpec, slotSpec } from "../testing/fixtures";
import { jsonRenderKernel, toJsonRenderSpec } from "./index";

kernelConformance(jsonRenderKernel);

describe("toJsonRenderSpec", () => {
  const registry = createConformanceRegistry();
  const run = (spec: Parameters<typeof resolve>[0]["spec"], value: object) =>
    resolve({
      spec,
      value,
      schemas: registry.schemas,
      bindingKinds: registry.bindingKinds,
      checks: createCheckRegistry(),
      expressions: createExpressionRegistry()
    });

  it("emits one static ComposerNode element per node, keyed by node id", () => {
    const out = toJsonRenderSpec(run(slotSpec, {}))!;
    expect(out.root).toBe("root");
    expect(out.elements.root).toEqual({
      type: "ComposerNode",
      props: { id: "root" },
      children: ["body"],
      slots: { header: ["h1", "h2"], footer: ["f1"] }
    });
    expect(out.elements.h1).toEqual({ type: "ComposerNode", props: { id: "h1" } });
  });

  it("carries no json-render semantics (no visible/repeat/on/watch/expressions)", () => {
    const out = toJsonRenderSpec(run(rowsSpec("Input"), { rows: [{ name: "a" }] }))!;
    for (const element of Object.values(out.elements)) {
      expect(Object.keys(element).sort()).toEqual(expect.arrayContaining(["props", "type"]));
      for (const field of ["visible", "repeat", "on", "watch"]) expect(element).not.toHaveProperty(field);
      expect(Object.keys(element.props)).toEqual(["id"]);
      expect(typeof (element.props as { id: unknown }).id).toBe("string");
    }
    expect(Object.keys(out.elements)).toHaveLength(5); // root, list, $row, label, name
  });

  it("returns null for an empty tree", () => {
    expect(
      toJsonRenderSpec({ root: null, specErrors: [], issues: [], warnings: [], repeats: {}, objectTargets: [] })
    ).toBeNull();
  });
});
