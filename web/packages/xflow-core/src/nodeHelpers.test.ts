import { describe, expect, it } from "vitest";
import { isSupplyNode, isTextEntryTarget, runtimeForNode, type RuntimeSnapshot } from "./index";

describe("isTextEntryTarget", () => {
  it("accepts form fields and contenteditable elements", () => {
    for (const tag of ["input", "textarea", "select"]) {
      expect(isTextEntryTarget(document.createElement(tag))).toBe(true);
    }
    const editable = document.createElement("div");
    editable.contentEditable = "true";
    // jsdom does not derive isContentEditable from the attribute.
    Object.defineProperty(editable, "isContentEditable", { value: true });
    expect(isTextEntryTarget(editable)).toBe(true);
  });

  it("rejects other elements, non-elements, and null", () => {
    expect(isTextEntryTarget(document.createElement("button"))).toBe(false);
    expect(isTextEntryTarget(document.createElement("div"))).toBe(false);
    expect(isTextEntryTarget(window)).toBe(false);
    expect(isTextEntryTarget(null)).toBe(false);
  });
});

describe("isSupplyNode", () => {
  it("matches the supply kind or the builtin supply type prefix", () => {
    expect(isSupplyNode({ kind: "supply", type: "custom.secret" })).toBe(true);
    expect(isSupplyNode({ type: "xflow.supply.credential" })).toBe(true);
    expect(isSupplyNode({ kind: "supply" })).toBe(true);
  });

  it("rejects other nodes and an absent node", () => {
    expect(isSupplyNode({ kind: "action", type: "xflow.http" })).toBe(false);
    expect(isSupplyNode({ type: "xflow.supplyish" })).toBe(false);
    expect(isSupplyNode({})).toBe(false);
    expect(isSupplyNode(undefined)).toBe(false);
  });
});

describe("runtimeForNode", () => {
  const runtime: RuntimeSnapshot = {
    nodes: {
      fetch: { status: "success" },
      "node-2": { status: "failed" },
      "node-1": { status: "running" }
    }
  };

  it("looks the node up by name first", () => {
    expect(runtimeForNode(runtime, { name: "fetch", id: "node-1" })).toEqual({ status: "success" });
  });

  it("falls back to the id when the name has no entry", () => {
    expect(runtimeForNode(runtime, { name: "transform", id: "node-2" })).toEqual({ status: "failed" });
    expect(runtimeForNode(runtime, { id: "node-2" })).toEqual({ status: "failed" });
  });

  it("defaults to pending without a matching entry or snapshot", () => {
    expect(runtimeForNode(runtime, { name: "missing", id: "missing" })).toEqual({ status: "pending" });
    expect(runtimeForNode(runtime, {})).toEqual({ status: "pending" });
    expect(runtimeForNode(undefined, { name: "fetch" })).toEqual({ status: "pending" });
  });
});
