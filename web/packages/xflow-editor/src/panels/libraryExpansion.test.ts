import { describe, expect, it } from "vitest";
import {
  COMMON_NODE_GROUP,
  LIBRARY_GROUP_FOLD_KEY,
  defaultGroupExpanded,
  expandedGroups,
  persistGroupOverrides,
  readGroupOverrides,
  workflowHasTrigger
} from "./libraryExpansion";

/** A Storage the tests own; the jsdom environment may ship none at all. */
function memoryStorage(seed: Record<string, string> = {}): Storage {
  const values = new Map(Object.entries(seed));
  return {
    get length() {
      return values.size;
    },
    clear: () => values.clear(),
    getItem: (key) => values.get(key) ?? null,
    key: (index) => [...values.keys()][index] ?? null,
    removeItem: (key) => values.delete(key),
    setItem: (key, value) => values.set(key, String(value))
  };
}

describe("library group folds (§6.0.1)", () => {
  it("opens 常用, opens 触发器 only while the workflow has none, and folds the rest", () => {
    expect(defaultGroupExpanded(COMMON_NODE_GROUP, true)).toBe(true);
    expect(defaultGroupExpanded(COMMON_NODE_GROUP, false)).toBe(true);
    expect(defaultGroupExpanded("触发器", false)).toBe(true);
    expect(defaultGroupExpanded("触发器", true)).toBe(false);
    for (const group of ["流程控制", "动作", "代码与转换", "人工", "其他"]) {
      expect(defaultGroupExpanded(group, false)).toBe(false);
      expect(defaultGroupExpanded(group, true)).toBe(false);
    }

    // The effective set follows the defaults until the user says otherwise.
    expect([...expandedGroups([COMMON_NODE_GROUP, "触发器", "动作"], false, {})]).toEqual([COMMON_NODE_GROUP, "触发器"]);
    expect([...expandedGroups([COMMON_NODE_GROUP, "触发器", "动作"], true, {})]).toEqual([COMMON_NODE_GROUP]);
  });

  it("lets a user choice override the default in either direction", () => {
    const overrides = { 常用: false, 触发器: true, 动作: true };
    expect([...expandedGroups([COMMON_NODE_GROUP, "触发器", "动作", "人工"], true, overrides)]).toEqual([
      "触发器",
      "动作"
    ]);
  });

  it("detects a workflow trigger from the start node, a trigger type or the trigger kind", () => {
    expect(workflowHasTrigger([])).toBe(false);
    expect(workflowHasTrigger([{ name: "fetch", type: "xflow.http" }])).toBe(false);
    expect(workflowHasTrigger([{ name: "start", type: "xflow.start" }])).toBe(true);
    expect(workflowHasTrigger([{ name: "hook", type: "xflow.trigger.webhook" }])).toBe(true);
    expect(workflowHasTrigger([{ name: "odd", type: "custom.thing", kind: "trigger" }])).toBe(true);
  });

  it("persists the whole map and reads back only boolean entries", () => {
    const storage = memoryStorage();
    persistGroupOverrides({ 常用: false, 动作: true }, storage);
    expect(JSON.parse(storage.getItem(LIBRARY_GROUP_FOLD_KEY) ?? "null")).toEqual({ 常用: false, 动作: true });
    expect(readGroupOverrides(storage)).toEqual({ 常用: false, 动作: true });

    const foreign = memoryStorage({ [LIBRARY_GROUP_FOLD_KEY]: '{"常用":"no","动作":true,"人工":0}' });
    expect(readGroupOverrides(foreign)).toEqual({ 动作: true });
  });

  it("reads malformed or blocked storage as no choices", () => {
    expect(readGroupOverrides(memoryStorage({ [LIBRARY_GROUP_FOLD_KEY]: "{" }))).toEqual({});
    const blocked = memoryStorage();
    blocked.setItem = () => {
      throw new Error("quota");
    };
    expect(() => persistGroupOverrides({ 常用: true }, blocked)).not.toThrow();
  });
});
