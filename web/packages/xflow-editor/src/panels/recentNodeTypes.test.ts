import { describe, expect, it } from "vitest";
import {
  DEFAULT_COMMON_NODE_TYPES,
  RECENT_NODE_TYPES_KEY,
  RECENT_NODE_TYPES_LIMIT,
  commonNodeTypes,
  readRecentNodeTypes,
  rememberRecentNodeType
} from "./recentNodeTypes";

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

describe("recent node types (§6.0.1 常用)", () => {
  it("reads nothing from a missing, foreign or malformed value", () => {
    expect(readRecentNodeTypes(memoryStorage())).toEqual([]);
    expect(readRecentNodeTypes(memoryStorage({ [RECENT_NODE_TYPES_KEY]: "not json" }))).toEqual([]);
    expect(readRecentNodeTypes(memoryStorage({ [RECENT_NODE_TYPES_KEY]: '{"type":"xflow.if"}' }))).toEqual([]);
    expect(readRecentNodeTypes(memoryStorage({ [RECENT_NODE_TYPES_KEY]: '["xflow.if", 3, ""]' }))).toEqual(["xflow.if"]);
  });

  it("puts the latest type first, drops duplicates and caps the list", () => {
    const storage = memoryStorage();
    expect(rememberRecentNodeType("xflow.http", storage)).toEqual(["xflow.http"]);
    expect(rememberRecentNodeType("xflow.if", storage)).toEqual(["xflow.if", "xflow.http"]);
    expect(rememberRecentNodeType("xflow.http", storage)).toEqual(["xflow.http", "xflow.if"]);

    for (const type of ["a", "b", "c", "d", "e", "f"]) rememberRecentNodeType(type, storage);
    expect(readRecentNodeTypes(storage)).toHaveLength(RECENT_NODE_TYPES_LIMIT);
    expect(readRecentNodeTypes(storage)[0]).toBe("f");
  });

  it("survives a storage that cannot write", () => {
    const blocked = memoryStorage();
    blocked.setItem = () => {
      throw new Error("quota");
    };
    expect(rememberRecentNodeType("xflow.http", blocked)).toEqual(["xflow.http"]);
  });

  it("fills the 常用 group with the recent types, then the defaults, skipping unknown types", () => {
    const descriptors = [
      { type: "xflow.http", label: "HTTP" },
      { type: "xflow.if", label: "If" },
      { type: "xflow.function", label: "Function" },
      { type: "xflow.switch", label: "Switch" },
      { type: "xflow.transform.set", label: "Set" },
      { type: "xflow.merge", label: "Merge" },
      { type: "xflow.wait", label: "Wait" }
    ];

    // Defaults in their declared order when nothing was added yet.
    expect(commonNodeTypes(descriptors, []).map((item) => item.type)).toEqual(DEFAULT_COMMON_NODE_TYPES);

    // A recent type moves to the front; a missing type is skipped, not padded.
    expect(commonNodeTypes(descriptors, ["xflow.wait", "gone.type"]).map((item) => item.type)).toEqual([
      "xflow.wait",
      "xflow.http",
      "xflow.if",
      "xflow.function",
      "xflow.switch",
      "xflow.transform.set"
    ]);

    // Never more than six, and recent types stay ahead of the defaults.
    expect(commonNodeTypes(descriptors, ["a", "b", "c", "d", "e", "f", "g"]).map((item) => item.type)).toEqual([
      "xflow.http",
      "xflow.if",
      "xflow.function",
      "xflow.switch",
      "xflow.transform.set",
      "xflow.merge"
    ]);
  });

  it("returns the registry's own descriptor objects", () => {
    const http = { type: "xflow.http", label: "HTTP" };
    expect(commonNodeTypes([http], [])).toEqual([http]);
    expect(commonNodeTypes([http], [])[0]).toBe(http);
  });
});
