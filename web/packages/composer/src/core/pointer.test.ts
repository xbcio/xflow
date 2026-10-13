import { describe, expect, it } from "vitest";
import { parsePointer } from "./pointer";

describe("parsePointer", () => {
  it("returns frozen token arrays so the shared cache cannot be mutated", () => {
    const tokens = parsePointer("/a/b~1c");
    expect(tokens).toEqual(["a", "b/c"]);
    expect(Object.isFrozen(tokens)).toBe(true);
    expect(Object.isFrozen(parsePointer(""))).toBe(true);
    expect(() => (tokens as string[]).push("x")).toThrow(TypeError);
    expect(parsePointer("/a/b~1c")).toEqual(["a", "b/c"]);
  });
});
