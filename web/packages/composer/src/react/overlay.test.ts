import { describe, expect, it } from "vitest";
import { reflects } from "./overlay";

describe("reflects", () => {
  it("treats a set as reflected when the value at its path matches", () => {
    expect(reflects({ name: "ab" }, { op: "set", path: "/name", value: "ab" })).toBe(true);
    expect(reflects({ name: "a" }, { op: "set", path: "/name", value: "ab" })).toBe(false);
  });

  it("treats an unset as reflected when the path is absent", () => {
    expect(reflects({}, { op: "unset", path: "/name" })).toBe(true);
    expect(reflects({ name: "a" }, { op: "unset", path: "/name" })).toBe(false);
  });

  it("treats an append as reflected once the array ends with the appended item", () => {
    const append = { op: "set", path: "/list/-", value: { n: 1 } } as const;
    expect(reflects({ list: [{ n: 0 }, { n: 1 }] }, append)).toBe(true);
    expect(reflects({ list: [{ n: 0 }] }, append)).toBe(false);
    expect(reflects({ list: [] }, append)).toBe(false);
    expect(reflects({}, append)).toBe(false);
  });
});
