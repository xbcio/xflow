import { describe, expect, it } from "vitest";
import { resolveAppearance } from "./appearance";
import { breakpoints } from "./breakpoints";

describe("resolveAppearance", () => {
  it("follows the system scheme for `system`", () => {
    expect(resolveAppearance("system", true)).toBe("dark");
    expect(resolveAppearance("system", false)).toBe("light");
  });

  it("ignores the system scheme for an explicit preference", () => {
    expect(resolveAppearance("light", true)).toBe("light");
    expect(resolveAppearance("light", false)).toBe("light");
    expect(resolveAppearance("dark", true)).toBe("dark");
    expect(resolveAppearance("dark", false)).toBe("dark");
  });
});

describe("breakpoints", () => {
  it("is the single source of the two layout thresholds", () => {
    expect(breakpoints).toEqual({ compact: 1280, narrow: 1024 });
  });
});
