import { describe, expect, it } from "vitest";
import { NODE_REGISTRY_VERSION } from "./index";

describe("node-registry placeholder", () => {
  it("exports version", () => {
    expect(NODE_REGISTRY_VERSION).toBe("0.1.0");
  });
});
