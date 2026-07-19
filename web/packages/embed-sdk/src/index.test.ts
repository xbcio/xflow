import { describe, expect, it } from "vitest";
import { EMBED_SDK_VERSION } from "./index";

describe("embed-sdk placeholder", () => {
  it("exports version", () => {
    expect(EMBED_SDK_VERSION).toBe("0.1.0");
  });
});
