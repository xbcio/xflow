import { describe, expect, it } from "vitest";
import { WORKFLOW_RENDERER_VERSION } from "./index";

describe("workflow-renderer placeholder", () => {
  it("exports version", () => {
    expect(WORKFLOW_RENDERER_VERSION).toBe("0.1.0");
  });
});
