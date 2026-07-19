import { describe, expect, it } from "vitest";
import { WORKFLOW_VIEWER_VERSION } from "./index";

describe("workflow-viewer placeholder", () => {
  it("exports version", () => {
    expect(WORKFLOW_VIEWER_VERSION).toBe("0.1.0");
  });
});
