import { describe, expect, it } from "vitest";
import { WORKFLOW_EDITOR_VERSION } from "./index";

describe("workflow-editor placeholder", () => {
  it("exports version", () => {
    expect(WORKFLOW_EDITOR_VERSION).toBe("0.1.0");
  });
});
