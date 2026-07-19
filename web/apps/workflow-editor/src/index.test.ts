import { describe, expect, it } from "vitest";
import { APP_WORKFLOW_EDITOR_VERSION } from "./index";

describe("app-workflow-editor placeholder", () => {
  it("exports version", () => {
    expect(APP_WORKFLOW_EDITOR_VERSION).toBe("0.1.0");
  });
});
