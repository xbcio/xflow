import { describe, expect, it } from "vitest";
import { APP_WORKFLOW_VIEWER_VERSION } from "./index";

describe("app-workflow-viewer placeholder", () => {
  it("exports version", () => {
    expect(APP_WORKFLOW_VIEWER_VERSION).toBe("0.1.0");
  });
});
