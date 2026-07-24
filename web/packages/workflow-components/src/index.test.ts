import { describe, expect, it } from "vitest";
import { WORKFLOW_COMPONENTS_VERSION } from "./index";

describe("workflow-components placeholder", () => {
  it("exports version", () => {
    expect(WORKFLOW_COMPONENTS_VERSION).toBe("0.1.0");
  });
});
