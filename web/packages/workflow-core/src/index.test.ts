import { describe, expect, it } from "vitest";
import { WORKFLOW_CORE_VERSION } from "./index";

describe("workflow-core placeholder", () => {
  it("exports version", () => {
    expect(WORKFLOW_CORE_VERSION).toBe("0.1.0");
  });
});
