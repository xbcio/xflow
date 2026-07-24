import { describe, expect, it } from "vitest";
import { WORKFLOW_PROVIDER_VERSION } from "./index";

describe("workflow-provider placeholder", () => {
  it("exports version", () => {
    expect(WORKFLOW_PROVIDER_VERSION).toBe("0.1.0");
  });
});
