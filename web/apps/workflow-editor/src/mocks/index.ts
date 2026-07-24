import type { WorkflowDef } from "./fixtures";

export type MockWorkflow = WorkflowDef;

export async function loadMockWorkflow(): Promise<MockWorkflow> {
  const m = await import("./fixtures");
  return m.workflowFixture;
}
