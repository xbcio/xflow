import type { WorkflowDef } from "@xflow/workflow-core";

export async function saveWorkflow(definition: WorkflowDef): Promise<void> {
  console.log("[editor] saveWorkflow", definition.name);
}

export async function validateWorkflow(definition: WorkflowDef): Promise<void> {
  console.log("[editor] validateWorkflow", definition.name);
}

export async function publishWorkflow(definition: WorkflowDef): Promise<void> {
  console.log("[editor] publishWorkflow", definition.name);
}

export async function runWorkflow(definition: WorkflowDef): Promise<void> {
  console.log("[editor] runWorkflow", definition.name);
}

export async function undo(): Promise<void> {
  console.log("[editor] undo");
}

export async function redo(): Promise<void> {
  console.log("[editor] redo");
}
