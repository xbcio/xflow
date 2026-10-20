import type { WaitExecutionResult } from '@xflow/api';
import type { ParamIssue, RuntimeSnapshot, WorkflowDef } from '@xflow/core';

/**
 * A definition the host has persisted: the save callback must hand back the
 * server id. A create receives the id the server issued; an update keeps the
 * route id. Encoding that in the type is what lets Run go straight from save to
 * execute without a runtime "not saved yet" check that can no longer happen.
 */
export type SavedWorkflowDef = WorkflowDef & { id: string };

export interface SavedWorkflowResult {
  workflow: SavedWorkflowDef;
  paramIssues?: ParamIssue[];
}

export interface SaveThenRunDeps {
  /** The host's save callback (creates or updates, and moves the route on create). */
  save: (workflow: WorkflowDef) => Promise<SavedWorkflowResult>;
  /** POST /v1/workflows/{id}/execute. */
  run: (workflowId: string) => Promise<{ executionId: string }>;
  /** GET /v1/executions/{id}/wait; a 202 resolves to the timeout shape. */
  wait: (executionId: string) => Promise<WaitExecutionResult>;
  /**
   * Maps the wait result to the editor snapshot. The host owns the split
   * between a timed-out status projection and a full detail projection.
   */
  toSnapshot: (result: WaitExecutionResult) => RuntimeSnapshot;
}

/**
 * Save-then-run: a Run must execute the definition the canvas currently shows.
 *
 * The editor's draft may be newer than the stored workflow, and a brand-new
 * draft has no id at all; executing by id always runs the server copy. Saving
 * first costs one idempotent PUT per Run and buys the guarantee that what runs
 * is what is on screen. Save failures (network, validation, or an enforce-mode
 * 400 carrying param_issues) propagate before any execution starts; warn-mode
 * param_issues on the saved result do not block the run. Run guarantees only
 * that they are non-blocking — placing them on fields belongs to the Save path.
 */
export async function saveThenRun(
  draft: WorkflowDef,
  { save, run, wait, toSnapshot }: SaveThenRunDeps,
): Promise<RuntimeSnapshot> {
  const saved = await save(draft);
  const execution = await run(saved.workflow.id);
  return toSnapshot(await wait(execution.executionId));
}
