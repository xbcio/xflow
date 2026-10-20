import type { WorkflowDef } from "@xflow/core";
import { workflowChanges } from "./workflowChanges";
import { versionLabel } from "./versioning";

export { workflowChanges } from "./workflowChanges";
export type { WorkflowChange, WorkflowChangeField, WorkflowChangeKind } from "./workflowChanges";

/**
 * How many places differ from the last loaded / successfully saved definition
 * (§6.3): one per added, removed or changed node, one for the connection map,
 * and one for every remaining top-level field that differs. Edits that cancel
 * out (undo back to the baseline) count zero again, which is why the top bar
 * derives its chip from this count rather than from a monotonic dirty flag.
 *
 * It is the length of the publish dialog's change array, not a parallel count:
 * §6.10 requires both surfaces to describe the same changes.
 */
export function countUnsavedChanges(baseline: WorkflowDef, current: WorkflowDef): number {
  return workflowChanges(baseline, current).length;
}

export interface VersionChipState {
  tone: "warning" | "success" | "neutral";
  label: string;
}

/**
 * The one version chip in the top bar (§6.3 / §6.10). There is no server-side
 * draft: a definition with no id has never been persisted, so it reads as
 * unsaved even when it already matches its baseline. `historicalVersion` is
 * the third state — an archived definition the host loaded read-only.
 */
export function versionChipState({
  persisted,
  version,
  changes,
  historicalVersion
}: {
  persisted: boolean;
  version?: string;
  changes: number;
  historicalVersion?: string;
}): VersionChipState {
  if (historicalVersion) {
    return { tone: "neutral", label: `${versionLabel(historicalVersion)} · 历史` };
  }
  if (persisted && changes === 0) {
    return { tone: "success", label: version ? `线上 ${versionLabel(version)}` : "已保存" };
  }
  if (changes === 0) return { tone: "warning", label: "未保存" };
  const base = version ? ` · 基于 ${versionLabel(version)}` : "";
  return { tone: "warning", label: `未保存 ${changes} 处${base}` };
}
