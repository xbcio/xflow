import { workflowStatusMeta, type NodeStatus, type RuntimeSnapshot, type WorkflowStatus } from "@xflow/core";

/**
 * Top-bar wording for the snapshot a run handler returned. A run request that
 * resolved is not a run that finished: the host may hand back a `running`
 * snapshot, so the label is read from the snapshot, never assumed.
 *
 * Wording that core's `workflowStatusMeta` already has is read from it; the
 * three terminal states keep the verb form the top bar has always used
 * ("运行完成" rather than the table's bare "成功").
 */
const labelByWorkflowStatus: Record<WorkflowStatus, string> = {
  pending: workflowStatusMeta.pending.label,
  running: workflowStatusMeta.running.label,
  success: "运行完成",
  failed: "运行失败",
  canceled: workflowStatusMeta.canceled.label,
  timeout: "运行超时"
};

/** The attempt is still in flight (or about to be): the run is not over. */
const activeNodeStatuses: ReadonlySet<NodeStatus> = new Set(["running", "committing", "waiting", "suspended"]);

/** The node will not change again and did not fail. */
const settledNodeStatuses: ReadonlySet<NodeStatus> = new Set(["success", "skipped", "pinned", "expired"]);

/**
 * The workflow-level status a snapshot reads as: its own `status` when the host
 * set one, otherwise derived from the node statuses. `undefined` when the
 * snapshot carries neither (nothing to report yet).
 */
export function workflowStatusFromRuntime(runtime: RuntimeSnapshot): WorkflowStatus | undefined {
  if (runtime.status !== undefined) return runtime.status;

  const statuses = Object.values(runtime.nodes ?? {}).map((node) => node.status);
  if (statuses.length === 0) return undefined;

  if (statuses.some((status) => activeNodeStatuses.has(status))) return "running";
  // `continued` is a failure the node's error policy let the run walk past; it
  // is not a clean finish, so it never reads as "运行完成".
  if (statuses.some((status) => status === "failed" || status === "continued")) return "failed";
  if (statuses.some((status) => status === "canceled")) return "canceled";
  if (statuses.every((status) => status === "pending")) return "pending";
  if (statuses.every((status) => settledNodeStatuses.has(status))) return "success";
  // Some nodes settled, the rest still pending: between nodes, not finished.
  return "running";
}

export function operationStatusFromRuntime(runtime: RuntimeSnapshot): string {
  const status = workflowStatusFromRuntime(runtime);
  return status === undefined ? "状态未知" : labelByWorkflowStatus[status];
}
