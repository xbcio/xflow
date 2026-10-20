import type { NodeStatus } from "./index";

/**
 * Status tables (spec §4). Every chip, card border, run bar and table row in
 * preview, editor and admin reads these, so a state can only look one way.
 * Icons are string keys: core carries no React or AntD, each renderer maps the
 * key to its own glyph.
 */
export type Tone = "neutral" | "info" | "success" | "warning" | "danger" | "suspend";

export type StatusIconKey =
  | "clock"
  | "loading"
  | "hourglass"
  | "pause"
  | "check"
  | "pin"
  | "warning"
  | "close"
  | "skip"
  | "stop"
  | "archive"
  | "disabled"
  | "clock-close";

/** The 12 server `NodeStatus` values plus the editor-only `disabled`. */
export type NodeStatusKey = NodeStatus | "disabled";

export interface StatusMeta {
  label: string;
  tone: Tone;
  icon: StatusIconKey;
  card: string;
  edge: string;
}

// Annotated rather than `satisfies`: tsc rejects a missing or extra key at build
// time, so adding a `NodeStatus` upstream breaks the build until it is listed here.
export const nodeStatusMeta: Record<NodeStatusKey, StatusMeta> = {
  pending: { label: "排队", tone: "neutral", icon: "clock", card: "虚线边框", edge: "默认" },
  running: { label: "运行中", tone: "info", icon: "loading", card: "主色边框 + 脉冲", edge: "流动虚线" },
  committing: { label: "提交中", tone: "info", icon: "loading", card: "info 实线边框（无脉冲）", edge: "流动虚线" },
  waiting: { label: "等待中", tone: "warning", icon: "hourglass", card: "warning 边框", edge: "流动虚线（慢）" },
  suspended: { label: "已挂起", tone: "suspend", icon: "pause", card: "violet 边框 + 等待条", edge: "实线" },
  success: { label: "成功", tone: "success", icon: "check", card: "success 边框", edge: "success 实线" },
  pinned: { label: "固定数据", tone: "success", icon: "pin", card: "success 边框 + 图钉角标", edge: "success 实线" },
  continued: { label: "失败已继续", tone: "warning", icon: "warning", card: "warning 边框 + 错误摘要", edge: "实线" },
  failed: { label: "失败", tone: "danger", icon: "close", card: "danger 边框 + 错误摘要", edge: "默认" },
  skipped: { label: "已跳过", tone: "neutral", icon: "skip", card: "边框 / 底色 / 图标压暗 + 虚线，文字不透明", edge: "淡虚线" },
  canceled: { label: "已取消", tone: "neutral", icon: "stop", card: "neutral 边框", edge: "—" },
  expired: { label: "记录已过期", tone: "neutral", icon: "archive", card: "neutral 边框 + 说明", edge: "—" },
  disabled: { label: "已禁用", tone: "neutral", icon: "disabled", card: "斜纹 + 边框 / 底色 / 图标压暗（编辑态），文字不透明", edge: "淡虚线" },
};

/**
 * How an edge is stroked. The names come from §4's 入边 column, not from a second
 * table: {@link edgeVariant} maps the strings that column already holds, so a
 * status can only be drawn one way.
 *
 * 排队 (`pending`) draws the resting line (「默认」): a dashed route cannot be told
 * apart from 已跳过's 淡虚线, and one status may not have two strokes.
 *
 * 失败 (`failed`) draws the same resting line: its own border and chip are
 * already danger-toned and the inbound edge carries no state of its own — the
 * upstream node that produced the failure is what the reader follows. Only
 * 已取消 / 记录已过期 keep 「—」, where there is no successor to point at.
 */
export type EdgeVariant = "default" | "flow" | "flow-slow" | "skipped" | "success" | "error" | "none";

const variantByAppearance: Record<string, EdgeVariant> = {
  "默认": "default",
  "实线": "default",
  "流动虚线": "flow",
  "流动虚线（慢）": "flow-slow",
  "淡虚线": "skipped",
  "success 实线": "success",
  "—": "none",
};

export function edgeVariant(status: NodeStatusKey): EdgeVariant {
  return variantByAppearance[nodeStatusMeta[status].edge] ?? "default";
}

/**
 * The server `ExecutionStatus` values (`types/execution.go`,
 * `ExecutionStatus` in api/openapi/xflow-v1.yaml). Declared here because core
 * cannot import `@xflow/api`, which depends on it.
 */
export type ExecutionStatusKey =
  | "pending"
  | "queued"
  | "running"
  | "paused"
  | "canceling"
  | "success"
  | "failed"
  | "canceled"
  | "timeout";

export type ExecutionOp = "pause" | "resume" | "cancel" | "rerun";

/**
 * 操作 → 按钮文案（§6.11）。按钮由 `workflowStatusMeta[status].ops` 生成，文案只此
 * 一份：状态表加了操作，按钮自己出现，不需要在某处再补一次。
 */
export const executionOpLabel: Record<ExecutionOp, string> = {
  pause: "暂停",
  resume: "继续",
  cancel: "取消",
  rerun: "重跑…",
};

export interface WorkflowStatusMeta {
  label: string;
  tone: Tone;
  icon: StatusIconKey;
  /** Operations offered in this status, before scope filtering (§6.11). */
  ops: ExecutionOp[];
  terminal: boolean;
}

export const workflowStatusMeta: Record<ExecutionStatusKey, WorkflowStatusMeta> = {
  pending: { label: "等待开始", tone: "neutral", icon: "clock", ops: ["cancel"], terminal: false },
  queued: { label: "排队", tone: "neutral", icon: "hourglass", ops: ["cancel"], terminal: false },
  running: { label: "运行中", tone: "info", icon: "loading", ops: ["pause", "cancel"], terminal: false },
  paused: { label: "已暂停", tone: "warning", icon: "pause", ops: ["resume", "cancel"], terminal: false },
  canceling: { label: "取消中", tone: "neutral", icon: "loading", ops: [], terminal: false },
  success: { label: "成功", tone: "success", icon: "check", ops: ["rerun"], terminal: true },
  failed: { label: "失败", tone: "danger", icon: "close", ops: ["rerun"], terminal: true },
  canceled: { label: "已取消", tone: "neutral", icon: "stop", ops: ["rerun"], terminal: true },
  timeout: { label: "超时", tone: "danger", icon: "clock-close", ops: ["rerun"], terminal: true },
};
