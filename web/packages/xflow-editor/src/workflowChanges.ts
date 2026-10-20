import type { WorkflowDef, WorkflowNode } from "@xflow/core";
import { listConnectionReferences, nodeKey } from "./panels/workflowOps";

/**
 * The structured definition diff §6.10 requires in the publish dialog, and the
 * one change array the top bar's 「未保存 N 处」 counts (same source, by
 * construction). One entry per added / removed / changed node, one for the
 * connection map, and one per differing top-level field; a changed node's
 * individual parameter and field edits are details inside its entry, so a node
 * that changed twice still counts once.
 */
export type WorkflowChangeKind = "add" | "mod" | "del";

export interface WorkflowChangeField {
  /** The field named in the dialog row (参数 <key>, 显示名, 类型, …). */
  field: string;
  /** The online value; absent when the field is new. */
  before?: string;
  /** The draft value; absent when the field was removed. */
  after?: string;
}

export interface WorkflowChange {
  id: string;
  kind: WorkflowChangeKind;
  /** Node key, 连线 for the connection map, or the top-level field name. */
  target: string;
  fields: WorkflowChangeField[];
}

/** What the diff needs from node-type schemas it does not hold itself. */
export interface WorkflowChangeOptions {
  /**
   * The card-summary text a node would draw (spec §6.1.1), for the 摘要 change
   * row. The caller owns the schemas: a diff alone cannot know which parameters
   * a card prints or what their display names are.
   */
  cardSummary?: (node: WorkflowNode) => string | undefined;
}

/** How a node's own fields are named in a diff row. Unlisted keys keep their wire name. */
const NODE_FIELD_LABELS: Record<string, string> = {
  type: "类型",
  kind: "种类",
  version: "版本",
  template: "模板",
  disabled: "禁用",
  on_error: "错误策略",
  runner_selector: "Runner 选择器",
  inputs: "输入端口",
  output_schema: "输出结构",
  output: "输出策略",
  retry: "重试",
  timeout: "超时",
  activation_replicas: "副本数",
  notes: "备注",
  position: "位置"
};

/** A value as one diff line. Long JSON is clipped: the dialog names the field, not the payload. */
function displayValue(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  const text = typeof value === "string" ? value : JSON.stringify(value);
  if (text === undefined) return undefined;
  return text.length > 80 ? `${text.slice(0, 79)}…` : text;
}

function nodeMap(workflow: WorkflowDef): Map<string, WorkflowNode> {
  const nodes = new Map<string, WorkflowNode>();
  (workflow.nodes ?? []).forEach((node, index) => nodes.set(nodeKey(node, index), node));
  return nodes;
}

function fieldsOf(
  before: WorkflowNode,
  after: WorkflowNode,
  options: WorkflowChangeOptions
): WorkflowChangeField[] {
  const fields: WorkflowChangeField[] = [];
  const beforeParams = before.parameters ?? {};
  const afterParams = after.parameters ?? {};
  const paramKeys = new Set([...Object.keys(beforeParams), ...Object.keys(afterParams)]);
  for (const key of paramKeys) {
    const beforeText = displayValue(beforeParams[key]);
    const afterText = displayValue(afterParams[key]);
    if (beforeText === afterText) continue;
    fields.push({ field: `参数 ${key}`, before: beforeText, after: afterText });
  }

  // The card summary is derived from those parameters but is its own surface: a
  // publish changes what the canvas shows even when the raw value diff is long.
  const beforeSummary = options.cardSummary?.(before);
  const afterSummary = options.cardSummary?.(after);
  if (beforeSummary !== afterSummary) {
    fields.push({ field: "摘要", before: beforeSummary, after: afterSummary });
  }

  const beforeLabel = displayValue(before.ui?.label);
  const afterLabel = displayValue(after.ui?.label);
  if (beforeLabel !== afterLabel) {
    fields.push({ field: "显示名", before: beforeLabel, after: afterLabel });
  }

  // Position lives in editor_metadata, not the wire definition, but a publish
  // stores it: report it like any other changed field instead of hiding it.
  const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
  for (const key of keys) {
    if (key === "parameters" || key === "ui") continue;
    const beforeText = displayValue((before as Record<string, unknown>)[key]);
    const afterText = displayValue((after as Record<string, unknown>)[key]);
    if (beforeText === afterText) continue;
    fields.push({ field: NODE_FIELD_LABELS[key] ?? key, before: beforeText, after: afterText });
  }
  return fields;
}

function connectionRefs(workflow: WorkflowDef): string[] {
  return listConnectionReferences(workflow).map(
    (reference) => `${reference.sourceName}.${reference.sourcePort} → ${reference.targetName}.${reference.targetPort}`
  );
}

/** One entry for the connection map, with each added / removed edge as a detail line. */
function connectionChange(baseline: WorkflowDef, current: WorkflowDef): WorkflowChange {
  const beforeRefs = connectionRefs(baseline);
  const afterRefs = connectionRefs(current);
  const beforeSet = new Set(beforeRefs);
  const afterSet = new Set(afterRefs);
  const fields: WorkflowChangeField[] = [
    ...afterRefs.filter((ref) => !beforeSet.has(ref)).map((ref) => ({ field: "新增", after: ref })),
    ...beforeRefs.filter((ref) => !afterSet.has(ref)).map((ref) => ({ field: "删除", before: ref }))
  ];
  // Same edges but a different wire shape (e.g. a connection type changed).
  if (fields.length === 0) fields.push({ field: "连接定义" });
  return { id: "connections", kind: "mod", target: "连线", fields };
}

export function workflowChanges(
  baseline: WorkflowDef,
  current: WorkflowDef,
  options: WorkflowChangeOptions = {}
): WorkflowChange[] {
  const before = nodeMap(baseline);
  const after = nodeMap(current);
  const changes: WorkflowChange[] = [];

  for (const [key, node] of after) {
    if (before.has(key)) continue;
    changes.push({ id: `add:${key}`, kind: "add", target: key, fields: [{ field: "节点", after: node.type ?? "节点" }] });
  }
  for (const [key, node] of before) {
    if (after.has(key)) continue;
    changes.push({ id: `del:${key}`, kind: "del", target: key, fields: [{ field: "节点", before: node.type ?? "节点" }] });
  }
  for (const [key, node] of after) {
    const previous = before.get(key);
    if (!previous || JSON.stringify(previous) === JSON.stringify(node)) continue;
    const fields = fieldsOf(previous, node, options);
    changes.push({
      id: `mod:${key}`,
      kind: "mod",
      target: key,
      fields: fields.length > 0 ? fields : [{ field: "定义" }]
    });
  }

  if (JSON.stringify(baseline.connections ?? {}) !== JSON.stringify(current.connections ?? {})) {
    changes.push(connectionChange(baseline, current));
  }

  const fields = new Set([...Object.keys(baseline), ...Object.keys(current)]);
  for (const field of fields) {
    if (field === "nodes" || field === "connections") continue;
    const beforeValue = (baseline as Record<string, unknown>)[field];
    const afterValue = (current as Record<string, unknown>)[field];
    if (JSON.stringify(beforeValue) === JSON.stringify(afterValue)) continue;
    changes.push({
      id: `field:${field}`,
      kind: "mod",
      target: field,
      fields: [{ field: "工作流字段", before: displayValue(beforeValue), after: displayValue(afterValue) }]
    });
  }

  return changes;
}
