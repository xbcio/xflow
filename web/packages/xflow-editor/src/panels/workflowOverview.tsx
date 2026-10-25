import { CheckIcon, WarnIcon } from "@xflow/icons";
import { workflowStatusMeta, type ExecutionStatusKey, type WorkflowDef, type WorkflowNode } from "@xflow/core";
import * as React from "react";

import { versionLabel } from "../versioning";
import { countWorkflowEdges, type WorkflowDiagnostic } from "./diagnostics";
import { executionClock, executionDuration } from "./executionList";
import { statusGlyph } from "./statusGlyph";
import { nodeLabel } from "./workflowOps";

/** The latest run slice the overview reads; the host supplies it, never the editor. */
export interface WorkflowOverviewRun {
  executionId: string;
  status: ExecutionStatusKey;
  createdAt?: string;
  updatedAt?: string;
}

/**
 * What the Inspector shows above the workflow settings when no node is
 * selected. It only states facts the editor already holds: counts from the
 * draft, triggers from its nodes, validation from the diagnostics it computed
 * for the dock, the version/save state from the top bar's own baseline, and —
 * when the host has a run — that run. Anything the host did not send stays
 * absent rather than becoming a placeholder.
 */
export interface WorkflowOverviewProps {
  workflow: WorkflowDef;
  sectionCount: number;
  diagnostics: readonly WorkflowDiagnostic[];
  /** The baseline's version label, without the leading `v`. */
  version?: string;
  /** The definition has a server id, so 已保存 is a real state. */
  persisted: boolean;
  unsavedCount: number;
  latestRun?: WorkflowOverviewRun;
  /** Opens the diagnostics dock on the validation counts. */
  onOpenDiagnostics?: () => void;
  /** Enters the replay for that execution. Omit and the row is not clickable. */
  onOpenRun?: (executionId: string) => void;
}

/** The overview's host-supplied slice; the workflow itself comes from the Inspector. */
export type WorkflowOverviewData = Omit<WorkflowOverviewProps, "workflow">;

function isTrigger(node: WorkflowNode): boolean {
  return (
    node.type === "xflow.start" ||
    Boolean(node.type?.startsWith("xflow.trigger.")) ||
    node.kind === "trigger"
  );
}

export function WorkflowOverview({
  workflow,
  sectionCount,
  diagnostics,
  version,
  persisted,
  unsavedCount,
  latestRun,
  onOpenDiagnostics,
  onOpenRun
}: WorkflowOverviewProps): React.ReactElement {
  const nodes = workflow.nodes ?? [];
  const triggers = nodes
    .map((node, index) => (isTrigger(node) ? nodeLabel(node, index) : undefined))
    .filter((label): label is string => Boolean(label));
  const errors = diagnostics.filter((item) => item.status === "error" && item.area !== "operation").length;
  const warnings = diagnostics.filter((item) => item.status === "warn").length;
  const validationText = errors === 0 && warnings === 0 ? "无问题" : `${errors} 错误 · ${warnings} 警告`;
  const runMeta = latestRun ? workflowStatusMeta[latestRun.status] : undefined;
  const runDuration = latestRun ? executionDuration(latestRun) : undefined;
  const runBody = latestRun && runMeta ? (
    <>
      <span className="xflow-editor-chip" data-status={latestRun.status} data-tone={runMeta.tone}>
        <span aria-hidden="true" className="xflow-editor-chip__icon">
          {statusGlyph[runMeta.icon]}
        </span>
        <span>{runMeta.label}</span>
      </span>
      <span className="xflow-editor-overview__run-time">
        {latestRun.createdAt ? executionClock(latestRun.createdAt) : "—"}
        {runDuration ? ` · ${runDuration}` : ""}
      </span>
      <span aria-hidden="true" className="xflow-editor-overview__run-open">
        {onOpenRun ? "回放" : null}
      </span>
    </>
  ) : null;

  return (
    <section aria-label="工作流概览" className="xflow-editor-overview">
      <div className="xflow-editor-overview__stats">
        <div className="xflow-editor-overview__stat">
          <span className="xflow-editor-overview__stat-value">{nodes.length}</span>
          <span className="xflow-editor-overview__stat-label">节点</span>
        </div>
        <div className="xflow-editor-overview__stat">
          <span className="xflow-editor-overview__stat-value">{countWorkflowEdges(workflow)}</span>
          <span className="xflow-editor-overview__stat-label">连线</span>
        </div>
        <div className="xflow-editor-overview__stat">
          <span className="xflow-editor-overview__stat-value">{sectionCount}</span>
          <span className="xflow-editor-overview__stat-label">分区</span>
        </div>
      </div>
      <div className="xflow-editor-overview__row">
        <span className="xflow-editor-overview__label">触发器</span>
        <span
          className="xflow-editor-overview__value"
          data-empty={triggers.length === 0 ? "true" : undefined}
          title={triggers.length > 0 ? triggers.join("、") : undefined}
        >
          {triggers.length > 0 ? triggers.join("、") : "未配置触发器"}
        </span>
      </div>
      <div className="xflow-editor-overview__row">
        <span className="xflow-editor-overview__label">校验</span>
        {onOpenDiagnostics ? (
          <button
            className="xflow-editor-overview__value xflow-editor-overview__link"
            data-tone={errors > 0 ? "danger" : warnings > 0 ? "warning" : "success"}
            title="打开诊断台"
            type="button"
            onClick={onOpenDiagnostics}
          >
            {errors > 0 ? <WarnIcon /> : <CheckIcon />}
            <span>{validationText}</span>
          </button>
        ) : (
          <span
            className="xflow-editor-overview__value"
            data-tone={errors > 0 ? "danger" : warnings > 0 ? "warning" : "success"}
          >
            {validationText}
          </span>
        )}
      </div>
      <div className="xflow-editor-overview__row">
        <span className="xflow-editor-overview__label">版本</span>
        <span className="xflow-editor-overview__value">
          {version ? versionLabel(version) : "未发布"}
          <span className="xflow-editor-overview__save" data-dirty={unsavedCount > 0 ? "true" : undefined}>
            {unsavedCount > 0 ? `${unsavedCount} 处未保存` : persisted ? "已保存" : "未保存"}
          </span>
        </span>
      </div>
      {latestRun ? (
        <div className="xflow-editor-overview__row">
          <span className="xflow-editor-overview__label">最近运行</span>
          {onOpenRun ? (
            <button
              className="xflow-editor-overview__value xflow-editor-overview__run"
              title={`在回放中打开 ${latestRun.executionId}`}
              type="button"
              onClick={() => onOpenRun(latestRun.executionId)}
            >
              {runBody}
            </button>
          ) : (
            <span className="xflow-editor-overview__value xflow-editor-overview__run">{runBody}</span>
          )}
        </div>
      ) : null}
    </section>
  );
}
