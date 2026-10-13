import * as React from "react";
import { Card, Input } from "antd";
import type { WorkflowDef } from "@xflow/core";

/**
 * Pieces shared by the editor shell (index.tsx) and the panels under
 * src/panels/: the panel frame, the editor-scoped AntD className hooks and the
 * workflow-level JSON field contract. Living here keeps the panels free of an
 * import cycle back into index.tsx.
 */

export const editorInputClassNames = {
  input: "xflow-editor-field__control"
};

export const editorTextAreaClassNames = {
  textarea: "xflow-editor-textarea__control"
};

export const editorSelectClassNames = {
  content: "xflow-editor-select__content",
  popup: {
    list: "xflow-editor-select__popup-list",
    listItem: "xflow-editor-select__popup-item",
    root: "xflow-editor-select__popup"
  },
  root: "xflow-editor-select__root"
};

export const editorTagClassNames = {
  content: "xflow-editor-runtime-tag__content",
  root: "xflow-editor-runtime-tag__root"
};

export const editorTooltipClassNames = {
  container: "xflow-editor-tooltip__container",
  root: "xflow-editor-tooltip"
};

/** Every Inspector writer goes through one of these, run against the latest committed draft. */
export type WorkflowUpdater = (latest: WorkflowDef) => WorkflowDef;

export function serializeJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

/** Workflow-level fields the editor also edits as raw JSON objects. */
export type WorkflowJsonField = "runner_selector" | "params" | "context" | "settings" | "credentials" | "pin_data";

/** Field labels as save/run blocking messages name them, in display order. */
export const workflowJsonLabels: Record<WorkflowJsonField, string> = {
  runner_selector: "Runner",
  params: "输入参数",
  context: "变量配置",
  settings: "执行设置",
  credentials: "密钥",
  pin_data: "Pin Data"
};

/**
 * A workflow-level JSON draft, held by the editor so it survives the panel
 * unmounting and can block save/run. Parseable text commits immediately and is
 * kept (as typed) only while `value` still matches the committed field.
 */
export interface WorkflowJsonDraft {
  text: string;
  error?: string;
  value?: Record<string, unknown>;
}

export type WorkflowJsonDrafts = Partial<Record<WorkflowJsonField, WorkflowJsonDraft>>;

export function workflowJsonValue(workflow: WorkflowDef, field: WorkflowJsonField): unknown {
  switch (field) {
    case "runner_selector":
      return workflow.runner_selector ?? { mode: "default", match_labels: {} };
    case "context":
      return workflow.context ?? { vars: {}, config: {} };
    default:
      return workflow[field] ?? {};
  }
}

/**
 * Same rule as the parameters JSON: an unparseable draft is never overwritten,
 * and the user's own parseable text stays as typed (no reformat, no caret
 * jump) as long as it still means the committed value.
 */
export function workflowJsonView(
  workflow: WorkflowDef,
  drafts: WorkflowJsonDrafts | undefined,
  field: WorkflowJsonField
): { text: string; error?: string } {
  const committed = serializeJson(workflowJsonValue(workflow, field));
  const draft = drafts?.[field];
  if (draft && (draft.error || serializeJson(draft.value) === committed)) return { text: draft.text, error: draft.error };
  return { text: committed };
}

/** The raw JSON editor of one workflow-level field, wired to the editor's draft/parse/commit path. */
export function WorkflowJsonTextArea({
  ariaLabel,
  field,
  workflow,
  drafts,
  readOnly = false,
  maxRows = 8,
  onText
}: {
  ariaLabel: string;
  field: WorkflowJsonField;
  workflow: WorkflowDef;
  drafts?: WorkflowJsonDrafts;
  readOnly?: boolean;
  maxRows?: number;
  onText?: (field: WorkflowJsonField, text: string) => void;
}): React.ReactElement {
  const view = workflowJsonView(workflow, drafts, field);
  return (
    <div className="xflow-editor-json-field">
      <Input.TextArea
        aria-label={ariaLabel}
        autoSize={{ minRows: 2, maxRows }}
        className="xflow-editor-textarea xflow-editor-textarea--code"
        classNames={editorTextAreaClassNames}
        readOnly={readOnly}
        status={view.error ? "error" : undefined}
        value={view.text}
        onChange={(event) => onText?.(field, event.target.value)}
      />
      {view.error ? <span>{view.error}</span> : null}
    </div>
  );
}

export interface EditorPanelProps {
  ariaLabel: string;
  bodyClassName?: string;
  children: React.ReactNode;
  className?: string;
  extra?: React.ReactNode;
  icon: React.ReactNode;
  iconAriaLabel?: string;
  subtitle?: React.ReactNode;
  title: React.ReactNode;
}

export function EditorPanel({
  ariaLabel,
  bodyClassName,
  children,
  className,
  extra,
  icon,
  iconAriaLabel,
  subtitle,
  title
}: EditorPanelProps): React.ReactElement {
  return (
    <Card
      aria-label={ariaLabel}
      className={`xflow-editor-panel-card ${className ?? ""}`}
      rootClassName="xflow-editor-panel-card__root"
      classNames={{
        body: `xflow-editor-panel-card__body ${bodyClassName ?? ""}`,
        extra: "xflow-editor-panel-card__extra",
        header: "xflow-editor-panel-card__header",
        title: "xflow-editor-panel-card__title"
      }}
      extra={extra}
      role="region"
      size="small"
      title={
        <div className="xflow-editor-panel-title">
          <span className="xflow-editor-panel-title__icon" aria-label={iconAriaLabel}>
            {icon}
          </span>
          <div className="xflow-editor-panel-title__text">
            <strong>{title}</strong>
            {subtitle ? <span>{subtitle}</span> : null}
          </div>
        </div>
      }
    >
      {children}
    </Card>
  );
}

/**
 * A titled card inside a panel body; `actions` sit at the right of its header.
 * A group rather than a region, so a card titled like its panel ("变量") does
 * not shadow the panel's landmark.
 */
export function SectionCard({
  ariaLabel,
  title,
  icon,
  meta,
  actions,
  children
}: {
  ariaLabel: string;
  title: React.ReactNode;
  icon?: React.ReactNode;
  meta?: React.ReactNode;
  actions?: React.ReactNode;
  children: React.ReactNode;
}): React.ReactElement {
  return (
    <div aria-label={ariaLabel} className="xflow-editor-section-card" role="group">
      <header className="xflow-editor-section-card__header">
        <span className="xflow-editor-section-card__title">
          {icon}
          <strong>{title}</strong>
          {meta !== undefined ? <em>{meta}</em> : null}
        </span>
        {actions ? <span className="xflow-editor-section-card__actions">{actions}</span> : null}
      </header>
      <div className="xflow-editor-section-card__body">{children}</div>
    </div>
  );
}
