import * as React from "react";
import { Input } from "antd";
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
  actions?: React.ReactNode;
  bodyClassName?: string;
  children: React.ReactNode;
  className?: string;
  count?: React.ReactNode;
  icon: React.ReactNode;
  title: React.ReactNode;
}

/**
 * The 40px panel title bar of §6.0. The section is a landmark named by the
 * visible heading itself (`aria-labelledby`), so the region name and the title
 * can never drift apart; the icon is decorative and hidden from assistive tech.
 */
export function EditorPanel({
  actions,
  bodyClassName,
  children,
  className,
  count,
  icon,
  title
}: EditorPanelProps): React.ReactElement {
  const titleId = React.useId();
  return (
    <section aria-labelledby={titleId} className={`xflow-editor-panel ${className ?? ""}`}>
      <header className="xflow-editor-panel-title">
        <span aria-hidden="true" className="xflow-editor-panel-title__icon">
          {icon}
        </span>
        <h3 className="xflow-editor-panel-title__heading" id={titleId}>
          {title}
        </h3>
        {count === undefined ? null : <span className="xflow-editor-panel-title__count">{count}</span>}
        {actions ? <span className="xflow-editor-panel-title__actions">{actions}</span> : null}
      </header>
      <div className={`xflow-editor-panel__body ${bodyClassName ?? ""}`}>{children}</div>
    </section>
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
