// Shared building blocks of the built-in form components (Doc B §5, §9):
// the labelled field frame (label ↔ control association, required marker,
// hint/description/issues wired through aria-describedby), the layout
// context set by Form / ArrayTable, the read-only raw-value fallback for
// values whose shape does not match the control, and small helpers.
//
// Nothing here writes on mount: every onChange call is the direct result of
// a user event.

import { HelpIcon, WarnIcon } from "@xflow/icons";
import { Button, Tooltip } from "antd";
import { createContext, useContext, useEffect, useId, type CSSProperties, type ReactNode } from "react";
import { z } from "zod";
import { deepEqual, type Issue } from "../core";
import { useRevealElement, useValuePath } from "../react";

// ---------------------------------------------------------------- layout

/**
 * horizontal / vertical come from `Form.props.layout`; "cell" is set by
 * ArrayTable for its row content (label kept for assistive technology only,
 * the field frame becomes a table cell); "inline" hides the label without
 * the cell role (KeyValue value editors).
 */
export type FieldLayout = "horizontal" | "vertical" | "cell" | "inline";

export const FieldLayoutContext = createContext<FieldLayout>("vertical");

export function useFieldLayout(): FieldLayout {
  return useContext(FieldLayoutContext);
}

/**
 * Where a field's `description` shows, from `Form.props.descriptionPlacement`:
 * "below" the control, or "tooltip" behind a help icon next to the label.
 * Fields with a hidden label (cell / inline layouts) always use "below".
 */
export type DescriptionPlacement = "below" | "tooltip";

export const DescriptionPlacementContext = createContext<DescriptionPlacement>("below");

/**
 * Set by a Section (FieldGroup / ObjectGroup): every FieldFrame below it
 * reports its issues (null on unmount), its value path and its root element
 * (so a collapsed section can both show that it hides errors and order them
 * by DOM position when locating the first one). `key` identifies the frames.
 */
export type IssueReporter = (key: string, issues: readonly Issue[] | null, path?: string, element?: HTMLElement) => void;

export const SectionIssuesContext = createContext<IssueReporter | null>(null);

// ----------------------------------------------------------- prop schemas

/** Props every field component accepts. */
export const fieldShape = {
  label: z.string().optional(),
  description: z.string().optional(),
  /**
   * Shows the required marker. ResolvedNode does not expose check
   * definitions, so the Spec author (or compileNodeForm) sets it next to a
   * `required` check.
   */
  required: z.boolean().optional(),
  disabled: z.boolean().optional()
};

export interface FieldProps {
  label?: string;
  description?: string;
  required?: boolean;
  disabled?: boolean;
}

/** Option of Select / Radio / MultiSelect. */
export const optionSchema = z.object({
  value: z.union([z.string(), z.number(), z.boolean()]),
  label: z.string().optional(),
  /** Secondary text: a second line in a Select popup, the tooltip of a Radio. */
  description: z.string().optional(),
  disabled: z.boolean().optional()
});

export type OptionValue = string | number | boolean;
export type Option = z.infer<typeof optionSchema>;

// -------------------------------------------------------------- helpers

export function cx(...names: (string | false | null | undefined)[]): string {
  return names.filter(Boolean).join(" ");
}

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

export function isPrimitive(value: unknown): value is OptionValue {
  return typeof value === "string" || (typeof value === "number" && Number.isFinite(value)) || typeof value === "boolean";
}

/** Stable string key of an option value (AntD option values must be strings/numbers). */
export function optionKey(value: unknown): string {
  return JSON.stringify(value) ?? "undefined";
}

export function safeJson(value: unknown, space?: number): string {
  try {
    return JSON.stringify(value, null, space) ?? String(value);
  } catch {
    return String(value);
  }
}

/** How a default value is shown: strings as-is, booleans as 开/关, the rest as JSON. */
export function displayValue(value: unknown): string {
  if (typeof value === "string") return value;
  if (typeof value === "boolean") return value ? "开" : "关";
  if (typeof value === "number") return String(value);
  return safeJson(value);
}

/** "默认：X" for an unset value with a defaultHint (display only, Doc B §5). */
export function hintText(defaultHint: unknown, display: (value: unknown) => string = displayValue): string | undefined {
  return defaultHint === undefined ? undefined : `默认：${display(defaultHint)}`;
}

export function labelOfOption(options: readonly Option[], value: unknown): string {
  const hit = options.find((option) => deepEqual(option.value, value));
  return hit ? (hit.label ?? displayValue(hit.value)) : displayValue(value);
}

export function worstSeverity(issues: readonly Issue[]): "error" | "warning" | undefined {
  if (issues.some((issue) => issue.severity === "error")) return "error";
  if (issues.some((issue) => issue.severity === "warning")) return "warning";
  return undefined;
}

// ------------------------------------------------------------ Field frame

export interface FieldIds {
  /** id for the labelable control (label[for]). */
  controlId: string;
  labelId: string;
  /** Space-separated ids for aria-describedby, or undefined. */
  describedBy: string | undefined;
  invalid: boolean;
  status: "error" | "warning" | undefined;
}

export interface FieldFrameProps extends FieldProps {
  /** Component class, e.g. "xflow-composer-input". */
  className: string;
  issues: readonly Issue[];
  /** Local issues of the component (e.g. JSON parse error of a draft). */
  localIssues?: readonly Issue[];
  /** Text shown under the control (e.g. the default hint for non-text controls). */
  hint?: ReactNode;
  /**
   * "control": the label is a <label for> of a single labelable control.
   * "group": the label names a role=group wrapper (radio groups, lists,
   * raw-value fallback).
   * "inline-after": the field labels itself inside its own row (Switch): the
   * rendered label is handed to `children` to place after the control, still
   * a <label for> of `controlId`.
   */
  labelMode?: "control" | "group" | "inline-after";
  /**
   * Role of the group wrapper in "group" mode; default "group". null: the
   * control carries the role and the ids itself (e.g. AntD Radio.Group).
   */
  groupRole?: "group" | null;
  /** Extra state classes / attributes on the frame. */
  stateClassName?: string;
  style?: CSSProperties;
  nodeId: string;
  /** `label` is the rendered label (or null); "inline-after" fields place it. */
  children(ids: FieldIds, label: ReactNode): ReactNode;
}

export function FieldFrame(props: FieldFrameProps) {
  const layout = useFieldLayout();
  const base = useId();
  const controlId = `${base}control`;
  const labelId = `${base}label`;
  const hintId = `${base}hint`;
  const descriptionId = `${base}description`;
  const issuesId = `${base}issues`;
  // The concrete pointer of the field's bound value (P1-c S5): the element
  // answers to it both as `data-composer-path` and in the reveal registry.
  const path = useValuePath();
  const rootRef = useRevealElement<HTMLDivElement>(path);
  const issues = props.localIssues?.length ? [...props.localIssues, ...props.issues] : props.issues;
  const status = worstSeverity(issues);
  const report = useContext(SectionIssuesContext);
  // Report errors only (all a collapsed section shows), keyed by content so a
  // re-render that rebuilds the same issue list does not re-report.
  const errors = issues.filter((issue) => issue.severity === "error");
  const errorKey = errors.map((issue) => `${issue.path ?? ""}\u0000${issue.check ?? ""}\u0000${issue.message}`).join("\u0001");
  useEffect(() => {
    if (!report) return;
    report(base, errorKey === "" ? null : errors, path, rootRef.current ?? undefined);
    return () => report(base, null, path);
    // `errors` is intentionally captured through errorKey.
  }, [report, base, errorKey, path]);
  const describedBy =
    [props.hint ? hintId : "", props.description ? descriptionId : "", issues.length > 0 ? issuesId : ""]
      .filter(Boolean)
      .join(" ") || undefined;
  const ids: FieldIds = { controlId, labelId, describedBy, invalid: status === "error", status };
  const groupMode = props.labelMode === "group";
  const inlineAfter = props.labelMode === "inline-after";
  const hiddenLabel = layout === "cell" || layout === "inline";
  // Tooltip mode needs a visible label to sit next to; the description then
  // stays in the DOM (visually hidden) so aria-describedby still reaches it.
  const tooltipHelp = useContext(DescriptionPlacementContext) === "tooltip" && !hiddenLabel && Boolean(props.label);

  const labelContent = props.label ? (
    <>
      <span className="xflow-composer-field-label-text">{props.label}</span>
      {props.required && (
        <>
          <span className="xflow-composer-field-required" aria-hidden="true">
            *
          </span>
          <span className="xflow-composer-visually-hidden">（必填）</span>
        </>
      )}
    </>
  ) : null;
  const labelClass = cx("xflow-composer-field-label", hiddenLabel && "xflow-composer-visually-hidden");
  const label = labelContent ? (
    groupMode ? (
      <span id={labelId} className={labelClass}>
        {labelContent}
      </span>
    ) : (
      <label id={labelId} htmlFor={controlId} className={labelClass}>
        {labelContent}
      </label>
    )
  ) : null;
  // The help button sits beside (not inside) the <label>, so clicking it does
  // not activate the control.
  const labelRow =
    tooltipHelp && props.description ? (
      <div className="xflow-composer-field-label-row">
        {label}
        <Tooltip title={props.description} rootClassName="xflow-composer-field-help-tooltip">
          <button type="button" className="xflow-composer-field-help" aria-label={`${props.label}说明`}>
            <HelpIcon aria-hidden="true" />
          </button>
        </Tooltip>
      </div>
    ) : (
      label
    );

  const control = props.children(ids, labelRow);
  return (
    <div
      ref={rootRef}
      className={cx(
        "xflow-composer-field",
        props.className,
        `xflow-composer-field--${layout}`,
        inlineAfter && "xflow-composer-field--label-after",
        status && `is-${status}`,
        props.disabled && "is-disabled",
        props.stateClassName
      )}
      role={layout === "cell" ? "cell" : undefined}
      data-composer-node={props.nodeId}
      data-composer-path={path}
      style={props.style}
    >
      {inlineAfter ? null : labelRow}
      <div className="xflow-composer-field-body">
        {groupMode && props.groupRole !== null ? (
          <div
            role="group"
            className="xflow-composer-field-group-control"
            aria-labelledby={label ? labelId : undefined}
            aria-describedby={describedBy}
            aria-required={props.required || undefined}
            aria-invalid={ids.invalid || undefined}
          >
            {control}
          </div>
        ) : (
          control
        )}
        {props.hint ? (
          <div id={hintId} className="xflow-composer-field-hint">
            {props.hint}
          </div>
        ) : null}
        {props.description ? (
          <div
            id={descriptionId}
            className={cx("xflow-composer-field-description", tooltipHelp && "xflow-composer-visually-hidden")}
          >
            {props.description}
          </div>
        ) : null}
        <IssueList id={issuesId} issues={issues} />
      </div>
    </div>
  );
}

export function IssueList({ id, issues }: { id: string; issues: readonly Issue[] }) {
  return (
    <ul id={id} className="xflow-composer-field-issues" aria-live="polite">
      {issues.map((issue, index) => (
        <li
          key={`${index}:${issue.check ?? ""}:${issue.message}`}
          className={cx("xflow-composer-issue", `xflow-composer-issue--${issue.severity}`)}
        >
          <WarnIcon aria-hidden className="xflow-composer-issue-icon" />
          {issue.message}
          {issue.detail ? <span className="xflow-composer-issue-detail">{issue.detail}</span> : null}
        </li>
      ))}
    </ul>
  );
}

// -------------------------------------------------------- raw fallback

export interface RawValueProps {
  value: unknown;
  /** What the control expects, e.g. "字符串". */
  expected: string;
  readOnly: boolean;
  /** Clears the value (writes unset) on explicit user action. */
  onClear?: () => void;
}

/**
 * Read-only view of a value whose shape does not match the control. The
 * value is never coerced; the only write is the explicit "清除此值".
 */
export function RawValue({ value, expected, readOnly, onClear }: RawValueProps) {
  return (
    <div className="xflow-composer-raw" data-composer-raw="">
      <p className="xflow-composer-raw-notice" role="note">
        当前值与控件期望的类型（{expected}）不符，以只读方式显示原始值。
      </p>
      <pre className="xflow-composer-raw-value" tabIndex={0}>
        {safeJson(value, 2)}
      </pre>
      {!readOnly && onClear ? (
        <Button size="small" className="xflow-composer-raw-clear" onClick={onClear}>
          清除此值
        </Button>
      ) : null}
    </div>
  );
}

/** True when a defined value does not satisfy `accepts` (null counts as a shape mismatch). */
export function isWrongShape(value: unknown, accepts: (value: unknown) => boolean): boolean {
  return value !== undefined && !accepts(value);
}

/** "恢复默认" / "清除" button for controls without a native clear gesture. */
export function ResetButton({ hasDefault, onClick, label }: { hasDefault: boolean; onClick: () => void; label?: string }) {
  const text = hasDefault ? "恢复默认" : "清除";
  return (
    <Button
      size="small"
      type="link"
      className="xflow-composer-reset"
      aria-label={label ? `${label}：${text}` : text}
      onClick={onClick}
    >
      {text}
    </Button>
  );
}
