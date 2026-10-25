// Structural components: Form, FieldGroup, ObjectGroup.

import { DownIcon, WarnIcon } from "@xflow/icons";
import { Button } from "antd";
import { useCallback, useContext, useId, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { z } from "zod";
import { zodProps, type Issue } from "../core";
import { useReveal, useRevealElement, useRevealExpansion, useValuePath, type ComposerComponent } from "../react";
import {
  cx,
  DescriptionPlacementContext,
  FieldLayoutContext,
  IssueList,
  isPlainObject,
  isWrongShape,
  RawValue,
  SectionIssuesContext,
  useFieldLayout,
  worstSeverity,
  type DescriptionPlacement,
  type IssueReporter
} from "./shared";

// ------------------------------------------------------------------- Form

export interface FormProps {
  layout?: "horizontal" | "vertical";
  /** Label column width (px) for the horizontal layout. */
  labelWidth?: number;
  /**
   * Where field descriptions show: "below" the control (default) or
   * "tooltip" behind a help icon next to the label.
   */
  descriptionPlacement?: DescriptionPlacement;
  /** Accessible name of the form region. */
  title?: string;
}

export const Form: ComposerComponent<FormProps> = {
  type: "Form",
  props: zodProps(
    z.strictObject({
      layout: z.enum(["horizontal", "vertical"]).optional(),
      labelWidth: z.number().positive().optional(),
      descriptionPlacement: z.enum(["below", "tooltip"]).optional(),
      title: z.string().optional()
    })
  ),
  bindings: {},
  render: ({ node, props, children, slots }) => {
    const layout = props.layout ?? "vertical";
    const style =
      props.labelWidth !== undefined ? ({ "--xflow-composer-label-width": `${props.labelWidth}px` } as CSSProperties) : undefined;
    return (
      <div
        className={cx("xflow-composer-form", `xflow-composer-form--${layout}`)}
        data-composer-node={node.id}
        role={props.title ? "group" : undefined}
        aria-label={props.title}
        style={style}
      >
        <DescriptionPlacementContext.Provider value={props.descriptionPlacement ?? "below"}>
          <FieldLayoutContext.Provider value={layout}>
            {slots.header}
            {children}
            {slots.footer}
          </FieldLayoutContext.Provider>
        </DescriptionPlacementContext.Provider>
      </div>
    );
  }
};

// ------------------------------------------------------------- sections

export interface SectionProps {
  className: string;
  nodeId: string;
  title?: string;
  description?: string;
  /** Short right-aligned status in the header (e.g. "继承工作流 · 重试 2 次"). */
  summary?: string;
  collapsible?: boolean;
  defaultCollapsed?: boolean;
  issues: readonly Issue[];
  actions?: ReactNode;
  children: ReactNode;
}

/** What one Frame reported: its errors, plus the pointer they belong to. */
interface FrameReport {
  issues: readonly Issue[];
  path?: string;
  /** The frame's root element, so the badge can order frames by DOM position. */
  element?: HTMLElement;
}

/** True when `element` sits before `anchor` in the document. */
function precedesInDom(element: HTMLElement | undefined, anchor: HTMLElement | undefined): boolean {
  if (!element?.isConnected || !anchor?.isConnected) return false;
  return (anchor.compareDocumentPosition(element) & Node.DOCUMENT_POSITION_PRECEDING) !== 0;
}

/**
 * Collects the issues FieldFrames below a section report. Returns the count
 * of distinct errors among them, and a lookup of the first error's field path
 * (the frame's own value path, else the issue's), for the badge's reveal.
 * Reports are forwarded to an enclosing section, so an outer group also
 * counts, and can reveal, its nested groups.
 */
function useHiddenErrors(): [IssueReporter, number, () => string | undefined] {
  const parent = useContext(SectionIssuesContext);
  const byFrame = useRef(new Map<string, FrameReport>());
  const [count, setCount] = useState(0);
  const report = useCallback<IssueReporter>(
    (key, issues, path, element) => {
      if (issues === null || issues.length === 0) byFrame.current.delete(key);
      else byFrame.current.set(key, { issues, path, element });
      // An issue reaches every element bound to its path (e.g. ExpressionInput
      // and its literal control), so count each path/check/message once.
      const errors = new Set<string>();
      for (const [frame, entry] of byFrame.current) {
        for (const issue of entry.issues) {
          if (issue.severity !== "error") continue;
          errors.add(`${issue.path ?? frame}\u0000${issue.check ?? ""}\u0000${issue.message}`);
        }
      }
      setCount(errors.size);
      parent?.(key, issues, path, element);
    },
    [parent]
  );
  // The first error the user would see after expanding: per frame its first
  // error with a pointer, across frames the one earliest in the DOM (a host
  // row reorder keeps elements mounted, so report order can go stale).
  const firstErrorPath = useCallback(() => {
    let best: FrameReport | undefined;
    let bestPath: string | undefined;
    for (const entry of byFrame.current.values()) {
      for (const issue of entry.issues) {
        if (issue.severity !== "error") continue;
        const hit = issue.path ?? entry.path;
        if (!hit) continue;
        if (bestPath === undefined || precedesInDom(entry.element, best?.element)) {
          best = entry;
          bestPath = hit;
        }
        break;
      }
    }
    return bestPath;
  }, []);
  return [report, count, firstErrorPath];
}

/** Titled, optionally collapsible section. Collapsed content stays mounted (drafts survive). */
export function Section(props: SectionProps) {
  const base = useId();
  const [collapsed, setCollapsed] = useState(props.defaultCollapsed === true);
  const [report, hiddenErrors, firstErrorPath] = useHiddenErrors();
  const reveal = useReveal();
  const path = useValuePath();
  const sectionRef = useRevealElement<HTMLElement>(path);
  const expand = useCallback(() => setCollapsed(false), []);
  // A reveal request expands every collapsed section on the target's path.
  useRevealExpansion(sectionRef, expand);
  const titleId = `${base}title`;
  const bodyId = `${base}body`;
  const issuesId = `${base}issues`;
  const badgeId = `${base}badge`;
  const status = worstSeverity(props.issues);
  const open = !props.collapsible || !collapsed;
  const badge = !open && hiddenErrors > 0;
  return (
    <section
      ref={sectionRef}
      className={cx("xflow-composer-section", props.className, status && `is-${status}`, !open && "is-collapsed", badge && "has-hidden-errors")}
      data-composer-node={props.nodeId}
      data-composer-path={path}
      aria-labelledby={props.title ? titleId : undefined}
      aria-describedby={props.issues.length > 0 ? issuesId : undefined}
    >
      {props.title || props.actions ? (
        <header className="xflow-composer-section-header">
          {props.collapsible ? (
            <button
              type="button"
              className="xflow-composer-section-toggle"
              aria-expanded={open}
              aria-controls={bodyId}
              aria-describedby={badge ? badgeId : undefined}
              onClick={() => setCollapsed((value) => !value)}
            >
              {open ? <DownIcon aria-hidden /> : <DownIcon aria-hidden style={{ transform: "rotate(-90deg)" }} />}
              <span id={titleId} className="xflow-composer-section-title">
                {props.title}
              </span>
            </button>
          ) : (
            <h3 id={titleId} className="xflow-composer-section-title">
              {props.title}
            </h3>
          )}
          {props.summary ? <span className="xflow-composer-section-summary">{props.summary}</span> : null}
          {badge ? (
            <button
              type="button"
              id={badgeId}
              className="xflow-composer-section-badge"
              aria-label="展开并定位第一个错误"
              onClick={() => {
                const target = firstErrorPath();
                if (target) reveal?.reveal(target);
                setCollapsed(false);
              }}
            >
              <WarnIcon aria-hidden className="xflow-composer-section-badge-icon" />
              {hiddenErrors} 个错误
            </button>
          ) : null}
          {props.actions ? <div className="xflow-composer-section-actions">{props.actions}</div> : null}
        </header>
      ) : null}
      {props.description ? <p className="xflow-composer-section-description">{props.description}</p> : null}
      <IssueList id={issuesId} issues={props.issues} />
      <div id={bodyId} className="xflow-composer-section-body" hidden={!open}>
        <SectionIssuesContext.Provider value={report}>{props.children}</SectionIssuesContext.Provider>
      </div>
    </section>
  );
}

const sectionShape = {
  title: z.string().optional(),
  description: z.string().optional(),
  summary: z.string().optional(),
  collapsible: z.boolean().optional(),
  defaultCollapsed: z.boolean().optional()
};

export interface FieldGroupProps {
  title?: string;
  description?: string;
  /** Short right-aligned status in the section header. */
  summary?: string;
  collapsible?: boolean;
  defaultCollapsed?: boolean;
}

export const FieldGroup: ComposerComponent<FieldGroupProps> = {
  type: "FieldGroup",
  props: zodProps(z.strictObject(sectionShape)),
  bindings: {},
  render: ({ node, props, children, issues }) => (
    <Section className="xflow-composer-field-group" nodeId={node.id} {...props} issues={issues}>
      {children}
    </Section>
  )
};

// ------------------------------------------------------------ ObjectGroup

export interface ObjectGroupProps extends FieldGroupProps {
  /** Alias of title, for symmetry with fields. */
  label?: string;
  /** Shows a "清空" action that unsets the whole object. */
  clearable?: boolean;
}

function ObjectGroupView({
  nodeId,
  props,
  value,
  onChange,
  issues,
  readOnly,
  children
}: {
  nodeId: string;
  props: ObjectGroupProps;
  value: unknown;
  onChange?: (next: unknown) => void;
  issues: readonly Issue[];
  readOnly: boolean;
  children: ReactNode;
}) {
  const layout = useFieldLayout();
  const wrong = isWrongShape(value, isPlainObject);
  const title = props.title ?? props.label;
  const canClear = props.clearable && !readOnly && onChange !== undefined && isPlainObject(value) && Object.keys(value).length > 0;
  const section = (
    <Section
      className={cx("xflow-composer-object-group", layout === "cell" && "xflow-composer-object-group--cell")}
      nodeId={nodeId}
      title={title}
      description={props.description}
      summary={props.summary}
      collapsible={props.collapsible}
      defaultCollapsed={props.defaultCollapsed}
      issues={issues}
      actions={
        canClear ? (
          <Button size="small" type="link" className="xflow-composer-reset" onClick={() => onChange({})}>
            清空
          </Button>
        ) : undefined
      }
    >
      {wrong ? (
        <RawValue value={value} expected="对象" readOnly={readOnly || onChange === undefined} onClear={() => onChange?.(undefined)} />
      ) : (
        // Sub-fields are full fields again: their labels show inside a cell or an inline slot.
        <FieldLayoutContext.Provider value={layout === "cell" || layout === "inline" ? "vertical" : layout}>{children}</FieldLayoutContext.Provider>
      )}
    </Section>
  );
  // Inside an ArrayTable row the group is one table cell.
  return layout === "cell" ? (
    <div role="cell" className="xflow-composer-cell">
      {section}
    </div>
  ) : (
    section
  );
}

export const ObjectGroup: ComposerComponent<ObjectGroupProps> = {
  type: "ObjectGroup",
  props: zodProps(z.strictObject({ ...sectionShape, label: z.string().optional(), clearable: z.boolean().optional() })),
  bindings: { value: "object" },
  render: ({ node, props, value, onChange, issues, readOnly, children }) => (
    <ObjectGroupView nodeId={node.id} props={props} value={value} onChange={onChange} issues={issues} readOnly={readOnly}>
      {children}
    </ObjectGroupView>
  )
};
