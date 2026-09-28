// Structural components: Form, FieldGroup, ObjectGroup.

import { DownOutlined, RightOutlined } from "@ant-design/icons";
import { Button } from "antd";
import { useId, useState, type CSSProperties, type ReactNode } from "react";
import { z } from "zod";
import { zodProps, type Issue } from "../core";
import type { ComposerComponent } from "../react";
import { cx, FieldLayoutContext, IssueList, isPlainObject, isWrongShape, RawValue, useFieldLayout, worstSeverity } from "./shared";

// ------------------------------------------------------------------- Form

export interface FormProps {
  layout?: "horizontal" | "vertical";
  /** Label column width (px) for the horizontal layout. */
  labelWidth?: number;
  /** Accessible name of the form region. */
  title?: string;
}

export const Form: ComposerComponent<FormProps> = {
  type: "Form",
  props: zodProps(
    z.strictObject({
      layout: z.enum(["horizontal", "vertical"]).optional(),
      labelWidth: z.number().positive().optional(),
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
        <FieldLayoutContext.Provider value={layout}>
          {slots.header}
          {children}
          {slots.footer}
        </FieldLayoutContext.Provider>
      </div>
    );
  }
};

// ------------------------------------------------------------- sections

interface SectionProps {
  className: string;
  nodeId: string;
  title?: string;
  description?: string;
  collapsible?: boolean;
  defaultCollapsed?: boolean;
  issues: readonly Issue[];
  actions?: ReactNode;
  children: ReactNode;
}

/** Titled, optionally collapsible section. Collapsed content stays mounted (drafts survive). */
function Section(props: SectionProps) {
  const base = useId();
  const [collapsed, setCollapsed] = useState(props.defaultCollapsed === true);
  const titleId = `${base}title`;
  const bodyId = `${base}body`;
  const issuesId = `${base}issues`;
  const status = worstSeverity(props.issues);
  const open = !props.collapsible || !collapsed;
  return (
    <section
      className={cx("xflow-composer-section", props.className, status && `is-${status}`, !open && "is-collapsed")}
      data-composer-node={props.nodeId}
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
              onClick={() => setCollapsed((value) => !value)}
            >
              {open ? <DownOutlined aria-hidden /> : <RightOutlined aria-hidden />}
              <span id={titleId} className="xflow-composer-section-title">
                {props.title}
              </span>
            </button>
          ) : (
            <h3 id={titleId} className="xflow-composer-section-title">
              {props.title}
            </h3>
          )}
          {props.actions ? <div className="xflow-composer-section-actions">{props.actions}</div> : null}
        </header>
      ) : null}
      {props.description ? <p className="xflow-composer-section-description">{props.description}</p> : null}
      <IssueList id={issuesId} issues={props.issues} />
      <div id={bodyId} className="xflow-composer-section-body" hidden={!open}>
        {props.children}
      </div>
    </section>
  );
}

const sectionShape = {
  title: z.string().optional(),
  description: z.string().optional(),
  collapsible: z.boolean().optional(),
  defaultCollapsed: z.boolean().optional()
};

export interface FieldGroupProps {
  title?: string;
  description?: string;
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
        <FieldLayoutContext.Provider value={layout === "cell" ? "vertical" : layout}>{children}</FieldLayoutContext.Provider>
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
