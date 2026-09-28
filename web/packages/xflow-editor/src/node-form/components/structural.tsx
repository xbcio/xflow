// NodeNameInput, ShapeGuard and FormNotice: host components that never write
// through a binding.
//
// - NodeNameInput (Doc C §4.2): `/name` is not a patch. The component reads
//   the name (a `$state` prop, not a binding), keeps a local draft and, on
//   blur or Enter, calls the editor's rename callback, closed over when the
//   registry is created (Doc B §5 v1). Escape restores the current name.
// - ShapeGuard (Doc C §3.2): renders its children only when the observed
//   value fits `expect`; otherwise a read-only raw view that sends the user
//   to the JSON tab. It has no binding and cannot write.
// - FormNotice: a static banner (unregistered type, undeclared params,
//   node templates).

import { Alert, Input as AntInput } from "antd";
import { useState } from "react";
import { z } from "zod";
import { zodProps } from "@xflow/composer/core";
import { FieldFrame } from "@xflow/composer/form";
import type { ComposerComponent, ComposerComponentProps } from "@xflow/composer/react";
import type { NoticeCode, ShapeExpectation } from "../components";
import type { ExpressionModeName } from "../schema";
import { cx, expectationSchema, isExpressionString, matchesShape, NODEFORM, RawNotice } from "./shared";

// ---------------------------------------------------------- NodeNameInput

export interface NodeNameInputProps {
  label?: string;
  description?: string;
  value?: unknown;
}

/** Called with the requested name on blur / Enter when it differs from the current one. */
export type RenameCallback = (requestedName: string) => void;

interface Draft {
  text: string;
  /** The name the draft was started from. */
  base: string;
}

function NodeNameField({
  node,
  props,
  issues,
  readOnly,
  onRename
}: ComposerComponentProps<NodeNameInputProps> & { onRename?: RenameCallback }) {
  const current = typeof props.value === "string" ? props.value : "";
  const [draft, setDraft] = useState<Draft | null>(null);
  // A new name from the host (rename applied, undo, another node) drops the draft.
  let active = draft;
  if (active && active.base !== current) {
    active = null;
    setDraft(null);
  }
  const text = active ? active.text : current;
  const locked = readOnly || onRename === undefined;
  const commit = () => {
    // A read-only form (e.g. while the parameters JSON is broken) never renames.
    if (locked) return;
    if (!active || active.text === current) {
      if (active) setDraft(null);
      return;
    }
    onRename?.(active.text);
  };
  return (
    <FieldFrame
      className={`${NODEFORM}-node-name`}
      nodeId={node.id}
      label={props.label}
      description={props.description}
      issues={issues}
      hint={active && active.text !== current ? "失焦或回车后改名" : undefined}
    >
      {(ids) => (
        <AntInput
          id={ids.controlId}
          rootClassName={`${NODEFORM}-node-name-control`}
          value={text}
          readOnly={locked}
          status={ids.status}
          spellCheck={false}
          autoComplete="off"
          aria-describedby={ids.describedBy}
          aria-invalid={ids.invalid || undefined}
          onChange={(event) => {
            if (!locked) setDraft({ text: event.target.value, base: current });
          }}
          onBlur={commit}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              commit();
            } else if (event.key === "Escape" && active) {
              event.preventDefault();
              setDraft(null);
            }
          }}
        />
      )}
    </FieldFrame>
  );
}

const nodeNameSchema = zodProps(
  z.strictObject({ label: z.string().optional(), description: z.string().optional(), value: z.unknown().optional() })
);

export function createNodeNameInput(onRename?: RenameCallback): ComposerComponent<NodeNameInputProps> {
  return {
    type: "NodeNameInput",
    props: nodeNameSchema,
    bindings: {},
    render: (p) => <NodeNameField {...p} onRename={onRename} />
  };
}

// ------------------------------------------------------------- ShapeGuard

export interface ShapeGuardProps {
  label?: string;
  observed?: unknown;
  expect: ShapeExpectation;
  mode: ExpressionModeName;
}

function ShapeGuardView({ node, props, children, issues }: ComposerComponentProps<ShapeGuardProps>) {
  const value = props.observed;
  if (matchesShape(value, props.expect)) return <>{children}</>;
  const reason = isExpressionString(value) ? "expression" : "shape";
  return (
    <FieldFrame
      className={cx(`${NODEFORM}-shape-guard`, `is-${reason}`)}
      nodeId={node.id}
      label={props.label}
      issues={issues}
      labelMode="group"
    >
      {() => <RawNotice value={value} reason={reason} />}
    </FieldFrame>
  );
}

export const ShapeGuard: ComposerComponent<ShapeGuardProps> = {
  type: "ShapeGuard",
  props: zodProps(
    z.strictObject({
      label: z.string().optional(),
      observed: z.unknown().optional(),
      expect: expectationSchema,
      mode: z.enum(["none", "pure", "template", "literal"])
    })
  ),
  bindings: {},
  render: (p) => <ShapeGuardView {...p} />
};

// ------------------------------------------------------------- FormNotice

export interface FormNoticeProps {
  tone: "info" | "warning";
  code: NoticeCode;
  message: string;
  count?: number;
}

export const FormNotice: ComposerComponent<FormNoticeProps> = {
  type: "FormNotice",
  props: zodProps(
    z.strictObject({
      tone: z.enum(["info", "warning"]),
      code: z.enum(["unregistered-type", "undeclared-params", "node-template"]),
      message: z.string(),
      count: z.number().int().nonnegative().optional()
    })
  ),
  bindings: {},
  render: ({ node, props }) => (
    <div className={cx(`${NODEFORM}-notice`, `is-${props.tone}`)} data-composer-node={node.id} data-notice-code={props.code}>
      <Alert
        type={props.tone}
        showIcon
        title={props.message}
        classNames={{ root: `${NODEFORM}-notice-alert` }}
      />
    </div>
  )
};
