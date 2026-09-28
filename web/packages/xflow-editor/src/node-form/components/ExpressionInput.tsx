// ExpressionInput (Doc C §4.1): the expression capability of a field.
//
// - mode "template" + fxToggle: two display states over ONE bound value.
//   Literal state renders the `literal` slot (the plain control compiled
//   next to it, bound to the same path); expression state renders a text
//   editor for the raw `${{ }}` text. A value containing a template opens in
//   the expression state. The fx button only flips what is displayed; it
//   writes nothing — the value changes only when the user edits.
// - mode "pure" (or template without toggle / literal slot): always the
//   expression editor.
// - Doc C §3.2 shape guard: a value that is neither an expression string
//   nor of the `expect` shape is shown read-only with "请在 JSON 页签修改".

import { FunctionOutlined } from "@ant-design/icons";
import { Button, Input as AntInput } from "antd";
import { useState } from "react";
import { z } from "zod";
import { zodProps } from "@xflow/composer/core";
import { FieldFrame } from "@xflow/composer/form";
import type { ComposerComponent, ComposerComponentProps } from "@xflow/composer/react";
import { EXPRESSION_LITERAL_SLOT, type ShapeExpectation } from "../components";
import { chromeShape, cx, expectationSchema, hintText, isExpressionString, matchesShape, NODEFORM, RawNotice, type ChromeProps } from "./shared";

export interface ExpressionInputProps extends ChromeProps {
  mode: "template" | "pure";
  fxToggle?: boolean;
  expect?: ShapeExpectation;
  placeholder?: string;
}

const DEFAULT_PLACEHOLDER = "${{ $input.value }}";

function ExpressionField({ node, props, value, onChange, defaultHint, issues, readOnly, slots }: ComposerComponentProps<ExpressionInputProps>) {
  const literal = slots[EXPRESSION_LITERAL_SLOT];
  const toggleable = props.mode === "template" && props.fxToggle === true && literal !== undefined && literal !== null;
  const expression = isExpressionString(value);
  // null: follow the value (an expression opens in the fx state); otherwise the user's choice.
  const [fxChoice, setFxChoice] = useState<boolean | null>(null);
  const fx = !toggleable || (fxChoice ?? expression);

  const expect = props.expect;
  // The editor edits strings; a toggled field may also replace a literal from
  // it. The literal state needs the expected shape (an expression is left to
  // the literal control, which shows it read-only when it cannot hold it).
  const content: "editor" | "literal" | "raw" = fx
    ? value === undefined || typeof value === "string" || toggleable
      ? "editor"
      : "raw"
    : expression || !expect || matchesShape(value, expect)
      ? "literal"
      : "raw";
  const unset = value === undefined;
  const disabled = props.disabled;
  const replacesLiteral = fx && toggleable && value !== undefined && typeof value !== "string";

  const toggle = toggleable && content !== "raw" ? (
    <Button
      size="small"
      type={fx ? "primary" : "default"}
      className={cx(`${NODEFORM}-fx`, fx && "is-active")}
      icon={<FunctionOutlined aria-hidden />}
      aria-pressed={fx}
      aria-label={fx ? `${props.label ?? "字段"}：切换为普通值` : `${props.label ?? "字段"}：切换为表达式`}
      title={fx ? "切换为普通值" : "切换为表达式"}
      onClick={() => setFxChoice(!fx)}
    />
  ) : null;

  return (
    <FieldFrame
      className={cx(`${NODEFORM}-expression`, fx ? "is-expression" : "is-literal", props.mode === "pure" && "is-pure")}
      nodeId={node.id}
      label={props.label}
      description={props.description}
      required={props.required}
      disabled={disabled}
      issues={issues}
      hint={replacesLiteral ? "当前为普通值；输入表达式后会替换它" : undefined}
      labelMode={content === "editor" ? "control" : "group"}
      stateClassName={unset ? "is-unset" : undefined}
    >
      {(ids) => {
        if (content === "raw") return <RawNotice value={value} reason="shape" />;
        const body =
          content === "editor" ? (
          <AntInput.TextArea
            id={ids.controlId}
            rootClassName={`${NODEFORM}-expression-editor`}
            value={typeof value === "string" ? value : ""}
            placeholder={unset && defaultHint !== undefined ? hintText(defaultHint) : (props.placeholder ?? DEFAULT_PLACEHOLDER)}
            readOnly={readOnly}
            disabled={disabled}
            status={ids.status}
            spellCheck={false}
            autoCapitalize="off"
            autoCorrect="off"
            autoSize={{ minRows: 1, maxRows: 8 }}
            aria-describedby={ids.describedBy}
            aria-invalid={ids.invalid || undefined}
            aria-required={props.required || undefined}
            onChange={(event) => {
              if (fxChoice === null) setFxChoice(true); // keep the fx state while the text loses its template
              onChange?.(event.target.value);
            }}
          />
        ) : (
          <div className={`${NODEFORM}-expression-literal`}>{literal}</div>
        );
        return (
          <div className={`${NODEFORM}-expression-row`}>
            <div className={`${NODEFORM}-expression-body`}>{body}</div>
            {toggle}
          </div>
        );
      }}
    </FieldFrame>
  );
}

export const ExpressionInput: ComposerComponent<ExpressionInputProps> = {
  type: "ExpressionInput",
  props: zodProps(
    z.strictObject({
      ...chromeShape,
      mode: z.enum(["template", "pure"]),
      fxToggle: z.boolean().optional(),
      expect: expectationSchema.optional(),
      placeholder: z.string().optional()
    })
  ),
  bindings: { value: "scalar" },
  render: (p) => <ExpressionField {...p} />
};
