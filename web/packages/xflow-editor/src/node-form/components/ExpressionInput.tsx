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

import { Button, Input as AntInput } from "antd";
import { useState } from "react";
import { z } from "zod";
import { zodProps } from "@xflow/composer/core";
import { FieldFrame, FieldLayoutContext } from "@xflow/composer/form";
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
// Pure params take a bare expr-lang expression, not a `${{ }}` template.
const PURE_PLACEHOLDER = "$input.value";
// The same cap the focused editor grows to (§6.4).
const EXPRESSION_MAX_ROWS = 8;
// The roots exprx actually exposes (exprx/exprx.go); n8n's `$json` / `$node`
// do not evaluate here. Shown only while the expression editor holds focus.
const EXPRESSION_ROOTS_HINT = "可用变量：$input · $nodes · $vars · $params";

function ExpressionField({ node, props, value, onChange, defaultHint, issues, readOnly, slots }: ComposerComponentProps<ExpressionInputProps>) {
  const literal = slots[EXPRESSION_LITERAL_SLOT];
  const toggleable = props.mode === "template" && props.fxToggle === true && literal !== undefined && literal !== null;
  const expression = isExpressionString(value);
  // null: follow the value (an expression opens in the fx state); otherwise the user's choice.
  const [fxChoice, setFxChoice] = useState<boolean | null>(null);
  const fx = !toggleable || (fxChoice ?? expression);
  const [focused, setFocused] = useState(false);

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

  // §6.4 (2026-11-06): blurred, the editor shows one line per REAL newline (at
  // most eight), each clipped with an ellipsis instead of soft-wrapping; the
  // 320px inspector otherwise folds an ordinary expression into 2–3 rows.
  // Focus restores the soft-wrapped, auto-growing textarea.
  const text = typeof value === "string" ? value : "";
  const placeholderText = unset && defaultHint !== undefined
    ? hintText(defaultHint)
    : (props.placeholder ?? (props.mode === "pure" ? PURE_PLACEHOLDER : DEFAULT_PLACEHOLDER));
  const clipLines = focused ? [] : (text === "" ? [placeholderText] : text.split("\n").slice(0, EXPRESSION_MAX_ROWS));
  // One 20px line box per row plus the control's own 8px vertical chrome
  // (1px border + 3px padding, top and bottom), so one line is the 28px control.
  const blurredHeight = clipLines.length === 0
    ? undefined
    : `calc(${clipLines.length} * var(--xflow-line-height-md) + var(--xflow-editor-nodeform-expression-chrome))`;

  const toggle = toggleable && content !== "raw" ? (
    <Button
      size="small"
      type="default"
      className={cx(`${NODEFORM}-fx`, fx && "is-active")}
      aria-pressed={fx}
      aria-label={fx ? `${props.label ?? "字段"}：切换为普通值` : `${props.label ?? "字段"}：切换为表达式`}
      title={fx ? "切换为普通值" : "切换为表达式"}
      onClick={() => setFxChoice(!fx)}
    >
      fx
    </Button>
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
      hint={
        replacesLiteral
          ? "当前为普通值；输入表达式后会替换它"
          : content === "editor" && focused && issues.length === 0
            ? EXPRESSION_ROOTS_HINT
            : undefined
      }
      labelMode={content === "editor" ? "control" : "group"}
      // A pure expression is a text editor with no literal alternative, so it
      // takes the full row (§6.4 D3); the template state stays in the side
      // column, where the fx key sits beside its control.
      labelPlacement={props.labelPlacement ?? (props.mode === "pure" ? "top" : undefined)}
      stateClassName={unset ? "is-unset" : undefined}
    >
      {(ids) => {
        if (content === "raw") return <RawNotice value={value} reason="shape" />;
        const body =
          content === "editor" ? (
          <div className={cx(`${NODEFORM}-expression-editor-wrap`, focused ? "is-focused" : "is-clipped")}>
            <AntInput.TextArea
              id={ids.controlId}
              rootClassName={`${NODEFORM}-expression-editor`}
              value={text}
              placeholder={placeholderText}
              readOnly={readOnly}
              disabled={disabled}
              status={ids.status}
              spellCheck={false}
              autoCapitalize="off"
              autoCorrect="off"
              autoSize={focused ? { minRows: 1, maxRows: EXPRESSION_MAX_ROWS } : false}
              style={blurredHeight === undefined ? undefined : { height: blurredHeight }}
              aria-describedby={ids.describedBy}
              aria-invalid={ids.invalid || undefined}
              aria-required={props.required || undefined}
              onFocus={() => setFocused(true)}
              onBlur={() => setFocused(false)}
              onChange={(event) => {
                if (fxChoice === null) setFxChoice(true); // keep the fx state while the text loses its template
                onChange?.(event.target.value);
              }}
            />
            {focused ? null : (
              /* The layer paints the clipped glyphs; the textarea underneath
               * keeps the value and every control semantic (id, aria-*, status,
               * disabled). It is invisible to assistive tech and to the pointer. */
              <div className={cx(`${NODEFORM}-expression-clip`, text === "" && "is-placeholder", disabled && "is-disabled")} aria-hidden="true">
                {clipLines.map((line, index) => (
                  <div className={`${NODEFORM}-expression-clip-line`} key={index}>{line === "" ? " " : line}</div>
                ))}
              </div>
            )}
          </div>
        ) : (
          // The literal control shares this frame's label and cell: "inline"
          // keeps its label for assistive technology only and never makes it
          // a second (nested) table cell inside an ArrayTable row.
          <div className={`${NODEFORM}-expression-literal`}>
            <FieldLayoutContext.Provider value="inline">{literal}</FieldLayoutContext.Provider>
          </div>
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
