// DateTimeInput (RFC 3339, Doc C §3 `datetime`) and CronInput (Doc C §3
// `cron`): text controls that write exactly what is typed and only display
// a reading of it — a local-time preview, or whether the cron expression
// parses. Validation that blocks is the compiled `format` check; the notes
// here are informational and never write.

import { Button, Input as AntInput } from "antd";
import type { ReactNode } from "react";
import { z } from "zod";
import { isCron, zodProps } from "@xflow/composer/core";
import { FieldFrame, RawValue } from "@xflow/composer/form";
import type { ComposerComponent, ComposerComponentProps } from "@xflow/composer/react";
import { chromeShape, cx, hintText, isExpressionString, NODEFORM, type ChromeProps } from "./shared";

// Go time.RFC3339 (with optional fractional seconds, as RFC3339Nano parses too).
const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;

export function isRfc3339(text: string): boolean {
  return RFC3339.test(text) && !Number.isNaN(Date.parse(text));
}

/** RFC 3339 of `date` in UTC, second precision. */
export function toRfc3339(date: Date): string {
  return date.toISOString().replace(/\.\d{3}Z$/, "Z");
}

interface TextFormatProps {
  className: string;
  placeholder: string;
  expected: string;
  /** Note under the control for the current text, or undefined. */
  note(text: string): { tone: "ok" | "warning"; text: string } | undefined;
  extra?(p: ComposerComponentProps<ChromeProps>): ReactNode;
}

function TextFormatField(p: ComposerComponentProps<ChromeProps> & { format: TextFormatProps }) {
  const { node, props, value, onChange, defaultHint, issues, readOnly, format } = p;
  const wrong = value !== undefined && typeof value !== "string";
  const text = typeof value === "string" ? value : "";
  const note = text !== "" && !isExpressionString(text) ? format.note(text) : undefined;
  return (
    <FieldFrame
      className={format.className}
      nodeId={node.id}
      label={props.label}
      description={props.description}
      required={props.required}
      disabled={props.disabled}
      issues={issues}
      hint={note ? <span className={cx(`${NODEFORM}-note`, `is-${note.tone}`)}>{note.text}</span> : undefined}
      labelMode={wrong ? "group" : "control"}
      stateClassName={value === undefined ? "is-unset" : undefined}
    >
      {(ids) =>
        wrong ? (
          <RawValue value={value} expected={format.expected} readOnly={readOnly} onClear={() => onChange?.(undefined)} />
        ) : (
          <div className={`${NODEFORM}-text-row`}>
            <AntInput
              id={ids.controlId}
              rootClassName={`${NODEFORM}-text-control`}
              value={text}
              placeholder={value === undefined && defaultHint !== undefined ? hintText(defaultHint) : format.placeholder}
              readOnly={readOnly}
              disabled={props.disabled}
              allowClear={!readOnly && !props.disabled}
              status={ids.status}
              spellCheck={false}
              autoComplete="off"
              aria-describedby={ids.describedBy}
              aria-invalid={ids.invalid || undefined}
              aria-required={props.required || undefined}
              onChange={(event) => onChange?.(event.target.value)}
            />
            {format.extra?.(p)}
          </div>
        )
      }
    </FieldFrame>
  );
}

const dateTimeFormat: TextFormatProps = {
  className: `${NODEFORM}-datetime`,
  placeholder: "例如 2026-01-01T08:00:00+08:00",
  expected: "RFC 3339 时间字符串",
  note: (text) =>
    isRfc3339(text)
      ? { tone: "ok", text: `本地时间：${new Date(Date.parse(text)).toLocaleString()}` }
      : { tone: "warning", text: "需要 RFC 3339 时间，例如 2026-01-01T08:00:00Z" },
  extra: ({ onChange, readOnly, props }) =>
    readOnly || props.disabled ? null : (
      <Button size="small" className={`${NODEFORM}-now`} onClick={() => onChange?.(toRfc3339(new Date()))}>
        当前时间
      </Button>
    )
};

const cronFormat: TextFormatProps = {
  className: `${NODEFORM}-cron`,
  placeholder: "分 时 日 月 周，例如 */5 * * * *",
  expected: "cron 表达式字符串",
  note: (text) => {
    if (!isCron(text)) return { tone: "warning", text: "cron 表达式无效（5 段：分 时 日 月 周；6 段时首段为秒；或 @daily、@every 1h）" };
    const fields = text.trim().split(/\s+/).length;
    if (text.trim().startsWith("@")) return { tone: "ok", text: "cron 表达式有效" };
    return { tone: "ok", text: fields === 6 ? "cron 表达式有效：秒 分 时 日 月 周" : "cron 表达式有效：分 时 日 月 周" };
  }
};

const textShape = zodProps(z.strictObject(chromeShape));

export const DateTimeInput: ComposerComponent<ChromeProps> = {
  type: "DateTimeInput",
  props: textShape,
  bindings: { value: "scalar" },
  render: (p) => <TextFormatField {...p} format={dateTimeFormat} />
};

export const CronInput: ComposerComponent<ChromeProps> = {
  type: "CronInput",
  props: textShape,
  bindings: { value: "scalar" },
  render: (p) => <TextFormatField {...p} format={cronFormat} />
};
