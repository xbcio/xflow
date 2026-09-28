// JsonEditor: JSON text bound to any JSON value.
//
// The text is a local draft. A draft that parses is written (when it
// differs from the value); a draft that does not parse is never written,
// stays in the editor and shows the parse error. The draft survives
// re-renders; it is dropped only when the value changes to something other
// than what the draft says (an external edit wins). Empty text clears the
// value (unset). Note that core never writes null: the text `null` also
// clears the value (Doc B §6 rule 1).

import { Input as AntInput } from "antd";
import { useState } from "react";
import { z } from "zod";
import { deepEqual, zodProps, type Issue } from "../core";
import type { ComposerComponent, ComposerComponentProps } from "../react";
import { FieldFrame, fieldShape, hintText, safeJson, type FieldProps } from "./shared";

export interface JsonEditorProps extends FieldProps {
  placeholder?: string;
  rows?: number;
}

type Parsed = { ok: true; value: unknown } | { ok: false; message: string };

export function parseJsonText(text: string): Parsed {
  if (text.trim() === "") return { ok: true, value: undefined };
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch (error) {
    return { ok: false, message: (error as Error).message };
  }
}

function formatJson(value: unknown): string {
  return value === undefined ? "" : safeJson(value, 2);
}

/** Whether `value` is what writing `parsed` leaves behind (null/undefined both clear). */
function reflects(parsed: unknown, value: unknown): boolean {
  if (parsed === undefined || parsed === null) return value === undefined || value === null;
  return deepEqual(parsed, value);
}

interface Draft {
  text: string;
  /** The value the draft was last reconciled with. */
  base: unknown;
}

function JsonField({ node, props, value, onChange, defaultHint, issues, readOnly }: ComposerComponentProps<JsonEditorProps>) {
  const [draft, setDraft] = useState<Draft | null>(null);

  // Reconcile the draft with a changed value during render (no effect, no write).
  let current = draft;
  if (current && !Object.is(current.base, value)) {
    const parsed = parseJsonText(current.text);
    current = parsed.ok && reflects(parsed.value, value) ? { text: current.text, base: value } : null;
    setDraft(current);
  }

  const text = current ? current.text : formatJson(value);
  const parsed = current ? parseJsonText(current.text) : null;
  const localIssues: Issue[] =
    parsed && !parsed.ok ? [{ message: `JSON 无效，未保存：${parsed.message}`, severity: "error", check: "json-draft" }] : [];
  const placeholder = value === undefined && defaultHint !== undefined ? hintText(defaultHint, (v) => safeJson(v)) : props.placeholder;

  return (
    <FieldFrame
      className="xflow-composer-json-editor"
      nodeId={node.id}
      {...props}
      issues={issues}
      localIssues={localIssues}
      stateClassName={value === undefined ? "is-unset" : undefined}
    >
      {(ids) => (
        <AntInput.TextArea
          id={ids.controlId}
          rootClassName="xflow-composer-control xflow-composer-code-textarea"
          value={text}
          placeholder={placeholder}
          readOnly={readOnly}
          disabled={props.disabled}
          spellCheck={false}
          autoCapitalize="off"
          autoCorrect="off"
          status={ids.status}
          autoSize={{ minRows: props.rows ?? 4, maxRows: 24 }}
          aria-describedby={ids.describedBy}
          aria-invalid={ids.invalid || undefined}
          aria-required={props.required || undefined}
          onChange={(event) => {
            const nextText = event.target.value;
            setDraft({ text: nextText, base: value });
            const next = parseJsonText(nextText);
            if (next.ok && !reflects(next.value, value)) onChange?.(next.value);
          }}
        />
      )}
    </FieldFrame>
  );
}

export const JsonEditor: ComposerComponent<JsonEditorProps> = {
  type: "JsonEditor",
  props: zodProps(
    z.strictObject({ ...fieldShape, placeholder: z.string().optional(), rows: z.number().int().positive().optional() })
  ),
  // Any JSON value: only undefined/null (the cleared text) count as empty.
  bindings: { value: "scalar" },
  render: (p) => <JsonField {...p} />
};
