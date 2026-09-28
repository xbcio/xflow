// Text controls: Input, TextArea, Password, CodeEditor. Value kind scalar
// (string); clearing the text writes "" which core turns into unset.

import { Input as AntInput } from "antd";
import { z } from "zod";
import { zodProps } from "../core";
import type { ComposerComponent, ComposerComponentProps } from "../react";
import { FieldFrame, fieldShape, hintText, isWrongShape, RawValue, type FieldProps } from "./shared";

const isString = (value: unknown) => typeof value === "string";

export interface InputProps extends FieldProps {
  placeholder?: string;
  maxLength?: number;
}

export interface TextAreaProps extends InputProps {
  rows?: number;
}

export interface CodeEditorProps extends FieldProps {
  placeholder?: string;
  /** Shown as a badge; v1 has no syntax highlighting. */
  language?: string;
  rows?: number;
}

const inputShape = {
  ...fieldShape,
  placeholder: z.string().optional(),
  maxLength: z.number().int().positive().optional()
};

type Variant = "input" | "textarea" | "password" | "code";

const classes: Record<Variant, string> = {
  input: "xflow-composer-input",
  textarea: "xflow-composer-textarea",
  password: "xflow-composer-password",
  code: "xflow-composer-code-editor"
};

function TextField({
  variant,
  node,
  props,
  value,
  onChange,
  defaultHint,
  issues,
  readOnly
}: ComposerComponentProps<TextAreaProps & CodeEditorProps> & { variant: Variant }) {
  const wrong = isWrongShape(value, isString);
  const text = typeof value === "string" ? value : "";
  const placeholder = value === undefined && defaultHint !== undefined ? hintText(defaultHint) : props.placeholder;
  const disabled = props.disabled;
  const badge = variant === "code" && props.language ? <span className="xflow-composer-code-language">{props.language}</span> : null;

  return (
    <FieldFrame
      className={classes[variant]}
      nodeId={node.id}
      label={props.label}
      description={props.description}
      required={props.required}
      disabled={disabled}
      issues={issues}
      labelMode={wrong ? "group" : "control"}
      stateClassName={value === undefined ? "is-unset" : undefined}
    >
      {(ids) => {
        if (wrong) return <RawValue value={value} expected="字符串" readOnly={readOnly} onClear={() => onChange?.(undefined)} />;
        const common = {
          id: ids.controlId,
          value: text,
          placeholder,
          readOnly,
          disabled,
          status: ids.status,
          maxLength: props.maxLength,
          "aria-describedby": ids.describedBy,
          "aria-invalid": ids.invalid || undefined,
          "aria-required": props.required || undefined
        };
        switch (variant) {
          case "input":
            return (
              <AntInput
                {...common}
                rootClassName="xflow-composer-control"
                allowClear={!readOnly && !disabled}
                onChange={(event) => onChange?.(event.target.value)}
              />
            );
          case "password":
            return (
              <AntInput.Password
                {...common}
                rootClassName="xflow-composer-control"
                autoComplete="off"
                onChange={(event) => onChange?.(event.target.value)}
              />
            );
          case "textarea":
            return (
              <AntInput.TextArea
                {...common}
                rootClassName="xflow-composer-control"
                autoSize={{ minRows: props.rows ?? 3, maxRows: 16 }}
                onChange={(event) => onChange?.(event.target.value)}
              />
            );
          case "code":
            return (
              <div className="xflow-composer-code-frame">
                {badge}
                <AntInput.TextArea
                  {...common}
                  rootClassName="xflow-composer-control xflow-composer-code-textarea"
                  spellCheck={false}
                  autoCapitalize="off"
                  autoCorrect="off"
                  autoSize={{ minRows: props.rows ?? 6, maxRows: 24 }}
                  onChange={(event) => onChange?.(event.target.value)}
                />
              </div>
            );
        }
      }}
    </FieldFrame>
  );
}

export const Input: ComposerComponent<InputProps> = {
  type: "Input",
  props: zodProps(z.strictObject(inputShape)),
  bindings: { value: "scalar" },
  render: (p) => <TextField {...p} variant="input" />
};

export const Password: ComposerComponent<InputProps> = {
  type: "Password",
  props: zodProps(z.strictObject(inputShape)),
  bindings: { value: "scalar" },
  render: (p) => <TextField {...p} variant="password" />
};

export const TextArea: ComposerComponent<TextAreaProps> = {
  type: "TextArea",
  props: zodProps(z.strictObject({ ...inputShape, rows: z.number().int().positive().optional() })),
  bindings: { value: "scalar" },
  render: (p) => <TextField {...p} variant="textarea" />
};

export const CodeEditor: ComposerComponent<CodeEditorProps> = {
  type: "CodeEditor",
  props: zodProps(
    z.strictObject({
      ...fieldShape,
      placeholder: z.string().optional(),
      language: z.string().optional(),
      rows: z.number().int().positive().optional()
    })
  ),
  bindings: { value: "scalar" },
  render: (p) => <TextField {...p} variant="code" />
};
