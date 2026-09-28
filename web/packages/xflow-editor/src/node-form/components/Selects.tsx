// CredentialSelect (Doc C §1 rule 4, §3 `credential-select`) and PortSelect
// (Doc C §3 `port-select`, §4.3): selects over host-provided lists.
//
// - CredentialSelect picks credential NAMES from `credentials` (compiled as
//   a read of `/$ctx/credentials`). There is no free-text entry, so no
//   plaintext secret can be typed into the parameters. `multiple` writes a
//   string array; clearing writes undefined (unset), never `[]`.
// - PortSelect picks one output port name from `ports` (static outputs, or
//   `/$ctx/ports` for dynamic-output nodes).
// A current value missing from the list is shown as-is with a note, never
// dropped or coerced.

import { CloseCircleFilled } from "@ant-design/icons";
import { Select as AntSelect } from "antd";
import { z } from "zod";
import { zodProps } from "@xflow/composer/core";
import { FieldFrame, RawValue } from "@xflow/composer/form";
import type { ComposerComponent, ComposerComponentProps } from "@xflow/composer/react";
import type { PortItem } from "../components";
import { chromeShape, hintText, NODEFORM, type ChromeProps } from "./shared";

const clearIcon = <CloseCircleFilled className={`${NODEFORM}-clear-icon`} aria-label="清除" />;

const isString = (value: unknown): value is string => typeof value === "string";
const isStringList = (value: unknown): value is string[] => Array.isArray(value) && value.every(isString);

function filterOption(input: string, option?: { label?: unknown; value?: unknown }) {
  return `${String(option?.label ?? "")} ${String(option?.value ?? "")}`.toLowerCase().includes(input.toLowerCase());
}

interface ChoiceFieldProps {
  className: string;
  expected: string;
  multiple: boolean;
  options: { value: string; label: string }[];
  emptyText: string;
  missingNote(values: string[]): string;
}

function ChoiceField({
  node,
  props,
  value,
  onChange,
  defaultHint,
  issues,
  readOnly,
  choice
}: ComposerComponentProps<ChromeProps> & { choice: ChoiceFieldProps }) {
  const wrong = value !== undefined && !(choice.multiple ? isStringList(value) : isString(value));
  const unset = value === undefined;
  const selected = wrong || unset ? [] : choice.multiple ? (value as string[]) : [value as string];
  const known = new Set(choice.options.map((option) => option.value));
  const missing = selected.filter((item) => !known.has(item));
  const disabled = readOnly || props.disabled;
  const placeholder =
    unset && defaultHint !== undefined
      ? hintText(defaultHint)
      : choice.options.length === 0
        ? choice.emptyText
        : "请选择";
  return (
    <FieldFrame
      className={choice.className}
      nodeId={node.id}
      label={props.label}
      description={props.description}
      required={props.required}
      disabled={props.disabled}
      issues={issues}
      hint={missing.length > 0 ? choice.missingNote(missing) : undefined}
      labelMode={wrong ? "group" : "control"}
      stateClassName={unset ? "is-unset" : undefined}
    >
      {(ids) =>
        wrong ? (
          <RawValue value={value} expected={choice.expected} readOnly={readOnly} onClear={() => onChange?.(undefined)} />
        ) : (
          <AntSelect<string | string[]>
            id={ids.controlId}
            mode={choice.multiple ? "multiple" : undefined}
            classNames={{ root: `${NODEFORM}-select`, popup: { root: `${NODEFORM}-select-popup` } }}
            value={choice.multiple ? selected : unset ? undefined : (value as string)}
            options={choice.options}
            placeholder={placeholder}
            showSearch={{ filterOption }}
            allowClear={disabled ? false : { clearIcon }}
            disabled={disabled}
            status={ids.status}
            notFoundContent={choice.emptyText}
            aria-describedby={ids.describedBy}
            aria-invalid={ids.invalid || undefined}
            aria-required={props.required || undefined}
            onChange={(next) => {
              if (Array.isArray(next)) onChange?.(next.length === 0 ? undefined : [...next]);
              else onChange?.(next === undefined || next === null ? undefined : next);
            }}
          />
        )
      }
    </FieldFrame>
  );
}

// ------------------------------------------------------ CredentialSelect

export interface CredentialSelectProps extends ChromeProps {
  multiple: boolean;
  credentials?: string[];
}

export const CredentialSelect: ComposerComponent<CredentialSelectProps> = {
  type: "CredentialSelect",
  props: zodProps(
    z.strictObject({ ...chromeShape, multiple: z.boolean(), credentials: z.array(z.string()).optional() })
  ),
  // Scalar for both shapes: a cleared multi-select writes undefined itself.
  bindings: { value: "scalar" },
  render: (p) => (
    <ChoiceField
      {...p}
      choice={{
        className: `${NODEFORM}-credential-select`,
        expected: p.props.multiple ? "凭据名列表" : "凭据名",
        multiple: p.props.multiple,
        options: [...new Set(p.props.credentials ?? [])].map((name) => ({ value: name, label: name })),
        emptyText: "工作流未声明凭据",
        missingNote: (names) => `不在工作流凭据中：${names.join("、")}`
      }}
    />
  )
};

// ------------------------------------------------------------ PortSelect

export interface PortSelectProps extends ChromeProps {
  ports?: PortItem[];
}

export const PortSelect: ComposerComponent<PortSelectProps> = {
  type: "PortSelect",
  props: zodProps(
    z.strictObject({
      ...chromeShape,
      ports: z.array(z.object({ name: z.string(), display_name: z.string().optional() })).optional()
    })
  ),
  bindings: { value: "scalar" },
  render: (p) => (
    <ChoiceField
      {...p}
      choice={{
        className: `${NODEFORM}-port-select`,
        expected: "端口名",
        multiple: false,
        options: (p.props.ports ?? []).map((port) => ({
          value: port.name,
          label: port.display_name && port.display_name !== port.name ? `${port.display_name}（${port.name}）` : port.name
        })),
        emptyText: "没有可用端口",
        missingNote: (names) => `端口不存在：${names.join("、")}`
      }}
    />
  )
};
