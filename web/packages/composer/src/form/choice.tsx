// Choice and number controls: InputNumber, Switch, Select, Radio,
// MultiSelect, Tags.
//
// Option values are primitives (string / number / boolean). AntD options
// are keyed by the JSON encoding of the value, so a boolean or a value that
// is not among the options is shown as-is instead of being coerced.

import { CloseCircleFilled } from "@ant-design/icons";
import {
  InputNumber as AntInputNumber,
  Radio as AntRadio,
  Select as AntSelect,
  Switch as AntSwitch,
  type SelectProps as AntSelectProps
} from "antd";
import { useRef, useState } from "react";
import { z } from "zod";
import { deepEqual, zodProps, type Issue } from "../core";
import type { ComposerComponent, ComposerComponentProps } from "../react";
import {
  cx,
  displayValue,
  FieldFrame,
  fieldShape,
  hintText,
  isPrimitive,
  isWrongShape,
  labelOfOption,
  optionKey,
  optionSchema,
  RawValue,
  ResetButton,
  type FieldProps,
  type Option
} from "./shared";

// ------------------------------------------------------------ InputNumber

export interface InputNumberProps extends FieldProps {
  placeholder?: string;
  min?: number;
  max?: number;
  step?: number;
  precision?: number;
}

const isFiniteNumber = (value: unknown) => typeof value === "number" && Number.isFinite(value);

/**
 * What a draft text means: `empty` writes unset, `incomplete` is not a whole
 * number yet (`-`, `1.`, `1e`, `.`, `+`), `out-of-range` is whole but outside
 * [min, max], `ok` carries the finite value to write (rounded to `precision`).
 */
type NumberDraft =
  | { kind: "empty" }
  | { kind: "incomplete" }
  | { kind: "out-of-range" }
  | { kind: "ok"; value: number };

/** A whole number: `1`, `-1.5`, `.5`, `007`, `+1`, `1e3`. Not `1.`, `-`, `1e`. */
const wholeNumber = /^[+-]?(\d+(\.\d+)?|\.\d+)([eE][+-]?\d+)?$/;

function parseNumberText(text: string, { min, max, precision }: InputNumberProps): NumberDraft {
  if (text.trim() === "") return { kind: "empty" };
  if (!wholeNumber.test(text)) return { kind: "incomplete" };
  const number = Number(text);
  if (!Number.isFinite(number)) return { kind: "incomplete" };
  const value = precision === undefined ? number : Number(number.toFixed(precision));
  if ((min !== undefined && value < min) || (max !== undefined && value > max)) return { kind: "out-of-range" };
  return { kind: "ok", value };
}

/** Whether writing the draft leaves `value` behind, so the draft may stay. */
function reflectsDraft(draft: NumberDraft, value: unknown): boolean {
  if (draft.kind === "empty") return value === undefined;
  return draft.kind === "ok" && value === draft.value;
}

function draftIssues(draft: NumberDraft, { min, max }: InputNumberProps): Issue[] {
  if (draft.kind === "incomplete") return [{ message: "请输入完整数字", severity: "error", check: "number-draft" }];
  if (draft.kind !== "out-of-range") return [];
  const message =
    min !== undefined && max !== undefined
      ? `超出范围：应在 ${min} 到 ${max} 之间`
      : min !== undefined
        ? `不能小于 ${min}`
        : `不能大于 ${max}`;
  return [{ message, severity: "error", check: "number-range" }];
}

interface NumberDraftState {
  text: string;
  /** The value the draft was last reconciled with. */
  base: unknown;
}

function NumberField({ node, props, value, onChange, defaultHint, issues, readOnly }: ComposerComponentProps<InputNumberProps>) {
  const [draft, setDraft] = useState<NumberDraftState | null>(null);
  // An IME session must not write: rc re-collects the text when it ends.
  const composing = useRef(false);
  const wrong = isWrongShape(value, isFiniteNumber);

  // Reconcile the draft with a changed value during render (no effect, no write).
  let current = draft;
  if (current && !Object.is(current.base, value)) {
    current = reflectsDraft(parseNumberText(current.text, props), value) ? { text: current.text, base: value } : null;
    setDraft(current);
  }

  const parsed = current ? parseNumberText(current.text, props) : null;
  const text = current ? current.text : isFiniteNumber(value) ? String(value) : null;
  const placeholder = value === undefined && defaultHint !== undefined ? hintText(defaultHint) : props.placeholder;
  const hint = parsed?.kind === "incomplete" ? "该值尚未写入定义，保存时会被忽略" : undefined;
  const locked = readOnly || props.disabled;
  // The draft text is the only write source: the control's own onChange cannot
  // tell `1.` from `1`, so only a whole, in-range draft ever writes.
  const write = (next: NumberDraft) => {
    if (next.kind === "empty") {
      if (value !== undefined) onChange?.(undefined);
    } else if (next.kind === "ok" && next.value !== value) {
      onChange?.(next.value);
    }
  };

  return (
    <FieldFrame
      className="xflow-composer-input-number"
      nodeId={node.id}
      {...props}
      issues={issues}
      localIssues={parsed ? draftIssues(parsed, props) : []}
      hint={hint}
      labelMode={wrong ? "group" : "control"}
      stateClassName={value === undefined ? "is-unset" : undefined}
    >
      {(ids) =>
        wrong ? (
          <RawValue value={value} expected="数字" readOnly={readOnly} onClear={() => onChange?.(undefined)} />
        ) : (
          <AntInputNumber<string>
            id={ids.controlId}
            rootClassName="xflow-composer-control"
            stringMode
            value={text}
            placeholder={placeholder}
            min={props.min === undefined ? undefined : String(props.min)}
            max={props.max === undefined ? undefined : String(props.max)}
            step={props.step}
            precision={props.precision}
            /* A blur flush would re-send the typed text, clamped/formatted; the draft owns every write. */
            changeOnBlur={false}
            readOnly={readOnly}
            disabled={props.disabled}
            status={ids.status}
            aria-describedby={ids.describedBy}
            aria-invalid={ids.invalid || undefined}
            aria-required={props.required || undefined}
            onInput={(raw) => {
              // rc fires onInput outside its composition guard; compositionEnd
              // re-collects, so a live IME session writes nothing until it ends.
              if (locked || composing.current) return;
              setDraft({ text: raw, base: value });
              write(parseNumberText(raw, props));
            }}
            onCompositionStart={() => {
              composing.current = true;
            }}
            onCompositionEnd={() => {
              composing.current = false;
            }}
            onStep={(next, info) => {
              if (locked) return;
              // rc steps the draft text while a draft owns the control: an
              // unparsable one counts as 0 and an out-of-range one is clamped.
              // Step from the committed value instead of that draft-derived one.
              const active = current ? parseNumberText(current.text, props) : null;
              if (active && active.kind !== "ok" && active.kind !== "empty") {
                setDraft(null);
                const base = typeof value === "number" && Number.isFinite(value) ? value : 0;
                const size = props.step ?? 1;
                write(parseNumberText(String(base + (info.type === "down" ? -size : size)), props));
                return;
              }
              // Steppers / arrow keys report here; they are not typing.
              setDraft(null);
              write(parseNumberText(next, props));
            }}
          />
        )
      }
    </FieldFrame>
  );
}

export const InputNumber: ComposerComponent<InputNumberProps> = {
  type: "InputNumber",
  props: zodProps(
    z.strictObject({
      ...fieldShape,
      placeholder: z.string().optional(),
      min: z.number().optional(),
      max: z.number().optional(),
      step: z.number().positive().optional(),
      precision: z.number().int().nonnegative().optional()
    })
  ),
  bindings: { value: "scalar" },
  render: (p) => <NumberField {...p} />
};

// ----------------------------------------------------------------- Switch

export interface SwitchProps extends FieldProps {
  /** Text of the unset default state ("开（默认）" / "关（默认）"); default 开 / 关. */
  checkedText?: string;
  uncheckedText?: string;
}

function SwitchField({ node, props, value, onChange, defaultHint, issues, readOnly }: ComposerComponentProps<SwitchProps>) {
  const wrong = isWrongShape(value, (v) => typeof v === "boolean");
  const unset = value === undefined;
  const effective = unset ? defaultHint === true : value === true;
  const onText = props.checkedText ?? "开";
  const offText = props.uncheckedText ?? "关";
  // Only the unset case needs the state line: it carries the default
  // ("开（默认）" / "默认：x" / "未设置"). A set value shows itself on the switch.
  const stateText = unset
    ? typeof defaultHint === "boolean"
      ? `${defaultHint ? onText : offText}（默认）`
      : (hintText(defaultHint) ?? "未设置")
    : undefined;
  return (
    <FieldFrame
      className="xflow-composer-switch"
      nodeId={node.id}
      {...props}
      issues={issues}
      labelMode={wrong ? "group" : "inline-after"}
      stateClassName={unset ? "is-unset" : undefined}
    >
      {(ids, label) =>
        wrong ? (
          <RawValue value={value} expected="布尔值" readOnly={readOnly} onClear={() => onChange?.(undefined)} />
        ) : (
          <div className={cx("xflow-composer-switch-row", !effective && "is-off")}>
            <AntSwitch
              size="small"
              id={ids.controlId}
              rootClassName="xflow-composer-switch-control"
              checked={effective}
              disabled={readOnly || props.disabled}
              aria-describedby={
                [stateText ? `${ids.controlId}-state` : "", ids.describedBy].filter(Boolean).join(" ") || undefined
              }
              aria-invalid={ids.invalid || undefined}
              onChange={(checked) => onChange?.(checked)}
            />
            {label}
            {stateText ? (
              <span id={`${ids.controlId}-state`} className="xflow-composer-switch-state">
                {stateText}
              </span>
            ) : null}
            {!unset && !readOnly && !props.disabled ? (
              <ResetButton hasDefault={defaultHint !== undefined} label={props.label} onClick={() => onChange?.(undefined)} />
            ) : null}
          </div>
        )
      }
    </FieldFrame>
  );
}

export const Switch: ComposerComponent<SwitchProps> = {
  type: "Switch",
  props: zodProps(
    z.strictObject({ ...fieldShape, checkedText: z.string().optional(), uncheckedText: z.string().optional() })
  ),
  bindings: { value: "scalar" },
  render: (p) => <SwitchField {...p} />
};

// ------------------------------------------------------ Select / Multi

export interface SelectProps extends FieldProps {
  options: Option[];
  placeholder?: string;
  showSearch?: boolean;
}

const clearIcon = <CloseCircleFilled className="xflow-composer-clear-icon" aria-label="清除" />;

function antOptions(options: readonly Option[]) {
  return options.map((option) => ({
    value: optionKey(option.value),
    label: option.label ?? displayValue(option.value),
    disabled: option.disabled,
    ...(option.description !== undefined && { description: option.description })
  }));
}

/** Radio options: the description becomes the option's tooltip. */
function radioOptions(options: readonly Option[]) {
  return options.map((option) => ({
    value: optionKey(option.value),
    label: option.label ?? displayValue(option.value),
    disabled: option.disabled,
    ...(option.description !== undefined && { title: option.description })
  }));
}

/**
 * Select popup row: the label, plus the option's description as a second
 * line. The selected value keeps showing the label alone (optionRender only
 * affects the popup).
 */
const renderOption: NonNullable<AntSelectProps["optionRender"]> = (option) => {
  const description = (option.data as { description?: unknown }).description;
  if (typeof description !== "string") return option.label;
  return (
    <span className="xflow-composer-option">
      <span className="xflow-composer-option-label">{option.label}</span>
      <span className="xflow-composer-option-description">{description}</span>
    </span>
  );
};

function fromKey(options: readonly Option[], key: string): unknown {
  const hit = options.find((option) => optionKey(option.value) === key);
  if (hit) return hit.value;
  try {
    return JSON.parse(key);
  } catch {
    return key;
  }
}

function notInOptions(options: readonly Option[], values: readonly unknown[]): unknown[] {
  return values.filter((value) => !options.some((option) => deepEqual(option.value, value)));
}

function selectFilter(input: string, option?: { label?: unknown }) {
  return String(option?.label ?? "")
    .toLowerCase()
    .includes(input.toLowerCase());
}

function SelectField({ node, props, value, onChange, defaultHint, issues, readOnly }: ComposerComponentProps<SelectProps>) {
  const wrong = isWrongShape(value, isPrimitive);
  const unset = value === undefined;
  const placeholder = unset && defaultHint !== undefined ? hintText(defaultHint, (v) => labelOfOption(props.options, v)) : props.placeholder;
  const unknown = !unset && !wrong ? notInOptions(props.options, [value]) : [];
  return (
    <FieldFrame
      className="xflow-composer-select"
      nodeId={node.id}
      {...props}
      issues={issues}
      hint={unknown.length > 0 ? "当前值不在可选项中" : undefined}
      labelMode={wrong ? "group" : "control"}
      stateClassName={unset ? "is-unset" : undefined}
    >
      {(ids) =>
        wrong ? (
          <RawValue value={value} expected="单个选项值" readOnly={readOnly} onClear={() => onChange?.(undefined)} />
        ) : (
          <AntSelect<string>
            id={ids.controlId}
            classNames={{ root: "xflow-composer-control", popup: { root: "xflow-composer-select-popup" } }}
            value={unset ? undefined : optionKey(value)}
            options={antOptions(props.options)}
            optionRender={renderOption}
            placeholder={placeholder}
            showSearch={props.showSearch ? { filterOption: selectFilter } : false}
            allowClear={readOnly || props.disabled ? false : { clearIcon }}
            disabled={readOnly || props.disabled}
            status={ids.status}
            aria-describedby={ids.describedBy}
            aria-invalid={ids.invalid || undefined}
            aria-required={props.required || undefined}
            onChange={(key) => onChange?.(key === undefined || key === null ? undefined : fromKey(props.options, key))}
          />
        )
      }
    </FieldFrame>
  );
}

const selectShape = {
  ...fieldShape,
  options: z.array(optionSchema),
  placeholder: z.string().optional(),
  showSearch: z.boolean().optional()
};

export const Select: ComposerComponent<SelectProps> = {
  type: "Select",
  props: zodProps(z.strictObject(selectShape)),
  bindings: { value: "scalar" },
  render: (p) => <SelectField {...p} />
};

const isPrimitiveList = (value: unknown) => Array.isArray(value) && value.every(isPrimitive);
const isStringList = (value: unknown) => Array.isArray(value) && value.every((item) => typeof item === "string");

function MultiField({
  node,
  props,
  value,
  onChange,
  defaultHint,
  issues,
  readOnly,
  tags
}: ComposerComponentProps<SelectProps & { separators?: string[] }> & { tags: boolean }) {
  const wrong = isWrongShape(value, tags ? isStringList : isPrimitiveList);
  const list = Array.isArray(value) ? value : [];
  const unset = value === undefined;
  const options = props.options ?? [];
  const placeholder =
    unset && defaultHint !== undefined
      ? hintText(defaultHint, (v) => (Array.isArray(v) ? v.map((item) => labelOfOption(options, item)).join("、") : displayValue(v)))
      : props.placeholder;
  const unknown = !tags && !wrong ? notInOptions(options, list) : [];
  return (
    <FieldFrame
      className={tags ? "xflow-composer-tags" : "xflow-composer-multi-select"}
      nodeId={node.id}
      {...props}
      issues={issues}
      hint={unknown.length > 0 ? "部分值不在可选项中" : undefined}
      labelMode={wrong ? "group" : "control"}
      stateClassName={unset ? "is-unset" : undefined}
    >
      {(ids) =>
        wrong ? (
          <RawValue
            value={value}
            expected={tags ? "字符串列表" : "选项值列表"}
            readOnly={readOnly}
            onClear={() => onChange?.(undefined)}
          />
        ) : (
          <AntSelect<string[]>
            id={ids.controlId}
            mode={tags ? "tags" : "multiple"}
            classNames={{ root: "xflow-composer-control", popup: { root: "xflow-composer-select-popup" } }}
            value={tags ? (list as string[]) : list.map(optionKey)}
            options={tags ? options.map((o) => ({ value: String(o.value), label: o.label ?? String(o.value) })) : antOptions(options)}
            optionRender={tags ? undefined : renderOption}
            placeholder={placeholder}
            tokenSeparators={tags ? (props.separators ?? [","]) : undefined}
            allowClear={readOnly || props.disabled ? false : { clearIcon }}
            disabled={readOnly || props.disabled}
            status={ids.status}
            aria-describedby={ids.describedBy}
            aria-invalid={ids.invalid || undefined}
            aria-required={props.required || undefined}
            onChange={(keys) => {
              const next = keys ?? [];
              onChange?.(tags ? [...next] : next.map((key) => fromKey(options, key)));
            }}
          />
        )
      }
    </FieldFrame>
  );
}

export const MultiSelect: ComposerComponent<SelectProps> = {
  type: "MultiSelect",
  props: zodProps(z.strictObject(selectShape)),
  bindings: { value: "array" },
  render: (p) => <MultiField {...p} tags={false} />
};

export interface TagsProps extends FieldProps {
  /** Suggestions (optional). */
  options?: Option[];
  placeholder?: string;
  /** Characters that split typed text into tags; default [","]. */
  separators?: string[];
}

export const Tags: ComposerComponent<TagsProps> = {
  type: "Tags",
  props: zodProps(
    z.strictObject({
      ...fieldShape,
      options: z.array(optionSchema).optional(),
      placeholder: z.string().optional(),
      separators: z.array(z.string().min(1)).optional()
    })
  ),
  bindings: { value: "array" },
  render: (p) => <MultiField {...(p as ComposerComponentProps<SelectProps & { separators?: string[] }>)} tags />
};

// ------------------------------------------------------------------ Radio

export interface RadioProps extends FieldProps {
  options: Option[];
  optionType?: "default" | "button";
}

function RadioField({ node, props, value, onChange, defaultHint, issues, readOnly }: ComposerComponentProps<RadioProps>) {
  const wrong = isWrongShape(value, isPrimitive);
  const unset = value === undefined;
  const unknown = !unset && !wrong && notInOptions(props.options, [value]).length > 0;
  const hint = unset
    ? hintText(defaultHint, (v) => labelOfOption(props.options, v))
    : unknown
      ? `当前值 ${displayValue(value)} 不在可选项中`
      : undefined;
  return (
    <FieldFrame
      className="xflow-composer-radio"
      nodeId={node.id}
      {...props}
      issues={issues}
      hint={hint}
      labelMode="group"
      groupRole={wrong ? "group" : null}
      stateClassName={unset ? "is-unset" : undefined}
    >
      {(ids) =>
        wrong ? (
          <RawValue value={value} expected="单个选项值" readOnly={readOnly} onClear={() => onChange?.(undefined)} />
        ) : (
          <div className="xflow-composer-radio-row">
            <AntRadio.Group
              rootClassName="xflow-composer-radio-group"
              name={ids.controlId}
              aria-labelledby={props.label ? ids.labelId : undefined}
              aria-describedby={ids.describedBy}
              aria-required={props.required || undefined}
              aria-invalid={ids.invalid || undefined}
              optionType={props.optionType ?? "default"}
              value={unset ? undefined : optionKey(value)}
              disabled={readOnly || props.disabled}
              options={radioOptions(props.options)}
              onChange={(event) => onChange?.(fromKey(props.options, String(event.target.value)))}
            />
            {!unset && !readOnly && !props.disabled ? (
              <ResetButton hasDefault={defaultHint !== undefined} label={props.label} onClick={() => onChange?.(undefined)} />
            ) : null}
          </div>
        )
      }
    </FieldFrame>
  );
}

export const Radio: ComposerComponent<RadioProps> = {
  type: "Radio",
  props: zodProps(
    z.strictObject({
      ...fieldShape,
      options: z.array(optionSchema),
      optionType: z.enum(["default", "button"]).optional()
    })
  ),
  bindings: { value: "scalar" },
  render: (p) => <RadioField {...p} />
};
