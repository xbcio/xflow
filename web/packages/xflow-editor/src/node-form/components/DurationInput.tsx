// DurationInput (Doc C §3 `duration`, §4.2 common fields).
//
// - unit "string": a Go duration string (`30s`, `1h30m`). The text is shown
//   and written exactly as typed — never reformatted, on mount or later; a
//   human reading of a valid value is shown below.
// - unit "ns" / "ms": an integer of that unit on the wire (`/timeout` is
//   Go time.Duration nanoseconds, retry intervals are milliseconds). Shown
//   as a number in a human display unit (the largest unit that shows the
//   value whole); typing writes `round(number × unit)`. The unit select is
//   display-only: switching it re-expresses the same value, writing nothing.

import { InputNumber as AntInputNumber, Input as AntInput, Select as AntSelect } from "antd";
import { useState } from "react";
import { z } from "zod";
import { zodProps } from "@xflow/composer/core";
import { FieldFrame, RawValue } from "@xflow/composer/form";
import type { ComposerComponent, ComposerComponentProps } from "@xflow/composer/react";
import { bestUnit, displayUnits, formatGoDuration, humanizeNs, parseGoDuration, wireToNs } from "./duration";
import { chromeShape, cx, hintText, isExpressionString, NODEFORM, type ChromeProps } from "./shared";

export interface DurationInputProps extends ChromeProps {
  unit: "string" | "ns" | "ms";
}

const isFiniteNumber = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);

function StringDuration({ node, props, value, onChange, defaultHint, issues, readOnly }: ComposerComponentProps<DurationInputProps>) {
  const wrong = value !== undefined && typeof value !== "string";
  const text = typeof value === "string" ? value : "";
  const parsed = text !== "" && !isExpressionString(text) ? parseGoDuration(text) : null;
  const hint = parsed !== null ? `= ${humanizeNs(parsed)}` : undefined;
  return (
    <FieldFrame
      className={cx(`${NODEFORM}-duration`, `${NODEFORM}-duration--string`)}
      nodeId={node.id}
      label={props.label}
      description={props.description}
      required={props.required}
      disabled={props.disabled}
      issues={issues}
      hint={hint}
      labelMode={wrong ? "group" : "control"}
      stateClassName={value === undefined ? "is-unset" : undefined}
    >
      {(ids) =>
        wrong ? (
          <RawValue value={value} expected="Go duration 字符串" readOnly={readOnly} onClear={() => onChange?.(undefined)} />
        ) : (
          <AntInput
            id={ids.controlId}
            rootClassName={`${NODEFORM}-duration-text`}
            value={text}
            placeholder={value === undefined && defaultHint !== undefined ? hintText(defaultHint) : "例如 30s、5m、1h30m"}
            readOnly={readOnly}
            disabled={props.disabled}
            allowClear={!readOnly && !props.disabled}
            status={ids.status}
            spellCheck={false}
            aria-describedby={ids.describedBy}
            aria-invalid={ids.invalid || undefined}
            aria-required={props.required || undefined}
            onChange={(event) => onChange?.(event.target.value)}
          />
        )
      }
    </FieldFrame>
  );
}

function IntegerDuration({ node, props, value, onChange, defaultHint, issues, readOnly }: ComposerComponentProps<DurationInputProps>) {
  const wire = props.unit === "ms" ? "ms" : "ns";
  const units = displayUnits(wire);
  const wrong = value !== undefined && !isFiniteNumber(value);
  const numeric = isFiniteNumber(value) ? value : undefined;
  // Display unit: the user's pick, else the best fit of the value (or of the default).
  const [picked, setPicked] = useState<string | null>(null);
  const fallbackUnit = bestUnit(numeric ?? (isFiniteNumber(defaultHint) ? defaultHint : undefined), wire);
  const unit = units.find((candidate) => candidate.key === picked) ?? fallbackUnit;
  const wireName = wire === "ns" ? "纳秒" : "毫秒";
  const nsPerWire = wireToNs(wire);
  const shown = numeric === undefined ? null : numeric / unit.factor;
  const hint =
    numeric !== undefined
      ? `= ${formatGoDuration(numeric * nsPerWire)}（线上值 ${numeric} ${wireName}）`
      : `以${wireName}整数保存`;
  const placeholder = numeric === undefined && isFiniteNumber(defaultHint) ? hintText(formatGoDuration(defaultHint * nsPerWire)) : undefined;
  const disabled = readOnly || props.disabled;

  return (
    <FieldFrame
      className={cx(`${NODEFORM}-duration`, `${NODEFORM}-duration--${wire}`)}
      nodeId={node.id}
      label={props.label}
      description={props.description}
      required={props.required}
      disabled={props.disabled}
      issues={issues}
      hint={wrong ? undefined : hint}
      labelMode={wrong ? "group" : "control"}
      stateClassName={value === undefined ? "is-unset" : undefined}
    >
      {(ids) =>
        wrong ? (
          <RawValue value={value} expected={`${wireName}整数`} readOnly={readOnly} onClear={() => onChange?.(undefined)} />
        ) : (
          <div className={`${NODEFORM}-duration-row`}>
            <AntInputNumber<number>
              id={ids.controlId}
              rootClassName={`${NODEFORM}-duration-number`}
              value={shown}
              placeholder={placeholder}
              readOnly={readOnly}
              disabled={props.disabled}
              status={ids.status}
              aria-describedby={ids.describedBy}
              aria-invalid={ids.invalid || undefined}
              aria-required={props.required || undefined}
              onChange={(next) => onChange?.(isFiniteNumber(next) ? Math.round(next * unit.factor) : undefined)}
            />
            <AntSelect<string>
              aria-label={`${props.label ?? "时长"}的显示单位`}
              classNames={{ root: `${NODEFORM}-duration-unit`, popup: { root: `${NODEFORM}-select-popup` } }}
              value={unit.key}
              options={units.map((candidate) => ({ value: candidate.key, label: candidate.label }))}
              disabled={disabled}
              onChange={(key) => setPicked(key)}
            />
          </div>
        )
      }
    </FieldFrame>
  );
}

export const DurationInput: ComposerComponent<DurationInputProps> = {
  type: "DurationInput",
  props: zodProps(z.strictObject({ ...chromeShape, unit: z.enum(["string", "ns", "ms"]) })),
  bindings: { value: "scalar" },
  render: (p) => (p.props.unit === "string" ? <StringDuration {...p} /> : <IntegerDuration {...p} />)
};
