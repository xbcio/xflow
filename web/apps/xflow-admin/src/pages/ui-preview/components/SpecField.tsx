import { WarningOutlined } from '@ant-design/icons';
import { Button, Input, InputNumber, Segmented, Select, Slider, Switch, Tooltip } from 'antd';
import type { ReactNode } from 'react';

import { cx } from './cx';
import type { FieldSpec } from '../data';

/** `10` → 10; anything else (`-`, `默认：30`, `1e3`) stays a placeholder. */
function toNumber(value: string | undefined, placeholder: boolean | undefined): number | undefined {
  if (placeholder || !value || !/^-?\d+(\.\d+)?$/.test(value)) return undefined;
  return Number(value);
}

/**
 * Renders one `FieldSpec` with real AntD controls (§6.4): label on top with the
 * required mark, the control itself, then the hint and the field-level message.
 * The message is `.xflow-pv-err` with a `data-tone`, so both severities come from
 * the token pair (`--xflow-warning` / `--xflow-danger`) instead of two styles.
 *
 * Two things are deliberately not left to the component: the focused sample is a
 * class (`.xflow-pv-focus`) rather than `autoFocus`, because focusing would
 * scroll a gallery, and `field.placeholder` means "render empty, use `value` as
 * the placeholder text", so the mockup's copy lives in one place.
 *
 * Width comes from Tailwind's `w-full` (utilities layer), not from this file, so
 * no rule here has to out-specify an AntD default just to stretch a control.
 */
export function SpecControl({ field }: { field: FieldSpec }) {
  const invalid = field.error?.tone === 'danger';
  const status = invalid ? ('error' as const) : undefined;
  const options = (field.options ?? (field.value ? [field.value] : [])).map((value) => ({ value, label: value }));
  const placeholder = field.placeholder ? field.value : undefined;
  const controlClass = cx('xflow-pv-spec-control w-full', field.focus && 'xflow-pv-focus');

  switch (field.control) {
    case 'select':
      return (
        <div className="xflow-pv-ctl-row">
          <Select className={controlClass} status={status} value={placeholder ? undefined : field.value} placeholder={placeholder} options={options} />
          {field.fx ? (
            <Tooltip title="表达式模式">
              <Button className="xflow-pv-fx" data-active="true">
                fx
              </Button>
            </Tooltip>
          ) : null}
        </div>
      );
    case 'multi':
      return (
        <Select
          className={controlClass}
          mode="multiple"
          status={status}
          defaultValue={field.options}
          options={options}
        />
      );
    case 'number':
      return (
        <span className={cx('xflow-pv-num', field.focus && 'xflow-pv-focus')}>
          <InputNumber
            className="xflow-pv-spec-control w-full"
            status={status}
            placeholder={placeholder}
            value={toNumber(field.value, field.placeholder)}
          />
          {field.unit ? <span className="xflow-pv-num-unit">{field.unit}</span> : null}
        </span>
      );
    case 'seg':
      return (
        <Segmented
          className="xflow-pv-spec-control"
          size="small"
          block
          value={field.value}
          options={field.options ?? (field.value ? [field.value] : [])}
        />
      );
    case 'slider':
      return (
        <span className="xflow-pv-slider-row">
          <Slider className="xflow-pv-slider" min={field.min} max={field.max} defaultValue={toNumber(field.value, false)} />
          <span className="xflow-pv-slider-value">
            {field.value}
            {field.unit ? ` ${field.unit}` : ''}
          </span>
        </span>
      );
    case 'switch':
      return (
        <span className="xflow-pv-switch-row">
          <Switch size="small" defaultChecked={field.switchOn ?? true} />
          <span className={cx('xflow-pv-switch-label', field.switchOn === false && 'xflow-pv-switch-label--off')}>
            {field.label}
          </span>
        </span>
      );
    case 'area':
      return (
        <Input.TextArea
          className={cx('xflow-pv-spec-control w-full', field.mono && 'xflow-pv-mono', field.focus && 'xflow-pv-focus')}
          status={status}
          rows={field.rows ?? 3}
          placeholder={placeholder}
          defaultValue={placeholder ? undefined : field.value}
          readOnly={field.readOnly}
        />
      );
    case 'expr':
      return (
        <span className={cx('xflow-pv-expr-box', field.focus && 'xflow-pv-focus')}>
          <Input
            className="xflow-pv-spec-control w-full"
            status={status}
            readOnly
            prefix={<span className="xflow-pv-var">{field.value}</span>}
            value="== 'vip'"
            suffix={
              <Tooltip title="表达式模式">
                <Button className="xflow-pv-fx" data-active="true">
                  fx
                </Button>
              </Tooltip>
            }
          />
        </span>
      );
    default:
      return (
        <Input
          className={cx(controlClass, field.mono && 'xflow-pv-mono')}
          status={status}
          placeholder={placeholder}
          defaultValue={placeholder ? undefined : field.value}
          readOnly={field.readOnly}
        />
      );
  }
}

/** Label + control + hint + message, the whole vertical field (§6.4). */
export function SpecField({ field, suffix }: { field: FieldSpec; suffix?: ReactNode }) {
  // A switch labels itself on the right; every other control labels on top.
  if (field.control === 'switch') {
    return (
      <div className="xflow-pv-field">
        <SpecControl field={field} />
      </div>
    );
  }
  return (
    <div className="xflow-pv-field" data-wide={field.wide ? 'true' : undefined}>
      <span className="xflow-pv-field-label">
        {field.label}
        {field.required ? (
          <span className="xflow-pv-req" title="必填">
            *
          </span>
        ) : null}
      </span>
      <SpecControl field={field} />
      {field.hint ? <span className="xflow-pv-hint">{field.hint}</span> : null}
      {field.error ? (
        <span className="xflow-pv-err" data-tone={field.error.tone}>
          <WarningOutlined className="xflow-pv-err-icon" />
          {field.error.text}
        </span>
      ) : null}
      {suffix}
    </div>
  );
}
