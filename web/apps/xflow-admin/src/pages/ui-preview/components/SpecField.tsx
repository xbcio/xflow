import { WarnIcon } from '@xflow/icons';
import { Button, Input, InputNumber, Segmented, Select, Slider, Switch, Tooltip } from 'antd';
import type { ReactNode } from 'react';

import { cx } from './cx';
import { FieldHelp } from './primitives';
import type { FieldSpec } from '../data';

/** `10` → 10; anything else (`-`, `默认：30`, `1e3`) stays a placeholder. */
function toNumber(value: string | undefined, placeholder: boolean | undefined): number | undefined {
  if (placeholder || !value || !/^-?\d+(\.\d+)?$/.test(value)) return undefined;
  return Number(value);
}

/**
 * Renders one `FieldSpec` with real AntD controls (§6.4): label in the left
 * column with the required mark, the control in the right one, then the hint and
 * the field-level message under the control (`layout: 'top'` stacks them).
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
          classNames={{ item: 'xflow-pv-tag-item' }}
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
        <span className="xflow-pv-ctl-line">
          {/*
           * Card-internal segmented (§6.4): the track takes the `field` colour
           * because it sits on the panel-raised card body; the top bar's
           * segmented stays on the hover track. Semantic hooks only — see
           * `.xflow-pv-seg-card` in ui-preview.css.
           */}
          <Segmented
            aria-label={field.label}
            className="xflow-pv-spec-control xflow-pv-seg-card"
            classNames={{ item: 'xflow-pv-seg-item' }}
            size="small"
            block
            value={field.value}
            options={field.options ?? (field.value ? [field.value] : [])}
          />
        </span>
      );
    case 'slider':
      return (
        <span className="xflow-pv-ctl-line">
          <Slider className="xflow-pv-slider" min={field.min} max={field.max} defaultValue={toNumber(field.value, false)} />
          <InputNumber
            className="xflow-pv-spec-control xflow-pv-slider-num"
            aria-label={field.label}
            min={field.min}
            max={field.max}
            suffix={field.unit}
            defaultValue={toNumber(field.value, false)}
          />
        </span>
      );
    case 'switch':
      return (
        <span className="xflow-pv-ctl-line">
          <Switch size="small" aria-label={field.label} defaultChecked={field.switchOn ?? true} />
        </span>
      );
    case 'pair':
      return (
        <span className="xflow-pv-pair">
          {(field.pair ?? []).map((bound) => (
            <InputNumber
              key={bound.prefix}
              className="xflow-pv-spec-control"
              aria-label={`${field.label}${bound.prefix}`}
              status={status}
              prefix={bound.prefix}
              defaultValue={toNumber(bound.value, false)}
            />
          ))}
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

/**
 * Label + control + hint + message, the whole field (§6.4). `side` (default) puts
 * the label in the left column and everything else in the right one, so every
 * control in a panel shares one left edge; `top` is the full-row exception. A
 * field's own `layout` wins over the prop.
 */
export function SpecField({
  field,
  suffix,
  layout = 'side',
}: {
  field: FieldSpec;
  suffix?: ReactNode;
  layout?: 'side' | 'top';
}) {
  const side = (field.layout ?? layout) === 'side';
  return (
    <div className={cx('xflow-pv-field', side && 'xflow-pv-field--side')} data-wide={field.wide ? 'true' : undefined}>
      <span className="xflow-pv-field-label">
        {field.label}
        {field.required ? (
          <span className="xflow-pv-req" title="必填">
            *
          </span>
        ) : null}
        {field.help ? <FieldHelp text={field.help} /> : null}
      </span>
      <SpecControl field={field} />
      {field.hint ? <span className="xflow-pv-hint">{field.hint}</span> : null}
      {field.error ? (
        <span className="xflow-pv-err" data-tone={field.error.tone}>
          <WarnIcon className="xflow-pv-err-icon" />
          {field.error.text}
        </span>
      ) : null}
      {suffix}
    </div>
  );
}
