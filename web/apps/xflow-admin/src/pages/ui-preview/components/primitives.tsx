import { Button, Divider, Tooltip, type ButtonProps } from 'antd';
import type { HTMLAttributes, ReactNode } from 'react';

import type { IconKey } from '../data';
import { typeIcon, type Category, type Tone } from '../meta';
import { cx } from './cx';

/** 28px category block (20px in dense rows): `-bg-subtle` background, `-fg` icon (§6.1). */
export function CategoryIcon({
  category,
  icon,
  size = 'md',
}: {
  category: Category;
  icon: IconKey;
  size?: 'md' | 'sm';
}) {
  return (
    <span className={cx('xflow-pv-cat', size === 'sm' && 'xflow-pv-cat--sm')} data-cat={category}>
      {typeIcon[icon]}
    </span>
  );
}

/** Status chip: tone colour + icon + text, 20px pill (§4). */
export function StatusChip({
  tone,
  icon,
  label,
  children,
}: {
  tone: Tone;
  icon: ReactNode;
  label: ReactNode;
  children?: ReactNode;
}) {
  return (
    <span className="xflow-pv-chip" data-tone={tone}>
      <span className="xflow-pv-chip-icon">{icon}</span>
      <span>{label}</span>
      {children}
    </span>
  );
}

/** 40px panel title bar shared by 节点 / 大纲 / 属性 / 诊断台 (§6.0). */
export function PanelTitle({
  icon,
  title,
  titleId,
  count,
  className,
  children,
}: {
  icon: ReactNode;
  title: ReactNode;
  titleId?: string;
  count?: ReactNode;
  className?: string;
  children?: ReactNode;
}) {
  return (
    <div className={cx('xflow-pv-panel-title', className)}>
      <span className="xflow-pv-panel-icon">{icon}</span>
      <h3 id={titleId} className="xflow-pv-panel-heading">
        {title}
      </h3>
      {count === undefined ? null : <span className="xflow-pv-panel-count">{count}</span>}
      {children ? <span className="xflow-pv-panel-actions">{children}</span> : null}
    </div>
  );
}

/**
 * Every floating bar on the canvas (add node, run bar, reopen chip, zoom bar)
 * shares this shell: 34px, 1px border, 4px padding, radius-md, elev-2.
 */
export function Float({
  className,
  children,
  ...rest
}: { className?: string; children: ReactNode } & HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={cx('xflow-pv-float', className)} {...rest}>
      {children}
    </div>
  );
}

/**
 * Icon button with a required `aria-label` and an optional pressed state. `small`
 * is the 24px control used inside floating bars and panel title bars; the
 * default is the 32px top-bar control.
 */
export function IconButton({
  label,
  active,
  small,
  className,
  ...rest
}: ButtonProps & { label: string; small?: boolean; active?: boolean }) {
  return (
    <Tooltip title={label}>
      <Button
        type="text"
        size={small ? 'small' : 'middle'}
        aria-label={label}
        aria-pressed={active}
        data-active={active === undefined ? undefined : String(active)}
        className={cx(small ? 'xflow-pv-tool' : 'xflow-pv-icon-btn', className)}
        {...rest}
      />
    </Tooltip>
  );
}

/** Kept as the float-sized spelling of {@link IconButton}. */
export function FloatButton(props: ButtonProps & { label: string; active?: boolean }) {
  return <IconButton small {...props} />;
}

export function Port({
  children,
  error,
  handle,
}: {
  children: ReactNode;
  error?: boolean;
  /** 出句柄挂在这个 chip 的下沿（§6.2：有 error 出口的节点，main 句柄落在 main chip 上）。 */
  handle?: boolean;
}) {
  return (
    <span className={cx('xflow-pv-port', error && 'xflow-pv-port--error')}>
      {children}
      {handle ? <span className="xflow-pv-handle xflow-pv-handle--port" data-port-handle={String(children)} /> : null}
    </span>
  );
}

/**
 * Section header (§6.0): title, then an optional trailing chip or note. Shared so
 * the workbench, the inspector gallery and the composer all label a group the
 * same way.
 */
export function SectionLabel({
  title,
  extra,
  children,
}: {
  title: string;
  extra?: ReactNode;
  children?: ReactNode;
}) {
  return (
    <span className="xflow-pv-sec-label">
      <span className="xflow-pv-sec-title">{title}</span>
      {extra ? <span className="xflow-pv-sec-extra">{extra}</span> : null}
      {children}
    </span>
  );
}

export function Kbd({ children }: { children: ReactNode }) {
  return <span className="xflow-pv-kbd">{children}</span>;
}

export function VerticalSep({ className }: { className?: string }) {
  return <Divider orientation="vertical" className={cx('xflow-pv-sep mx-0', className)} />;
}
