import { Button } from 'antd';
import type { ReactNode } from 'react';

import type { IconKey } from '../data';
import { typeIcon, type Tone } from '../meta';
import { cx } from './cx';

export interface BannerAction {
  label: string;
  disabled?: boolean;
}

/**
 * 横幅（§6.5.1 / §6.10）：异常态卡片、发布对话框的校验警告、保存失败态共用同一个壳。
 *
 * `showActions` 是给冲突对话框用的：那里页脚已经有一组「重新加载线上版本 / 另存为
 * 副本」，横幅再画一遍就是同一个对话框里同一个操作出现两次（G4）。关掉的只是按钮，
 * 文案与 tone 仍然来自 `wbExceptionCards` 那一份 —— 两处不可能说两种话。
 */
export function Banner({
  tone,
  icon,
  strong,
  text,
  actions,
  showActions = true,
  className,
}: {
  tone: Tone;
  icon: IconKey;
  strong?: string;
  text: ReactNode;
  actions?: BannerAction[];
  showActions?: boolean;
  className?: string;
}) {
  const shown = showActions ? (actions ?? []) : [];
  return (
    <div className={cx('xflow-pv-banner', className)} data-tone={tone}>
      <span className="xflow-pv-banner-icon">{typeIcon[icon]}</span>
      {strong ? <strong className="xflow-pv-banner-strong">{strong}</strong> : null}
      <span className="xflow-pv-banner-text">{text}</span>
      {shown.length ? (
        <span className="xflow-pv-banner-actions">
          {shown.map((action) => (
            <Button size="small" key={action.label} disabled={action.disabled}>
              {action.label}
            </Button>
          ))}
        </span>
      ) : null}
    </div>
  );
}
