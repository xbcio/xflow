import { useRef } from 'react';

import { useElementWidth } from '../components/useElementWidth';
import { COMPACT_LEFT, COMPACT_RIGHT } from '../metrics';
import { WorkbenchSection } from './wb';

/**
 * A workbench in a fixed-width host. The shell decides from its own measured
 * width (not the window's), so a narrower host exercises the compact layout the
 * same way the standalone editor does — that is the whole point of the number
 * columns next to each frame.
 */
function CompactFrame({ id, width, note }: { id: string; width: number; note: string }) {
  const frameRef = useRef<HTMLDivElement>(null);
  const measured = useElementWidth(frameRef);
  const leftCollapsed = measured > 0 && measured < COMPACT_LEFT;
  const rightCollapsed = measured > 0 && measured < COMPACT_RIGHT;

  return (
    <figure className="xflow-pv-figure xflow-pv-compact-figure">
      <div className="xflow-pv-compact-readout" id={`${id}-readout`}>
        <strong>宿主 {width}px</strong>
        <span className="xflow-pv-mono">
          实测 {measured || '—'}px{measured > 0 && measured < width - 0.5 ? ' · 受窗口宽度限制' : ''}
        </span>
        <span>
          左栏 {leftCollapsed ? '抽屉' : '固定'}（断点 {COMPACT_LEFT}）
        </span>
        <span>
          属性 {rightCollapsed ? '抽屉' : '固定'}（断点 {COMPACT_RIGHT}）
        </span>
      </div>
      <div className="xflow-pv-compact-frame" style={{ width }} ref={frameRef}>
        <WorkbenchSection idPrefix={id} />
      </div>
      <figcaption>{note}</figcaption>
    </figure>
  );
}

/**
 * 5 · compact layout. Both frames are live workbenches, so the collapse is the
 * editor's own behaviour (automatic on first crossing, then manual) and not a
 * second mock of it: the rail button reopens 属性 as a drawer, and the top bar
 * folds 撤销 / 重做 / 校验 / 主题 into ⋯ once the left column is a drawer.
 */
export function CompactSection() {
  return (
    <>
      <CompactFrame
        id="compact-1280"
        width={1200}
        note="宿主 1200px（< 1280）：左栏自动折叠为抽屉、rail 可再打开，顶栏次要操作收进 ⋯；属性面板仍固定。"
      />
      <CompactFrame
        id="compact-1024"
        width={960}
        note="宿主 960px（< 1024）：两栏都折叠为抽屉，画布拿到全部宽度；rail 上的「属性」按钮是恢复入口。"
      />
    </>
  );
}
