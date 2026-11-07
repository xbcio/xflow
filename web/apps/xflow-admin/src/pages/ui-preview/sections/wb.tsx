import {
  CheckIcon,
  CodeIcon,
  ConnectIcon,
  CopyIcon,
  CursorIcon,
  DockDownIcon,
  DockMaxIcon,
  DownIcon,
  EditIcon,
  FitIcon,
  GridIcon,
  HelpIcon,
  HistoryIcon,
  LocateIcon,
  MoreIcon,
  NodeIndexIcon,
  PlayIcon,
  PlusIcon,
  RedoIcon,
  SaveIcon,
  SearchIcon,
  ShieldIcon,
  SideLeftIcon,
  SideRightIcon,
  SkipIcon,
  SlidersIcon,
  UndoIcon,
  UploadIcon,
  VarIcon,
  WarnIcon,
} from '@xflow/icons';
import { Button, Collapse, Input, Segmented, Select, Switch, Tabs, Tooltip } from 'antd';
import {
  Fragment,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type CSSProperties,
  type KeyboardEvent as ReactKeyboardEvent,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
} from 'react';

import { cx } from '../components/cx';
import { Minimap } from '../components/Minimap';
import { NodeLibrary, Outline } from '../components/LeftPanels';
import { NodeCard } from '../components/NodeCard';
import { CategoryIcon, FieldHelp, Float, IconButton, Kbd, PanelTitle, SectionLabel, StatusChip, VerticalSep } from '../components/primitives';
import { Ruler } from '../components/Ruler';
import { RunBar } from '../components/RunBar';
import { Todo } from '../components/Todo';
import { useCanvasMetrics } from '../components/useCanvasMetrics';
import { useElementWidth } from '../components/useElementWidth';
import { ZoomBar } from '../components/ZoomBar';
import { edgePathOf } from '../edge-path';
import {
  edgeKindOf,
  wbChecksPassed,
  wbDefinitionSummary,
  wbEdgeLabels,
  wbEdges,
  wbExec,
  wbFrames,
  wbGraphStats,
  wbIssues,
  wbLibraryCommon,
  wbLibraryGroups,
  wbLibraryTotal,
  wbNodeMix,
  wbNodes,
  wbPendingChanges,
  wbOutline,
  wbOutlineTotal,
  wbRunVariant,
  wbWorkflow,
  type PreviewFrame,
} from '../data';
import { COMPACT_LEFT, COMPACT_RIGHT, fitScale, FRAME_LABEL_OFFSET, RULER, canvasPanY } from '../metrics';
import { typeIcon } from '../meta';
import { clampPanelWidth, fitPanelWidths, PANEL_WIDTHS, RAIL_WIDTH, CANVAS_MIN, readStoredWidth, RESIZE_STEP, storePanelWidth, type PanelSide } from './panel-resize';

const wbRun = wbRunVariant;

/**
 * §6.0（2026-11-06）：停靠态面板的内侧边线手柄。平时不可见，悬停 / 拖动时
 * 显示 2px primary 竖线；指针移动超过 3px 才设 pointer capture，双击才能复位。
 * 手柄挂在 `.xflow-pv-wb-body`（position: relative）下，而不是面板里，避免
 * 属性面板的 `overflow: hidden` 裁掉一半命中区。
 */
function PanelResizer({
  side,
  value,
  limit,
  onChange,
  onReset,
  onDragChange,
}: {
  side: PanelSide;
  value: number;
  limit: number;
  onChange: (width: number) => void;
  onReset: () => void;
  onDragChange: (dragging: boolean) => void;
}) {
  const { min, max, label } = PANEL_WIDTHS[side];
  const [dragging, setDragging] = useState(false);
  const drag = useRef<{ pointerId: number; startX: number; startWidth: number; moved: boolean } | null>(null);
  const cleanup = useRef<(() => void) | null>(null);
  const set = (width: number) => onChange(clampPanelWidth(side, width, limit));

  useEffect(() => () => cleanup.current?.(), []);

  const onPointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return;
    // 按下会顺手选中文字并移动焦点；这里先掐掉，再把焦点交给手柄自己。
    event.preventDefault();
    const handle = event.currentTarget;
    handle.focus();
    const start = { pointerId: event.pointerId, startX: event.clientX, startWidth: value, moved: false };
    drag.current = start;
    const move = (e: PointerEvent) => {
      const state = drag.current;
      if (!state || e.pointerId !== state.pointerId) return;
      const delta = e.clientX - state.startX;
      if (!state.moved && Math.abs(delta) <= 3) return;
      if (!state.moved) {
        state.moved = true;
        setDragging(true);
        onDragChange(true);
        try {
          handle.setPointerCapture(state.pointerId);
        } catch {
          // 指针已经离开；document 上的监听仍会继续跟随。
        }
      }
      const next = side === 'left' ? state.startWidth + delta : state.startWidth - delta;
      set(next);
    };
    const end = (e: PointerEvent) => {
      if (e.pointerId !== start.pointerId) return;
      cleanup.current?.();
      cleanup.current = null;
      drag.current = null;
      setDragging(false);
      onDragChange(false);
    };
    document.addEventListener('pointermove', move);
    document.addEventListener('pointerup', end);
    document.addEventListener('pointercancel', end);
    cleanup.current = () => {
      document.removeEventListener('pointermove', move);
      document.removeEventListener('pointerup', end);
      document.removeEventListener('pointercancel', end);
    };
  };


  const onKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    // ←/→ 朝面板变宽的方向走：左栏 → 变宽，属性面板 ← 变宽。
    const grow = side === 'left' ? 'ArrowRight' : 'ArrowLeft';
    const shrink = side === 'left' ? 'ArrowLeft' : 'ArrowRight';
    if (event.key === grow) {
      event.preventDefault();
      set(value + RESIZE_STEP);
    } else if (event.key === shrink) {
      event.preventDefault();
      set(value - RESIZE_STEP);
    } else if (event.key === 'Home') {
      event.preventDefault();
      set(min);
    } else if (event.key === 'End') {
      event.preventDefault();
      set(max);
    } else if (event.key === 'Enter') {
      event.preventDefault();
      onReset();
    }
  };

  return (
    <div
      className="xflow-pv-resizer"
      data-side={side}
      data-dragging={dragging ? 'true' : undefined}
      role="separator"
      aria-orientation="vertical"
      aria-label={label}
      aria-valuemin={min}
      aria-valuemax={Math.max(min, Math.min(max, limit))}
      aria-valuenow={value}
      tabIndex={0}
      onPointerDown={onPointerDown}
      onKeyDown={onKeyDown}
      onDoubleClick={onReset}
    />
  );
}

/**
 * 左栏上下分隔线（§6.0.1 左栏重设计）：8px 命中、1px 可见线，role=separator、
 * aria-orientation=horizontal；↑↓ 每次 28，双击恢复自适应高度。
 */
function LibDivider({ onResize, onReset }: { onResize: (delta: number) => void; onReset: () => void }) {
  const drag = useRef<{ pointerId: number; y: number } | null>(null);

  const onPointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return;
    event.preventDefault();
    const handle = event.currentTarget;
    handle.focus();
    handle.setPointerCapture(event.pointerId);
    drag.current = { pointerId: event.pointerId, y: event.clientY };
    const move = (e: PointerEvent) => {
      const state = drag.current;
      if (!state || e.pointerId !== state.pointerId) return;
      // 分隔线上移 → 大纲变高。
      onResize(state.y - e.clientY);
      state.y = e.clientY;
    };
    const end = (e: PointerEvent) => {
      if (!state0(e)) return;
      drag.current = null;
      handle.removeEventListener('pointermove', move);
      handle.removeEventListener('pointerup', end);
      handle.removeEventListener('pointercancel', end);
    };
    const state0 = (e: PointerEvent) => drag.current?.pointerId === e.pointerId;
    handle.addEventListener('pointermove', move);
    handle.addEventListener('pointerup', end);
    handle.addEventListener('pointercancel', end);
  };
  const onKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      onResize(28);
    } else if (event.key === 'ArrowDown') {
      event.preventDefault();
      onResize(-28);
    }
  };

  return (
    <div
      className="xflow-pv-lib-divider"
      role="separator"
      aria-orientation="horizontal"
      aria-label="调整节点库与大纲高度"
      tabIndex={0}
      onPointerDown={onPointerDown}
      onKeyDown={onKeyDown}
      onDoubleClick={onReset}
    />
  );
}

/**
 * Same glyph as the real editor's theme trigger (`ThemeContrastIcon` in
 * `web/packages/xflow-editor/src/index.tsx`): a half-filled circle. AntD has no
 * contrast glyph, and its paint bucket reads as a colour picker.
 */
function ThemeContrastIcon() {
  return (
    <svg aria-hidden="true" className="xflow-pv-theme-glyph" viewBox="0 0 24 24">
      <circle cx="12" cy="12" r="8.5" />
      <path d="M12 3.5v17a8.5 8.5 0 0 0 0-17z" fill="currentColor" />
    </svg>
  );
}

/**
 * Topbar fold thresholds (§6.3), re-measured 2026-11-07 on this header in a
 * real browser after the save key gained its icon. Measured on the header
 * itself rather than the window; both side tracks are equal 1fr, so the wider
 * cluster sets every threshold: header = 48 (24 padding + 2 × 12 gaps) + that
 * tier's tools + 2 × max(right natural, left width).
 *
 *   right natural: 533.36 (tier 0) / 412.36 (tier 1–3) / 297 (tier 4, the mode
 *   switch is icon-only and the ⌘S keycap is gone). The save key is 110.36
 *   with the icon (89.36 without) and 73 at tier 4, so the icon and its 8px gap
 *   widened tiers 0–3 by 21px.
 *   left width = logo 28 + gap 8 + title row, sample name 客户同步, dirty
 *   (version chip + unsaved chip). The subtitle ellipsizes, so it is excluded:
 *   396.19 at tier 0–1 (spec chip still shown) / 327.78 at tier 2–4.
 *   tools: 298 / 139 (tier 3 folds the canvas extras into ⋯).
 *
 *   tier 0: 48 + 298 + 2 × 533.36 = 1412.72 → 1413 (the right cluster decides)
 *   tier 1: 48 + 298 + 2 × 412.36 = 1170.72 → 1171 (right wins by 3.28px over
 *   the 396.19 left; left alone would be 1138.38)
 *   tier 2 hides the subtitle and spec chip, but the right and tools do not
 *   change and the left was not deciding, so its fit stays 1170.72 → 1171.
 *   Tiers 1 and 2 therefore share one boundary: fold 2 buys no width and is
 *   never reported — below 1171 the bar goes straight to tier 3.
 *   tier 3: 48 + 139 + 2 × 412.36 = 1011.72 → 1012, so tier 4 starts there.
 *   tier 4 makes the mode switch icon-only (names move to tooltips) and drops
 *   the ⌘S keycap: 48 + 139 + 2 × 327.78 = 842.56 → 843 is the bottom of the
 *   supported range (the left cluster decides; right alone would be 781).
 *   Below it the title itself is squeezed, and there is no further fold step.
 */
const TOP_FOLD_1 = 1413;
const TOP_FOLD_2 = 1171;
const TOP_FOLD_3 = 1171;
const TOP_FOLD_4 = 1012;

function RailButton({
  icon,
  text,
  label,
  active,
  onClick,
}: {
  icon: ReactNode;
  text: string;
  label: string;
  active?: boolean;
  onClick?: () => void;
}) {
  return (
    <Button
      type="text"
      className="xflow-pv-rail-btn"
      aria-label={label}
      aria-pressed={active}
      data-active={active ? 'true' : undefined}
      onClick={onClick}
    >
      <span className="flex flex-col items-center gap-[3px]">
        {icon}
        <span className="xflow-pv-rail-text">{text}</span>
      </span>
    </Button>
  );
}

/** Frame = visual section (§6.2) or co-location group, which must look different. */
function Frame({ frame }: { frame: PreviewFrame }) {
  return (
    <div
      className={cx('xflow-pv-frame', frame.colo && 'xflow-pv-frame--colo', frame.labelRight && 'xflow-pv-frame--label-right')}
      data-cat={frame.category}
      style={{ left: frame.left, top: frame.top, width: frame.width, height: frame.height }}
    >
      <span title={frame.labelTitle}>
        {frame.labelIcon ? (
          <span className="xflow-pv-frame-icon">{typeIcon[frame.labelIcon]}</span>
        ) : null}
        {frame.labelTodo ? (
          <Todo label={frame.labelTodo}>
            <span className="xflow-pv-frame-label">
              {frame.label}
              {frame.labelMono ? <span className="xflow-pv-mono">{frame.labelMono}</span> : null}
            </span>
          </Todo>
        ) : (
          <span className="xflow-pv-frame-label">
            {frame.label}
            {frame.labelMono ? <span className="xflow-pv-mono">{frame.labelMono}</span> : null}
          </span>
        )}
      </span>
    </div>
  );
}

function Field({
  label,
  help,
  children,
  layout = 'side',
}: {
  label: ReactNode;
  /** Field description (D-3): behind the label's `?`, not a permanent line. */
  help?: { label: string; text: string };
  children: ReactNode;
  layout?: 'side' | 'top';
}) {
  return (
    <div className={cx('xflow-pv-field', layout === 'side' && 'xflow-pv-field--side')}>
      <span className="xflow-pv-field-label">
        {label}
        {help ? <FieldHelp text={help.text} /> : null}
      </span>
      {children}
    </div>
  );
}

/**
 * 配置 tab（§2.2，2026-11-04 定稿）：基础信息不可折叠，请求是首屏唯一的
 * 主卡；罕用的头/查询、高级、执行策略、备注默认折叠，卡头用摘要代替内容。
 * 「错误与重试」卡被执行策略取代，不再出现。
 */
function InspectorConfig() {
  return (
    <Collapse
      ghost
      className="xflow-pv-secs xflow-pv-secs--flat"
      defaultActiveKey={['base', 'request']}
      expandIconPlacement="start"
      expandIcon={({ isActive }) => <DownIcon style={{ transform: isActive ? undefined : 'rotate(-90deg)' }} />}
      classNames={{ header: 'xflow-pv-sec-head', body: 'xflow-pv-sec-body p-0' }}
      items={[
        {
          key: 'base',
          className: 'xflow-pv-sec-fixed',
          /*
           * 基础信息只放身份字段，展开且不可折叠：折起来会把它变成一屏里最先
           * 消失的东西，而改名是面板里最常用的动作。`collapsible: 'icon'` 加上
           * 隐藏箭头，卡头就没有可点的开关了。
           */
          collapsible: 'icon',
          showArrow: false,
          label: <SectionLabel title="基础信息" icon="info" />,
          children: (
            <div className="xflow-pv-sec-fields">
              <Field
                help={{ label: '节点名称', text: '引用名：连线和表达式靠它找到节点，改名会同步更新引用' }}
                label={
                  <>
                    节点名称
                    <span className="xflow-pv-req" title="必填">
                      *
                    </span>
                  </>
                }
              >
                <Input defaultValue="fetch_profile" />
              </Field>
              <Field
                label="显示名"
                help={{ label: '显示名', text: '只改画布上显示的文字，不影响引用；留空时显示节点名称' }}
              >
                <Input defaultValue="拉取客户档案" placeholder="fetch_profile" />
              </Field>
            </div>
          ),
        },
        {
          key: 'request',
          label: (
            <SectionLabel title="请求" icon="globe">
              <StatusChip tone="success" icon={<CheckIcon />} label="有效" />
            </SectionLabel>
          ),
          children: (
            <div className="xflow-pv-sec-fields">
              <Field label="方法">
                <Select
                  className="w-full"
                  defaultValue="GET"
                  options={[
                    { value: 'GET', label: 'GET' },
                    { value: 'POST', label: 'POST' },
                  ]}
                />
              </Field>
              {/* D2/D3: only TextArea-style controls and pure expressions take
                  the whole row; a template field keeps the side label column. */}
              <Field
                help={{ label: 'URL', text: '可用变量：$input · $nodes · $vars · $params' }}
                label={
                  <>
                    URL
                    <span className="xflow-pv-req" title="必填">
                      *
                    </span>
                  </>
                }
              >
                <div className="flex gap-1.5">
                  {/* The side column cannot hold the URL and a variable chip: the
                      editor renders this field as an input too, so the sample is
                      the same ellipsized control rather than a 0-width text. */}
                  <Input
                    className="xflow-pv-expr-control xflow-pv-spec-control w-full xflow-pv-focus"
                    defaultValue="https://crm.example.test/api/customers{{ $input.id }}"
                  />
                  <Button className="xflow-pv-fx" aria-pressed data-active="true">
                    fx
                  </Button>
                </div>
              </Field>
              <Field label="认证">
                <Select
                  className="w-full"
                  defaultValue="crm_token"
                  options={[{ value: 'crm_token', label: 'crm_token' }]}
                />
              </Field>
              {/* §2.3：http 主卡里 method/url/auth/body 同屏；body 只在 json 模式出现。 */}
              <Field label="请求体" layout="top">
                <Input.TextArea rows={2} defaultValue={'{\n  "tier": "vip"\n}'} />
              </Field>
            </div>
          ),
        },
        {
          key: 'headers',
          label: (
            <SectionLabel title="请求头与查询" icon="link" extra="已填 2 项" />
          ),
          children: (
            <div className="xflow-pv-sec-fields">
              <Field label="请求头">
                <Input defaultValue="Accept: application/json" />
              </Field>
              <Field label="查询参数">
                <Input defaultValue="page: 1" />
              </Field>
            </div>
          ),
        },
        {
          key: 'advanced',
          label: (
            <SectionLabel title="高级" icon="grid" extra="未设置" />
          ),
          children: (
            <div className="xflow-pv-sec-fields">
              <Field label="编码模式">
                <Select
                  className="w-full"
                  defaultValue="json"
                  options={[
                    { value: 'json', label: 'json' },
                    { value: 'raw', label: 'raw' },
                  ]}
                />
              </Field>
              <Field label="原始请求体（base64）">
                <Input placeholder="仅 raw 模式可编辑" />
              </Field>
              <Field label="选项">
                <Select
                  className="w-full"
                  defaultValue="默认"
                  options={[{ value: '默认', label: '默认' }]}
                />
              </Field>
            </div>
          ),
        },
        {
          key: 'execution',
          label: <SectionLabel title="执行策略" icon="shield" extra="继承工作流 · 重试 2 次" />,
          children: (
            <div className="xflow-pv-sec-fields">
              <Field label="错误处理">
                <Select
                  className="w-full"
                  defaultValue="继承工作流（停止）"
                  options={[{ value: '继承工作流（停止）', label: '继承工作流（停止）' }]}
                />
              </Field>
              {/* 与参数里的「审批超时」「等待超时」区分开（§2.3）。 */}
              <Field label="节点执行超时">
                <Input placeholder="继承工作流（30s）" />
              </Field>
              <Field label="重试">
                <Input defaultValue="3 次" />
              </Field>
              <Field label="Runner 选择">
                <Select
                  className="w-full"
                  defaultValue="继承工作流"
                  options={[{ value: '继承工作流', label: '继承工作流' }]}
                />
              </Field>
              <Field label="禁用">
                <Switch size="small" aria-label="禁用节点" />
              </Field>
            </div>
          ),
        },
        {
          key: 'notes',
          label: <SectionLabel title="备注" icon="flag" extra="无" />,
          children: (
            <div className="xflow-pv-sec-fields">
              <Input.TextArea rows={2} placeholder="给同事看的说明，不参与运行" />
            </div>
          ),
        },
      ]}
    />
  );
}

function StatusBar() {
  return (
    <div className="xflow-pv-statusbar">
      <span className="xflow-pv-status-ok">
        <CheckIcon className="xflow-pv-status-icon" />
        校验通过
      </span>
      <span className="xflow-pv-status-warn">
        <WarnIcon className="xflow-pv-status-icon" />
        {wbIssues.filter((issue) => issue.severity === 'warning').length} 警告
      </span>
      <VerticalSep />
      <span className="flex items-center gap-1.5">
        最近运行 <span className="xflow-pv-mono">{wbExec.id}</span>
      </span>
      <span className="flex-1" />
      {/* `data-graph-stats`：状态栏左边紧挨着「最近运行 exec-8432」，文字拼起来
          会把节点数读成 84327，所以图规模由这个 span 自己报（S9 核对用）。 */}
      <span className="xflow-pv-faint" data-graph-stats={`${wbGraphStats.nodes}/${wbGraphStats.edges}`}>
        {wbGraphStats.nodes} 节点 · {wbGraphStats.edges} 连线
      </span>
    </div>
  );
}

function Diagnostics({ idPrefix }: { idPrefix: string }) {
  return (
    <div className="xflow-pv-drawer">
      <div className="xflow-pv-drawer-head">
        <PanelTitle icon={<NodeIndexIcon />} title="诊断台" titleId={`${idPrefix}-diag`} />
        <Tabs
          size="small"
          className="xflow-pv-drawer-tabs"
          defaultActiveKey="issues"
          classNames={{ content: 'hidden', body: 'hidden' }}
          items={[
            { key: 'issues', label: `问题 ${wbIssues.length}`, children: null },
            { key: 'logs', label: '运行日志', children: null },
            { key: 'input', label: '输入', children: null },
            { key: 'output', label: '输出', children: null },
          ]}
        />
        <span className="xflow-pv-drawer-actions">
          <IconButton small label="扩展到画布高度" icon={<DockMaxIcon />} />
          <IconButton small label="收起诊断台 ⌘J" icon={<DockDownIcon />} />
        </span>
      </div>
      <div className="xflow-pv-issues">
        {wbIssues.map((issue) => (
          <div key={issue.id} className="xflow-pv-issue" data-tone={issue.severity}>
            {/* The wrapper carries the tone; `.xflow-icon` strokes in
             * `currentColor`, so the wrapper's colour reaches the glyph. */}
            <span className="xflow-pv-issue-sev">
              <WarnIcon />
            </span>
            <span>{issue.target}</span>
            <span>{issue.text}</span>
            <Button type="link" size="small" className="xflow-pv-issue-action">
              {issue.action}
            </Button>
          </div>
        ))}
        <div className="xflow-pv-issue-pass">
          <span className="xflow-pv-issue-ok">
            <CheckIcon />
          </span>
          {wbChecksPassed}
          <Button type="link" size="small" className="ml-auto">
            展开
          </Button>
        </div>
      </div>
      <aside className="xflow-pv-drawer-side" aria-label="摘要">
        <div>
          <h4 className="xflow-pv-drawer-side-title">定义摘要</h4>
          <dl className="xflow-pv-kvlist">
            <dt>节点</dt>
            {/*
             * Count + mix are composed here rather than read off a pre-joined
             * string: each category is its own `nowrap` span, so the row can only
             * wrap on a `·` (see `.xflow-pv-mix-item`). `title` carries the same
             * text flat, for when the wrap cuts a category off the fold.
             */}
            <dd className="xflow-pv-kvlist-wrap" title={wbGraphStats.mix}>
              {wbGraphStats.nodes}
              {wbNodeMix.length > 0 ? (
                <>
                  （
                  {wbNodeMix.map((entry, index) => (
                    <Fragment key={entry.label}>
                      <span className="xflow-pv-mix-item">
                        {entry.label} {entry.count}
                      </span>
                      {/* The spaces around the `·` are written as one string so JSX
                          keeps them: the trailing one is the row's only break
                          opportunity, and a wrap after `·` keeps every label
                          whole, which `overflow-wrap: anywhere` did not. */}
                      {index < wbNodeMix.length - 1 ? <span className="xflow-pv-mix-sep">{' · '}</span> : null}
                    </Fragment>
                  ))}
                  ）
                </>
              ) : null}
            </dd>
            <dt>连线</dt>
            <dd>{wbDefinitionSummary.edges}</dd>
            <dt>凭证</dt>
            <dd className="xflow-pv-mono">{wbDefinitionSummary.credentials}</dd>
            <dt>版本</dt>
            <dd>{wbDefinitionSummary.version}</dd>
          </dl>
        </div>
        <div>
          <h4 className="xflow-pv-drawer-side-title">最近运行</h4>
          <dl className="xflow-pv-kvlist">
            <dt>执行</dt>
            <dd className="xflow-pv-mono">{wbExec.id}</dd>
            <dt>当前节点</dt>
            <dd>{wbExec.currentNode}</dd>
            <dt>命中分支</dt>
            <dd>{wbExec.branch}</dd>
          </dl>
        </div>
      </aside>
    </div>
  );
}

/**
 * §6.2 canvas: rulers (on by default), one floating add-node bar, the run bar,
 * the reopen chip when the inspector is collapsed, the zoom bar and the minimap.
 * Canvas content is panned down by `canvasPanY` so nothing sits under the
 * floating row — the padding is reserved in screen pixels, exactly like a
 * fitView with a top inset.
 */
export function WorkbenchSection({ idPrefix = 'pt' }: { idPrefix?: string } = {}) {
  const canvasRef = useRef<HTMLDivElement>(null);
  const shellRef = useRef<HTMLDivElement>(null);
  const topRef = useRef<HTMLElement>(null);
  const libRef = useRef<HTMLElement>(null);
  const inspRef = useRef<HTMLElement>(null);
  /** Which panel asked for focus back when it collapsed (§6.0, 2026-11-06). */
  const focusReturn = useRef<'left' | 'right' | null>(null);
  /**
   * `null` means "follow the fitView"; a number is the user's manual zoom. The
   * mock opens fitted rather than at a fixed 62% (the mockup's value), because
   * a hand-picked zoom is what let the graph run off the canvas in the first
   * place — see `fitScale`.
   */
  const [zoom, setZoom] = useState<number | null>(null);
  const [leftOpen, setLeftOpen] = useState(true);
  const [rightOpen, setRightOpen] = useState(true);
  /** §6.0（2026-11-06）：停靠态面板宽度，收起再展开保持；存 localStorage。 */
  const [leftWidth, setLeftWidth] = useState(() => readStoredWidth('left'));
  const [rightWidth, setRightWidth] = useState(() => readStoredWidth('right'));
  const [resizing, setResizing] = useState<PanelSide | null>(null);
  const [ruler, setRuler] = useState(true);
  // 展示态：这一页按 mockup 从打开开始；产品默认关（spec §6.2，preview 包实现）。
  const [minimap, setMinimap] = useState(true);
  const [selectedId, setSelectedId] = useState('fetch_profile');
  /** 左栏上下分隔线拖过之后的大纲高度；null = 按内容自适应（§6.0.1）。 */
  const [outlineHeight, setOutlineHeight] = useState<number | null>(null);
  const resizeOutline = (delta: number) => {
    const lib = libRef.current;
    const pane = lib?.querySelector<HTMLElement>('.xflow-pv-lib-pane--outline');
    if (!lib || !pane) return;
    const max = lib.getBoundingClientRect().height / 2;
    // 最小 = 标题栏 40 + 3 行 × 28。
    const next = Math.min(max, Math.max(40 + 3 * 28, pane.getBoundingClientRect().height + delta));
    setOutlineHeight(next);
  };
  const metrics = useCanvasMetrics(canvasRef);
  /**
   * Compact layout (§6.0.1): at or below `COMPACT_RIGHT` both panes stop taking
   * a column and open as overlays instead of squeezing the canvas. The
   * comparison is inclusive, matching the editor's `layoutTier`
   * (`width <= breakpoints.compact`), and is measured on this shell so the
   * collapse follows the box the workbench was given.
   */
  const shellWidth = useElementWidth(shellRef);
  const topWidth = useElementWidth(topRef);
  const compactLeft = shellWidth > 0 && shellWidth <= COMPACT_LEFT;
  const compactRight = shellWidth > 0 && shellWidth <= COMPACT_RIGHT;
  /** Tier 0 until the header has been measured, so the first paint is never folded. */
  const fold = topWidth === 0 || topWidth >= TOP_FOLD_1
    ? 0
    : topWidth >= TOP_FOLD_2
      ? 1
      : topWidth >= TOP_FOLD_3
        ? 2
        : topWidth >= TOP_FOLD_4
          ? 3
          : 4;
  const leftDocked = leftOpen && !compactLeft;
  const rightDocked = rightOpen && !compactRight;

  useEffect(() => storePanelWidth('left', leftWidth), [leftWidth]);
  useEffect(() => storePanelWidth('right', rightWidth), [rightWidth]);

  /**
   * 窗口变窄、两侧存值放不下时（§6.0，2026-11-07，以真实编辑器为准）：先压
   * 属性面板、再压左栏。`leftWidth` / `rightWidth` 始终是用户存的值，回缩只改
   * 当前渲染宽度；窗口变宽后存值自然重新生效。
   */
  const fitted = fitPanelWidths({
    room: shellWidth ? shellWidth - 2 - RAIL_WIDTH : 0,
    left: leftWidth,
    right: rightWidth,
    leftDocked,
    rightDocked,
  });
  const shownLeft = leftDocked ? fitted.left : leftWidth;
  const shownRight = rightDocked ? fitted.right : rightWidth;

  /**
   * 画布始终保留 480：先压正在拖的这一侧，另一侧不动。`available` 是这一侧
   * 能拿到的最大值；尚未量到宽度时按硬上限处理，不让第一帧把面板压到最小。
   */
  const panelLimit = (side: PanelSide) => {
    if (!shellWidth) return PANEL_WIDTHS[side].max;
    const other = side === 'left' ? (rightDocked ? shownRight : 0) : (leftDocked ? shownLeft : 0);
    return shellWidth - 2 - RAIL_WIDTH - other - CANVAS_MIN;
  };
  const resetWidth = (side: PanelSide) => {
    if (side === 'left') setLeftWidth(PANEL_WIDTHS.left.fallback);
    else setRightWidth(PANEL_WIDTHS.right.fallback);
  };
  /**
   * First time the shell is narrow enough, the pane closes itself once — that is
   * the compact default (§6.0.1). After that the button is the user's: reopening
   * at a compact width shows the pane as an overlay instead of a column.
   */
  const autoCollapsed = useRef({ left: false, right: false });
  /**
   * §6.0 (2026-11-06): if the focus is inside the panel being collapsed it must
   * land on the recovery entry, never on `body`; a click on the rail leaves the
   * focus where the user put it because the rail button is outside the panel.
   */
  const collapseLeft = () => {
    if (libRef.current?.contains(document.activeElement)) focusReturn.current = 'left';
    setLeftOpen(false);
  };
  const collapseRight = () => {
    if (inspRef.current?.contains(document.activeElement)) focusReturn.current = 'right';
    setRightOpen(false);
  };
  useLayoutEffect(() => {
    const pending = focusReturn.current;
    if (pending === 'right' && !rightOpen) {
      shellRef.current?.querySelector<HTMLButtonElement>('[aria-label="展开属性面板"]')?.focus();
      focusReturn.current = null;
    } else if (pending === 'left' && !leftOpen) {
      shellRef.current?.querySelector<HTMLButtonElement>('.xflow-pv-rail-btn[aria-label="节点与大纲"]')?.focus();
      focusReturn.current = null;
    }
  }, [leftOpen, rightOpen]);
  useLayoutEffect(() => {
    // 变窄时收起的只记「自动」那一次：进入紧凑宽度时面板还开着，才在变宽时
    // 还原（§9 accept-rd 2026-11-07，与编辑器一致 —— 编辑器宽屏直接恢复停靠列，
    // 存值宽度照用）。宽屏下用户自己收起的面板不会被重新打开。
    if (compactLeft) {
      if (!autoCollapsed.current.left && leftOpen) {
        autoCollapsed.current.left = true;
        if (libRef.current?.contains(document.activeElement)) focusReturn.current = 'left';
        setLeftOpen(false);
      }
    } else if (autoCollapsed.current.left) {
      autoCollapsed.current.left = false;
      setLeftOpen(true);
    }
    if (compactRight) {
      if (!autoCollapsed.current.right && rightOpen) {
        autoCollapsed.current.right = true;
        if (inspRef.current?.contains(document.activeElement)) focusReturn.current = 'right';
        setRightOpen(false);
      }
    } else if (autoCollapsed.current.right) {
      autoCollapsed.current.right = false;
      setRightOpen(true);
    }
  }, [compactLeft, compactRight, leftOpen, rightOpen]);

  const scale = zoom ?? (metrics.content ? fitScale(metrics, metrics.content) : 1);
  const minContentTop = Math.min(...wbFrames.map((frame) => frame.top - FRAME_LABEL_OFFSET));
  const panY = canvasPanY(scale, minContentTop);
  const selectedRect = metrics.rects.find((rect) => rect.id === selectedId);
  const selectedNode = wbNodes.find((node) => node.id === selectedId);
  /**
   * 宽度用内联变量覆盖 CSS 默认值；只在停靠态写，收起 / 抽屉态留给样式表把
   * 变量置 0（内联样式会盖过样式表，这里不写才收得起来）。
   */
  const widthStyle = {
    '--xflow-pv-left': leftDocked ? `${fitted.left}px` : undefined,
    '--xflow-pv-right': rightDocked ? `${fitted.right}px` : undefined,
  } as CSSProperties;

  return (
    <div
      className="xflow-pv-wb"
      ref={shellRef}
      style={widthStyle}
      data-resizing={resizing ?? undefined}
      data-compact={compactRight ? 'both' : compactLeft ? 'left' : undefined}
      data-left={leftDocked ? undefined : leftOpen ? 'drawer' : 'collapsed'}
      data-right={rightDocked ? undefined : rightOpen ? 'drawer' : 'collapsed'}
    >
      <header className="xflow-pv-top" data-fold={fold} ref={topRef}>
        <div className="xflow-pv-top-id">
          {/* 早期稿：logo 取代返回箭头（2026-10-30 用户指示），点击仍回列表。 */}
          <Button type="text" className="xflow-pv-logo" aria-label="返回工作流列表">
            X
          </Button>
          <div className="xflow-pv-top-title">
            <div className="xflow-pv-title">
              <strong title={`${wbWorkflow.id} · ${wbWorkflow.version}`}>{wbWorkflow.name}</strong>
              {fold < 2 ? (
                <span className="xflow-pv-chip" data-tone="neutral">spec {wbWorkflow.specVersion}</span>
              ) : null}
              {/* 版本 chip 是版本历史的入口（§6.3 / §6.10），预览里只做点击态和 tooltip。 */}
              <Tooltip title="版本历史">
                <button type="button" className="xflow-pv-chip xflow-pv-chip--action" data-tone="info">
                  {wbWorkflow.version}
                  <DownIcon />
                </button>
              </Tooltip>
              {/* 未保存数 = 草稿差异数组的长度，与发布对话框的差异行数是同一份（S10）。 */}
              <StatusChip
                tone="warning"
                icon={<WarnIcon />}
                label={`未保存 ${wbPendingChanges.length} 处 · 基于 ${wbWorkflow.version}`}
              />
            </div>
            {fold < 2 ? (
              <p className="xflow-pv-subtitle">
                {wbWorkflow.id} · {wbGraphStats.nodes} 个节点 · Asia/Shanghai · 超时 168h
              </p>
            ) : null}
          </div>
        </div>
        {/*
         * 居中带边框的画布工具组（§6.3，2026-11-04）：顺序 撤销/重做 │ 选择/
         * 连线/添加/适应 │ 测试此节点/运行。只有运行带文字，且是顶栏唯一的
         * primary。宿主没接 onRunNode，所以第 7 个按钮挂「待接入」标注。
         */}
        <div className="xflow-pv-top-tools" role="toolbar" aria-label="画布工具">
          <IconButton small label="撤销 ⌘Z" icon={<UndoIcon />} />
          <IconButton small label="重做 ⌘⇧Z" icon={<RedoIcon />} />
          {fold < 3 ? (
            <>
              <VerticalSep />
              <IconButton small label="选择 V" icon={<CursorIcon />} active />
              <IconButton small label="连线 L" icon={<ConnectIcon />} active={false} />
              <IconButton small label="添加节点 A" icon={<PlusIcon />} />
              <IconButton small label="适应视图" icon={<FitIcon />} />
              <VerticalSep />
              <Todo label="宿主未接 onRunNode">
                <IconButton small label="运行到选中节点" icon={<SkipIcon />} />
              </Todo>
              <VerticalSep />
            </>
          ) : null}
          <Tooltip title="运行工作流 ⌘↵">
            <Button type="primary" size="small" className="xflow-pv-top-run" icon={<PlayIcon />}>
              运行
            </Button>
          </Tooltip>
        </div>
        <div className="xflow-pv-top-right">
          {/*
           * 视图分段（§6.3）：编辑 | 运行记录，两项都带图标。编辑器没有预览
           * 模式（2026-10-19 已由运行记录取代），W0 的「预览」作废。
           */}
          <Segmented
            aria-label="视图模式"
            value="edit"
            options={[
              // 第 4 档：分段只留图标，名字与快捷键进 tooltip；sr-only 文本
              // 保住 radio 无障碍名。
              {
                value: 'edit',
                label: fold >= 4 ? <span className="sr-only">编辑</span> : '编辑',
                icon: <EditIcon />,
                ...(fold >= 4 ? { tooltip: '编辑' } : {}),
              },
              {
                value: 'runs',
                label: fold >= 4 ? <span className="sr-only">运行记录</span> : '运行记录',
                icon: <HistoryIcon />,
                ...(fold >= 4 ? { tooltip: '运行记录' } : {}),
              },
            ]}
          />
          {/*
           * 收纳第 1 档：校验、主题收进 ⋯。保存 / 发布始终保留。⋯ 本身在任何
           * 宽度都是最后一项，收纳不会让它移位（2026-10-17 裁定）。
           */}
          {fold < 1 ? (
            <>
              <Button type="text" className="xflow-pv-top-text" icon={<ShieldIcon />}>
                校验
              </Button>
              <IconButton label="主题设置" icon={<ThemeContrastIcon />} />
            </>
          ) : null}
          <IconButton label="更多操作" icon={<MoreIcon />} />
          <VerticalSep />
          <Tooltip title={fold >= 4 ? '保存（⌘S）' : '保存'}>
            <Button className="xflow-pv-top-text" icon={<SaveIcon />}>
              保存 {fold < 4 ? <Kbd>⌘S</Kbd> : null}
            </Button>
          </Tooltip>
          <Button className="xflow-pv-top-text" icon={<UploadIcon />}>
            发布
          </Button>
        </div>
      </header>

      <div className="xflow-pv-wb-body">
        <nav className="xflow-pv-rail" aria-label="侧栏">
          <RailButton
            icon={<GridIcon />}
            text="节点"
            label="节点与大纲"
            active={leftOpen}
            onClick={() => setLeftOpen((open) => !open)}
          />
          <RailButton icon={<HistoryIcon />} text="执行" label="执行记录" />
          <RailButton icon={<VarIcon />} text="变量" label="变量" />
          <span className="flex-1" />
          {/* Compact: the inspector has no column left, so the rail opens it as an overlay. */}
          {compactRight ? (
            <RailButton
              icon={<SlidersIcon />}
              text="属性"
              label="属性面板"
              active={rightOpen}
              onClick={() => setRightOpen((open) => !open)}
            />
          ) : null}
          <RailButton icon={<HelpIcon />} text="帮助" label="快捷键与帮助" />
        </nav>

        <aside className="xflow-pv-lib" aria-label="节点与大纲" ref={libRef}>
          <NodeLibrary
            groups={wbLibraryGroups}
            common={wbLibraryCommon}
            total={wbLibraryTotal}
            expanded={['common']}
            headerExtra={
              <IconButton
                small
                label="收起左栏 ⌘B"
                icon={<SideLeftIcon />}
                active={leftOpen}
                onClick={collapseLeft}
              />
            }
          />

          <LibDivider onResize={resizeOutline} onReset={() => setOutlineHeight(null)} />

          <Outline
            rows={wbOutline}
            total={wbOutlineTotal}
            mode="run"
            selectedId={selectedId}
            onSelect={setSelectedId}
            style={outlineHeight === null ? undefined : { height: outlineHeight }}
            headerExtra={<IconButton small label="定位选中节点" icon={<LocateIcon />} />}
            titleTodo={{
              label: '分组需要新字段',
              title:
                '画布分区（入口 / VIP 审批 / 普通客户）在 DSL 里没有声明字段；已有的分组字段是 groups[]（组放置与调度语义，见同机执行框），不是这套视觉分区',
            }}
          />
        </aside>

        <main className="xflow-pv-canvas" aria-label="画布" ref={canvasRef}>
          <div className="xflow-pv-float-row">
            <Float className="xflow-pv-addnode">
              <Button size="small" type="text" icon={<PlusIcon />} aria-label="添加节点 (A)">
                添加节点
              </Button>
              {/*
               * ⌘K locate has no topbar search box (user ruling 2026-10-17); this
               * is its mouse entry, beside the other canvas-level action.
               */}
              <IconButton small label="定位节点 ⌘K" icon={<SearchIcon />} />
            </Float>
            <RunBar variant={wbRun} onLocate={setSelectedId} />
            {rightOpen ? null : (
              <Float className="xflow-pv-reopen">
                <Button
                  size="small"
                  type="text"
                  icon={<SideRightIcon />}
                  aria-label="展开属性面板"
                  title="展开属性面板 ⌥⌘B"
                  onClick={() => setRightOpen(true)}
                >
                  属性 · {selectedNode?.label ?? ''}
                </Button>
              </Float>
            )}
          </div>

          {ruler ? (
            <>
              <Ruler
                axis="x"
                length={metrics.width}
                scale={scale}
                pan={0}
                highlight={
                  selectedRect
                    ? { start: selectedRect.x * scale, size: selectedRect.w * scale }
                    : undefined
                }
              />
              <Ruler
                axis="y"
                length={metrics.height}
                scale={scale}
                pan={panY}
                highlight={
                  selectedRect
                    ? { start: panY + selectedRect.y * scale, size: selectedRect.h * scale }
                    : undefined
                }
              />
              <span className="xflow-pv-ruler-corner" />
            </>
          ) : null}

          <div
            className="xflow-pv-viewport"
            style={{ transform: `translate(${RULER}px, ${RULER + panY}px) scale(${scale})` }}
          >
            <svg className="xflow-pv-edges" aria-hidden="true">
              {/* 线型来自目标节点的状态（§4 入边），路径来自实测的卡片矩形（edgePath）——
                  runs 回放共用这两个函数，回放挪动卡片时连线自己跟着走。 */}
              {wbEdges.map((edge) => {
                const kind = edgeKindOf(edge, (id) => wbNodes.find((node) => node.id === id)?.status);
                const d = edgePathOf(edge, metrics.rects, metrics.ports);
                if (kind === 'none' || !d) return null;
                return (
                  <path
                    key={edge.id}
                    className={`xflow-pv-edge xflow-pv-edge--${kind}`}
                    data-edge={edge.id}
                    data-from={edge.from}
                    data-to={edge.to}
                    d={d}
                  />
                );
              })}
            </svg>
            {wbFrames.map((frame) => (
              <Frame key={frame.id} frame={frame} />
            ))}
            {wbNodes.map((node) => (
              <NodeCard
                key={node.id}
                node={node}
                selected={node.id === selectedId}
                onSelect={setSelectedId}
              />
            ))}
            {wbEdgeLabels.map((label) => (
              <span
                key={label.id}
                className="xflow-pv-edge-label"
                style={{ left: label.left, top: label.top }}
              >
                {label.text}
              </span>
            ))}
          </div>

          <ZoomBar
            scale={scale}
            onScaleChange={setZoom}
            onFit={() => setZoom(null)}
            ruler={ruler}
            onRulerChange={setRuler}
            minimap={minimap}
            onMinimapChange={setMinimap}
          />
          {minimap ? (
            <Minimap
              rects={metrics.rects}
              canvasWidth={metrics.width}
              canvasHeight={metrics.height}
              scale={scale}
            />
          ) : null}
        </main>

        <aside className="xflow-pv-insp" aria-labelledby={`${idPrefix}-props`} ref={inspRef}>
          <PanelTitle
            icon={<SlidersIcon />}
            title="属性"
            titleId={`${idPrefix}-props`}
            className="xflow-pv-panel-title--bar"
          >
            <IconButton
              small
              label="收起属性面板 ⌥⌘B"
              icon={<SideRightIcon />}
              active={rightOpen}
              onClick={collapseRight}
            />
          </PanelTitle>
          <div className="xflow-pv-insp-head">
            <CategoryIcon category="action" icon="globe" />
            <span className="xflow-pv-insp-node">拉取客户档案</span>
            <StatusChip tone="success" icon={<CheckIcon />} label="成功" />
            <IconButton small label="以 JSON 编辑参数" icon={<CodeIcon />} />
            <IconButton small label="复制节点" icon={<CopyIcon />} />
            <IconButton small label="更多操作" icon={<MoreIcon />} />
            {/*
             * 第二行是「节点名 · 类型 · vN」（§2.1）：节点名和显示名分两行，
             * 才看得出一处是引用名、一处是画布文案。
             */}
            <div className="xflow-pv-insp-type">fetch_profile · xflow.http · v1</div>
          </div>
          <Tabs
            size="small"
            className="xflow-pv-insp-tabs"
            defaultActiveKey="config"
            // 「以 JSON 编辑参数」已移出 tab 栏，放到节点头部（§6.4 属性面板重设计）。
            classNames={{
              header: 'xflow-pv-insp-tabs-head',
              body: 'xflow-pv-insp-tabs-body',
              content: 'xflow-pv-insp-tabs-content',
            }}
            items={[
              { key: 'config', label: '配置', children: <InspectorConfig /> },
              {
                key: 'connections',
                label: (
                  <span data-tab-count={2}>
                    连接<span className="xflow-pv-faint"> 2</span>
                  </span>
                ),
                children: <p className="xflow-pv-tab-note">连接 tab 见预览页「属性面板」一节。</p>,
              },
              {
                key: 'runs',
                label: '运行',
                children: <p className="xflow-pv-tab-note">运行 tab 见预览页「属性面板」一节。</p>,
              },
            ]}
          />
          {/*
           * 宿主没传 onRunNode 时整条不渲染（§2.1）：基准页照旧用「待接入」
           * 标注能力缺口，但真实编辑器里这一条不存在，省出 53px 高度。
           */}
          <div className="xflow-pv-insp-foot">
            <Todo label="宿主未接 onRunNode">
              <Button block icon={<PlayIcon />}>
                测试此节点
              </Button>
            </Todo>
          </div>
        </aside>

        {/* 停靠态才给手柄：收起 / 抽屉（≤1280）不提供拖动，宽度保持默认。 */}
        {leftDocked ? (
          <PanelResizer
            side="left"
            value={shownLeft}
            limit={panelLimit('left')}
            onChange={setLeftWidth}
            onReset={() => resetWidth('left')}
            onDragChange={(on) => setResizing(on ? 'left' : null)}
          />
        ) : null}
        {rightDocked ? (
          <PanelResizer
            side="right"
            value={shownRight}
            limit={panelLimit('right')}
            onChange={setRightWidth}
            onReset={() => resetWidth('right')}
            onDragChange={(on) => setResizing(on ? 'right' : null)}
          />
        ) : null}
      </div>

      <div className="xflow-pv-bottom">
        <StatusBar />
        <Diagnostics idPrefix={idPrefix} />
      </div>
    </div>
  );
}
