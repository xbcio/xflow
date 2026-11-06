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
  OutlineIcon,
  PlayIcon,
  PlusIcon,
  RedoIcon,
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
import { Fragment, useLayoutEffect, useRef, useState, type ReactNode } from 'react';

import { cx } from '../components/cx';
import { Minimap } from '../components/Minimap';
import { NodeCard } from '../components/NodeCard';
import { CategoryIcon, Float, IconButton, Kbd, PanelTitle, SectionLabel, StatusChip, VerticalSep } from '../components/primitives';
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
import { nodeStatusMeta, typeIcon } from '../meta';

const wbRun = wbRunVariant;

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
 * Topbar fold thresholds (§6.3, 2026-11-04 ruling), measured on the header
 * itself rather than the window. The two side tracks are equal 1fr, so the
 * wider of the two clusters sets every threshold: header = 24 padding + 24
 * gaps + tools + 2 × side. Every tier's floor follows one rule:
 *
 *   floor = 48 + that tier's tools + 2 × max(right natural, left width)
 *
 * Measured 2026-11-05 on the density theme:
 *
 *   right natural: 512.36 (tier 0) / 391.36 (tier 1–3) / 276 (tier 4, the
 *   mode switch is icon-only and the ⌘S keycap is gone)
 *   left width = logo 28 + gap 8 + title row, sample name 客户同步, dirty
 *   (version chip + unsaved chip). The subtitle ellipsizes, so it is excluded:
 *   396.19 at tier 0–1 (spec chip still shown) / 327.78 at tier 2–4
 *
 *   tier 0: 48 + 298 + 2 × 512.36 = 1370.72 → 1371
 *   tier 1: 48 + 298 + 2 × 396.19 = 1138.38 → 1139 (the left cluster decides;
 *   right alone would be 1128.72)
 *   tier 2 hides the subtitle and spec chip: 48 + 298 + 2 × 391.36 =
 *   1128.72 → 1129. TOP_FOLD_2 is tier 1's floor rounded up to the next 50px
 *   band: 1150.
 *   tier 3 folds the canvas extras into ⋯: 48 + 139 + 2 × 391.36 = 969.72 →
 *   970, so tier 4 starts there.
 *   tier 4 makes the mode switch icon-only (names move to tooltips) and drops
 *   the ⌘S keycap: 48 + 139 + 2 × 327.78 = 842.56 → 843 is the bottom of the
 *   supported range (the left cluster decides; right alone would be 739).
 *   Below it the right cluster is squeezed, and there is no further fold step.
 */
const TOP_FOLD_1 = 1371;
const TOP_FOLD_2 = 1150;
const TOP_FOLD_3 = 1129;
const TOP_FOLD_4 = 970;

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

function Field({ label, children, layout = 'side' }: { label: ReactNode; children: ReactNode; layout?: 'side' | 'top' }) {
  return (
    <div className={cx('xflow-pv-field', layout === 'side' && 'xflow-pv-field--side')}>
      <span className="xflow-pv-field-label">{label}</span>
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
      className="xflow-pv-secs"
      defaultActiveKey={['base', 'request']}
      expandIconPlacement="start"
      expandIcon={({ isActive }) => <DownIcon style={{ transform: isActive ? undefined : 'rotate(-90deg)' }} />}
      classNames={{ header: 'xflow-pv-sec-head', body: 'xflow-pv-sec-body p-0' }}
      items={[
        {
          key: 'base',
          className: 'xflow-pv-sec-card xflow-pv-sec-fixed',
          /*
           * 基础信息只放身份字段，展开且不可折叠：折起来会把它变成一屏里最先
           * 消失的东西，而改名是面板里最常用的动作。`collapsible: 'icon'` 加上
           * 隐藏箭头，卡头就没有可点的开关了。
           */
          collapsible: 'icon',
          showArrow: false,
          label: <SectionLabel title="基础信息" />,
          children: (
            <div className="xflow-pv-sec-fields">
              <Field
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
                <span className="xflow-pv-hint">引用名：连线和表达式靠它找到节点，改名会同步更新引用</span>
              </Field>
              <Field label="显示名">
                <Input defaultValue="拉取客户档案" placeholder="fetch_profile" />
                <span className="xflow-pv-hint">只改画布上显示的文字，不影响引用；留空时显示节点名称</span>
              </Field>
            </div>
          ),
        },
        {
          key: 'request',
          className: 'xflow-pv-sec-card',
          label: (
            <SectionLabel title="请求">
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
              {/* Template-mode long text stays a full row; awaiting user ruling (D2). */}
              <Field
                label={
                  <>
                    URL
                    <span className="xflow-pv-req" title="必填">
                      *
                    </span>
                  </>
                }
                layout="top"
              >
                <div className="flex gap-1.5">
                  <div className="xflow-pv-ctl xflow-pv-ctl--focus xflow-pv-ctl--expr">
                    <span className="xflow-pv-ctl-text">https://crm.example.test/api/customers</span>
                    <span className="xflow-pv-var">{'{{ $input.id }}'}</span>
                  </div>
                  <Button className="xflow-pv-fx" aria-pressed data-active="true">
                    fx
                  </Button>
                </div>
                <span className="xflow-pv-hint">可用变量：$input · $nodes · $vars · $params</span>
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
          className: 'xflow-pv-sec-card',
          label: (
            <SectionLabel title="请求头与查询" extra="已填 2 项" />
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
          className: 'xflow-pv-sec-card',
          label: (
            <SectionLabel title="高级" extra="未设置" />
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
          className: 'xflow-pv-sec-card',
          label: <SectionLabel title="执行策略" extra="继承工作流 · 重试 2 次" />,
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
          className: 'xflow-pv-sec-card',
          label: <SectionLabel title="备注" extra="无" />,
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
  const [ruler, setRuler] = useState(true);
  // 展示态：这一页按 mockup 从打开开始；产品默认关（spec §6.2，preview 包实现）。
  const [minimap, setMinimap] = useState(true);
  const [selectedId, setSelectedId] = useState('fetch_profile');
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
    if (compactLeft && !autoCollapsed.current.left) {
      autoCollapsed.current.left = true;
      if (libRef.current?.contains(document.activeElement)) focusReturn.current = 'left';
      setLeftOpen(false);
    }
    if (compactRight && !autoCollapsed.current.right) {
      autoCollapsed.current.right = true;
      if (inspRef.current?.contains(document.activeElement)) focusReturn.current = 'right';
      setRightOpen(false);
    }
  }, [compactLeft, compactRight]);

  const scale = zoom ?? (metrics.content ? fitScale(metrics, metrics.content) : 1);
  const minContentTop = Math.min(...wbFrames.map((frame) => frame.top - FRAME_LABEL_OFFSET));
  const panY = canvasPanY(scale, minContentTop);
  const selectedRect = metrics.rects.find((rect) => rect.id === selectedId);
  const selectedNode = wbNodes.find((node) => node.id === selectedId);

  return (
    <div
      className="xflow-pv-wb"
      ref={shellRef}
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
            <Button className="xflow-pv-top-text">
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
          <section className="xflow-pv-lib-pane">
            <PanelTitle
              icon={<GridIcon />}
              title="节点"
              count={wbLibraryTotal}
              className="xflow-pv-panel-title--bar"
            >
              <IconButton
                small
                label="收起左栏 ⌘B"
                icon={<SideLeftIcon />}
                active={leftOpen}
                onClick={collapseLeft}
              />
            </PanelTitle>
            <div className="px-3 pb-2.5">
              <Input
                className="xflow-pv-search"
                prefix={<SearchIcon />}
                placeholder="搜索 Webhook / If / HTTP…"
                suffix={<Kbd>/</Kbd>}
              />
            </div>
            <div className="xflow-pv-lib-list">
              {wbLibraryGroups.map((group) => (
                <Fragment key={group.id}>
                  <div className="xflow-pv-lib-group">
                    <span>{group.label}</span>
                    <span className="xflow-pv-lib-count">{group.count}</span>
                  </div>
                  {group.items.map((item) => (
                    <Button
                      key={item.id}
                      type="text"
                      block
                      className="xflow-pv-lib-item"
                    >
                      <CategoryIcon category={item.category} icon={item.icon} />
                      <span className="xflow-pv-lib-text">
                        <span className="xflow-pv-lib-name">{item.label}</span>
                        <span className="xflow-pv-lib-hint">{item.hint}</span>
                      </span>
                    </Button>
                  ))}
                </Fragment>
              ))}
            </div>
          </section>

          <section className="xflow-pv-lib-pane">
            {/*
             * The marker belongs on the panel, not on every row: what needs a new
             * field is the canvas grouping itself, and only `VIP 审批` has a field
             * to name on its own row. The old `row.todo ?? '需新字段'` fallback
             * stamped 入口 and 普通客户 too, which claimed those two groups rest
             * on nothing — they do not, they are plain visual sections.
             */}
            <Todo
              label="分组需要新字段"
              className="xflow-pv-todo-wrap--bar"
              title="画布分区（入口 / VIP 审批 / 普通客户）在 DSL 里没有声明字段；已有的分组字段是 groups[]（组放置与调度语义，见同机执行框），不是这套视觉分区"
            >
              <PanelTitle
                icon={<OutlineIcon />}
                title="大纲"
                count={wbOutlineTotal}
                className="xflow-pv-panel-title--bar"
              >
                <IconButton small label="定位选中节点" icon={<LocateIcon />} />
                <IconButton small label="搜索大纲" icon={<SearchIcon />} />
              </PanelTitle>
            </Todo>
            <div className="xflow-pv-outline" role="tree" aria-label="工作流大纲">
              {wbOutline.map((row) => {
                if (row.kind !== 'section') {
                  return (
                    <Button
                      key={row.id}
                      type="text"
                      block
                      role="treeitem"
                      aria-current={row.current ? 'true' : undefined}
                      className={cx('xflow-pv-ol-item', row.indent && 'xflow-pv-ol-item--indent')}
                      data-status={row.status}
                      data-tone={nodeStatusMeta[row.status].tone}
                      data-active={row.current ? 'true' : undefined}
                      onClick={() => setSelectedId(row.id)}
                    >
                      <CategoryIcon category={row.category} icon={row.icon} size="sm" />
                      <span className="xflow-pv-ol-name">
                        {row.label} <span className="xflow-pv-ol-sub">{row.nodeName}</span>
                      </span>
                      <span
                        className="xflow-pv-ol-dot"
                        title={nodeStatusMeta[row.status].label}
                      />
                    </Button>
                  );
                }
                /*
                 * The badge is right-aligned to what it tags, so it has to tag
                 * the full head row: hung off the 2-character label it would
                 * reach back over the colour bar and out of the pane. Rows with
                 * no `todo` of their own render the head bare.
                 */
                const head = (
                  <span className="xflow-pv-ol-head">
                    <span className="xflow-pv-ol-section-label">{row.label}</span>
                    <span className="xflow-pv-ol-count">{row.count}</span>
                  </span>
                );
                return (
                  <div key={row.id} className="xflow-pv-ol-section" data-cat={row.category}>
                    <span className="xflow-pv-ol-bar" />
                    {row.todo ? <Todo label={row.todo}>{head}</Todo> : head}
                  </div>
                );
              })}
            </div>
          </section>
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
            // 「以 JSON 编辑参数」从参数卡头搬进 tab 栏右端（§2.1 / §6.1）。
            tabBarExtraContent={<IconButton small label="以 JSON 编辑参数" icon={<CodeIcon />} />}
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
      </div>

      <div className="xflow-pv-bottom">
        <StatusBar />
        <Diagnostics idPrefix={idPrefix} />
      </div>
    </div>
  );
}
