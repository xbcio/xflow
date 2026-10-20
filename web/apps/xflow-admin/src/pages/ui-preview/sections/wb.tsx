import {
  AimOutlined,
  AppstoreOutlined,
  ArrowLeftOutlined,
  BgColorsOutlined,
  CaretRightOutlined,
  CheckOutlined,
  CopyOutlined,
  DownOutlined,
  FunctionOutlined,
  HistoryOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  MoreOutlined,
  NodeIndexOutlined,
  PlusOutlined,
  QuestionCircleOutlined,
  RedoOutlined,
  SafetyCertificateOutlined,
  SearchOutlined,
  SlidersOutlined,
  UndoOutlined,
  UnorderedListOutlined,
  VerticalAlignBottomOutlined,
  VerticalAlignTopOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { Button, Collapse, Input, Segmented, Select, Tabs } from 'antd';
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

function Field({ label, children }: { label: ReactNode; children: ReactNode }) {
  return (
    <div className="xflow-pv-field">
      <span className="xflow-pv-field-label">{label}</span>
      {children}
    </div>
  );
}

/**
 * Inspector body for the workbench mock: the request group is the only one
 * expanded, mirroring the static mockup. The three-tab gallery and the real
 * composer controls are the second batch's `inspector` / `composer` sections.
 */
function InspectorConfig() {
  return (
    <Collapse
      ghost
      className="xflow-pv-secs"
      defaultActiveKey={['request']}
      expandIconPlacement="start"
      expandIcon={({ isActive }) => <DownOutlined rotate={isActive ? 0 : -90} />}
      classNames={{ header: 'xflow-pv-sec-head', body: 'xflow-pv-sec-body p-0' }}
      items={[
        {
          key: 'base',
          label: <SectionLabel title="基础信息" extra="拉取客户档案 · 无模板" />,
          children: null,
        },
        {
          key: 'request',
          label: (
            <span className="xflow-pv-sec-label">
              <span className="xflow-pv-sec-title">请求</span>
              <StatusChip tone="success" icon={<CheckOutlined />} label="有效" />
            </span>
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
              <Field label="URL">
                <div className="flex gap-1.5">
                  <div className="xflow-pv-ctl xflow-pv-ctl--focus xflow-pv-ctl--expr">
                    <span className="xflow-pv-ctl-text">https://crm.example.test/api/customers/</span>
                    <span className="xflow-pv-var">{'{{ $json.id }}'}</span>
                  </div>
                  <Button className="xflow-pv-fx" aria-pressed data-active="true">
                    fx
                  </Button>
                </div>
                <span className="xflow-pv-hint">可用变量：$json · $node · $vars</span>
              </Field>
              <Field label="认证">
                <Select
                  className="w-full"
                  defaultValue="crm_token"
                  options={[{ value: 'crm_token', label: 'crm_token' }]}
                />
                <span className="xflow-pv-hint">来自工作流变量 crm_token，运行时由宿主注入</span>
              </Field>
            </div>
          ),
        },
        {
          key: 'headers',
          label: (
            <span className="xflow-pv-sec-label">
              <span className="xflow-pv-sec-title">请求头与查询</span>
              <StatusChip tone="neutral" icon={<UnorderedListOutlined />} label="3 项" />
            </span>
          ),
          children: null,
        },
        {
          key: 'errors',
          label: <SectionLabel title="错误与重试" extra="继承工作流 · 重试 2 次" />,
          children: null,
        },
      ]}
    />
  );
}

function StatusBar() {
  return (
    <div className="xflow-pv-statusbar">
      <span className="xflow-pv-status-ok">
        <CheckOutlined className="xflow-pv-status-icon" />
        校验通过
      </span>
      <span className="xflow-pv-status-warn">
        <WarningOutlined className="xflow-pv-status-icon" />
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
        <PanelTitle icon={<NodeIndexOutlined />} title="诊断台" titleId={`${idPrefix}-diag`} />
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
          <IconButton small label="扩展到画布高度" icon={<VerticalAlignTopOutlined />} />
          <IconButton small label="收起诊断台 ⌘J" icon={<VerticalAlignBottomOutlined />} />
        </span>
      </div>
      <div className="xflow-pv-issues">
        {wbIssues.map((issue) => (
          <div key={issue.id} className="xflow-pv-issue" data-tone={issue.severity}>
            {/* The wrapper carries the tone colour: `@ant-design/icons` injects an
             * unlayered `.anticon { color: inherit }`, which outranks every layer,
             * so a colour set on the icon element itself would be lost. */}
            <span className="xflow-pv-issue-sev">
              <WarningOutlined />
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
            <CheckOutlined />
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
   * Compact layout (§6.0.1): below `COMPACT_LEFT` the two panes stop taking a
   * column and open as overlays instead of squeezing the canvas; below
   * `COMPACT_RIGHT` the inspector does the same. The breakpoints are measured on
   * this shell, so the collapse follows the box the workbench was given.
   */
  const shellWidth = useElementWidth(shellRef);
  const compactLeft = shellWidth > 0 && shellWidth < COMPACT_LEFT;
  const compactRight = shellWidth > 0 && shellWidth < COMPACT_RIGHT;
  const leftDocked = leftOpen && !compactLeft;
  const rightDocked = rightOpen && !compactRight;
  /**
   * First time the shell is narrow enough, the pane closes itself once — that is
   * the compact default (§6.0.1). After that the button is the user's: reopening
   * at a compact width shows the pane as an overlay instead of a column.
   */
  const autoCollapsed = useRef({ left: false, right: false });
  useLayoutEffect(() => {
    if (compactLeft && !autoCollapsed.current.left) {
      autoCollapsed.current.left = true;
      setLeftOpen(false);
    }
    if (compactRight && !autoCollapsed.current.right) {
      autoCollapsed.current.right = true;
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
      <header className="xflow-pv-top">
        <Button type="text" className="xflow-pv-icon-btn" aria-label="返回工作流列表" icon={<ArrowLeftOutlined />} />
        <div className="xflow-pv-title">
          <strong title={`${wbWorkflow.id} · ${wbWorkflow.version}`}>{wbWorkflow.name}</strong>
          {/* 未保存数 = 草稿差异数组的长度，与发布对话框的差异行数是同一份（S10）。 */}
          <StatusChip
            tone="warning"
            icon={<WarningOutlined />}
            label={`未保存 ${wbPendingChanges.length} 处 · 基于 ${wbWorkflow.version}`}
          />
        </div>
        {/*
         * The marker tags the whole segmented control. Hung off the option's
         * label instead, the badge is clipped away: AntD wraps that label in an
         * `overflow: hidden` box, and the badge sits outside it.
         */}
        <span className="mx-auto">
          <Todo label="需新字段">
            <Segmented
              value="edit"
              options={[{ value: 'edit', label: '编辑' }, { value: 'runs', label: '运行记录' }]}
            />
          </Todo>
        </span>
        <div className="xflow-pv-top-right">
          {/*
           * Compact (§6.0.1): 撤销 / 重做 / 校验 / 主题 collapse into one menu
           * button, and 保存 / 运行 stay — they are the two the operator needs on
           * a narrow screen. The title keeps its 160px minimum either way.
           */}
          {compactLeft ? (
            <IconButton label="更多操作" icon={<MoreOutlined />} />
          ) : (
            <>
              <IconButton label="撤销" icon={<UndoOutlined />} />
              <IconButton label="重做" icon={<RedoOutlined />} />
              <VerticalSep />
              <Button type="text" className="xflow-pv-top-text" icon={<SafetyCertificateOutlined />}>
                校验
              </Button>
              <IconButton label="主题设置" icon={<BgColorsOutlined />} />
              {/*
               * 沉浸布局 / 新建工作流 live in this menu at every width (user
               * ruling 2026-10-17); it is the last item before 保存, as the
               * compact ⋯ is, so collapsing the cluster does not move it.
               */}
              <IconButton label="更多操作" icon={<MoreOutlined />} />
              <VerticalSep />
            </>
          )}
          <Button className="xflow-pv-top-text">
            保存 <Kbd>⌘S</Kbd>
          </Button>
          <span className="xflow-pv-split">
            <Button type="primary" className="xflow-pv-split-btn" icon={<CaretRightOutlined />}>
              运行
            </Button>
            <Button
              type="primary"
              className="xflow-pv-split-btn"
              aria-label="运行选项"
              icon={<DownOutlined />}
            />
          </span>
          <Todo label="待接入">
            <Button className="xflow-pv-top-text">发布</Button>
          </Todo>
        </div>
      </header>

      <div className="xflow-pv-wb-body">
        <nav className="xflow-pv-rail" aria-label="侧栏">
          <RailButton
            icon={<AppstoreOutlined />}
            text="节点"
            label="节点与大纲"
            active={leftOpen}
            onClick={() => setLeftOpen((open) => !open)}
          />
          <RailButton icon={<HistoryOutlined />} text="执行" label="执行记录" />
          <RailButton icon={<FunctionOutlined />} text="变量" label="变量" />
          <span className="flex-1" />
          {/* Compact: the inspector has no column left, so the rail opens it as an overlay. */}
          {compactRight ? (
            <RailButton
              icon={<SlidersOutlined />}
              text="属性"
              label="属性面板"
              active={rightOpen}
              onClick={() => setRightOpen((open) => !open)}
            />
          ) : null}
          <RailButton icon={<QuestionCircleOutlined />} text="帮助" label="快捷键与帮助" />
        </nav>

        <aside className="xflow-pv-lib" aria-label="节点与大纲">
          <section className="xflow-pv-lib-pane">
            <PanelTitle
              icon={<AppstoreOutlined />}
              title="节点"
              count={wbLibraryTotal}
              className="xflow-pv-panel-title--bar"
            >
              <IconButton
                small
                label="收起左栏 ⌘B"
                icon={<MenuFoldOutlined />}
                active={leftOpen}
                onClick={() => setLeftOpen(false)}
              />
            </PanelTitle>
            <div className="px-3 pb-2.5">
              <Input
                className="xflow-pv-search"
                prefix={<SearchOutlined />}
                placeholder="搜索 Webhook / If / HTTP…"
                suffix={<Kbd>/</Kbd>}
              />
            </div>
            <div className="xflow-pv-lib-list">
              {wbLibraryGroups.map((group) => (
                <Fragment key={group.id}>
                  <div className="xflow-pv-lib-group">
                    <span>{group.label}</span>
                    <span className="xflow-pv-faint">{group.count}</span>
                  </div>
                  {group.items.map((item) => (
                    <Button
                      key={item.id}
                      type="text"
                      block
                      className="xflow-pv-lib-item"
                    >
                      <CategoryIcon category={item.category} icon={item.icon} />
                      <span className="flex min-w-0 flex-col items-start">
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
                icon={<UnorderedListOutlined />}
                title="大纲"
                count={wbOutlineTotal}
                className="xflow-pv-panel-title--bar"
              >
                <IconButton small label="定位选中节点" icon={<AimOutlined />} />
                <IconButton small label="搜索大纲" icon={<SearchOutlined />} />
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
              <Button size="small" type="text" icon={<PlusOutlined />} aria-label="添加节点 (A)">
                添加节点
              </Button>
              {/*
               * ⌘K locate has no topbar search box (user ruling 2026-10-17); this
               * is its mouse entry, beside the other canvas-level action.
               */}
              <IconButton small label="定位节点 ⌘K" icon={<SearchOutlined />} />
            </Float>
            <RunBar variant={wbRun} onLocate={setSelectedId} />
            {rightOpen ? null : (
              <Float className="xflow-pv-reopen">
                <Button
                  size="small"
                  type="text"
                  icon={<MenuUnfoldOutlined />}
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

        <aside className="xflow-pv-insp" aria-labelledby={`${idPrefix}-props`}>
          <PanelTitle
            icon={<SlidersOutlined />}
            title="属性"
            titleId={`${idPrefix}-props`}
            className="xflow-pv-panel-title--bar"
          >
            <IconButton
              small
              label="收起属性面板 ⌥⌘B"
              icon={<MenuUnfoldOutlined />}
              active={rightOpen}
              onClick={() => setRightOpen(false)}
            />
          </PanelTitle>
          <div className="xflow-pv-insp-head">
            <CategoryIcon category="action" icon="globe" />
            <span className="xflow-pv-insp-node">fetch_profile</span>
            <StatusChip tone="success" icon={<CheckOutlined />} label="成功" />
            <IconButton small label="复制节点" icon={<CopyOutlined />} />
            <IconButton small label="更多操作" icon={<MoreOutlined />} />
            {/*
             * The type id gets a row of its own instead of sharing one with the
             * name and the chip: three items and two icon buttons in a 360px
             * panel left it 60px wide and `xflow.http · v1` rendered as 「xflo…」.
             */}
            <div className="xflow-pv-insp-type">xflow.http · v1</div>
          </div>
          <Tabs
            size="small"
            className="xflow-pv-insp-tabs"
            defaultActiveKey="params"
            classNames={{
              header: 'xflow-pv-insp-tabs-head',
              body: 'xflow-pv-insp-tabs-body',
              content: 'xflow-pv-insp-tabs-content',
            }}
            items={[
              { key: 'params', label: '参数', children: <InspectorConfig /> },
              {
                key: 'settings',
                label: '设置',
                children: <p className="xflow-pv-tab-note">设置 tab 见预览页「属性面板」一节。</p>,
              },
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
          <div className="xflow-pv-insp-foot">
            <Todo label="宿主未接 onRunNode">
              <Button block icon={<CaretRightOutlined />}>
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
