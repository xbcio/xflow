import {
  AppstoreOutlined,
  ArrowLeftOutlined,
  CaretRightOutlined,
  ClockCircleOutlined,
  CopyOutlined,
  FunctionOutlined,
  HistoryOutlined,
  MoreOutlined,
  SlidersOutlined,
} from '@ant-design/icons';
import { Button, Segmented, Tabs, Tooltip } from 'antd';
import { Fragment, useRef, useState, type ReactNode } from 'react';

import { cx } from '../components/cx';
import { Minimap } from '../components/Minimap';
import { NodeCard } from '../components/NodeCard';
import { Float, CategoryIcon, IconButton, PanelTitle, SectionLabel, StatusChip, VerticalSep } from '../components/primitives';
import { Ruler } from '../components/Ruler';
import { Todo } from '../components/Todo';
import { useCanvasMetrics } from '../components/useCanvasMetrics';
import { ZoomBar } from '../components/ZoomBar';
import { edgePathOf } from '../edge-path';
import {
  edgeKindOf,
  upstreamOf,
  wbEdges,
  wbEdgeLabels,
  wbExecFilters,
  wbExecList,
  wbFrames,
  wbNodes,
  wbRunDetail,
  wbRunReplay,
  wbWorkflow,
  replayLayout,
  replayMetaLabels,
  type PreviewFrame,
  type PreviewNode,
} from '../data';
import { canvasPanY, fitScale, FRAME_LABEL_OFFSET, RULER } from '../metrics';
import { nodeStatusMeta, typeIcon, workflowStatusMeta, type NodeStatus } from '../meta';

/**
 * The replay draws the workbench's own seven nodes wearing this run's statuses.
 * Same ids, same coordinates, same edges — §6.8 replays the snapshot of *this*
 * workflow, so a canvas with fewer nodes would be a different graph, and the
 * counts in the status bar would stop matching the picture.
 */
const replayPatch = new Map(wbRunReplay.nodes.map((patch) => [patch.id, patch]));

const replayNodes = wbNodes.map((node) => {
  const patch = replayPatch.get(node.id);
  const y = replayLayout.nodeY[node.id] ?? node.y;
  if (!patch) return { ...node, y };
  return { ...node, y, status: patch.status, chipLabel: patch.chipLabel, error: patch.error, badge: patch.badge, invalid: patch.invalid };
});

/*
 * 回放没有自己的一份连线：`wbEdges` 的 route 由实测矩形解算（`edgePathOf`），
 * 回放把卡片挪了、把它们撑高了，连线自己跟着走到新的边框上。
 */

const replayFrames = wbFrames.map((frame) =>
  frame.id === replayLayout.frame.id ? { ...frame, top: replayLayout.frame.top, height: replayLayout.frame.height } : frame,
);

const replayStatusOf = (id: string) => replayNodes.find((node) => node.id === id)?.status;

/** 状态 → 线型：与工作台同一个函数，回放不另写一份线型表（§4 入边）。 */
const replayEdgeKind = (edge: (typeof wbEdges)[number]) => edgeKindOf(edge, replayStatusOf);

/**
 * 重跑起点的上游都直接沿用了上一次的结果（§6.8）。集合由 `wbEdges` 反向走一遍
 * 得到，不按名字写：起点换了、连线改了，标注跟着变。
 */
const replayUpstream = upstreamOf(wbEdges, wbRunReplay.lineage.startNode);

/** 卡片右侧备注（重试次数）只有这次运行知道；左侧两行是算出来的。 */
function replayMetaOf(node: PreviewNode) {
  const right = replayPatch.get(node.id)?.metaRight;
  if (node.id === wbRunReplay.lineage.startNode) return { left: replayMetaLabels.restart, right };
  if (replayUpstream.has(node.id)) return { left: replayMetaLabels.reuse, right };
  return undefined;
}

/** Status bar counts: derived from the replay, so a status can only appear once. */
const replayCounts = replayNodes.reduce<Partial<Record<NodeStatus, number>>>((counts, node) => {
  if (node.status) counts[node.status] = (counts[node.status] ?? 0) + 1;
  return counts;
}, {});

const replayCountText = (Object.keys(nodeStatusMeta) as NodeStatus[])
  .filter((status) => replayCounts[status])
  .map((status) => `${replayCounts[status]} ${nodeStatusMeta[status].label}`)
  .join(' · ');

function RailButton({ icon, text, label, active }: { icon: ReactNode; text: string; label: string; active?: boolean }) {
  return (
    <Tooltip title={label} placement="right">
      <button type="button" className="xflow-pv-rail-btn" aria-label={label} aria-pressed={active} data-active={active}>
        {icon}
        <span className="xflow-pv-rail-text">{text}</span>
      </button>
    </Tooltip>
  );
}

function ReplayFrame({ frame }: { frame: PreviewFrame }) {
  const label = (
    <span className="xflow-pv-frame-label">
      {frame.label}
      {frame.labelMono ? <span className="xflow-pv-mono">{frame.labelMono}</span> : null}
    </span>
  );
  return (
    <div
      className={cx('xflow-pv-frame', frame.colo && 'xflow-pv-frame--colo', frame.labelRight && 'xflow-pv-frame--label-right')}
      data-cat={frame.category}
      style={{ left: frame.left, top: frame.top, width: frame.width, height: frame.height }}
    >
      <span title={frame.labelTitle}>
        {frame.labelIcon ? <span className="xflow-pv-frame-icon">{typeIcon[frame.labelIcon]}</span> : null}
        {frame.labelTodo ? <Todo label={frame.labelTodo}>{label}</Todo> : label}
      </span>
    </div>
  );
}

/** Read-only canvas: no node library, no 添加节点, no run bar (§6.8). */
function ReplayCanvas() {
  const canvasRef = useRef<HTMLDivElement>(null);
  /*
   * 展示态：这个基线页的标尺与缩略图都从打开开始，因为 mockup 画的是打开态。
   * 产品默认是关的（spec §6.2，preview 包按规范实现）；这里只把开关接上线，
   * 让「本图所示」和「能切换」两件事不再互相矛盾。
   */
  const [ruler, setRuler] = useState(true);
  const [minimap, setMinimap] = useState(true);
  const metrics = useCanvasMetrics(canvasRef);
  const scale = metrics.content ? fitScale(metrics, metrics.content) : 1;
  const minContentTop = Math.min(...wbFrames.map((frame) => frame.top - FRAME_LABEL_OFFSET));
  const panY = canvasPanY(scale, minContentTop);

  return (
    <main className="xflow-pv-canvas" aria-label="执行快照（只读）" ref={canvasRef}>
      <div className="xflow-pv-float-row">
        <Float className="xflow-pv-readonly">
          <span className="xflow-pv-faint">只读回放 · 节点不可拖动、不可连线</span>
        </Float>
      </div>

      {/* 关掉标尺时两条标尺与角落方块一起消失，与工作台同一处理（wb.tsx）。 */}
      {ruler ? (
        <>
          <Ruler axis="x" length={metrics.width} scale={scale} pan={0} />
          <Ruler axis="y" length={metrics.height} scale={scale} pan={panY} />
          <span className="xflow-pv-ruler-corner" />
        </>
      ) : null}

      <div
        className="xflow-pv-viewport"
        style={{ transform: `translate(${RULER}px, ${RULER + panY}px) scale(${scale})` }}
      >
        <svg className="xflow-pv-edges" aria-hidden="true">
          {wbEdges.map((edge) => {
            const kind = replayEdgeKind(edge);
            const d = edgePathOf(edge, metrics.rects, metrics.ports);
            // §4 的「—」：已取消 / 已过期的节点不画入边（失败画默认实线）。
            if (kind === 'none' || !d) return null;
            return (
              <path
                key={edge.id}
                className={cx('xflow-pv-edge', `xflow-pv-edge--${kind}`)}
                data-edge={edge.id}
                data-from={edge.from}
                data-to={edge.to}
                d={d}
              />
            );
          })}
        </svg>
        {replayFrames.map((frame) => (
          <ReplayFrame key={frame.id} frame={frame} />
        ))}
        {replayNodes.map((node) => (
          /* 只读回放：卡片不带任何操作（§6.8），等待说明只属于 suspended 的编辑态。 */
          <NodeCard key={node.id} node={{ ...node, meta: replayMetaOf(node) }} selected={node.id === wbRunDetail.nodeId} readOnly />
        ))}
        {wbEdgeLabels.map((label) => (
          <span key={label.id} className="xflow-pv-edge-label" style={{ left: label.left, top: replayLayout.labelTop[label.id] ?? label.top }}>
            {label.text}
          </span>
        ))}
      </div>

      <ZoomBar
        scale={scale}
        onScaleChange={() => {}}
        onFit={() => {}}
        ruler={ruler}
        onRulerChange={setRuler}
        minimap={minimap}
        onMinimapChange={setMinimap}
      />
      {minimap ? (
        <Minimap rects={metrics.rects} canvasWidth={metrics.width} canvasHeight={metrics.height} scale={scale} />
      ) : null}
    </main>
  );
}

function ExecList() {
  return (
    <aside className="xflow-pv-lib xflow-pv-lib-pane" aria-label="执行列表">
      <PanelTitle
        icon={<HistoryOutlined />}
        title="执行"
        className="xflow-pv-panel-title--bar"
        count={
          <Todo label="需新字段" title="按工作流过滤的执行列表：服务端 store 无此列（§6.7）">
            本工作流 · {wbExecList.length}
          </Todo>
        }
      />
      <div className="xflow-pv-exec-filters">
        <Segmented size="small" value={wbExecFilters[0]} options={wbExecFilters} />
        <Button size="small">最近 24 小时</Button>
      </div>
      <div className="xflow-pv-exec-list" role="list">
        {wbExecList.map((row) => {
          const meta = workflowStatusMeta[row.status];
          return (
            <div
              className="xflow-pv-exec-item"
              key={row.id}
              role="listitem"
              aria-current={row.id === wbRunReplay.execId ? 'true' : undefined}
            >
              <span className="xflow-pv-exec-id xflow-pv-mono">{row.id}</span>
              <StatusChip
                tone={meta.tone}
                icon={meta.icon}
                label={row.queuePosition ? `排队 · 第 ${row.queuePosition} 位` : meta.label}
              />
              <span className="xflow-pv-exec-sub">
                {row.created}
                {row.duration ? ` · ${row.duration}` : ''}
                {row.detail ? ` · ${row.detail}` : ''}
              </span>
            </div>
          );
        })}
      </div>
    </aside>
  );
}

function RunInspector() {
  const meta = nodeStatusMeta[wbRunDetail.status];
  return (
    <aside className="xflow-pv-insp" aria-label="执行详情">
      <PanelTitle icon={<SlidersOutlined />} title="属性" className="xflow-pv-panel-title--bar" />
      <div className="xflow-pv-insp-head">
        <CategoryIcon category="code" icon="code" />
        <span className="xflow-pv-insp-node" title={wbRunDetail.nodeLabel}>
          {wbRunDetail.nodeLabel}
        </span>
        <StatusChip tone={meta.tone} icon={meta.icon} label={meta.label} />
        <IconButton small label="复制节点" icon={<CopyOutlined />} />
        <IconButton small label="更多操作" icon={<MoreOutlined />} />
        <div className="xflow-pv-insp-type" title={`${wbRunDetail.nodeId} · ${wbRunDetail.nodeType}`}>
          {wbRunDetail.nodeId} · {wbRunDetail.nodeType}
        </div>
      </div>
      <div className="xflow-pv-insp-tabs">
        <Tabs
          size="small"
          defaultActiveKey="run"
          className="xflow-pv-insp-tabs-head"
          items={[
            { key: 'config', label: '配置', children: null },
            { key: 'links', label: '连接', children: null },
            { key: 'run', label: '运行', children: null },
          ]}
        />
        <div className="xflow-pv-insp-tabs-body xflow-pv-run-body">
          <dl className="xflow-pv-kvlist">
            {wbRunDetail.rows.map((row) => (
              <Fragment key={row.label}>
                <dt>{row.label}</dt>
                <dd title={row.hint}>
                  {row.todo ? (
                    <Todo label={row.todo} placement="inline">
                      {row.value}
                    </Todo>
                  ) : (
                    row.value
                  )}
                </dd>
              </Fragment>
            ))}
          </dl>
          <div className="xflow-pv-secs">
            <div className="xflow-pv-sec">
              <div className="xflow-pv-sec-head">
                <SectionLabel title="错误" />
              </div>
              <div className="xflow-pv-sec-body">
                <div className="xflow-pv-sec-pad">
                  <pre className="xflow-pv-code">{wbRunDetail.error.text}</pre>
                  <dl className="xflow-pv-kvlist">
                    {wbRunDetail.error.details.map((detail) => (
                      <Fragment key={detail.key}>
                        <dt className="xflow-pv-mono">{detail.key}</dt>
                        <dd className="xflow-pv-mono">{detail.value}</dd>
                      </Fragment>
                    ))}
                  </dl>
                  {/* 说明文字，不是又一行 key/value（F8）。 */}
                  <p className="xflow-pv-field-note">{wbRunDetail.error.hint}</p>
                </div>
              </div>
            </div>
            <div className="xflow-pv-sec">
              <div className="xflow-pv-sec-head">
                <SectionLabel title="输出" extra={<span className="xflow-pv-faint">无</span>} />
              </div>
            </div>
          </div>
        </div>
      </div>
      <div className="xflow-pv-insp-foot">
        <Button block icon={<CaretRightOutlined />}>
          从此节点重跑
        </Button>
      </div>
    </aside>
  );
}

export function RunsSection() {
  return (
    <div className="xflow-pv-wb xflow-pv-wb--runs">
      <header className="xflow-pv-top">
        <Button type="text" className="xflow-pv-icon-btn" aria-label="返回工作流列表" icon={<ArrowLeftOutlined />} />
        <div className="xflow-pv-title">
          <strong title={`${wbWorkflow.id} · ${wbWorkflow.version}`}>{wbWorkflow.name}</strong>
          <StatusChip tone="neutral" icon={<ClockCircleOutlined />} label={`运行版本 ${wbRunReplay.version}`} />
        </div>
        <span className="mx-auto">
          <Todo label="需新字段">
            <Segmented value="runs" options={[{ value: 'edit', label: '编辑' }, { value: 'runs', label: '运行记录' }]} />
          </Todo>
        </span>
        <div className="xflow-pv-top-right">
          <span className="xflow-pv-mono xflow-pv-faint">{wbRunReplay.execId}</span>
          <StatusChip
            tone={workflowStatusMeta[wbRunReplay.status].tone}
            icon={workflowStatusMeta[wbRunReplay.status].icon}
            label={workflowStatusMeta[wbRunReplay.status].label}
          />
          <span className="xflow-pv-exec-lineage">
            重跑自 {wbRunReplay.lineage.from} · 起点 {wbRunReplay.lineage.startNode}
          </span>
          <VerticalSep />
          <Button type="primary" icon={<CaretRightOutlined />}>
            重跑…
          </Button>
          <IconButton label="更多操作" icon={<MoreOutlined />} />
        </div>
      </header>

      <div className="xflow-pv-wb-body">
        <nav className="xflow-pv-rail" aria-label="侧栏">
          <RailButton icon={<AppstoreOutlined />} text="节点" label="节点与大纲" />
          <RailButton icon={<HistoryOutlined />} text="执行" label="执行记录" active />
          <RailButton icon={<FunctionOutlined />} text="变量" label="变量" />
        </nav>
        <ExecList />
        <ReplayCanvas />
        <RunInspector />
      </div>

      <div className="xflow-pv-statusbar">
        <span className="flex items-center gap-1.5">
          执行 <span className="xflow-pv-mono">{wbRunReplay.execId}</span>
        </span>
        <VerticalSep />
        <span className="xflow-pv-faint">
          创建 {wbRunReplay.created} · 更新 {wbRunReplay.updated}
        </span>
        <span className="flex-1" />
        <span>
          {replayNodes.length} 节点 · {replayCountText}
        </span>
        <span className="xflow-pv-faint">只读回放 · 定义为当次运行版本</span>
      </div>
    </div>
  );
}
