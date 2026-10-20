import type { Category, EdgeVariant, ExecutionStatus, NodeStatus, Tone } from './meta';
import { nodeCategoryMeta } from '@xflow/core';

import { edgeVariant, nodeStatusMeta, workflowStatusMeta } from './meta';
import type { typeIcon } from './meta';
import type { EdgeRoute } from './edge-path';
import { HANDLE_Y } from './metrics';

/**
 * Static data for the dev-only UI preview route.
 *
 * It deliberately lives in `data.ts`: `scripts/check-production-fixtures.mjs`
 * rejects any import path segment named mock/fixture/testing, and renames here
 * would only hide the real guard, which is the `__XFLOW_DEV_ONLY__` marker the
 * page renders and the production bundle must not contain.
 *
 * Values are descriptors, not JSX, so this file stays `.ts`; `NodeCard` owns
 * the rendering of every kind.
 */

export type TodoLabel = '待接入' | '需新字段' | '宿主未接 onRunNode' | '分组需要新字段';

export type IconKey = keyof typeof typeIcon;

/** One segment of a `mix` summary value; `mono` picks the monospace face. */
export interface MixPart {
  text: string;
  mono?: boolean;
}

/** One "label · value" row of the card summary (spec §6.1.1). */
export type SummaryValue =
  | { kind: 'plain'; text: string }
  | { kind: 'mono'; text: string }
  /**
   * Inline run with mixed weights (§6.1.1). `parts` render left to right joined
   * by `separator` (a plain space by default); blank parts are dropped before
   * joining, so a half-filled row can never end in a dangling separator.
   */
  | { kind: 'mix'; parts: MixPart[]; separator?: string }
  | { kind: 'expr'; text: string }
  | { kind: 'ref'; text: string }
  | { kind: 'secret' }
  | { kind: 'missing'; text: string }
  | { kind: 'more'; text: string };

export interface SummaryRow {
  label: string;
  value: SummaryValue;
  todo?: TodoLabel;
}

export interface PreviewNode {
  id: string;
  label?: string;
  type: string;
  category: Category;
  icon: IconKey;
  x: number;
  y: number;
  status?: NodeStatus;
  summary?: SummaryRow[];
  ports?: string[];
  /** Which connection handles the canvas card shows; ports carry their own edges. */
  handles?: 'in' | 'out' | 'both';
  meta?: { left: string; leftTitle?: string; right?: string; rightTodo?: TodoLabel };
  error?: string;
  note?: { text: string; action?: string; actionTodo?: TodoLabel };
  selected?: boolean;
  pinned?: boolean;
  /**
   * Replaces the status chip's text where the run knows something more useful
   * than the status name — a branch node's chip shows the port it took (§6.8).
   * The tone and icon still come from `nodeStatusMeta[status]`.
   */
  chipLabel?: string;
  /** Edit-time validation state: danger border + count badge (§6.2). */
  invalid?: boolean;
  badge?: number;
  /**
   * 草稿里该节点**界面上不展示**的参数值（键是服务端 DSL 的参数名）。只放不展示的：
   * 卡片摘要已经画出来的那些以 `summary` 为准，同一个值不能有两个出处（H1）。
   */
  params?: Record<string, string>;
}

export interface PreviewFrame {
  id: string;
  left: number;
  top: number;
  width: number;
  height: number;
  label: string;
  labelMono?: string;
  labelIcon?: IconKey;
  labelTodo?: TodoLabel;
  labelTitle?: string;
  category?: Category;
  /** Co-location group (`groups[]`): neutral solid border, stripes, label top-right. */
  colo?: boolean;
  /**
   * Label right-aligned on the frame's top edge instead of left-aligned. Used
   * where the branch edges drop past the frame's left corner: `普通客户`'s label
   * sat at x 573–643 and `is_vip`'s true branch drops at x 600, i.e. straight
   * through it. Moving the chip to the right end of the same top edge keeps it
   * on its own frame and clears both drops (600 and 700, the false branch).
   */
  labelRight?: boolean;
}

/**
 * A drawn connection. Geometry only: the stroke is derived from the **target
 * node's status** through §4's 入边 column ({@link edgeKindOf}), so the same
 * edge on the same graph cannot be solid in the workbench and dashed in the
 * replay. `kind` is the one exception — an edge leaving an `error` port is
 * danger-toned by §6.2 regardless of the target's status, because that rule is
 * about the source port, not about the node it lands on.
 */
export interface PreviewEdge {
  id: string;
  /**
   * Hand-written `d`, for the fixed-size illustration canvases only (the edge
   * gallery and the exception-state samples, whose cards are placed by hand at
   * 12 / 292 / 576). Anything drawn on a real canvas carries a `route` instead
   * and gets its `d` from {@link edgePath} against the measured card rects.
   */
  d?: string;
  /** How the route meets its two cards, resolved by `edgePath` at render time. */
  route?: EdgeRoute;
  /** Source node id, for graph walks (the replay's 上游 collection). */
  from?: string;
  /** Target node id: its status decides the stroke. */
  to?: string;
  /**
   * 离开源卡的出口 chip 名。声明了它，边的起点就不在卡片边界上而在这个 chip 的下沿
   * （§6.2：有 error 出口的节点，main 句柄挂在 main chip 上），S2 改按 chip 量起点。
   */
  fromPort?: string;
  kind?: EdgeVariant;
}

/**
 * §4 is the single source: an edge is stroked like the 入边 of the node it
 * enters. A target with no status yet (the exception-state samples) draws the
 * resting line; a target that 已取消/记录已过期 draws nothing at all, which is
 * what §4's 「—」 means.
 */
export function edgeKindOf(edge: PreviewEdge, statusOf: (id: string) => NodeStatus | undefined): EdgeVariant {
  if (edge.kind) return edge.kind;
  const status = edge.to ? statusOf(edge.to) : undefined;
  return status ? edgeVariant(status) : 'default';
}

/** Nodes reachable by walking edges backwards from `startId` (start excluded). */
export function upstreamOf(edges: PreviewEdge[], startId: string): Set<string> {
  const parents = new Map<string, string[]>();
  for (const edge of edges) {
    if (!edge.from || !edge.to) continue;
    parents.set(edge.to, [...(parents.get(edge.to) ?? []), edge.from]);
  }
  const seen = new Set<string>();
  const queue = [...(parents.get(startId) ?? [])];
  while (queue.length) {
    const id = queue.shift()!;
    if (seen.has(id)) continue;
    seen.add(id);
    queue.push(...(parents.get(id) ?? []));
  }
  return seen;
}

export interface PreviewEdgeLabel {
  id: string;
  left: number;
  top: number;
  text: string;
}

export interface LibraryItem {
  id: string;
  label: string;
  hint: string;
  category: Category;
  icon: IconKey;
}

export interface LibraryGroup {
  id: string;
  label: string;
  count: number;
  items: LibraryItem[];
}

export type OutlineRow =
  | { kind: 'section'; id: string; label: string; count: number; category: Category; todo?: TodoLabel }
  | {
      kind: 'item';
      id: string;
      label: string;
      nodeName: string;
      category: Category;
      icon: IconKey;
      status: NodeStatus;
      indent?: boolean;
      current?: boolean;
    };

export interface PreviewIssue {
  id: string;
  severity: 'warning' | 'danger';
  target: string;
  text: string;
  action: string;
}

export interface RunVariant {
  status: ExecutionStatus;
  execId: string;
  /** `mm:ss`, shown in a tabular-nums box that never reflows (§6.3). */
  elapsed?: string;
  queuePosition?: number;
  counts?: { suspend?: number; danger?: number };
  /** Terminal states stay for 5s and say so. */
  dismissHint?: string;
}

export const wbWorkflow = {
  name: '客户同步',
  id: 'customer-sync',
  version: 'v1.2.0',
  unsaved: true,
};

export const wbExec = {
  id: 'exec-8432',
  status: 'running' as ExecutionStatus,
  elapsed: '00:12',
  currentNode: 'manager_approval',
  branch: 'is_vip → true',
  counts: { suspend: 1 },
};

/** The workbench's own run bar state; the variants wall reuses the same entry. */
export const wbRunVariant: RunVariant = {
  status: 'running',
  execId: wbExec.id,
  elapsed: wbExec.elapsed,
  counts: { suspend: 1 },
};

/*
 * `wbDefinitionSummary` lives below the graph arrays on purpose: every number in
 * it is counted off `wbNodes` / `wbEdges`, so the copy cannot drift from the
 * drawing. It used to be declared up here with the counts written by hand, and
 * it said "8 节点" over a canvas that draws 7.
 */

/**
 * Canvas coordinates are a compact relayout of the mockup's 1524×530 graph: the
 * mockup draws it at 62% because it does not fit an 850×529 canvas at 100%. The
 * three card columns sit at x = 6 / 286 / 566 (264px cards, 16px apart); the
 * levels are y = 0 (webhook → fetch_profile → is_vip), y = 169 for the true
 * branch's two cards, and a stacked false branch in the third column at
 * y = 158 / 228. Every group frame wraps its cards with 6px padding. Together
 * that is 836×365 content units, which `fitScale` renders at ~97%.
 *
 * Why the false branch stacks instead of taking a third full-width row: at any
 * zoom ≥ 90% a 449-unit-tall graph fills the canvas top to bottom, and the
 * floating zoom bar (bottom-left, 34px at bottom 16) would then sit on top of
 * the last row. Stacking the false branch under `is_vip` keeps the bottom-left
 * corner empty — content there is what the bar may cover — and its edges stay
 * vertical because both branch edges already leave from the ports on `is_vip`'s
 * bottom edge.
 *
 * Do not re-space these by hand: the frames and edges below are drawn from the
 * same numbers, and `useCanvasMetrics` feeds the fit from the rendered boxes.
 */
/**
 * The approval node's chain — one list, four readers: the canvas card's wait
 * note (「依次审批 2/3 · 等待 finance-mgr」), the card gallery's 审批人 row, the
 * approval dialog's ledger, and the dialog's 2/3. Written out per reader, the
 * same node showed a four-person chain in the gallery and 2/3 in the panel.
 */
export const wbApprovalChain = {
  nodeId: 'manager_approval',
  nodeLabel: '主管审批',
  mode: '依次审批',
  /** 审批超时：卡片摘要的「方式」行与变更清单的改后值都从这一处读。 */
  timeout: '72h',
  approvers: ['ops-lead', 'finance-mgr', 'cfo'],
  /** 当前登录用户：在链里，且正轮到它。 */
  actor: 'finance-mgr',
};

export const wbApprovalIndex = wbApprovalChain.approvers.indexOf(wbApprovalChain.actor);
/** `2/3` — 1-based position of the current approver over the chain length. */
export const wbApprovalPosition = `${wbApprovalIndex + 1}/${wbApprovalChain.approvers.length}`;

export const wbNodes: PreviewNode[] = [
  {
    id: 'webhook',
    label: '客户回调',
    type: 'xflow.trigger.webhook',
    category: 'trigger',
    icon: 'link',
    x: 6,
    y: 0,
    status: 'success',
    handles: 'out',
    summary: [
      {
        label: '路径',
        value: { kind: 'mix', parts: [{ text: 'POST', mono: true }, { text: '/hooks/customer' }] },
      },
    ],
    meta: { left: '触发于 14:02:11', right: '8 ms' },
  },
  {
    id: 'fetch_profile',
    label: '拉取客户档案',
    type: 'xflow.http',
    category: 'action',
    icon: 'globe',
    x: 286,
    y: 0,
    status: 'success',
    selected: true,
    handles: 'both',
    summary: [
      {
        label: '请求',
        value: {
          kind: 'mix',
          parts: [{ text: 'GET', mono: true }, { text: 'crm.example.test/api/customers/{{id}}' }],
        },
      },
      { label: '认证', value: { kind: 'ref', text: 'crm_token' } },
    ],
    meta: { left: '200 · 1.2 KB', right: '412 ms · 第 2 次', rightTodo: '需新字段' },
  },
  {
    id: 'is_vip',
    label: '是否 VIP',
    type: 'xflow.if',
    category: 'control',
    icon: 'branch',
    x: 566,
    y: 0,
    status: 'success',
    handles: 'in',
    summary: [{ label: '条件', value: { kind: 'expr', text: '$json.tier == "vip"' } }],
    ports: ['true', 'false'],
  },
  {
    id: 'manager_approval',
    label: '主管审批',
    type: 'xflow.approval',
    category: 'human',
    icon: 'user',
    x: 6,
    y: 169,
    status: 'suspended',
    handles: 'both',
    // 等待说明与「处理」由状态（suspended）与审批链决定，不手写人数。
    note: {
      text: `${wbApprovalChain.mode} ${wbApprovalPosition} · 等待 ${wbApprovalChain.actor}`,
      action: '处理',
      actionTodo: '待接入',
    },
    ports: ['approved', 'rejected', 'returned', 'timeout'],
  },
  {
    id: 'notify_sales',
    label: '通知销售',
    type: 'xflow.notification',
    category: 'action',
    icon: 'bell',
    x: 566,
    y: 158,
    status: 'skipped',
    handles: 'both',
  },
  {
    id: 'upsert_customer',
    label: '写入客户',
    type: 'xflow.database',
    category: 'action',
    icon: 'db',
    x: 286,
    y: 169,
    status: 'pending',
    handles: 'in',
    // retry 不在卡片上画：这一项的改后值（H1 的 chg-4）只能从节点自己读。
    params: { retry: '5' },
    summary: [
      {
        label: '操作',
        // The mockup writes the verb first, then the separator, then the table
        // name in mono — the «·» belongs between the two, not inside either.
        value: {
          kind: 'mix',
          parts: [{ text: 'upsert' }, { text: 'customers', mono: true }],
          separator: ' · ',
        },
      },
      { label: '连接', value: { kind: 'ref', text: 'db_conn' } },
    ],
  },
  {
    id: 'enrich_score',
    label: '计算评分',
    type: 'xflow.function',
    category: 'code',
    icon: 'code',
    x: 566,
    y: 228,
    status: 'skipped',
    handles: 'in',
  },
];

export const wbFrames: PreviewFrame[] = [
  {
    id: 'frame-approval',
    left: 0,
    top: 163,
    width: 556,
    height: 185,
    label: 'VIP 审批',
    category: 'human',
    labelTodo: '需新字段',
  },
  {
    id: 'frame-colo',
    left: 280,
    top: -6,
    width: 556,
    height: 155,
    label: '同机执行 profile_colo',
    labelMono: 'region=cn',
    labelIcon: 'server',
    labelTitle: 'groups[] · 同机执行，成员整组派发',
    colo: true,
  },
  {
    id: 'frame-normal',
    left: 560,
    top: 152,
    width: 276,
    height: 140,
    label: '普通客户',
    category: 'action',
    labelRight: true,
  },
];

/**
 * Branch edges leave from the ports on `is_vip`'s bottom edge: the true branch
 * runs left into the VIP 审批 frame, the false branch drops straight into the
 * 普通客户 stack below it, and that stack's two cards connect the same way.
 *
 * The true branch (`e3`) is a smoothstep polyline, not a bezier: it drops from
 * the `true` port chip into the corridor between the two frames (`lane`, 155),
 * crosses it horizontally, then turns down into its target's top edge. A bezier
 * cut the corner and ran over the 普通客户 frame label; the corridor is the one
 * line between the frames that nothing occupies. The lane is 155, not 159: the
 * run has to clear 通知销售's top edge (158) as well as frame-normal's top border
 * (152) and frame-colo's bottom border (149) — at 159 it clipped the card's
 * top-left corner by one unit under the frame's own top edge. The two frames are
 * 16 units apart horizontally, which is why the true branch runs in that
 * corridor and not down the seam between them.
 *
 * `e6` is the approval's approved outlet. Without it `upsert_customer` waits in
 * 排队 (it cannot start before the approval passes) while nothing on the canvas
 * points at it, i.e. the picture contradicted the card's own status. It is the
 * one route that joins two cards standing side by side, so it is a straight
 * handle-to-handle line across the 16-unit channel between the two columns.
 *
 * No geometry is written down here: `route` says which borders the edge meets
 * (and, for the elbow, which port chip it leaves and which corridor it runs in),
 * and `edgePath` resolves it against the measured card rects. That is what makes
 * the replay immune to the replay's own card heights — the same six routes draw
 * both canvases. No `kind` here either: every stroke comes from the target's
 * status (§4 入边) via `edgeKindOf`, so the replay of another execution
 * re-strokes the same routes without touching this array. The replay's own
 * spacing (its cards are taller) lives in `replayLayout`, one entry per place
 * that moves — never a second copy of the graph.
 */
export const wbEdges: PreviewEdge[] = [
  { id: 'e1', from: 'webhook', to: 'fetch_profile', route: { kind: 'h' } },
  { id: 'e2', from: 'fetch_profile', to: 'is_vip', route: { kind: 'h' } },
  {
    id: 'e3',
    from: 'is_vip',
    to: 'manager_approval',
    route: { kind: 'elbow', lane: 155, fromPort: 'true', toFrac: 0.5 },
  },
  { id: 'e4', from: 'is_vip', to: 'notify_sales', route: { kind: 'v' } },
  { id: 'e5', from: 'notify_sales', to: 'enrich_score', route: { kind: 'v' } },
  { id: 'e6', from: 'manager_approval', to: 'upsert_customer', route: { kind: 'h' } },
];

/** Labels ride the middle of their edge's longest run, never on a port end. */
export const wbEdgeLabels: PreviewEdgeLabel[] = [
  { id: 'l-true', left: 438, top: 155, text: 'true' },
  { id: 'l-false', left: 700, top: 137, text: 'false' },
];

/**
 * 本地未保存的改动（§6.10 的「与线上的结构化 diff」）。**一份**：顶栏 chip 说的
 * 「未保存 N 处 · 基于 vX」与发布对话框列出的差异条目都从这里算，所以两处不可能各说各话
 * ——静态稿的顶栏写「3 处」而发布对话框列了 4 条。
 *
 * 每条变更带 `before` / `after`（H1）。**改后值不在这张表里写死**：它由
 * {@link pendingChangeAfter} 从图里读 —— 卡片显示名读 `wbNodes[].label`、摘要行读
 * `summary`、审批超时读 `wbApprovalChain.timeout`、不展示的参数读 `params`。
 * 「改后值」是草稿里的现状，凡是在页面上能看到的（`visibleIn`），画出来的文字必须
 * 就是它；「改前值」才是线上与草稿不同的那一半，草稿里看不到，只能写下来。
 */
export type ChangeAfterSource =
  | { kind: 'nodeLabel'; nodeId: string }
  | { kind: 'nodeSummary'; nodeId: string; row: string }
  | { kind: 'nodeParam'; nodeId: string; param: string }
  | { kind: 'approvalTimeout' };

export interface PendingChange {
  id: string;
  kind: 'add' | 'mod' | 'del';
  /** 指向工作台图里的节点 / 连线；被删除的对象没有。 */
  nodeId?: string;
  edgeId?: string;
  /** 删除的节点：名字与类型（图上已经没有它了）。 */
  target?: string;
  /** 改了哪个字段（界面上看得见的说法）。 */
  field?: string;
  /** 线上值。草稿里看不到，所以是这一行里唯一写下来的值。 */
  before?: string;
  /** 改后值的出处，见 {@link pendingChangeAfter}。 */
  afterSource?: ChangeAfterSource;
  /**
   * 改后值在草稿界面上的展示位置：`exact` 表示该元素的全部文字就是改后值
   * （节点显示名、参数行的 `<dd>`），否则改后值是它的一部分（「依次审批 · 72h 后驳回」）。
   */
  visibleIn?: { section: string; selector: string; exact?: boolean };
  detail?: string;
}

/** 摘要值的**可见文字**：与 `SummaryValueView` 的渲染逐字对应（含 mix 的空白剔除）。 */
export function summaryValueText(value: SummaryValue): string {
  switch (value.kind) {
    case 'mix':
      return value.parts
        .filter((part) => part.text.trim() !== '')
        .map((part) => part.text)
        .join(value.separator ?? ' ');
    // 脱敏值画出来就是六个圆点，节点里存的那串不算「可见文字」。
    case 'secret':
      return '••••••';
    default:
      return value.text;
  }
}

/** 改后值：一律从图里读，不在这张表里重抄一份（H1 / S13）。 */
export function pendingChangeAfter(change: PendingChange): string | undefined {
  const source = change.afterSource;
  if (!source) return undefined;
  switch (source.kind) {
    case 'nodeLabel':
      return wbNodes.find((node) => node.id === source.nodeId)?.label;
    case 'nodeSummary': {
      const node = wbNodes.find((entry) => entry.id === source.nodeId);
      const row = node?.summary?.find((entry) => entry.label === source.row);
      return row ? summaryValueText(row.value) : undefined;
    }
    case 'nodeParam':
      return wbNodes.find((node) => node.id === source.nodeId)?.params?.[source.param];
    case 'approvalTimeout':
      return wbApprovalChain.timeout;
  }
}

/**
 * 草稿相对线上 v1.2.0 的差异：**只有修改** —— 节点集合与连线集合都和线上一致。
 *
 * 早先这份清单里写着「新增 enrich_score / 新增连线 / 删除 legacy_sync」，可同一屏的
 * 回放与重跑对话框都说 exec-8429 是在 v1.2.0 上跑的、里面 enrich_score 已经跑过，
 * list 页也写着 v1.2.0 是 7 个节点。线上版本不可能既有又没有这个节点。两处只能留一处，
 * 留的是「图没变、参数变了」：这也正是 v1.2.0 那 7 个节点能画出快照的前提。
 *
 * 四条改后值各自有出处（H1）：前两条与第三条的展示位置连选择器一起写在 `visibleIn` 里，
 * 断言脚本按它去 DOM 里比字；第四条（`upsert_customer` 的 retry）卡片上不展示，只存在于
 * 节点自己的 `params` 里，因此没有 `visibleIn`。
 */
export const wbPendingChanges: PendingChange[] = [
  {
    id: 'chg-1',
    kind: 'mod',
    nodeId: 'notify_sales',
    field: '显示名',
    before: '通知销售团队',
    afterSource: { kind: 'nodeLabel', nodeId: 'notify_sales' },
    visibleIn: {
      section: 'wb',
      selector: '.xflow-pv-node[data-node-id="notify_sales"] .xflow-pv-node-name-label',
      exact: true,
    },
  },
  {
    id: 'chg-2',
    kind: 'mod',
    nodeId: 'is_vip',
    field: '条件表达式',
    before: '$json.tier == "gold"',
    afterSource: { kind: 'nodeSummary', nodeId: 'is_vip', row: '条件' },
    visibleIn: {
      section: 'wb',
      selector: '.xflow-pv-node[data-node-id="is_vip"] .xflow-pv-node-props dd[data-summary-row="条件"]',
      exact: true,
    },
  },
  {
    id: 'chg-3',
    kind: 'mod',
    nodeId: 'manager_approval',
    field: '审批超时',
    before: '48h',
    afterSource: { kind: 'approvalTimeout' },
    // 该节点的卡片摘要写「依次审批 · 72h 后驳回」：改后值是这句话里的 72h，不是整句。
    visibleIn: {
      section: 'cardprops',
      selector: '[data-card-gallery="approval"] .xflow-pv-node-props dd[data-summary-row="方式"]',
    },
  },
  {
    id: 'chg-4',
    kind: 'mod',
    nodeId: 'upsert_customer',
    field: '参数 retry',
    before: '3',
    afterSource: { kind: 'nodeParam', nodeId: 'upsert_customer', param: 'retry' },
  },
];

/** 变更行的文案：名字与类型来自图本身，不在对话框里重抄一份。 */
export function pendingChangeText(change: PendingChange): { target: string; detail: string } {
  const node = change.nodeId ? wbNodes.find((entry) => entry.id === change.nodeId) : undefined;
  const after = pendingChangeAfter(change);
  const diff = change.before !== undefined && after !== undefined ? `${change.before} → ${after}` : undefined;
  if (node) return { target: node.id, detail: change.detail ?? diff ?? node.type };
  const edge = change.edgeId ? wbEdges.find((entry) => entry.id === change.edgeId) : undefined;
  if (edge) return { target: `${edge.from} → ${edge.to}`, detail: change.detail ?? diff ?? '连线' };
  return { target: change.target ?? '—', detail: change.detail ?? diff ?? '' };
}

/**
 * 节点在服务端 DSL 里的**参数键**（不是 UI 文案）。校验失败要把出错位置写成
 * `nodes[i].params.<key>`，索引由节点在 `wbNodes` 里的下标算出、键从这张表取 ——
 * 两者都不是手写的字面量（G3）。
 */
export const wbNodeParams: Record<string, string[]> = {
  fetch_profile: ['method', 'url', 'timeout', 'retry'],
  is_vip: ['expression'],
  upsert_customer: ['table', 'retry'],
  notify_sales: ['channel', 'template'],
  enrich_score: ['url', 'timeout'],
};

/**
 * 校验失败定位到的字段：节点 + 参数键，路径与那句话都由这两项拼出来。
 *
 * `message` 是**服务端才能判定**的原因（H2）：不是格式不合法，也不是本地校验能发现的
 * 东西 —— 客户端无从知道出口域名有没有登记过，所以这句话只能由服务端给出。
 */
export const wbSaveFailure = {
  nodeId: 'fetch_profile',
  param: 'url',
  label: 'URL',
  message: 'url 的主机不在允许列表内，请先在服务端登记出口域名',
};

/** 变更类别：三种，计数、tone 与符号都从这里来（S10 按 `data-k` 核对）。 */
export const pendingChangeKinds = [
  { kind: 'add' as const, label: '新增', tone: 'success' as Tone, sign: '+' },
  { kind: 'mod' as const, label: '修改', tone: 'warning' as Tone, sign: '~' },
  { kind: 'del' as const, label: '删除', tone: 'danger' as Tone, sign: '−' },
];

/** 版本历史（§6.10）：当前线上在首行，其余按退役先后。 */
export interface VersionRow {
  version: string;
  hash: string;
  online?: boolean;
  /** `ErrWorkflowVersionUnavailable`：该行「不可回滚」并给出原因。 */
  blocked?: string;
}

export const wbVersions: VersionRow[] = [
  { version: wbWorkflow.version, hash: 'c07d…19', online: true },
  { version: 'v1.1.4', hash: 'a91c…e2' },
  { version: 'v1.1.3', hash: '5b20…8f' },
  { version: 'v1.1.0', hash: 'e4aa…03', blocked: '当前编译器重算哈希不一致，服务端会拒绝' },
  { version: 'v1.0.0', hash: '0f12…7a' },
];

/** 保留期（§6.10）：默认 50 个归档版本，更早的已清理。 */
export const wbVersionRetention = 50;

/**
 * 发布对话框（§6.10）。新版本号是 patch +1 的建议值，也是主按钮上的那个版本号：
 * 输入框与按钮读同一个字段，按钮不可能说另一个版本。
 */
export const wbPublish = {
  suggestedVersion: '1.3.0',
  notesHint: '写给查看版本历史的人',
  inflightNote: '发布后新触发的执行使用新版本，在途执行不受影响。',
};

/**
 * Every count the workbench shows is read off the graph arrays — the status bar,
 * the definition summary and the outline all call these. The canvas draws
 * `wbNodes` and `wbEdges`, so a number and the picture can no longer disagree;
 * adding a node to `wbNodes` moves all four at once.
 */
function countNodesByCategory(nodes: PreviewNode[]): { label: string; count: number }[] {
  return Object.entries(nodeCategoryMeta)
    .map(([category, meta]) => ({ label: meta.label, count: nodes.filter((node) => node.category === category).length }))
    .filter((entry) => entry.count > 0);
}

/**
 * The mix stays structured, not pre-joined: the drawer renders one span per
 * category so the only place that row may wrap is the `·` between two of them.
 * Joined into a single string, `overflow-wrap: anywhere` broke it inside a label
 * (`人` / `工 1`). `wbGraphStats.mix` below is the same data as plain text, for
 * the row's `title`.
 */
export const wbNodeMix = countNodesByCategory(wbNodes);

export const wbGraphStats = {
  nodes: wbNodes.length,
  edges: wbEdges.length,
  /** `触发器 1 · 流程控制 1 · 动作 3 · 代码 1 · 人工 1`, `` when empty. */
  mix: wbNodeMix.map((entry) => `${entry.label} ${entry.count}`).join(' · '),
};

export const wbDefinitionSummary = {
  edges: String(wbGraphStats.edges),
  credentials: 'crm_token, db_conn（运行时注入）',
  /** 「未保存 N 处 · 基于 vX」与发布对话框的差异条目同一份数据（§6.10）。 */
  version: `未保存 ${wbPendingChanges.length} 处 · 基于 ${wbWorkflow.version}`,
};

/** The panel header counts what the group list actually holds. */


export const wbLibraryGroups: LibraryGroup[] = [
  {
    id: 'trigger',
    label: '触发器',
    count: 5,
    items: [
      { id: 'webhook', label: 'Webhook', hint: 'HTTP 回调触发', category: 'trigger', icon: 'link' },
      { id: 'cron', label: '定时 Cron', hint: '按表达式周期触发', category: 'trigger', icon: 'clock' },
    ],
  },
  {
    id: 'control',
    label: '流程控制',
    count: 7,
    items: [
      { id: 'if', label: '条件判断', hint: 'true / false 两路', category: 'control', icon: 'branch' },
      { id: 'wait', label: '等待', hint: '延时或等待信号', category: 'control', icon: 'hourglass' },
    ],
  },
  {
    id: 'action',
    label: '动作',
    count: 5,
    items: [
      { id: 'http', label: 'HTTP 请求', hint: '调用外部 API', category: 'action', icon: 'globe' },
      { id: 'database', label: '数据库', hint: '查询与写入', category: 'action', icon: 'db' },
      { id: 'notification', label: '通知', hint: '邮件 / IM', category: 'action', icon: 'bell' },
    ],
  },
  {
    id: 'code',
    label: '代码与转换',
    count: 10,
    items: [
      { id: 'function', label: '函数', hint: '运行注册函数', category: 'code', icon: 'code' },
      { id: 'filter', label: '过滤', hint: '按条件保留条目', category: 'transform', icon: 'filter' },
    ],
  },
  {
    id: 'human',
    label: '人工',
    count: 1,
    items: [{ id: 'approval', label: '审批', hint: '通过 / 驳回 / 退回', category: 'human', icon: 'user' }],
  },
];

/**
 * The outline is a view of the same graph, so its rows come from `wbNodes` and
 * its section counts are the number of rows each section actually holds — the
 * panel header counts the same rows. Only the grouping (which node sits under
 * which section, and whether a section indents) is authored here.
 *
 * `done` used to have a row of its own with no card on the canvas, which is what
 * made the old "8 节点" copy possible. A node the canvas does not draw is not a
 * node: add it to `wbNodes` (with its coordinates) and it appears here, in the
 * status bar, in the definition summary and on the canvas at once.
 */
const wbOutlineSections: {
  id: string;
  label: string;
  category: Category;
  todo?: TodoLabel;
  indent?: boolean;
  members: string[];
}[] = [
  { id: 'sec-entry', label: '入口', category: 'trigger', members: ['webhook', 'fetch_profile', 'is_vip'] },
  {
    id: 'sec-vip',
    label: 'VIP 审批',
    category: 'human',
    todo: '需新字段',
    indent: true,
    members: ['manager_approval', 'upsert_customer'],
  },
  {
    id: 'sec-normal',
    label: '普通客户',
    category: 'action',
    indent: true,
    members: ['notify_sales', 'enrich_score'],
  },
];

export const wbOutline: OutlineRow[] = wbOutlineSections.flatMap((section) => {
  const members = section.members
    .map((id) => wbNodes.find((node) => node.id === id))
    .filter((node): node is PreviewNode => !!node);
  return [
    {
      kind: 'section' as const,
      id: section.id,
      label: section.label,
      count: members.length,
      category: section.category,
      todo: section.todo,
    },
    ...members.map((node) => ({
      kind: 'item' as const,
      id: node.id,
      label: node.label ?? node.id,
      nodeName: node.id,
      category: node.category,
      icon: node.icon,
      status: node.status ?? 'pending',
      indent: section.indent,
      // Selection is the editor's state, and the canvas card already carries it.
      current: node.selected,
    })),
  ];
});

export const wbOutlineTotal = wbOutline.filter((row) => row.kind === 'item').length;

/** The panel header counts what the group list actually holds. */
export const wbLibraryTotal = wbLibraryGroups.reduce((total, group) => total + group.count, 0);

/**
 * The workbench mock is self-consistent with a running execution that took the
 * `is_vip → true` branch: the approval node hangs, the normal-customer branch is
 * skipped (so nothing failed). The failed appearance is demonstrated in the run
 * bar variants and the state wall instead.
 */
export const wbIssues: PreviewIssue[] = [
  {
    id: 'issue-timeout',
    severity: 'warning',
    target: 'fetch_profile',
    text: '未设置超时，将使用默认 30s',
    action: '定位节点',
  },
  {
    id: 'issue-error-branch',
    severity: 'warning',
    target: '工作流',
    text: 'upsert_customer 没有错误分支',
    action: '定位节点',
  },
];

export const wbChecksPassed = '5 项检查通过：DSL 结构 · 节点类型 · 连线端口 · 变量引用 · 模板';

export const runVariants: RunVariant[] = [
  { status: 'pending', execId: 'exec-8434' },
  { status: 'queued', execId: 'exec-8433', queuePosition: 2 },
  wbRunVariant,
  { status: 'paused', execId: 'exec-8430', elapsed: '00:12' },
  { status: 'canceling', execId: 'exec-8428' },
  { status: 'success', execId: 'exec-8406', dismissHint: '5s 后收起' },
  { status: 'failed', execId: 'exec-8429', counts: { danger: 1 }, dismissHint: '5s 后收起' },
  { status: 'canceled', execId: 'exec-8425', dismissHint: '5s 后收起' },
  { status: 'timeout', execId: 'exec-8420', dismissHint: '5s 后收起' },
];

/** Card body shown for a node status in the §4 state wall. */
export type StateCardBody =  | { kind: 'meta'; left: string; right?: string; title?: string }
  | { kind: 'meta-error'; left: string; right: string; error: string }
  | { kind: 'note'; text: string; action: string };

export function stateCardBody(status: NodeStatus): StateCardBody | undefined {
  switch (status) {
    case 'failed':
    case 'continued':
      return {
        kind: 'meta-error',
        left: '1.2 s',
        right: '第 3 次',
        error: 'connection refused: 10.0.0.12:5432',
      };
    case 'suspended':
      return { kind: 'note', text: '等待信号 approval.decided', action: '处理' };
    case 'running':
    case 'committing':
    case 'waiting':
      return { kind: 'meta', left: '已运行 4.2 s', right: '第 1 次' };
    case 'success':
    case 'pinned':
      return { kind: 'meta', left: '120 条', right: '86 ms' };
    case 'expired':
      return { kind: 'meta', left: '记录已按 TTL 清理', title: '临时分组完成后记录已按 TTL 清理' };
    default:
      return undefined;
  }
}

/* ======================================================================
 * Second batch (2a). Same rule as above: descriptors, not JSX.
 * ==================================================================== */

/** A composer field in the inspector gallery (§6.4). */
export interface FieldSpec {
  label: string;
  value?: string;
  /** `select` / `expr` render the display control's own affordances. */
  control?: 'text' | 'select' | 'multi' | 'expr' | 'area' | 'number' | 'seg' | 'switch' | 'slider';
  /** `select` / `seg` choices; a `multi` control takes the selected tags from here. */
  options?: string[];
  mono?: boolean;
  /**
   * Render the field empty, with `value` as the placeholder text. The static
   * mockup uses this for "no template", "describe the workflow" and the
   * default-value hints, so the copy stays in `value` either way.
   */
  placeholder?: boolean;
  required?: boolean;
  readOnly?: boolean;
  /** Draw the focus ring: `autoFocus` would steal focus in a gallery. */
  focus?: boolean;
  /** Trailing `fx` toggle inside the control. */
  fx?: boolean;
  hint?: string;
  /** Trailing unit inside a numeric control (`个执行`, `分钟`). */
  unit?: string;
  /** Slider bounds (only for `control: 'slider'`). */
  min?: number;
  max?: number;
  /** Textarea height in lines (only for `control: 'area'`; defaults to 3). */
  rows?: number;
  /** Switch position (only for `control: 'switch'`; defaults to on). */
  switchOn?: boolean;
  /** Field spans the full row (the workflow description). */
  wide?: boolean;
  /** Field-level message; `tone` picks the `--xflow-warning` / `--xflow-danger` pair. */
  error?: { tone: 'warning' | 'danger'; text: string };
}

export interface RuleSpec {
  id: string;
  title: string;
  port: string;
  expr?: string;
  note?: string;
  error?: { tone: 'warning' | 'danger'; text: string };
}

export interface PortRowSpec {
  id: string;
  port: string;
  to: string;
  /** `error` is the unconnected error outlet (§6.6). */
  tone?: 'danger';
  /** Label of the fix-up action shown in place of the target. */
  link?: string;
}

/** 1b · the four healthy panels of the inspector gallery, plus the failing one. */
export const inspSwitch = {
  label: '金额路由',
  nodeName: 'route_by_amount',
  type: 'xflow.switch',
  category: 'control' as Category,
  icon: 'branch' as IconKey,
  base: [
    { label: '显示名', value: '金额路由' },
    {
      label: '节点名',
      value: 'route_by_amount',
      mono: true,
      required: true,
      hint: '连线与表达式引用此名称',
    },
    { label: '模板', value: '无', control: 'select', placeholder: true },
  ] as FieldSpec[],
  rules: [
    { id: 'r1', title: '规则 1', port: 'level_1', expr: '$params.amount < $vars.l1_max' },
    { id: 'r2', title: '规则 2', port: 'level_2', expr: '$params.amount < $vars.l2_max' },
    { id: 'r3', title: '默认', port: 'level_3', note: '其余金额进入三级审批' },
  ] as RuleSpec[],
};

export const inspConnections = {
  inputs: [{ id: 'in-main', port: 'main', to: '查询审批链' }] as PortRowSpec[],
  outputs: [
    { id: 'o1', port: 'level_1', to: '一级主管审批' },
    { id: 'o2', port: 'level_2', to: '二级主管审批' },
    { id: 'o3', port: 'level_3', to: 'VP 与财务并行' },
    { id: 'o4', port: 'error', to: '未连接', tone: 'danger', link: '添加错误处理' },
  ] as PortRowSpec[],
  policy: [{ label: '失败时', value: '继承工作流（停止）', control: 'select' }] as FieldSpec[],
};

/**
 * 连接 tab 的计数 = 已连线的边数（入 + 出，用户裁定 2026-10-17）：未连接的
 * error 出口不算，从同一份端口行算出，tab 上的数与面板里的行不会各说各的。
 */
export const inspConnectedCount = [...inspConnections.inputs, ...inspConnections.outputs].filter(
  (row) => row.tone !== 'danger',
).length;

/**
 * 设置 tab（node-form 的 settings 半边：错误处理 / 超时 / 重试）。与参数 tab
 * 分开是真实信息架构（用户裁定 2026-10-17），默认值都写「继承工作流」。
 */
export const inspSettings = [
  {
    key: 'error',
    title: '错误处理',
    fields: [
      { label: '出错时', value: '继承工作流（停止）', control: 'select', options: ['继承工作流（停止）', '继续后续分支', '跳到错误分支'] },
    ] as FieldSpec[],
  },
  {
    key: 'timeout',
    title: '超时',
    fields: [{ label: '超时', value: '继承工作流（30s）', placeholder: true }] as FieldSpec[],
  },
  {
    key: 'retry',
    title: '重试',
    fields: [
      { label: '最多尝试', value: '3', control: 'number', unit: '次' },
      { label: '退避', value: '指数 · 1s 起 ×2', control: 'select', options: ['固定 · 1s', '指数 · 1s 起 ×2'] },
    ] as FieldSpec[],
  },
];

export const inspRuns = {
  kv: [
    { label: '执行', value: 'exec-8432', mono: true },
    { label: '耗时', value: '412 ms' },
    { label: '尝试', value: '第 2 次（第 1 次 502）' },
    { label: '输出端口', value: 'main', port: true },
  ],
  input: '{ "id": "cus_1024" }',
  output: [
    '{',
    '  "status": 200,',
    '  "body": {',
    '    "id": "cus_1024",',
    '    "tier": "vip"',
    '  },',
    '  "durationMs": 412',
    '}',
  ].join('\n'),
};

export const wfSettings = {
  basic: [
    { label: '名称', value: '客户同步' },
    { label: '标识', value: 'customer-sync', mono: true },
    { label: '版本', value: '1.2.0', mono: true },
    { label: '时区', value: 'Asia/Shanghai', control: 'select' },
    { label: '描述', value: '这个工作流做什么', control: 'area', placeholder: true, wide: true, rows: 2 },
  ] as FieldSpec[],
  entry: [{ id: 'e1', port: 'main', to: '客户回调 · Webhook' }] as PortRowSpec[],
  concurrency: [
    { label: '上限', value: '10', control: 'number', unit: '个执行' },
    /* A `seg` control with no `options` renders a one-option segmented control —
     * a track with a single, immovable thumb. Every `seg` sample declares its
     * choices; the value only says which one is selected. */
    { label: '超限时', value: '排队', control: 'seg', options: ['排队', '丢弃', '失败'] },
    { label: '队列上限', value: '100', control: 'number' },
    { label: '排队超时', value: '10', control: 'number', unit: '分钟' },
  ] as FieldSpec[],
  retry: [
    { label: '出错时', value: '停止执行', control: 'select', options: ['停止执行', '继续后续分支', '跳到错误分支'] },
    { label: '自动重试', value: '开', control: 'switch' },
    { label: '最多尝试', value: '3', control: 'number', unit: '次' },
    { label: '退避', value: '指数 · 1s 起 ×2', control: 'select', options: ['固定 · 1s', '指数 · 1s 起 ×2', '指数 · 5s 起 ×2'] },
  ] as FieldSpec[],
  advanced: [
    { label: '标签', value: '', control: 'multi', options: ['生产', '财务', '灰度'] },
    { label: '日志级别', value: 'info', control: 'select', options: ['debug', 'info', 'warn', 'error'] },
    { label: '运行记录保留', value: '30', control: 'number', unit: '天' },
  ] as FieldSpec[],
};

/** One collapsible group inside the inspector body. */
export interface InspectorSectionSpec {
  key: string;
  title: string;
  extra?: string;
  /** Rendered as a danger chip in the section header. */
  errors?: number;
  fields: FieldSpec[];
}

/** The failing-field panel: one danger field and one warning field (§6.4). */
export const inspInvalid = {
  label: '拉取客户档案',
  nodeName: 'fetch_profile',
  type: 'xflow.http',
  category: 'action' as Category,
  icon: 'globe' as IconKey,
  badge: 1,
  sections: [
    {
      key: 'request',
      title: '请求',
      errors: 1,
      fields: [
        {
          label: 'URL',
          value: '输入请求地址',
          control: 'text',
          required: true,
          placeholder: true,
          error: { tone: 'danger', text: '未填写：http 节点必须声明 URL' },
        },
      ],
    },
    {
      key: 'auth',
      title: '认证',
      extra: '环境变量引用',
      fields: [
        {
          label: '凭证',
          value: 'crm_token',
          control: 'select',
          error: { tone: 'warning', text: '当前环境未声明该凭证，运行时将注入空值' },
        },
      ],
    },
    {
      key: 'advanced',
      title: '超时与重试',
      extra: '留空 = 继承工作流默认',
      fields: [
        {
          label: '超时（秒）',
          value: '',
          control: 'number',
          placeholder: true,
          hint: '留空使用默认 30s',
        },
      ],
    },
  ] as InspectorSectionSpec[],
};

/** 2b · the card-summary gallery: one entry per node kind the summary must cover. */
export interface CardSample {
  id: string;
  caption: string;
  node: PreviewNode;
}

function card(
  id: string,
  caption: string,
  type: string,
  category: Category,
  icon: IconKey,
  summary?: SummaryRow[],
  extra?: Partial<PreviewNode>,
): CardSample {
  return { id, caption, node: { id: `sample_${id}`, type, category, icon, x: 0, y: 0, summary, ...extra } };
}

export const cardGallery: CardSample[] = [
  card('webhook', '触发器：入口参数', 'xflow.trigger.webhook', 'trigger', 'link', [
    { label: '方法', value: { kind: 'mix', parts: [{ text: 'POST', mono: true }, { text: '/hooks/customer' }] } },
    { label: '鉴权', value: { kind: 'ref', text: 'webhook_secret' } },
  ], { id: 'customer_callback', label: '客户回调' }),
  card('cron', 'Cron：表达式 + 时区', 'xflow.trigger.cron', 'trigger', 'clock', [
    { label: '表达式', value: { kind: 'mono', text: '0 2 * * *' } },
    { label: '时区', value: { kind: 'plain', text: 'Asia/Shanghai' } },
  ], { id: 'nightly_reconcile', label: '夜间对账' }),
  card('http', 'HTTP：方法 + URL 合成一行，凭证只显示引用名', 'xflow.http', 'action', 'globe', [
    {
      label: '请求',
      value: {
        kind: 'mix',
        parts: [{ text: 'GET', mono: true }, { text: 'crm.example.test/api/customers/{{id}}' }],
      },
    },
    { label: '认证', value: { kind: 'ref', text: 'crm_token' } },
    { label: '重试', value: { kind: 'plain', text: '3 次 · 指数退避' } },
  ], { id: 'fetch_profile', label: '拉取客户档案' }),
  /*
   * The truncation sample deliberately carries no credential: a DSN with a user
   * name in it is a secret in a screenshot, not a long string. The long value is
   * a filter expression instead, and the connection shows its reference name
   * like every other card.
   */
  card('long', '长值截断：单行省略号，不换行、不撑破卡片', 'xflow.database', 'action', 'db', [
    { label: '连接', value: { kind: 'ref', text: 'db_conn' } },
    {
      label: '过滤',
      value: {
        kind: 'mono',
        text: "archived_at < now() - interval '90 days' AND owner_team = 'sales-cn'",
      },
    },
    { label: '主键', value: { kind: 'mono', text: 'customer_id' } },
  ], { id: 'archive_customer', label: '归档客户' }),
  card('if', 'If：条件表达式', 'xflow.if', 'control', 'branch', [
    { label: '条件', value: { kind: 'expr', text: '$json.tier == "vip"' } },
  ], { id: 'is_vip', label: '是否 VIP', ports: ['true', 'false'] }),
  card('switch', 'Switch：规则数 + 依据字段，规则详情在端口', 'xflow.switch', 'control', 'branch', [
    { label: '规则', value: { kind: 'plain', text: '3 条 + 默认' } },
    { label: '依据', value: { kind: 'expr', text: '$params.amount' } },
  ], { id: 'route_by_amount', label: '金额路由', ports: ['level_1', 'level_2', 'level_3'] }),
  card('database', '数据库：操作 + 表 + 连接', 'xflow.database', 'action', 'db', [
    {
      label: '操作',
      value: { kind: 'mix', parts: [{ text: 'upsert' }, { text: 'customers', mono: true }], separator: ' · ' },
    },
    { label: '连接', value: { kind: 'ref', text: 'db_conn' } },
    { label: '主键', value: { kind: 'mono', text: 'customer_id' } },
  ], { id: 'upsert_customer', label: '写入客户' }),
  card('function', '代码：不显示代码正文，只显示语言和规模', 'xflow.function', 'code', 'code', [
    { label: '语言', value: { kind: 'plain', text: 'JavaScript · 42 行' } },
    { label: '超时', value: { kind: 'plain', text: '5 s' } },
  ], { id: 'compute_score', label: '计算评分' }),
  card('approval', '审批：审批链 + 模式 + 超时策略', 'xflow.approval', 'human', 'user', [
    /*
     * 审批链只有一条（wbApprovalChain）。此处手写过四个人名，于是同一个节点在
     * 画廊里是 4 人链、在审批弹窗里是 2/3：一行名字和一次链长必须同源。
     */
    { label: '审批人', value: { kind: 'plain', text: wbApprovalChain.approvers.join(' → ') } },
    { label: '方式', value: { kind: 'plain', text: `${wbApprovalChain.mode} · ${wbApprovalChain.timeout} 后驳回` } },
  ], { id: 'manager_approval', label: '主管审批', ports: ['approved', 'rejected', 'returned', 'timeout'] }),
  card('secret', '密钥字段打码；第 3 行起收进"+n 项"，每张卡最多 3 行', 'xflow.notification', 'action', 'bell', [
    { label: '渠道', value: { kind: 'plain', text: '企业微信' } },
    { label: '密钥', value: { kind: 'secret' } },
    /* The third row is the summary row, so a five-field node still keeps the
     * "≤ 3 行" promise: 2 rows shown, the remaining 3 behind the count. */
    { label: '更多', value: { kind: 'more', text: '+3 项' } },
  ], { id: 'notify_sales', label: '通知销售' }),
  card('missing', '必填缺失：该行红字，卡片进校验态', 'xflow.http', 'action', 'globe', [
    { label: '请求', value: { kind: 'missing', text: 'URL 未填写' } },
    { label: '认证', value: { kind: 'plain', text: '无' } },
  ], { id: 'risk_check', label: '调用风控', invalid: true, badge: 1 }),
  card('run-ok', '运行态：配置摘要保留，运行 meta 在下', 'xflow.http', 'action', 'globe', [
    {
      label: '请求',
      value: {
        kind: 'mix',
        parts: [{ text: 'GET', mono: true }, { text: 'crm.example.test/api/customers/{{id}}' }],
      },
    },
    { label: '认证', value: { kind: 'ref', text: 'crm_token' } },
  ], {
    id: 'fetch_profile_run',
    label: '拉取客户档案',
    status: 'success',
    meta: { left: '200 · 1.2 KB', right: '412 ms · 第 2 次', rightTodo: '需新字段' },
  }),
  card('run-failed', '失败：错误行在最下；摘要里没有运行细节', 'xflow.function', 'code', 'code', [
    { label: '语言', value: { kind: 'plain', text: 'JavaScript · 42 行' } },
    { label: '超时', value: { kind: 'plain', text: '5 s' } },
  ], {
    id: 'compute_score_run',
    label: '计算评分',
    status: 'failed',
    meta: { left: '第 3 次', right: '重试耗尽' },
    error: 'score service returned 503: upstream unavailable',
  }),
];

/** 3 · composer samples. Controls are AntD; only the copy lives here. */
export const composerSelects = {
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  approvers: ['ops-lead', 'finance-mgr'],
  timezones: ['Asia/Shanghai', 'UTC', 'America/New_York'],
  modes: ['任一通过', '全部通过'],
};

/** One cell of the composer wall: a caption plus the fields it samples. */
export interface ComposerCard {
  id: string;
  caption: string;
  fields: FieldSpec[];
}

export const composerCards: ComposerCard[] = [
  {
    id: 'input',
    caption: 'Input · 默认 / 聚焦 / 错误 / 只读',
    fields: [
      { label: '节点名称', value: 'fetch_profile', required: true },
      { label: '路径', value: '/hooks/customer', focus: true },
      {
        label: '表名',
        value: '输入表名',
        placeholder: true,
        required: true,
        error: { tone: 'danger', text: '必填' },
      },
      { label: '节点类型', value: 'xflow.http', mono: true, readOnly: true },
    ],
  },
  {
    id: 'select',
    caption: 'Select / MultiSelect / Segmented',
    fields: [
      { label: '方法', value: 'POST', control: 'select', options: composerSelects.methods, fx: true },
      { label: '审批人', value: '', control: 'multi', options: composerSelects.approvers },
      { label: '审批模式', value: composerSelects.modes[0], control: 'seg', options: composerSelects.modes },
    ],
  },
  {
    id: 'number',
    caption: 'InputNumber / Switch / Slider',
    fields: [
      {
        label: '超时（秒）',
        value: '默认：30',
        control: 'number',
        placeholder: true,
        hint: '留空使用默认值',
      },
      {
        label: '重试次数',
        value: '-',
        control: 'number',
        placeholder: true,
        hint: '该值尚未写入定义，保存时会被忽略',
        error: { tone: 'danger', text: '请输入完整数字' },
      },
      { label: '失败后继续', control: 'switch' },
      { label: '禁用此节点', control: 'switch', switchOn: false },
      { label: '采样率', value: '30', control: 'slider', min: 0, max: 100, unit: '%' },
    ],
  },
  {
    id: 'expr',
    caption: 'Expression / 参数 JSON',
    fields: [
      { label: '条件', value: '{{ $json.tier }}', control: 'expr', required: true },
      {
        label: '参数（JSON）',
        /* Three lines in a `rows={3}` box: the fixture must not scroll, or the
         * last line renders half cut and reads as a layout bug. */
        value: '{"limit": 20,\n "sort":\n}',
        control: 'area',
        mono: true,
        error: { tone: 'danger', text: 'JSON 无效，未保存：第 2 行缺少值' },
      },
    ],
  },
];

/** Key/value rows with a row-level error (§6.4). */
/**
 * A duplicated key puts every row holding it in error, one line per row: neither
 * row is written, so neither may read as saved (user ruling 2026-10-16).
 */
export const composerHeaders = [
  { id: 'h1', key: 'Accept', value: 'application/json', error: '键重复，未保存' },
  { id: 'h2', key: 'Accept', value: 'text/html', error: '键重复，未保存' },
];

/** A collapsed section whose error count has to stay visible (§6.4). */
export const composerCollapsed = [
  { id: 'advanced', label: '高级设置', chip: { tone: 'danger' as Tone, label: '2 个错误' } },
  { id: 'retry', label: '重试策略', chip: { tone: 'neutral' as Tone, label: '已配置' } },
];

/* ===================== 7 · 运行记录模式（§6.8） ===================== */

/**
 * The execution list is an array, not a hand-written list of rows: the panel
 * header, the status bar and the filter counts all read it, and the node replay
 * below is keyed by execution id, so the same run cannot be 失败 in one place and
 * 运行中 in another.
 */
export interface ExecRow {
  id: string;
  status: ExecutionStatus;
  created: string;
  /** `updated_at − created_at`, only shown once the run is terminal (§6.8). */
  duration?: string;
  /** First line of the failure, shown only when the run has an error. */
  detail?: string;
  /** Queue position while the run waits for a slot. */
  queuePosition?: number;
}

export const wbExecList: ExecRow[] = [
  /** The elapsed time is the run bar's own field, so the two cannot disagree. */
  { id: wbExec.id, status: 'running', created: '14:02:11', duration: `已运行 ${wbExec.elapsed}` },
  { id: 'exec-8430', status: 'paused', created: '13:58:40' },
  {
    id: 'exec-8429',
    status: 'failed',
    created: '13:51:02',
    duration: '4.1 s',
    detail: 'score service returned 503',
  },
  { id: 'exec-8427', status: 'queued', created: '13:50:59', queuePosition: 2 },
  { id: 'exec-8411', status: 'failed', created: '13:20:44', duration: '3.9 s' },
  { id: 'exec-8406', status: 'success', created: '13:02:18', duration: '2.2 s' },
  { id: 'exec-8399', status: 'canceled', created: '12:40:03' },
];

export const wbExecFilters = ['全部', '失败', '运行中', '已暂停'];

/** `exec-8432` is the run the workbench section is showing, so it has to stay 运行中. */
export const wbSelectedExec = wbExecList.find((row) => row.id === 'exec-8429')!;

/**
 * Per-node replay of the selected execution. Keyed by node id and covering every
 * node of `wbNodes` — the snapshot is of the same workflow, so the read-only
 * canvas draws the same seven nodes with this run's statuses rather than a
 * smaller picture of its own.
 *
 * The run is a rerun of `exec-8411` starting at `enrich_score`: upstream nodes
 * carry "沿用原执行结果", the start node carries "重跑起点" (§6.8).
 */
export interface ReplayPatch {
  id: string;
  status: NodeStatus;
  /** Overrides the status chip's text where the run has something better to say. */
  chipLabel?: string;
  /** The run's own right-hand note (retry count …); the left-hand one is derived. */
  metaRight?: string;
  error?: string;
  badge?: number;
  invalid?: boolean;
}

export const wbRunReplay = {
  execId: wbSelectedExec.id,
  version: wbWorkflow.version,
  created: wbSelectedExec.created,
  updated: '13:51:06',
  status: wbSelectedExec.status,
  lineage: { from: 'exec-8411', startNode: 'enrich_score' },
  /** Definition age warning when the snapshot's version is gone (§6.8). */
  staleDefinition: false,
  /**
   * Only the run's own values live here. 「沿用原执行结果」 is not one of them: it
   * is the *upstream set of the start node*, walked backwards from `wbEdges`, and
   * the replay marks every node in that set. Hand-written, the label was on two
   * of the four reused nodes.
   */
  nodes: [
    { id: 'webhook', status: 'success' },
    { id: 'fetch_profile', status: 'success', metaRight: '第 2 次' },
    { id: 'is_vip', status: 'success', chipLabel: 'false' },
    { id: 'manager_approval', status: 'skipped' },
    { id: 'upsert_customer', status: 'skipped' },
    { id: 'notify_sales', status: 'success' },
    {
      id: 'enrich_score',
      status: 'failed',
      metaRight: '第 3 次',
      error: 'score service returned 503: upstream unavailable',
    },
  ] as ReplayPatch[],
};

/** 重跑的回放对上游节点用的两条标注（runs 一节据此渲染 meta 行）。 */
export const replayMetaLabels = { reuse: '沿用原执行结果', restart: '重跑起点' };

/**
 * 回放画布比工作台低的那几格（§6.8）。回放的卡片更高：上游多一行
 * 「沿用原执行结果」，起点多一行错误摘要。照编辑态的坐标画，通知销售会压住
 * 计算评分、is_vip 会压住「普通客户」的框标签。静态稿的回放画布同样是给
 * 卡片按高度单独排的（计算评分的 top:254 对主管审批的 44）。
 *
 * 只列**会动的那几处**：节点 id、连线（`wbEdges` 的 route 由实测卡片矩形解算，
 * 回放挪动卡片时连线自己跟着走）、分区框成员、线型来源都还是工作台那一份，
 * 没有任何第二张图。
 */
export const replayLayout = {
  nodeY: { notify_sales: 186, enrich_score: 286 } as Record<string, number>,
  frame: { id: 'frame-normal', top: 178, height: 174 },
  labelTop: { 'l-false': 167 } as Record<string, number>,
};

/** The inspector's 运行 tab for the node the replay has selected (§6.4). */
export const wbRunDetail = {
  nodeId: 'enrich_score',
  nodeLabel: '计算评分',
  nodeType: 'xflow.function',
  status: 'failed' as NodeStatus,
  execId: wbRunReplay.execId,
  rows: [
    { label: '尝试', value: '第 3 次（重试耗尽）' },
    { label: '输出端口', value: '—', hint: '节点失败，没有命中任何出口' },
    { label: '耗时', value: '3.1 s', todo: '需新字段' as TodoLabel, hint: '节点耗时 NodeDetail 无此字段' },
  ],
  error: {
    text: 'score service returned 503: upstream unavailable',
    details: [
      { key: 'status', value: '503' },
      { key: 'endpoint', value: '"score.internal/v2"' },
    ],
    hint: 'error_details 为服务端脱敏后的扁平字段。',
  },
  output: { empty: true },
};

/* ===================== 8 · 审批处理（§6.12） ===================== */

/**
 * The approval node in `wbNodes` is `manager_approval`, suspended on `exec-8432` —
 * the execution the workbench's run bar is showing. The dialog, the ledger and the
 * entry points all read that same pair, so the run cannot be waiting on one node
 * in the canvas and a different one in the panel.
 */
export const wbApproval = {
  nodeId: wbApprovalChain.nodeId,
  nodeLabel: wbApprovalChain.nodeLabel,
  nodeType: 'xflow.approval',
  execId: wbExec.id,
  /** `<node>/approval/<当前用户>` in sequential mode (§6.12). */
  signal: `${wbApprovalChain.nodeId}/approval/${wbApprovalChain.actor}`,
  mode: wbApprovalChain.mode,
  actor: wbApprovalChain.actor,
  /** 审批链的唯一来源（见 wbApprovalChain）：卡片备注、卡片画廊、本对话框共用。 */
  chain: wbApprovalChain,
  position: wbApprovalPosition,
  /**
   * The four outlets of the card's port row. The three decisions map to the
   * first three; `timeout` is the footer's policy, not a button.
   */
  ports: [
    { id: 'approved', label: '同意', port: 'approved', tone: 'success' as Tone, hint: '走 approved 出口' },
    { id: 'rejected', label: '驳回', port: 'rejected', tone: 'danger' as Tone, hint: '走 rejected 出口' },
    { id: 'returned', label: '退回', port: 'returned', tone: 'warning' as Tone, hint: '走 returned 出口' },
  ],
  /** `timeout` + `timeout_action` (§6.12 页脚). */
  timeout: { port: 'timeout', policy: '超时 72h 后自动驳回' },
  /**
   * One row per approver, in chain order — the ledger *is* the chain, so the
   * 2/3 in the header, the 已处理 / 待处理 counts and the row count cannot
   * disagree with the names the card gallery shows.
   */
  ledger: wbApprovalChain.approvers.map((approver, index) => ({
    id: approver,
    actor: approver === wbApprovalChain.actor ? `你（${approver}）` : approver,
    state: (index < wbApprovalIndex ? 'decided' : index === wbApprovalIndex ? 'current' : 'waiting') as
      | 'decided'
      | 'current'
      | 'waiting',
    action: index < wbApprovalIndex ? '同意' : '',
    comment: index < wbApprovalIndex ? '金额在额度内' : index === wbApprovalIndex ? '当前待你处理' : '未到',
    when: index < wbApprovalIndex ? '13:58' : index === wbApprovalIndex ? '等待 4 分钟' : '',
  })),
  secondary: [
    { id: 'delegate', label: '转交…', icon: 'user' as IconKey },
    { id: 'add-signer', label: '加签…', icon: 'link' as IconKey },
    { id: 'force', label: '强制通过', icon: 'shield' as IconKey, danger: true, title: '仅 force_approvers 中的人可见' },
  ],
  /** 加签 dialog (§6.12): position only matters in sequential mode. */
  addSigner: {
    positions: ['在我之前', '在我之后'],
    hint: '只在「依次审批」模式下影响顺序；审批链最多 32 人。',
    ignoredHint: '提交后若信号未被采纳（不在审批链、已决定等），会在审批记录的「被忽略」中显示原因。',
  },
  /** 当前用户不在审批人里：对话框只读（§6.12）. */
  outsider: { actor: 'qa-viewer', note: '你不是审批人，本次审批只读' },
};

/** Entry points that open the dialog (§6.12); all three read one node + exec pair. */
export const wbApprovalEntries = [
  { id: 'card', text: `${wbApproval.nodeLabel} 卡片的「处理」按钮`, where: '画布' },
  { id: 'runbar', text: `${wbExec.counts.suspend} 等待审批`, where: '运行条' },
  { id: 'admin', text: '处理审批', where: '管理端' },
];

/* ===================== 5b · 边的外观、端口标签、选中与悬停 ===================== */

/**
 * The gallery is grouped by what is *drawn*, and each row's status list is read
 * off `nodeStatusMeta[...].edge` through `edgeVariant` — adding a status to the
 * table puts it in a row here without touching this file.
 */
export interface EdgeSampleRow {
  variant: EdgeVariant;
  statuses: NodeStatus[];
  /** Distinct `入边` strings §4 uses for these statuses. */
  appearances: string[];
  note?: string;
}

const edgeRowOrder: EdgeVariant[] = ['default', 'success', 'flow', 'flow-slow', 'skipped', 'none'];
const allNodeStatuses = Object.keys(nodeStatusMeta) as NodeStatus[];

export const wbEdgeSamples: EdgeSampleRow[] = edgeRowOrder
  .map((variant) => {
    const statuses = allNodeStatuses.filter((status) => edgeVariant(status) === variant);
    return {
      variant,
      statuses,
      appearances: [...new Set(statuses.map((status) => nodeStatusMeta[status].edge))],
    };
  })
  .filter((row) => row.statuses.length > 0);

/** §6.2: the error outlet is a red port whose edge and label are danger-toned. */
export const wbEdgeLabelRows = [
  { id: 'true', text: 'true', tone: 'neutral' as Tone, caption: '条件分支的真出口（§6.2 连线标签）' },
  { id: 'false', text: 'false', tone: 'neutral' as Tone, caption: '条件分支的假出口' },
  { id: 'error', text: 'error', tone: 'danger' as Tone, caption: 'on_error=output 的错误出口，端口与标签同为 danger' },
];

/**
 * §6.2 defines the resting line (1.5px `border-strong`) and hover (2px primary).
 * Edge *selection* is defined nowhere — neither §6.2 nor the mockup — so the
 * selected row below is this build's proposal (2px primary + primary-toned
 * label) and is reported as an open question rather than presented as settled.
 */
export const wbEdgeStateRows = [
  { id: 'rest', caption: '默认 · 1.5px border-strong', variant: 'default' as const, state: 'rest' },
  { id: 'hover', caption: '悬停 · 2px primary（§6.2）', variant: 'default' as const, state: 'hover' },
  { id: 'selected', caption: '选中 · 规范未定义，本稿提案：2px primary + 主色标签', variant: 'default' as const, state: 'selected' },
];

/* ---- 5b · 编辑态与异常态（§6.5.1 + §6.2 的错误出口与低缩放） ---- */

export interface ExceptionAction {
  label: string;
  disabled?: boolean;
}

export interface ExceptionCard {
  id: string;
  title: string;
  note: string;
  /** Banner-styled cards (§6.5.1). `conflict` records a spec/mockup disagreement. */
  banner?: {
    tone: Tone;
    icon: IconKey;
    strong: string;
    text: string;
    actions: ExceptionAction[];
    conflict?: string;
  };
  empty?: { title: string; text: string; actions: string[] };
  json?: string;
  /** Canvas cards: the nodes and edges to draw, plus the LOD scale when set. */
  nodes?: PreviewNode[];
  edges?: PreviewEdge[];
  lodScale?: number;
}

const exceptionNode = (
  id: string,
  label: string,
  type: string,
  category: Category,
  icon: IconKey,
  x: number,
  y: number,
  extra: Partial<PreviewNode> = {},
): PreviewNode => ({ id, label, type, category, icon, x, y, ...extra });

const line = (from: number, y1: number, to: number, y2: number) =>
  `M${from} ${y1} C ${from + (to - from) / 2} ${y1}, ${from + (to - from) / 2} ${y2}, ${to} ${y2}`;

/*
 * 异常态画布是定尺插图：卡片由 `exceptionNode` 摆在 12 / 292 / 576，没有实测矩形，
 * 所以上面这些 `line(...)` 直接写端点。端点都按同一条约定算：横连的 y = 卡顶 +
 * `HANDLE_Y`（句柄中心），纵连落在目标卡左缘的句柄高度。
 */

export const wbExceptionCards: ExceptionCard[] = [
  {
    id: 'empty',
    title: '空画布',
    note: '仅 0 节点时出现',
    empty: {
      title: '添加触发器开始',
      text: '从 Webhook、Cron、Kafka 等入口开始，或导入已有 DSL。',
      actions: ['选择触发器', '导入 DSL'],
    },
  },
  {
    id: 'degraded',
    title: '节点类型加载失败',
    note: '参数降级为 JSON',
    banner: {
      tone: 'warning',
      icon: 'warn',
      strong: '节点类型加载失败',
      text: '参数仅可按 JSON 编辑；画布与连线不受影响。',
      actions: [{ label: '重试' }],
    },
    json: '{\n  "method": "GET",\n  "url": "{{ $vars.crm_base }}/customers",\n  "timeout": "5s"\n}',
    nodes: [exceptionNode('fetch_profile', '拉取客户档案', 'xflow.http', 'unknown', 'grid', 12, 16)],
  },
  {
    id: 'conflict',
    title: '保存冲突',
    note: '服务端修订号已变化',
    banner: {
      tone: 'danger',
      icon: 'x',
      strong: '保存失败：工作流已被更新',
      text: '你基于的版本已不是最新。本地修改仍保留。',
      actions: [{ label: '重新加载' }, { label: '另存为副本' }],
    },
    nodes: [
      exceptionNode('webhook', '客户回调', 'xflow.trigger.webhook', 'trigger', 'link', 12, 20, { handles: 'out' }),
      exceptionNode('fetch_profile', '拉取客户档案', 'xflow.http', 'action', 'globe', 292, 20, { handles: 'in' }),
    ],
    edges: [{ id: 'x1', from: 'webhook', to: 'fetch_profile', d: line(276, 20 + HANDLE_Y, 292, 20 + HANDLE_Y) }],
  },
  {
    id: 'readonly',
    title: '只读',
    note: '无编辑权限 / 查看历史版本',
    banner: {
      tone: 'neutral',
      icon: 'shield',
      strong: '只读',
      text: '你在当前命名空间只有查看权限。可以查看定义与运行，不能保存或发布。',
      actions: [],
      conflict:
        '静态稿画的是两个禁用按钮（保存 / 发布）；规范 §6.5.1 与 §6.11 都要求「隐藏而不是禁用」，此处按规范隐藏',
    },
    nodes: [
      exceptionNode('webhook', '客户回调', 'xflow.trigger.webhook', 'trigger', 'link', 12, 20, { handles: 'out' }),
      exceptionNode('fetch_profile', '拉取客户档案', 'xflow.http', 'action', 'globe', 292, 20, { handles: 'in' }),
    ],
    edges: [{ id: 'x2', from: 'webhook', to: 'fetch_profile', d: line(276, 20 + HANDLE_Y, 292, 20 + HANDLE_Y) }],
  },
  {
    id: 'outlet',
    title: '错误出口 + 编辑态校验',
    note: 'on_error=output',
    nodes: [
      exceptionNode('charge_payment', '扣款', 'charge_payment · xflow.http', 'action', 'globe', 12, 16, {
        handles: 'out',
        ports: ['main', 'error'],
        meta: { left: 'error 出口已连', right: 'main → 标记已付款' },
      }),
      exceptionNode('mark_paid', '标记已付款', 'mark_paid · xflow.database', 'action', 'db', 292, 140, { handles: 'in' }),
      exceptionNode('notify_failure', '失败通知', 'notify_failure · xflow.notification', 'action', 'bell', 292, 16, {
        handles: 'in',
        invalid: true,
        badge: 1,
        error: '缺少必填参数：channel',
      }),
    ],
    edges: [
      /*
       * §6.2: 有 error 出口的节点，main 出口的句柄挂在 main chip 的下沿（标题行没有出
       * 句柄）。端口行是卡片最后一行，所以这个句柄离卡底只有 11px（10px 行内边距 +
       * 1px 边框）：线从句柄出发，只有一小段被卡片盖住就露出来。起点 = 句柄中心：
       * x = 卡片左缘 12 + chip 中心 32.27 ≈ 44，y = 卡顶 16 + chip 下沿 108 = 124
       * （浏览器实测）。线先垂直落到卡片下方、再横穿到目标的入句柄
       * （292, 140 + HANDLE_Y）。
       * 声明 `fromPort`，S2 因此按 main chip 而不是整卡量起点。
       */
      {
        id: 'x3',
        from: 'charge_payment',
        fromPort: 'main',
        to: 'mark_paid',
        d: `M44 124 V ${140 + HANDLE_Y - 8} Q 44 ${140 + HANDLE_Y} 52 ${140 + HANDLE_Y} H 292`,
      },
      /*
       * §6.2: the error outlet's route is danger-coloured because of the *port it
       * leaves*, not because of the node it lands on — the one edge that carries
       * an explicit kind. Its start point is the error handle: `M276 115` = the
       * card's right edge (12 + 264) at the port row, y measured in the browser
       * as the handle's own centre (card top 16 + 99). It ends on the target's
       * in-handle (292, 16 + HANDLE_Y). E1 re-measures the start.
       */
      { id: 'x4', from: 'charge_payment', to: 'notify_failure', kind: 'error', d: `M276 115 C 284 115, 284 ${16 + HANDLE_Y}, 292 ${16 + HANDLE_Y}` },
    ],
  },
  {
    id: 'lod',
    title: '低缩放（< 50%）',
    note: '只留图标 + 名称 + 状态图标',
    /*
     * 低缩放看的是同一张画布的「缩小」，不是「挤在一起」：卡片在画布单位里仍然是
     * 264 宽，缩放只是把整层乘 0.45。列距因此必须 ≥ 卡片宽（284 = 264 + 20 间距），
     * 用 160 的话缩到 45% 时相邻卡片会真的叠在一起（C2 扫描抓到的就是这个）。
     */
    lodScale: 0.45,
    nodes: [
      exceptionNode('webhook', '客户回调', 'webhook', 'trigger', 'link', 8, 12, { handles: 'out', status: 'success' }),
      exceptionNode('is_vip', '是否 VIP', 'xflow.if', 'control', 'branch', 292, 8, {
        handles: 'both',
        status: 'success',
        ports: ['true', 'false'],
      }),
      exceptionNode('manager_approval', '主管审批', 'xflow.approval', 'human', 'user', 576, 4, {
        handles: 'in',
        status: 'suspended',
        note: { text: '等待审批' },
      }),
      exceptionNode('enrich_score', '计算评分', 'xflow.function', 'code', 'code', 576, 128, {
        handles: 'in',
        status: 'failed',
        error: 'score service returned 503',
      }),
    ],
    /*
     * 端点落在句柄中心：出句柄在源卡右缘（y = 卡顶 + `HANDLE_Y`），入句柄在目标卡
     * 左缘。线型同样由目标状态决定：`x7` 指向失败的 计算评分，§4 判它画**默认实线**
     * —— 失败是目标节点自己的边框与 chip 的事，入边不带这个状态（与 runs 回放里
     * enrich_score 用同一条 `edgeKindOf` 得到的结果一致）。
     */
    edges: [
      { id: 'x5', from: 'webhook', to: 'is_vip', d: line(272, 12 + HANDLE_Y, 292, 12 + HANDLE_Y) },
      { id: 'x6', from: 'is_vip', to: 'manager_approval', d: line(556, 8 + HANDLE_Y, 576, 8 + HANDLE_Y) },
      { id: 'x7', from: 'is_vip', to: 'enrich_score', d: line(556, 8 + HANDLE_Y, 576, 128 + HANDLE_Y) },
    ],
  },
];


/* ===================== 6 · 管理端控制台（§6.6） ===================== */

export interface AdminExecRow {
  id: string;
  workflowId: string;
  status: ExecutionStatus;
  created: string;
  updated: string;
  error?: string;
  queuePosition?: number;
}

/**
 * 管理端执行列表 = 工作台那份执行列表（`wbExecList`）加上另外两个工作流的行。
 * 同 id 的行直接读 `wbExecList` 的 status / created，不重抄一份状态：同一个执行
 * 在运行记录列表、运行条、管理端列表里只能是同一个状态（S7）。
 */
const adminExec = (id: string, workflowId: string, updated: string): AdminExecRow => {
  const row = wbExecList.find((exec) => exec.id === id)!;
  return {
    id: row.id,
    workflowId,
    status: row.status,
    created: row.created,
    updated,
    error: row.detail,
    queuePosition: row.queuePosition,
  };
};

/**
 * 管理端执行列表：**客户同步的行就是 `wbExecList` 全量**，一行不少也一行不多，归属
 * 一律是 `wbWorkflow.id`。早先这份数组只挑了其中几行、还把 `exec-8427` 判给了夜间对账
 * —— 同一个 exec id 在两处属于不同工作流，运行记录面板说它是客户同步的排队执行。
 * 别的执行 id 全在下面单列，工作流各自不同。
 *
 * 顺序按 updated 倒序：`adminLastRunOf` 取的是每个工作流的第一行。
 */
export const adminExecutions: AdminExecRow[] = [
  adminExec('exec-8432', wbWorkflow.id, '14:02:23'),
  { id: 'exec-8431', workflowId: 'purchase-approval', status: 'running', created: '14:00:02', updated: '14:00:05' },
  adminExec('exec-8430', wbWorkflow.id, '13:59:02'),
  adminExec('exec-8429', wbWorkflow.id, '13:51:06'),
  adminExec('exec-8427', wbWorkflow.id, '13:50:59'),
  {
    /* 夜间对账自己的执行，id 不与运行条变体墙（8434 / 8433 / 8432 / 8430 / 8428 /
       8406 / 8429 / 8425 / 8420）或 `wbExecList` 重复。 */
    id: 'exec-8426',
    workflowId: 'nightly-reconcile',
    status: 'queued',
    created: '13:50:31',
    updated: '13:50:31',
  },
  {
    id: 'exec-8420',
    workflowId: 'incident-triage',
    status: 'timeout',
    created: '13:31:40',
    updated: '13:41:40',
    error: 'execution deadline exceeded',
  },
  adminExec('exec-8411', wbWorkflow.id, '13:20:47'),
  adminExec('exec-8406', wbWorkflow.id, '13:02:20'),
  adminExec('exec-8399', wbWorkflow.id, '12:40:09'),
];

export interface AdminWorkflowRow {
  id: string;
  name: string;
  version: string;
  /** 触发类型（§6.6：列表接口还没有这一列）。 */
  entry: string;
}

/** `updated − created`，仅终态行显示（§6.8）；运行中的行时长还在增长。 */
export function adminExecDuration(row: AdminExecRow): string {
  if (!workflowStatusMeta[row.status].terminal) return '—';
  const toSeconds = (value: string) => {
    const [hours, minutes, seconds] = value.split(':').map(Number);
    return hours * 3600 + minutes * 60 + seconds;
  };
  const delta = toSeconds(row.updated) - toSeconds(row.created);
  if (!Number.isFinite(delta) || delta < 0) return '—';
  return delta < 60 ? `${delta} s` : `${Math.floor(delta / 60)}m ${delta % 60}s`;
}

/** 工作台那个工作流的那一行读工作台的字段，不在管理端重抄（S9）。 */
export const adminWorkflows: AdminWorkflowRow[] = [
  {
    id: wbWorkflow.id,
    name: wbWorkflow.name,
    version: wbWorkflow.version,
    entry: 'Webhook',
  },
  { id: 'purchase-approval', name: '采购审批', version: 'v7', entry: 'Cron' },
  { id: 'incident-triage', name: '告警归因', version: 'v18', entry: 'Kafka' },
  { id: 'nightly-reconcile', name: '夜间对账', version: 'v1.4.0', entry: 'Cron' },
];

/** 每一行的「最近运行」= 该工作流在 `adminExecutions` 里的第一条（S9 用它核对）。 */
export const adminLastRunOf = (workflowId: string): AdminExecRow | undefined =>
  adminExecutions.find((exec) => exec.workflowId === workflowId);

/** 分页：总页数由行数算出，不写死「第 1 / 3 页」。 */
export const adminPageSize = 20;
export const adminPageCount = (total: number) => Math.max(1, Math.ceil(total / adminPageSize));

/**
 * 左导航（§6.6）：计数从列表行数算。哪一项是当前页由 `AdminShell` 的 `current`
 * prop 决定 —— 数据层不知道这一屏渲染的是哪一节。
 */
export interface AdminNavItem {
  id: string;
  label: string;
  icon: IconKey;
  count?: number;
}

export const adminNavGroups: { id: string; label: string; items: AdminNavItem[] }[] = [
  {
    id: 'workflow-center',
    label: '工作流中心',
    items: [
      { id: 'workflows', label: '工作流', icon: 'branch', count: adminWorkflows.length },
      { id: 'executions', label: '运行记录', icon: 'clock', count: adminExecutions.length },
    ],
  },
  {
    id: 'platform',
    label: '平台',
    items: [
      { id: 'node-types', label: '节点库', icon: 'grid' },
      { id: 'runners', label: 'Runner 与池', icon: 'server' },
    ],
  },
];

/** 集群状态（左导航底部）：管理接口未开启时整组隐藏（§6.6）。 */
export const adminCluster = { pod: 'prod.crm', runners: '12 / 12 在线', leader: 'server-a' };

/** 三张状态卡：`source` 是各自的接口（§6.6）。 */
export const adminEnvCards = [
  { id: 'runners', icon: 'server' as IconKey, value: '12 / 12', label: 'Runner 在线', source: 'management/runners' },
  { id: 'pools', icon: 'grid' as IconKey, value: '3', label: 'Runner 池', source: 'management/runner-pools' },
  { id: 'types', icon: 'code' as IconKey, value: '28', label: '可用节点类型', source: 'node-types' },
];

/** 创建入口：模板库没有数据源，不放（§6.6）。 */
export const adminCreateCards = [
  { id: 'blank', icon: 'branch' as IconKey, title: '从空白创建', text: '进入编辑器，从触发器开始搭建。', action: '新建' },
  { id: 'import', icon: 'code' as IconKey, title: '导入 DSL', text: '导入 YAML / JSON 定义，注册前完成校验。', action: '选择文件' },
];

/** 最近运行卡：跨工作流，`GET /v1/executions?page_size=3`（§6.6，用户裁定 26）。 */
export const adminRecentRuns = adminExecutions.slice(0, 3);

/** 管理端列表的状态筛选：只列出行里真的出现过的状态（S11）。 */
export const adminExecFilters = [
  '全部',
  ...(Object.keys(workflowStatusMeta) as ExecutionStatus[])
    .filter((status) => adminExecutions.some((row) => row.status === status))
    .map((status) => workflowStatusMeta[status].label),
];

/** 执行行 → 工作流名字：两列读的是同一份 `adminWorkflows`，不各写一个名字。 */
export const adminWorkflowNameOf = (workflowId: string): string =>
  adminWorkflows.find((row) => row.id === workflowId)?.name ?? workflowId;

/**
 * 执行列表的重跑起点（§6.9）：默认选中快照里第一个失败节点，没有失败节点时退回
 * 起点节点。选项来自回放图 —— 那个图就是被调度器接受的修订。
 */
export const adminRerunTargets = wbRunReplay.nodes
  .filter((patch) => patch.status === 'failed')
  .map((patch) => patch.id);
export const adminRerunDefault = adminRerunTargets[0] ?? wbRunReplay.lineage.startNode;

/** 空态文案（§6.6）：加载态用 AntD 表格自己的 loading，不另写一句文字。 */
export const adminEmptyText = '没有匹配的记录';
