import * as React from "react";
import {
  CheckIcon,
  ClockIcon,
  CopyIcon,
  DockDownIcon,
  DockMaxIcon,
  DockRestoreIcon,
  NodeIndexIcon,
  WarnIcon,
  XMarkIcon
} from "@xflow/icons";
import { Button, Tabs, Tooltip } from "antd";
import { categoryOf, nodeCategoryMeta, nodeStatusMeta, runtimeForNode, workflowStatusMeta } from "@xflow/core";
import type {
  ExecutionStatusKey,
  NodeCategory,
  NodeStatus,
  RuntimeSnapshot,
  WorkflowDef,
  WorkflowNode
} from "@xflow/core";
import { workflowStatusFromRuntime } from "../operationStatus";
import { editorTooltipClassNames, serializeJson } from "./shared";
import { connectionTypeFor, nodeLabel, nodeName, targetsFor, workflowNodeByName } from "./workflowOps";

/**
 * §6.5 status bar + diagnostics drawer. Both hang off one root-level dock
 * (spanning all three workbench columns, like the baseline's `.xflow-pv-bottom`):
 * the 28px status bar is always there and is itself the expand button; the
 * 256px drawer (40px title bar + issue list and a 360px summary column) mounts
 * only while open.
 */

export interface WorkflowDiagnostic {
  status: "pass" | "warn" | "error";
  /** Internal grouping key (never shown as text; the pass line maps it to a label). */
  area: string;
  message: string;
  /** The node the finding is about, set where it is created (never inferred from the message). */
  nodeName?: string;
  /** JSON Pointer into that node (a param_issue's field), when the finding has one. */
  path?: string;
  /**
   * Where a parameter finding came from: the live node form (`local`) or the
   * backend's `param_issues` (`server`). Only `server` is marked in the row —
   * local findings are what the visible draft reports. Absent for the
   * workflow-structure findings validateWorkflow creates.
   */
  source?: "local" | "server";
}

export interface DiagnosticsDockProps {
  workflow: WorkflowDef;
  runtime?: RuntimeSnapshot;
  diagnostics: WorkflowDiagnostic[];
  operationError?: string;
  /** Nodes whose unparseable parameters JSON blocked the last save/run. */
  blockedDraftNodes?: string[];
  /** Declared descriptor category per node type (spec §6.13), for the category mix. */
  nodeCategories?: Readonly<Record<string, string>>;
  /** The top bar's unsaved wording (§6.3), reused for the summary's 版本 row. */
  versionLabel: string;
  /** Local changes against the saved baseline (§6.3); drives 本地 N 处未保存. */
  unsavedCount: number;
  /** The node the canvas has selected, for the 输出 tab (WB-04). */
  selectedNode?: WorkflowNode;
  /** That node's resolved name — the runtime snapshot's key. */
  selectedNodeName?: string;
  /**
   * The live execution behind {@link runtime}, when the host has one. The
   * structural slice is widened past `XFlowExecution` with the per-node
   * `startedAt` / `finishedAt` stamps: a host that passes the API's
   * `ExecutionDetail` carries them, and WB-11 prefers them over the
   * execution-level `created_at` / `updated_at` pair.
   */
  latestExecution?: DiagnosticsRun;
  dock: DockState;
  onDockChange: (next: DockState) => void;
  /** Filled with the dock's open/close entry so the ⌘J handler can reuse its focus handling. */
  controllerRef?: React.MutableRefObject<DiagnosticsDockController | null>;
  /** Selects the node, brings it into view and lands focus on the canvas. */
  onLocateNode?: (nodeName: string) => void;
  /** Jumps to the finding's node and parameter field (P1-c S5). */
  onLocateField?: (nodeName: string, path: string) => void;
  onRevealJson?: (nodeName: string) => void;
  /**
   * Maps a run's trace id to its page in the host's tracing system (WB-06).
   * Omit it and the 运行日志 tab shows the id without an external link — the
   * editor never guesses a tracing URL.
   */
  traceHref?: (traceId: string) => string;
}

/** One node of the latest run, as the 最近运行 card reads it. */
export interface DiagnosticsRunNode {
  name: string;
  status: NodeStatus;
  /** The execution detail's attempt number, shown as 尝试 N (WB-06). */
  attempt?: number;
  /** The output port that carried the result (§6.8 命中端口). */
  port?: string;
  /** The node's failure text; the timeline shows only its first line. */
  error?: string;
  startedAt?: string;
  finishedAt?: string;
}

/** The latest-run slice the summary column reads (WB-10 / WB-11 / WB-12). */
export interface DiagnosticsRun {
  executionId?: string;
  status?: ExecutionStatusKey;
  /** The external tracing correlation id recorded at submission (WB-06). */
  traceId?: string;
  createdAt?: string;
  updatedAt?: string;
  nodes?: readonly DiagnosticsRunNode[];
}

/** The dock's three heights: status bar only, 256px drawer, drawer stretched to the canvas top. */
export type DockState = "collapsed" | "docked" | "expanded";

export interface DiagnosticsDockController {
  /** ⌘J: opens a closed dock (focus into the drawer), closes an open one. */
  toggle: () => void;
  /** Opens a closed dock without closing one that is already open. */
  open: () => void;
}

const dockShortcut = "⌘J";

/** What the "N 项检查通过" line calls each validator area. */
const passAreaLabel: Record<string, string> = {
  DSL: "DSL 结构",
  nodes: "节点",
  edges: "连线",
  dependencies: "依赖",
  credentials: "凭证",
  pin_data: "Pin Data",
  parameters: "参数",
  templates: "模板",
  variables: "变量引用",
  runner: "Runner",
  runtime: "运行态"
};

const passLabel = (area: string): string => passAreaLabel[area] ?? area;

/** Data + dependency edges, read from the workflow arrays (the validator counts the same set). */
export function countWorkflowEdges(workflow: WorkflowDef): number {
  const connected = Object.values(workflow.connections ?? {}).reduce(
    (count, ports) =>
      count + Object.values(ports).reduce((inner, portConnections) => inner + targetsFor(portConnections).length, 0),
    0
  );
  return connected + (workflow.dependency_edges?.length ?? 0);
}

/**
 * Node statuses whose run is expected to have produced an output. When the
 * runtime snapshot carries none for one of these, the engine's fail-closed
 * inspection withheld it (a private node never publishes its output), so the
 * drawer must say 已遮蔽 instead of pretending the node produced nothing.
 */
const outputBearingStatuses: ReadonlySet<NodeStatus> = new Set(["success", "pinned", "continued"]);
const outputInFlightStatuses: ReadonlySet<NodeStatus> = new Set(["running", "committing", "waiting", "suspended"]);

const noSelectionOutputNote = "选择一个节点后查看它的输出。";
const notRunOutputNote = "节点尚未运行，暂无输出。";
const runningOutputNote = "节点正在运行，暂无输出。";
const noOutputNote = "节点没有可显示的输出。";
const emptyOutputNote = "节点运行完成，但没有输出。";
const privateOutputNote = "输出已遮蔽：该节点标记为私有输出，引擎不会返回原始值。";
const redactedOutputNote = "输出已遮蔽：该输出被标记为已遮蔽，不展示原始值。";
const shieldedOutputNote = "输出已遮蔽：引擎未返回该节点的公开输出（fail-closed 遮蔽），不展示原始值。";

/** What the 输出 tab draws for one selected node (WB-04). */
export type DiagnosticsOutputView =
  | { kind: "note"; note: string; tone?: "warning" }
  | { kind: "data"; text: string };

/**
 * The 输出 tab's state machine. A node whose definition marks `output.private`
 * is shown as 已遮蔽 even if a host handed the drawer its raw output: the
 * engine already refuses to publish it, and the editor must not become the
 * weaker path. An absent output on a node that did run is the same verdict,
 * because that is exactly what fail-closed inspection returns.
 */
export function diagnosticsOutputView(
  node: WorkflowNode | undefined,
  runtime: RuntimeSnapshot | undefined
): DiagnosticsOutputView {
  if (!node) return { kind: "note", note: noSelectionOutputNote };
  if (node.output?.private === true) return { kind: "note", note: privateOutputNote, tone: "warning" };
  const snapshot = runtimeForNode(runtime, node);
  const output = snapshot.output;
  if (output !== undefined && output !== null && output.redacted === true) {
    return { kind: "note", note: redactedOutputNote, tone: "warning" };
  }
  if (output === undefined || output === null) {
    if (outputBearingStatuses.has(snapshot.status)) return { kind: "note", note: shieldedOutputNote, tone: "warning" };
    if (outputInFlightStatuses.has(snapshot.status)) return { kind: "note", note: runningOutputNote };
    if (snapshot.status === "pending") return { kind: "note", note: notRunOutputNote };
    return { kind: "note", note: noOutputNote };
  }
  if (Object.keys(output).length === 0) return { kind: "note", note: emptyOutputNote };
  return { kind: "data", text: serializeJson(output) };
}

/**
 * Writes to the clipboard, swallowing a policy refusal. The text stays
 * selectable in the code block either way; nothing is logged, and the caller
 * disables the button when the environment has no clipboard at all.
 */
function copyText(text: string, clipboard: Clipboard | undefined): void {
  if (!clipboard) return;
  try {
    const result = clipboard.writeText(text);
    if (result && typeof result.catch === "function") void result.catch(() => undefined);
  } catch {
    // A refused clipboard (permission policy) is not an error worth surfacing.
  }
}

/** One incoming data edge of the selected node, as the 输入 tab (WB-05) draws it. */
export interface DiagnosticsInputEdge {
  sourceName: string;
  /** The upstream node, absent when the connection names one the definition lacks. */
  sourceNode?: WorkflowNode;
  /** The upstream output port that feeds this edge. */
  sourcePort: string;
  /** The port on the selected node the edge lands on (`main` when omitted). */
  targetPort: string;
}

/**
 * The data edges landing on `targetName`, in definition order. Dependency ports
 * are not inputs: a dependency edge binds a supply to its consumer and carries
 * no node output, so listing it here would promise a value that never flows.
 *
 * Only the graph and the runtime are read. Node parameters are deliberately
 * never consulted (WB-05): the runner resolves them, so they may hold parsed
 * credentials that must not be republished through the diagnostics drawer.
 */
export function diagnosticsInputEdges(workflow: WorkflowDef, targetName: string): DiagnosticsInputEdge[] {
  const edges: DiagnosticsInputEdge[] = [];
  for (const [sourceName, ports] of Object.entries(workflow.connections ?? {})) {
    for (const [sourcePort, portConnections] of Object.entries(ports)) {
      if (connectionTypeFor(portConnections) !== "data") continue;
      for (const target of targetsFor(portConnections)) {
        if (target.node !== targetName) continue;
        edges.push({
          sourceName,
          sourceNode: workflowNodeByName(workflow, sourceName),
          sourcePort,
          targetPort: target.input ?? "main"
        });
      }
    }
  }
  return edges;
}

/** One row of the node event timeline (WB-06). */
export interface DiagnosticsTimelineRow {
  name: string;
  label: string;
  status: NodeStatus;
  attempt?: number;
  durationMs?: number;
  error?: string;
  startedAt?: string;
}

/**
 * The node event timeline: every node of the definition, with the execution's
 * node snapshot preferred over the runtime projection for status, attempts,
 * error and timing. Rows with a `startedAt` are ordered by that event; nodes
 * without one keep graph order after them (a stable sort with an infinite key),
 * so "no timestamps at all" falls back to exactly the definition's order.
 *
 * This is not a log stream and never claims to be one: it only joins facts the
 * inspect API already publishes.
 */
export function diagnosticsTimelineRows(
  workflow: WorkflowDef,
  runtime: RuntimeSnapshot | undefined,
  execution: DiagnosticsRun | undefined
): DiagnosticsTimelineRow[] {
  const runNodes = new Map((execution?.nodes ?? []).map((node) => [node.name, node]));
  const rows = (workflow.nodes ?? []).map((node, index) => {
    const name = nodeName(node, index);
    const runNode = runNodes.get(name);
    const snapshot = runtimeForNode(runtime, node);
    let durationMs = snapshot.durationMs;
    const started = parseTimestamp(runNode?.startedAt);
    const finished = parseTimestamp(runNode?.finishedAt);
    if (started !== undefined && finished !== undefined && finished >= started) {
      durationMs = finished - started;
    }
    return {
      name,
      label: nodeLabel(node, index),
      status: runNode?.status ?? snapshot.status,
      attempt: runNode?.attempt ?? snapshot.attempts,
      durationMs,
      error: runNode?.error ?? snapshot.error,
      startedAt: runNode?.startedAt
    };
  });
  return rows.sort((left, right) => {
    const leftAt = parseTimestamp(left.startedAt) ?? Number.POSITIVE_INFINITY;
    const rightAt = parseTimestamp(right.startedAt) ?? Number.POSITIVE_INFINITY;
    if (leftAt === rightAt) return 0;
    return leftAt < rightAt ? -1 : 1;
  });
}

/** The first line of a multi-line error; the full text stays on the row's title. */
function errorFirstLine(error: string): string {
  return error.split(/\r?\n/, 1)[0] ?? "";
}

const parseTimestamp = (value: string | undefined): number | undefined => {
  if (!value) return undefined;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : undefined;
};

/** Human-readable duration: 412 ms / 1.2s / 2m 3s (WB-11). */
export function formatRunDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return "—";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const whole = Math.round(seconds);
  const minutes = Math.floor(whole / 60);
  const rest = whole % 60;
  if (minutes < 60) return `${minutes}m ${rest}s`;
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

/** The run's elapsed span from the node stamps the host recorded, if any. */
function runSpanMs(run: DiagnosticsRun): number | undefined {
  const starts: number[] = [];
  const finishes: number[] = [];
  for (const node of run.nodes ?? []) {
    const started = parseTimestamp(node.startedAt);
    const finished = parseTimestamp(node.finishedAt);
    // Only a complete pair says anything about elapsed time; mixing one node's
    // start with another's finish would invent a span across a gap.
    if (started === undefined || finished === undefined || finished < started) continue;
    starts.push(started);
    finishes.push(finished);
  }
  if (starts.length === 0) return undefined;
  return Math.max(...finishes) - Math.min(...starts);
}

/**
 * 耗时 (WB-11): node `started_at → finished_at` first, execution
 * `created_at → updated_at` as the fallback. A non-terminal run says 进行中
 * rather than reporting the time to its latest status transition as elapsed.
 */
export function runDurationText(run: DiagnosticsRun | undefined, status: ExecutionStatusKey | undefined): string {
  if (!status) return "—";
  if (!workflowStatusMeta[status].terminal) return "进行中";
  if (!run) return "—";
  const span = runSpanMs(run);
  if (span !== undefined) return formatRunDuration(span);
  const created = parseTimestamp(run.createdAt);
  const updated = parseTimestamp(run.updatedAt);
  if (created !== undefined && updated !== undefined && updated >= created) {
    return formatRunDuration(updated - created);
  }
  return "—";
}

/**
 * 命中分支 (WB-12): the port of the most recently finished node that took one.
 * Without node stamps the last run node carrying a port wins; the runtime
 * snapshot is the fallback when the host passed no execution detail. Both are
 * visible text in the card, never a tooltip-only value.
 */
export function hitPortOf(run: DiagnosticsRun | undefined, runtime: RuntimeSnapshot | undefined): string | undefined {
  const candidates = (run?.nodes ?? []).filter((node) => Boolean(node.port));
  if (candidates.length > 0) {
    let best = candidates[0];
    let bestAt = parseTimestamp(best.finishedAt) ?? parseTimestamp(best.startedAt) ?? -Infinity;
    for (const node of candidates.slice(1)) {
      const at = parseTimestamp(node.finishedAt) ?? parseTimestamp(node.startedAt) ?? -Infinity;
      if (at >= bestAt) {
        best = node;
        bestAt = at;
      }
    }
    return best.port;
  }
  const fallback = Object.entries(runtime?.nodes ?? {}).filter(([, snapshot]) => Boolean(snapshot.port));
  return fallback.length > 0 ? fallback[fallback.length - 1][1].port : undefined;
}

/** Node counts per category, in the category table's order, empty categories left out. */
function nodeMix(
  nodes: readonly WorkflowNode[],
  nodeCategories?: Readonly<Record<string, string>>
): { label: string; count: number }[] {
  const counts = new Map<NodeCategory, number>();
  for (const node of nodes) {
    const category = categoryOf(node.type, node.type ? nodeCategories?.[node.type] : undefined);
    counts.set(category, (counts.get(category) ?? 0) + 1);
  }
  return (Object.keys(nodeCategoryMeta) as NodeCategory[])
    .filter((category) => counts.has(category))
    .map((category) => ({ label: nodeCategoryMeta[category].label, count: counts.get(category) ?? 0 }));
}

interface StatusBarProps {
  buttonRef: React.RefObject<HTMLButtonElement | null>;
  /** The expanded drawer is drawn over the strip: keep it out of the tab order and the tree meanwhile. */
  covered: boolean;
  drawerId: string;
  expanded: boolean;
  waiting: boolean;
  errorCount: number;
  warningCount: number;
  runLabel?: string;
  unsavedCount: number;
  nodeCount: number;
  edgeCount: number;
  onToggle: () => void;
}

/**
 * The whole 28px strip is one button. Its name is 展开诊断台 while closed and
 * 诊断台摘要 while open, so the drawer's own 收起诊断台 is the only open/close
 * entry by name at any moment (§6.5). ⇧P toggles the summary / expanded states
 * from the same strip (CP-02), so its title names that chord as well.
 */
function DiagnosticsStatusBar({
  buttonRef,
  covered,
  drawerId,
  expanded,
  waiting,
  errorCount,
  warningCount,
  runLabel,
  unsavedCount,
  nodeCount,
  edgeCount,
  onToggle
}: StatusBarProps): React.ReactElement {
  return (
    <button
      aria-controls={expanded ? drawerId : undefined}
      aria-expanded={expanded}
      aria-keyshortcuts="Meta+J Control+J Shift+P"
      aria-label={expanded ? "诊断台摘要" : "展开诊断台"}
      className="xflow-editor-statusbar"
      inert={covered}
      ref={buttonRef}
      title={expanded ? `诊断台摘要（${dockShortcut} 收起 · ⇧P 切换展开）` : `展开诊断台（${dockShortcut} · ⇧P 切换摘要/展开）`}
      type="button"
      onClick={onToggle}
    >
      {waiting ? (
        <span className="xflow-editor-statusbar__item" data-tone="neutral">
          <ClockIcon />
          等待节点
        </span>
      ) : errorCount > 0 ? (
        <span className="xflow-editor-statusbar__item" data-tone="danger">
          <XMarkIcon />
          {errorCount} 错误
        </span>
      ) : (
        <span className="xflow-editor-statusbar__item" data-tone="success">
          <CheckIcon />
          校验通过
        </span>
      )}
      {warningCount > 0 ? (
        <span className="xflow-editor-statusbar__item" data-tone="warning">
          <WarnIcon />
          {warningCount} 警告
        </span>
      ) : null}
      {runLabel ? (
        <>
          <span aria-hidden="true" className="xflow-editor-statusbar__sep" />
          <span className="xflow-editor-statusbar__item">
            最近运行 <span className="xflow-editor-statusbar__run">{runLabel}</span>
          </span>
        </>
      ) : null}
      {unsavedCount > 0 ? (
        <span className="xflow-editor-statusbar__item" data-tone="warning">
          本地 {unsavedCount} 处未保存
        </span>
      ) : null}
      <span className="xflow-editor-statusbar__spacer" />
      <span className="xflow-editor-statusbar__stats" data-graph-stats={`${nodeCount}/${edgeCount}`}>
        {nodeCount} 节点 · {edgeCount} 连线
      </span>
    </button>
  );
}

/**
/**
 * Every tab now has a real panel behind it (WB-04 / WB-05 / WB-06), so the
 * drawer keeps no placeholder note. The keys are also the tab suffix the Tabs
 * component uses for its element ids.
 */
const tabKeys = ["issues", "logs", "input", "output"] as const;
type DiagnosticsTabKey = (typeof tabKeys)[number];

const outputTabKey: DiagnosticsTabKey = "output";

/**
 * The Tabs component owns the focusable `role="tab"` elements and offers no
 * hook for their attributes, so one layout effect points each tab's
 * `aria-controls` at the panel rendered below and clears any stale
 * placeholder description. The ids are `${tabsId}-tab-${key}` because the Tabs
 * `id` is passed in explicitly.
 */
function useTabPanels(
  drawerRef: React.RefObject<HTMLDivElement | null>,
  tabsId: string,
  panelIds: Readonly<Record<DiagnosticsTabKey, string>>
): void {
  React.useLayoutEffect(() => {
    for (const key of tabKeys) {
      const tab = drawerRef.current?.querySelector(`[role="tab"][id="${tabsId}-tab-${key}"]`);
      tab?.setAttribute("aria-controls", panelIds[key]);
      tab?.removeAttribute("aria-description");
    }
  });
}

const diagnosticsTabsClassNames = {
  body: "xflow-editor-diagnostics-tabs__body",
  content: "xflow-editor-diagnostics-tabs__content",
  header: "xflow-editor-diagnostics-tabs__header",
  item: "xflow-editor-diagnostics-tabs__item"
};

interface IssueRowProps {
  item: WorkflowDiagnostic;
  nodes: readonly WorkflowNode[];
  blockedDraftNodes?: string[];
  onLocateNode?: (nodeName: string) => void;
  onLocateField?: (nodeName: string, path: string) => void;
  onRevealJson?: (nodeName: string) => void;
}

/** severity icon | target | message | actions. Internal ids stay out of the visible text. */
function IssueRow({
  item,
  nodes,
  blockedDraftNodes,
  onLocateNode,
  onLocateField,
  onRevealJson
}: IssueRowProps): React.ReactElement {
  const nodeIndex = item.nodeName ? nodes.findIndex((node, index) => nodeName(node, index) === item.nodeName) : -1;
  const target = nodeIndex >= 0 ? nodeLabel(nodes[nodeIndex], nodeIndex) : "工作流";
  const isError = item.status === "error";

  return (
    <li className="xflow-editor-issue" data-area={item.area} data-source={item.source} data-tone={isError ? "danger" : "warning"}>
      {/* The wrapper carries the tone colour; the sprite glyph strokes in
          currentColor, so the icon itself inherits it. */}
      <span aria-label={isError ? "错误" : "警告"} className="xflow-editor-issue__sev" role="img">
        {isError ? <XMarkIcon /> : <WarnIcon />}
      </span>
      <span className="xflow-editor-issue__target" title={target}>
        {target}
      </span>
      <span className="xflow-editor-issue__text">
        {/* WB-13: a param_issue that the live form does not report is marked,
            so the user can tell a stale save-time finding from the draft. */}
        {item.source === "server" ? <span className="xflow-editor-issue__source">服务端 · </span> : null}
        {item.message}
      </span>
      <span className="xflow-editor-issue__actions">
        {item.area === "operation" && blockedDraftNodes?.length
          ? blockedDraftNodes.map((blockedName) => (
              <Button
                className="xflow-editor-issue__action"
                key={blockedName}
                size="small"
                type="link"
                onClick={() => onRevealJson?.(blockedName)}
              >
                修正 {blockedName} 的参数 JSON
              </Button>
            ))
          : nodeIndex >= 0 && item.nodeName
            ? (
              <>
                <Button
                  aria-label={`定位节点 ${item.nodeName}`}
                  className="xflow-editor-issue__action"
                  size="small"
                  type="link"
                  onClick={() => onLocateNode?.(item.nodeName!)}
                >
                  定位节点
                </Button>
                {item.path ? (
                  <Button
                    aria-label={`定位字段 ${item.nodeName} ${item.path}`}
                    className="xflow-editor-issue__action"
                    size="small"
                    type="link"
                    onClick={() => onLocateField?.(item.nodeName!, item.path!)}
                  >
                    定位字段
                  </Button>
                ) : null}
              </>
            )
            : null}
      </span>
    </li>
  );
}

interface DrawerProps
  extends Omit<
    DiagnosticsDockProps,
    "dock" | "onDockChange" | "controllerRef" | "diagnostics" | "operationError" | "unsavedCount"
  > {
  drawerId: string;
  titleId: string;
  drawerRef: React.RefObject<HTMLDivElement | null>;
  expanded: boolean;
  issues: WorkflowDiagnostic[];
  passes: WorkflowDiagnostic[];
  edgeCount: number;
  runLabel?: string;
  onCollapse: () => void;
  onToggleExpanded: () => void;
}

interface InputPanelProps {
  panelId: string;
  labelledBy: string;
  workflow: WorkflowDef;
  runtime?: RuntimeSnapshot;
  selectedNode?: WorkflowNode;
  selectedNodeName?: string;
}

/**
 * 输入 tab (WB-05, 改做形态). The evaluated input is never persisted — the runner
 * resolves parameters at dispatch, and those may contain parsed credentials —
 * so this panel derives what is publicly observable instead: each incoming data
 * edge with its upstream node's public output. It reads the graph and the
 * runtime only; `node.parameters` is never touched, and a withheld upstream
 * output is reported as 已遮蔽 rather than filled in from the definition.
 */
function InputPanel({
  panelId,
  labelledBy,
  workflow,
  runtime,
  selectedNode,
  selectedNodeName
}: InputPanelProps): React.ReactElement {
  const clipboard = typeof navigator !== "undefined" ? navigator.clipboard : undefined;
  const edges = selectedNode && selectedNodeName ? diagnosticsInputEdges(workflow, selectedNodeName) : [];

  return (
    <div
      aria-labelledby={labelledBy}
      className="xflow-editor-diagnostics-panel xflow-editor-input"
      id={panelId}
      role="tabpanel"
    >
      <p className="xflow-editor-input__notice">求值后的参数不保留，这里只显示上游节点的公开输出。</p>
      {!selectedNode || !selectedNodeName ? (
        <p className="xflow-editor-output__note">选择一个节点后查看它的输入。</p>
      ) : edges.length === 0 ? (
        <p className="xflow-editor-output__note">该节点没有入边，输入来自提交参数或触发器。</p>
      ) : (
        <ul aria-label={`${selectedNodeName} 的输入`} className="xflow-editor-input__list">
          {edges.map((edge, index) => {
            const sourceSnapshot = edge.sourceNode ? runtimeForNode(runtime, edge.sourceNode) : undefined;
            const view = edge.sourceNode ? diagnosticsOutputView(edge.sourceNode, runtime) : undefined;
            const text = view && view.kind === "data" ? view.text : undefined;
            const copyOutput = text && clipboard ? () => copyText(text, clipboard) : undefined;
            return (
              <li
                className="xflow-editor-input__row"
                data-source={edge.sourceName}
                key={`${edge.sourceName}\u0000${edge.sourcePort}\u0000${edge.targetPort}\u0000${index}`}
              >
                <div className="xflow-editor-input__head">
                  <span className="xflow-editor-input__label" title={edge.sourceName}>
                    {edge.sourceNode
                      ? nodeLabel(edge.sourceNode, workflow.nodes?.indexOf(edge.sourceNode) ?? 0)
                      : edge.sourceName}
                  </span>
                  <span className="xflow-editor-input__name">{edge.sourceName}</span>
                  <span className="xflow-editor-input__ports">
                    <span title="源端口">{edge.sourcePort}</span>
                    {" → "}
                    <span title="目标端口">{edge.targetPort}</span>
                  </span>
                  {sourceSnapshot?.port ? (
                    <span className="xflow-editor-input__hit">命中 {sourceSnapshot.port}</span>
                  ) : null}
                  {copyOutput ? (
                    <Tooltip classNames={editorTooltipClassNames} title="复制上游输出">
                      <Button
                        aria-label={`复制 ${edge.sourceName} 的输出`}
                        className="xflow-editor-input__copy"
                        icon={<CopyIcon />}
                        size="small"
                        type="text"
                        onClick={copyOutput}
                      />
                    </Tooltip>
                  ) : null}
                </div>
                {view === undefined ? (
                  <p className="xflow-editor-output__note" data-tone="warning">
                    上游节点不在工作流定义中，无法读取它的公开输出。
                  </p>
                ) : view.kind === "data" ? (
                  <pre className="xflow-editor-run-code xflow-editor-input__code" tabIndex={0}>
                    {view.text}
                  </pre>
                ) : (
                  <p className="xflow-editor-output__note" data-tone={view.tone}>
                    {view.note}
                  </p>
                )}
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

interface RunLogPanelProps {
  panelId: string;
  labelledBy: string;
  workflow: WorkflowDef;
  runtime?: RuntimeSnapshot;
  latestExecution?: DiagnosticsRun;
  traceHref?: (traceId: string) => string;
}

/**
 * 运行日志 tab (WB-06, 改做形态). There is no server-side log pipeline, so this
 * is explicitly a node-event timeline built from inspect facts (status,
 * attempt, timing, error, trace id) — labelled as such, never dressed up as a
 * raw log stream. The trace id is the only hop to real logs, through the host's
 * optional `traceHref`.
 */
function RunLogPanel({
  panelId,
  labelledBy,
  workflow,
  runtime,
  latestExecution,
  traceHref
}: RunLogPanelProps): React.ReactElement {
  const clipboard = typeof navigator !== "undefined" ? navigator.clipboard : undefined;
  const traceId = runtime?.traceId ?? latestExecution?.traceId;
  const traceUrl = traceId && traceHref ? traceHref(traceId) : undefined;
  const copyTrace = traceId && clipboard ? () => copyText(traceId, clipboard) : undefined;
  const rows = diagnosticsTimelineRows(workflow, runtime, latestExecution);
  const nodeCount = (workflow.nodes ?? []).length;
  const hasSnapshot =
    Object.keys(runtime?.nodes ?? {}).length > 0 || (latestExecution?.nodes?.length ?? 0) > 0;

  return (
    <div
      aria-labelledby={labelledBy}
      className="xflow-editor-diagnostics-panel xflow-editor-timeline"
      id={panelId}
      role="tabpanel"
    >
      <div className="xflow-editor-timeline__trace">
        <span className="xflow-editor-timeline__trace-label">trace_id</span>
        <span
          className="xflow-editor-timeline__trace-id"
          data-empty={traceId ? undefined : "true"}
          title={traceId}
        >
          {traceId ?? "未记录"}
        </span>
        <Tooltip
          classNames={editorTooltipClassNames}
          title={!traceId ? "本次执行没有 trace_id" : copyTrace ? "复制 trace_id" : "当前环境不支持剪贴板"}
        >
          <Button
            aria-label="复制 trace_id"
            className="xflow-editor-timeline__copy"
            disabled={!copyTrace}
            icon={<CopyIcon />}
            size="small"
            type="text"
            onClick={copyTrace}
          />
        </Tooltip>
        {traceId && traceUrl ? (
          <a
            className="xflow-editor-timeline__trace-link"
            href={traceUrl}
            rel="noopener noreferrer"
            target="_blank"
          >
            在追踪系统打开
          </a>
        ) : null}
      </div>
      <p className="xflow-editor-input__notice">这是节点事件时间线，不是原始运行日志。</p>
      {nodeCount === 0 ? (
        <p className="xflow-editor-output__note">定义中没有节点。</p>
      ) : !latestExecution && !runtime ? (
        <p className="xflow-editor-output__note">运行工作流后，这里显示节点事件时间线。</p>
      ) : !hasSnapshot ? (
        <p className="xflow-editor-output__note">没有节点快照，无法生成事件时间线。</p>
      ) : (
        <ol aria-label="节点事件时间线" className="xflow-editor-timeline__list">
          {rows.map((row) => {
            const meta = nodeStatusMeta[row.status];
            return (
              <li className="xflow-editor-timeline__row" data-node={row.name} key={row.name}>
                <div className="xflow-editor-timeline__head">
                  <span className="xflow-editor-timeline__label" title={row.name}>
                    {row.label}
                  </span>
                  <span className="xflow-editor-timeline__name">{row.name}</span>
                  <span className="xflow-editor-timeline__status" data-tone={meta.tone}>
                    {meta.label}
                  </span>
                  <span className="xflow-editor-timeline__meta">尝试 {row.attempt ?? "—"}</span>
                  <span className="xflow-editor-timeline__meta" data-role="duration">
                    {row.durationMs === undefined ? "—" : formatRunDuration(row.durationMs)}
                  </span>
                </div>
                {row.error ? (
                  <p className="xflow-editor-timeline__error" title={row.error}>
                    {errorFirstLine(row.error)}
                  </p>
                ) : null}
              </li>
            );
          })}
        </ol>
      )}
    </div>
  );
}

function DiagnosticsDrawer({
  drawerId,
  titleId,
  drawerRef,
  expanded,
  workflow,
  runtime,
  latestExecution,
  selectedNode,
  selectedNodeName,
  issues,
  passes,
  edgeCount,
  runLabel,
  blockedDraftNodes,
  nodeCategories,
  versionLabel,
  onCollapse,
  onToggleExpanded,
  onLocateNode,
  onLocateField,
  onRevealJson,
  traceHref
}: DrawerProps): React.ReactElement {
  const [passesOpen, setPassesOpen] = React.useState(false);
  const tabsId = React.useId();
  const panelIds = {
    issues: React.useId(),
    logs: React.useId(),
    input: React.useId(),
    output: React.useId()
  };
  const [activeTab, setActiveTab] = React.useState<DiagnosticsTabKey>("issues");
  useTabPanels(drawerRef, tabsId, panelIds);
  const nodes = workflow.nodes ?? [];
  const mix = nodeMix(nodes, nodeCategories);
  const credentials = Object.keys(workflow.credentials ?? {}).join(", ");
  const runningNode = Object.entries(runtime?.nodes ?? {}).find(([, snapshot]) => snapshot.status === "running")?.[0];
  const passSummary = [...new Set(passes.map((item) => passLabel(item.area)))].join(" · ");
  const mixText = mix.map((entry) => `${entry.label} ${entry.count}`).join(" · ");
  const expandLabel = expanded ? "还原诊断台高度" : "扩展到画布高度";
  const runStatus = (runtime ? workflowStatusFromRuntime(runtime) : undefined) ?? latestExecution?.status;
  const runTerminal = runStatus ? workflowStatusMeta[runStatus].terminal : false;
  const durationText = runDurationText(latestExecution, runStatus);
  const hitPort = hitPortOf(latestExecution, runtime);
  const outputView = diagnosticsOutputView(selectedNode, runtime);
  const clipboard = typeof navigator !== "undefined" ? navigator.clipboard : undefined;
  const outputText = outputView.kind === "data" ? outputView.text : undefined;
  const copyOutput = outputText && clipboard ? () => copyText(outputText, clipboard) : undefined;

  return (
    <div className="xflow-editor-drawer" data-dock={expanded ? "expanded" : "docked"} id={drawerId} ref={drawerRef}>
      <div className="xflow-editor-panel-title xflow-editor-drawer__head">
        {/* Icon + heading are one group 8px apart (the baseline's PanelTitle);
            the head's 16px separates that group from the tabs and the actions. */}
        <span className="xflow-editor-drawer__title">
          <span aria-hidden="true" className="xflow-editor-panel-title__icon">
            <NodeIndexIcon />
          </span>
          <h3 className="xflow-editor-panel-title__heading" id={titleId}>
            诊断台
          </h3>
        </span>
        <Tabs
          activeKey={activeTab}
          animated={false}
          className="xflow-editor-diagnostics-tabs"
          classNames={diagnosticsTabsClassNames}
          id={tabsId}
          items={[
            { key: "issues", label: `问题 ${issues.length}` },
            { key: "logs", label: "运行日志" },
            { key: "input", label: "输入" },
            { key: outputTabKey, label: "输出" }
          ]}
          size="small"
          onChange={(key) => setActiveTab(key as DiagnosticsTabKey)}
        />
        <span className="xflow-editor-panel-title__actions">
          {/* One button, two states: stretch to the canvas top, or restore the 256px drawer. */}
          <Tooltip classNames={editorTooltipClassNames} title={expandLabel}>
            <Button
              aria-label={expandLabel}
              aria-pressed={expanded}
              className="xflow-editor-panel-collapse"
              color="default"
              icon={expanded ? <DockRestoreIcon /> : <DockMaxIcon />}
              size="small"
              variant="text"
              onClick={onToggleExpanded}
            />
          </Tooltip>
          <Tooltip classNames={editorTooltipClassNames} title={`收起诊断台 ${dockShortcut}`}>
            <Button
              aria-keyshortcuts="Meta+J Control+J"
              aria-label="收起诊断台"
              className="xflow-editor-panel-collapse"
              color="default"
              icon={<DockDownIcon />}
              size="small"
              variant="text"
              onClick={onCollapse}
            />
          </Tooltip>
        </span>
      </div>

      <div className="xflow-editor-issues">
        <div
          aria-labelledby={`${tabsId}-tab-issues`}
          className="xflow-editor-issues__problems"
          hidden={activeTab !== "issues"}
          id={panelIds.issues}
          role="tabpanel"
        >
          {issues.length === 0 && passes.length === 0 ? (
            <p className="xflow-editor-issues__empty">添加节点后，这里列出校验结果。</p>
          ) : (
            <ul aria-label="诊断问题" className="xflow-editor-issues__list">
              {issues.map((item, index) => (
                <IssueRow
                  blockedDraftNodes={blockedDraftNodes}
                  item={item}
                  key={`${item.area}-${index}`}
                  nodes={nodes}
                  onLocateField={onLocateField}
                  onRevealJson={onRevealJson}
                  onLocateNode={onLocateNode}
                />
              ))}
              {passes.length > 0 ? (
                <>
                  <li className="xflow-editor-issues__pass">
                    <span className="xflow-editor-issues__ok">
                      <CheckIcon />
                    </span>
                    <span className="xflow-editor-issues__pass-text">
                      {passes.length} 项检查通过：{passSummary}
                    </span>
                    <Button
                      aria-expanded={passesOpen}
                      aria-label={passesOpen ? "收起通过项" : "展开通过项"}
                      size="small"
                      type="link"
                      onClick={() => setPassesOpen((open) => !open)}
                    >
                      {passesOpen ? "收起" : "展开"}
                    </Button>
                  </li>
                  {passesOpen
                    ? passes.map((item, index) => (
                        <li className="xflow-editor-issue" data-area={item.area} data-tone="success" key={`${item.area}-${index}`}>
                          <span aria-label="通过" className="xflow-editor-issue__sev" role="img">
                            <CheckIcon />
                          </span>
                          <span className="xflow-editor-issue__target">{passLabel(item.area)}</span>
                          <span className="xflow-editor-issue__text">{item.message}</span>
                        </li>
                      ))
                    : null}
                </>
              ) : null}
            </ul>
          )}
        </div>
        <div hidden={activeTab !== "logs"}>
          <RunLogPanel
            labelledBy={`${tabsId}-tab-logs`}
            latestExecution={latestExecution}
            panelId={panelIds.logs}
            runtime={runtime}
            traceHref={traceHref}
            workflow={workflow}
          />
        </div>
        <div hidden={activeTab !== "input"}>
          <InputPanel
            labelledBy={`${tabsId}-tab-input`}
            panelId={panelIds.input}
            runtime={runtime}
            selectedNode={selectedNode}
            selectedNodeName={selectedNodeName}
            workflow={workflow}
          />
        </div>
        <div
          aria-labelledby={`${tabsId}-tab-${outputTabKey}`}
          className="xflow-editor-output"
          hidden={activeTab !== outputTabKey}
          id={panelIds.output}
          role="tabpanel"
        >
          <div className="xflow-editor-output__head">
            <span className="xflow-editor-output__title" title={selectedNodeName}>
              {selectedNodeName ? `${selectedNodeName} 的输出` : "节点输出"}
            </span>
            {outputView.kind === "data" ? (
              <Tooltip classNames={editorTooltipClassNames} title={copyOutput ? "复制输出" : "当前环境不支持剪贴板"}>
                <Button
                  aria-label="复制输出"
                  className="xflow-editor-output__copy"
                  disabled={!copyOutput}
                  icon={<CopyIcon />}
                  size="small"
                  type="text"
                  onClick={copyOutput}
                />
              </Tooltip>
            ) : null}
          </div>
          {outputView.kind === "data" ? (
            // Read-only JSON in the Inspector's own code block: monospace,
            // independently scrollable and selectable, so long outputs stay
            // copyable without an edit surface.
            <pre className="xflow-editor-run-code xflow-editor-output__code" tabIndex={0}>
              {outputView.text}
            </pre>
          ) : (
            <p className="xflow-editor-output__note" data-tone={outputView.tone}>
              {outputView.note}
            </p>
          )}
        </div>
      </div>

      <aside aria-label="摘要" className="xflow-editor-drawer__side">
        <div>
          <h4 className="xflow-editor-drawer__side-title">定义摘要</h4>
          <dl className="xflow-editor-kvlist">
            <dt>节点</dt>
            {/* Count and mix are composed from parts: each category is its own
                nowrap span, so the row can only wrap on a `·`. */}
            <dd className="xflow-editor-kvlist__wrap" title={mixText || undefined}>
              {nodes.length}
              {mix.length > 0 ? (
                <>
                  （
                  {mix.map((entry, index) => (
                    <React.Fragment key={entry.label}>
                      <span className="xflow-editor-kvlist__mix">
                        {entry.label} {entry.count}
                      </span>
                      {index < mix.length - 1 ? <span>{" · "}</span> : null}
                    </React.Fragment>
                  ))}
                  ）
                </>
              ) : null}
            </dd>
            <dt>连线</dt>
            <dd>{edgeCount}</dd>
            <dt>凭证</dt>
            <dd className="xflow-editor-kvlist__mono" title={credentials || undefined}>
              {credentials || "—"}
            </dd>
            <dt>版本</dt>
            <dd title={versionLabel}>{versionLabel}</dd>
          </dl>
        </div>
        <div>
          <h4 className="xflow-editor-drawer__side-title">最近运行</h4>
          <dl className="xflow-editor-kvlist">
            <dt>执行</dt>
            <dd className="xflow-editor-kvlist__mono" title={latestExecution?.executionId}>
              {latestExecution?.executionId ?? "—"}
            </dd>
            <dt>状态</dt>
            <dd>
              <span
                className="xflow-editor-kvlist__status"
                data-tone={runStatus ? workflowStatusMeta[runStatus].tone : undefined}
              >
                {runLabel ?? "—"}
              </span>
            </dd>
            <dt>当前节点</dt>
            <dd className="xflow-editor-kvlist__mono" title={runningNode}>
              {runningNode ?? "—"}
            </dd>
            <dt>命中分支</dt>
            <dd title={hitPort ? `命中 ${hitPort}` : undefined}>{hitPort ? `命中 ${hitPort}` : "—"}</dd>
            <dt>耗时</dt>
            <dd data-run-duration={runTerminal ? "final" : "live"}>{durationText}</dd>
          </dl>
        </div>
      </aside>
    </div>
  );
}

/**
 * The bottom dock: status bar always, drawer while open. The region is named
 * 诊断台 either way -- by the drawer's heading while it is open, by label while
 * only the status bar is there.
 *
 * Three heights (§6.5): `collapsed` (strip only), `docked` (256px drawer under
 * the strip) and `expanded` (the drawer stretched to the toolbar's lower edge).
 * The dock keeps the same footprint in the last two -- expanding is an overlay,
 * so the canvas, its panels and the preview never resize or refit.
 */
export function DiagnosticsDock({
  workflow,
  runtime,
  diagnostics: allDiagnostics,
  operationError,
  unsavedCount,
  dock,
  onDockChange,
  controllerRef,
  onLocateNode,
  onLocateField,
  onRevealJson,
  ...drawerProps
}: DiagnosticsDockProps): React.ReactElement {
  const titleId = React.useId();
  const drawerId = React.useId();
  const statusButtonRef = React.useRef<HTMLButtonElement | null>(null);
  const drawerRef = React.useRef<HTMLDivElement | null>(null);
  // Where focus goes once the next dock change has rendered. Set only by the
  // user's own open / close (strip, drawer buttons, ⌘J), never by the
  // auto-reveal on a failed save or run.
  const focusAfterRef = React.useRef<"drawer" | "strip" | null>(null);
  const dockRef = React.useRef(dock);
  const collapsed = dock === "collapsed";
  const expanded = dock === "expanded";
  const nodes = workflow.nodes ?? [];
  const waiting = nodes.length === 0 && !operationError;
  const findings = waiting ? [] : allDiagnostics;
  const errors = findings.filter((item) => item.status === "error");
  const warnings = findings.filter((item) => item.status === "warn");
  const passes = findings.filter((item) => item.status === "pass");
  const edgeCount = countWorkflowEdges(workflow);
  const runStatus = runtime ? workflowStatusFromRuntime(runtime) : undefined;
  const runLabel = runStatus ? workflowStatusMeta[runStatus].label : undefined;

  const open = React.useCallback(() => {
    focusAfterRef.current = "drawer";
    onDockChange("docked");
  }, [onDockChange]);

  /**
   * Focus leaves the drawer before it unmounts, so it never falls to <body>.
   * The drawer's own 收起 button always hands focus to the strip (Safari does
   * not focus a clicked button, so "focus is inside" cannot be read there);
   * ⌘J only does when focus really is inside, and leaves it alone otherwise.
   * An expanded drawer covers the (inert) strip, so the hand-over is finished
   * by the layout effect below, in the commit that unmounts the drawer.
   */
  const close = React.useCallback(
    (fromDrawerButton = false) => {
      if (fromDrawerButton || drawerRef.current?.contains(document.activeElement)) {
        statusButtonRef.current?.focus();
        focusAfterRef.current = "strip";
      }
      onDockChange("collapsed");
    },
    [onDockChange]
  );

  React.useLayoutEffect(() => {
    dockRef.current = dock;
    const target = focusAfterRef.current;
    if (target === "drawer" && dock !== "collapsed") {
      focusAfterRef.current = null;
      drawerRef.current?.querySelector<HTMLElement>('[role="tab"][aria-selected="true"]')?.focus();
    } else if (target === "strip" && dock === "collapsed") {
      focusAfterRef.current = null;
      statusButtonRef.current?.focus();
    }
  }, [dock]);

  React.useLayoutEffect(() => {
    if (!controllerRef) return;
    controllerRef.current = { toggle: () => (dockRef.current === "collapsed" ? open() : close()), open };
    return () => {
      controllerRef.current = null;
    };
  }, [close, controllerRef, open]);

  // An expanded drawer is drawn over the workbench, so acting on a finding
  // hands the canvas / inspector back first.
  const restoreThen =
    <Args extends unknown[]>(action?: (...args: Args) => void) =>
    (...args: Args) => {
      if (expanded) onDockChange("docked");
      action?.(...args);
    };

  return (
    <footer
      aria-label={collapsed ? "诊断台" : undefined}
      aria-labelledby={collapsed ? undefined : titleId}
      className="xflow-editor-bottom"
      data-dock={dock}
      role="region"
    >
      <DiagnosticsStatusBar
        buttonRef={statusButtonRef}
        covered={expanded}
        drawerId={drawerId}
        edgeCount={edgeCount}
        errorCount={errors.length}
        expanded={!collapsed}
        nodeCount={nodes.length}
        runLabel={runLabel}
        unsavedCount={unsavedCount}
        waiting={waiting}
        warningCount={warnings.length}
        onToggle={collapsed ? open : () => close()}
      />
      {collapsed ? null : (
        <DiagnosticsDrawer
          {...drawerProps}
          drawerId={drawerId}
          drawerRef={drawerRef}
          edgeCount={edgeCount}
          expanded={expanded}
          issues={[...errors, ...warnings]}
          passes={passes}
          runLabel={runLabel}
          runtime={runtime}
          titleId={titleId}
          workflow={workflow}
          onCollapse={() => close(true)}
          onLocateField={restoreThen(onLocateField)}
          onLocateNode={restoreThen(onLocateNode)}
          onRevealJson={restoreThen(onRevealJson)}
          onToggleExpanded={() => onDockChange(expanded ? "docked" : "expanded")}
        />
      )}
    </footer>
  );
}
