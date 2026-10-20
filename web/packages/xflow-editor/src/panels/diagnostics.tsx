import * as React from "react";
import {
  CheckOutlined,
  ClockCircleOutlined,
  CloseCircleOutlined,
  NodeIndexOutlined,
  VerticalAlignBottomOutlined,
  VerticalAlignMiddleOutlined,
  VerticalAlignTopOutlined,
  WarningOutlined
} from "@ant-design/icons";
import { Button, Tabs, Tooltip } from "antd";
import { categoryOf, nodeCategoryMeta, workflowStatusMeta } from "@xflow/core";
import type { NodeCategory, RuntimeSnapshot, WorkflowDef, WorkflowNode } from "@xflow/core";
import { workflowStatusFromRuntime } from "../operationStatus";
import { editorTooltipClassNames } from "./shared";
import { nodeLabel, nodeName, targetsFor } from "./workflowOps";

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
  dock: DockState;
  onDockChange: (next: DockState) => void;
  /** Filled with the dock's open/close entry so the ⌘J handler can reuse its focus handling. */
  controllerRef?: React.MutableRefObject<DiagnosticsDockController | null>;
  /** Selects the node, brings it into view and lands focus on the canvas. */
  onLocateNode?: (nodeName: string) => void;
  /** Jumps to the finding's node and parameter field (P1-c S5). */
  onLocateField?: (nodeName: string, path: string) => void;
  onRevealJson?: (nodeName: string) => void;
}

/** The dock's three heights: status bar only, 256px drawer, drawer stretched to the canvas top. */
export type DockState = "collapsed" | "docked" | "expanded";

export interface DiagnosticsDockController {
  /** ⌘J: opens a closed dock (focus into the drawer), closes an open one. */
  toggle: () => void;
}

const dockShortcut = "⌘J";
const notConnected = "未接入";

/** What the "N 项检查通过" line calls each validator area. */
const passAreaLabel: Record<string, string> = {
  DSL: "DSL 结构",
  nodes: "节点",
  edges: "连线",
  dependencies: "依赖",
  credentials: "凭证",
  pin_data: "Pin Data",
  parameters: "参数",
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
  nodeCount: number;
  edgeCount: number;
  onToggle: () => void;
}

/**
 * The whole 28px strip is one button. Its name is 展开诊断台 while closed and
 * 诊断台摘要 while open, so the drawer's own 收起诊断台 is the only open/close
 * entry by name at any moment (§6.5).
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
  nodeCount,
  edgeCount,
  onToggle
}: StatusBarProps): React.ReactElement {
  return (
    <button
      aria-controls={expanded ? drawerId : undefined}
      aria-expanded={expanded}
      aria-keyshortcuts="Meta+J Control+J"
      aria-label={expanded ? "诊断台摘要" : "展开诊断台"}
      className="xflow-editor-statusbar"
      inert={covered}
      ref={buttonRef}
      title={expanded ? `诊断台摘要（${dockShortcut} 收起）` : `展开诊断台（${dockShortcut}）`}
      type="button"
      onClick={onToggle}
    >
      {waiting ? (
        <span className="xflow-editor-statusbar__item" data-tone="neutral">
          <ClockCircleOutlined />
          等待节点
        </span>
      ) : errorCount > 0 ? (
        <span className="xflow-editor-statusbar__item" data-tone="danger">
          <CloseCircleOutlined />
          {errorCount} 错误
        </span>
      ) : (
        <span className="xflow-editor-statusbar__item" data-tone="success">
          <CheckOutlined />
          校验通过
        </span>
      )}
      {warningCount > 0 ? (
        <span className="xflow-editor-statusbar__item" data-tone="warning">
          <WarningOutlined />
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
      <span className="xflow-editor-statusbar__spacer" />
      <span className="xflow-editor-statusbar__stats" data-graph-stats={`${nodeCount}/${edgeCount}`}>
        {nodeCount} 节点 · {edgeCount} 连线
      </span>
    </button>
  );
}

/** The three tabs with no data source yet: baseline look, a note, no panel behind them. */
const unavailableTabs = [
  { key: "logs", label: "运行日志" },
  { key: "input", label: "输入" },
  { key: "output", label: "输出" }
] as const;

/**
 * The note has to reach the focusable `role="tab"` element, not just the label
 * inside it: a hover tooltip alone is out of reach for the keyboard and for
 * screen readers. The Tabs component owns that element and offers no hook for
 * its attributes, so one layout effect sets `aria-description` on it and drops
 * the `aria-controls` that would point at a panel these tabs never render. The
 * ids are `${tabsId}-tab-${key}` because the Tabs `id` is passed in explicitly.
 * Focus on the tab never reaches the label's tooltip, so the drawer tracks the
 * focused key and the tooltip opens on focus as well as hover.
 */
function useUnavailableTabNotes(drawerRef: React.RefObject<HTMLDivElement | null>, tabsId: string): void {
  React.useLayoutEffect(() => {
    for (const { key } of unavailableTabs) {
      const tab = drawerRef.current?.querySelector(`[role="tab"][id="${tabsId}-tab-${key}"]`);
      tab?.setAttribute("aria-description", notConnected);
      tab?.removeAttribute("aria-controls");
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
    <li className="xflow-editor-issue" data-area={item.area} data-tone={isError ? "danger" : "warning"}>
      {/* The wrapper carries the tone colour: `@ant-design/icons` injects an
          unlayered `.anticon { color: inherit }`, which outranks every layer. */}
      <span aria-label={isError ? "错误" : "警告"} className="xflow-editor-issue__sev" role="img">
        {isError ? <CloseCircleOutlined /> : <WarningOutlined />}
      </span>
      <span className="xflow-editor-issue__target" title={target}>
        {target}
      </span>
      <span className="xflow-editor-issue__text">{item.message}</span>
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
  extends Omit<DiagnosticsDockProps, "dock" | "onDockChange" | "controllerRef" | "diagnostics" | "operationError"> {
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

/** Which unwired tab, if any, the event's target is. */
function unavailableTabKeyOf(target: EventTarget | null, tabsId: string): string | undefined {
  const id = target instanceof HTMLElement && target.getAttribute("role") === "tab" ? target.id : "";
  return unavailableTabs.find(({ key }) => id === `${tabsId}-tab-${key}`)?.key;
}

function DiagnosticsDrawer({
  drawerId,
  titleId,
  drawerRef,
  expanded,
  workflow,
  runtime,
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
  onRevealJson
}: DrawerProps): React.ReactElement {
  const [passesOpen, setPassesOpen] = React.useState(false);
  const tabsId = React.useId();
  const [hoveredTab, setHoveredTab] = React.useState<string>();
  const [focusedTab, setFocusedTab] = React.useState<string>();
  useUnavailableTabNotes(drawerRef, tabsId);
  const nodes = workflow.nodes ?? [];
  const mix = nodeMix(nodes, nodeCategories);
  const credentials = Object.keys(workflow.credentials ?? {}).join(", ");
  const runningNode = Object.entries(runtime?.nodes ?? {}).find(([, snapshot]) => snapshot.status === "running")?.[0];
  const passSummary = [...new Set(passes.map((item) => passLabel(item.area)))].join(" · ");
  const mixText = mix.map((entry) => `${entry.label} ${entry.count}`).join(" · ");
  const expandLabel = expanded ? "还原诊断台高度" : "扩展到画布高度";

  return (
    <div className="xflow-editor-drawer" data-dock={expanded ? "expanded" : "docked"} id={drawerId} ref={drawerRef}>
      <div className="xflow-editor-panel-title xflow-editor-drawer__head">
        {/* Icon + heading are one group 8px apart (the baseline's PanelTitle);
            the head's 16px separates that group from the tabs and the actions. */}
        <span className="xflow-editor-drawer__title">
          <span aria-hidden="true" className="xflow-editor-panel-title__icon">
            <NodeIndexOutlined />
          </span>
          <h3 className="xflow-editor-panel-title__heading" id={titleId}>
            诊断台
          </h3>
        </span>
        <Tabs
          activeKey="issues"
          animated={false}
          className="xflow-editor-diagnostics-tabs"
          classNames={diagnosticsTabsClassNames}
          id={tabsId}
          items={[
            { key: "issues", label: `问题 ${issues.length}` },
            ...unavailableTabs.map(({ key, label }) => ({
              key,
              label: (
                <Tooltip
                  classNames={editorTooltipClassNames}
                  open={hoveredTab === key || focusedTab === key}
                  title={notConnected}
                  onOpenChange={(open) => setHoveredTab(open ? key : undefined)}
                >
                  <span>{label}</span>
                </Tooltip>
              )
            }))
          ]}
          size="small"
          onBlur={() => setFocusedTab(undefined)}
          onFocus={(event) => setFocusedTab(unavailableTabKeyOf(event.target, tabsId))}
        />
        <span className="xflow-editor-panel-title__actions">
          {/* One button, two states: stretch to the canvas top, or restore the 256px drawer. */}
          <Tooltip classNames={editorTooltipClassNames} title={expandLabel}>
            <Button
              aria-label={expandLabel}
              aria-pressed={expanded}
              className="xflow-editor-panel-collapse"
              color="default"
              icon={expanded ? <VerticalAlignMiddleOutlined /> : <VerticalAlignTopOutlined />}
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
              icon={<VerticalAlignBottomOutlined />}
              size="small"
              variant="text"
              onClick={onCollapse}
            />
          </Tooltip>
        </span>
      </div>

      <div className="xflow-editor-issues">
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
                    <CheckOutlined />
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
                          <CheckOutlined />
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
            <dd>{runLabel ?? "—"}</dd>
            <dt>当前节点</dt>
            <dd className="xflow-editor-kvlist__mono" title={runningNode}>
              {runningNode ?? "—"}
            </dd>
            <dt>命中分支</dt>
            <dd>—</dd>
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
    controllerRef.current = { toggle: () => (dockRef.current === "collapsed" ? open() : close()) };
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
