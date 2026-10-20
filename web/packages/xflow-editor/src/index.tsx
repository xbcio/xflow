import {
  ApartmentOutlined,
  AppstoreAddOutlined,
  AppstoreOutlined,
  ArrowLeftOutlined,
  AuditOutlined,
  BgColorsOutlined,
  BranchesOutlined,
  CaretRightOutlined,
  CheckCircleOutlined,
  CheckOutlined,
  CloudServerOutlined,
  ClockCircleOutlined,
  CodeOutlined,
  CompressOutlined,
  DatabaseOutlined,
  DeleteOutlined,
  DesktopOutlined,
  DownOutlined,
  EditOutlined,
  ExportOutlined,
  EyeOutlined,
  FileSearchOutlined,
  FunctionOutlined,
  GlobalOutlined,
  HistoryOutlined,
  LinkOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  MergeCellsOutlined,
  MoreOutlined,
  MoonOutlined,
  NotificationOutlined,
  PartitionOutlined,
  PlayCircleOutlined,
  PlusOutlined,
  QuestionCircleOutlined,
  RedoOutlined,
  SafetyCertificateOutlined,
  SearchOutlined,
  SlidersOutlined,
  StepForwardOutlined,
  SunOutlined,
  ThunderboltOutlined,
  UndoOutlined,
  WarningOutlined
} from "@ant-design/icons";
import * as React from "react";
import {
  Button,
  ConfigProvider,
  Divider,
  Drawer,
  Dropdown,
  Input,
  Modal,
  Popconfirm,
  Popover,
  Segmented,
  Tag,
  Tooltip,
  type InputRef,
  type MenuProps
} from "antd";
import type {
  CardSummaryRow,
  Connections,
  NodeTypesResponse,
  ParamIssue,
  RuntimeSnapshot,
  Viewport,
  WorkflowDef,
  WorkflowNode,
  XFlowEditorCapabilities
} from "@xflow/core";
import {
  APPROVAL_NODE_TYPE,
  approvalDecisions,
  approvalParamsFrom,
  approvalProgress,
  approvalWaitText,
  breakpoints,
  cardSummaryRows,
  declaredOutputPorts,
  executionOpLabel,
  isSupplyNode,
  isTextEntryTarget,
  missingCapabilityScopes,
  nodeStatusMeta,
  resolveAppearance,
  workflowStatusMeta
} from "@xflow/core";
import type { Patch } from "@xflow/composer/core";
import type { FocusRequest } from "@xflow/composer/react";
import { XFlowPreview, XFLOW_NODE_DRAG_MIME, type PreviewConnection, type XFlowPreviewHandle } from "@xflow/preview";
import {
  createNodeFormRegistry,
  danglingPorts,
  externalIssuesByNode,
  localizeNodeTypes,
  NodeFormCompiler,
  nodeFormCatalogs,
  nodeHasTemplate,
  portsForNode,
  reduceNodePatches,
  translateNodeFormText,
  type DanglingEdge
} from "./node-form";
import type { NodeFormKind, NodeFormPort, NodeFormPorts, NodeFormSchema, NodeFormSource } from "./node-form/schema";
import { statusGlyph } from "./panels/statusGlyph";
import { VariablesPanel } from "./panels/variables";
import type { WorkflowDiagnostic } from "./panels/diagnostics";
import { DiagnosticsDock, type DiagnosticsDockController, type DockState } from "./panels/diagnostics";
import { NodeSearch, type NodeSearchOption } from "./panels/nodeSearch";
import { NodeCategoryIcon } from "./panels/categoryIcon";
import { OutlinePanel } from "./panels/outlinePanel";
import { RailKey } from "./panels/rail";
import { BannerStack, editorBanners, isWorkflowConflictError } from "./panels/banner";
import { Inspector, type NodeFormHost } from "./panels/inspector";
import { RunBar, type ExecutionControlAction, type XFlowExecution } from "./panels/runBar";
import {
  ExecutionListPanel,
  executionClock,
  type XFlowExecutionListQuery,
  type XFlowExecutionListState
} from "./panels/executionList";
import { RerunDialog, type XFlowRerunRequest, type XFlowRerunResult } from "./panels/rerunDialog";
import { PublishDialog } from "./panels/publishDialog";
import { ApprovalDialog, type XFlowApprovalSignal } from "./panels/approvalDialog";
import { VersionHistory, type XFlowWorkflowVersions } from "./panels/versionHistory";
import { CanvasContextMenu, ContextMenuLabel, type ContextMenuRequest } from "./panels/contextMenu";
import { defaultNodeBox, layoutWorkflowPositions, type LayoutPoint } from "./panels/autoLayout";
import {
  addConnection,
  connectionTypeFor,
  formSchemaForNode,
  listConnectionReferences,
  nodeKey,
  nodeLabel,
  nodeName,
  parseParametersText,
  removeConnectionFromWorkflow,
  supportedInputPorts,
  supportedOutputPorts,
  targetsFor,
  updateNodeByKey,
  withTargets,
  workflowNodeByName,
  type ParameterDraft,
  type ParameterTab
} from "./panels/workflowOps";
import {
  EditorPanel,
  editorTooltipClassNames,
  serializeJson,
  workflowJsonLabels,
  type WorkflowJsonDraft,
  type WorkflowJsonField,
  type WorkflowUpdater
} from "./panels/shared";
import { operationStatusFromRuntime } from "./operationStatus";
import { versionChipState } from "./unsavedChanges";
import { workflowChanges } from "./workflowChanges";
import {
  bareVersion,
  publishErrorText,
  rollbackErrorText,
  suggestNextVersion,
  versionLabel,
  versionUnavailableReason
} from "./versioning";
import { createEditorThemeConfig, type ResolvedXFlowEditorAppearance, type XFlowEditorAppearance } from "./theme";
// Tokens declare the --xflow-* variables every stylesheet below reads. Both sheets are
// layered — the composer's in `@layer xflow.composer`, this editor's in `@layer
// xflow.editor`. The host declares the order (tailwind.css) with xflow.editor after
// xflow.composer, so the editor overrides the composer's variable values; Tailwind
// utilities are declared after both and win the layout. Import order decides nothing.
import "@xflow/core/tokens.css";
import "@xflow/composer/form/styles.css";
import "./styles.css";

/**
 * Node Descriptor text arrives in English (GET /v1/node-types); the editor UI
 * is Chinese, so the response is localized once where it enters the editor
 * (Doc C §8 item 1). Custom types without catalog entries stay English.
 */
const nodeFormCatalog = nodeFormCatalogs["zh-CN"];

export type { XFlowEditorAppearance } from "./theme";
export type {
  ExecutionControlAction,
  XFlowExecution,
  XFlowExecutionNode
} from "./panels/runBar";
export type {
  XFlowExecutionListQuery,
  XFlowExecutionListState,
  XFlowExecutionListItem
} from "./panels/executionList";
export type { XFlowRerunRequest, XFlowRerunResult } from "./panels/rerunDialog";
export type { XFlowApprovalSignal } from "./panels/approvalDialog";
export type { XFlowWorkflowVersion, XFlowWorkflowVersions } from "./panels/versionHistory";
type LayoutPolicy = "auto" | "compact";
/** a = both panels docked, b = left rail + docked inspector, c = both as rails. */
type EditorLayout = "a" | "b" | "c";
type CanvasTool = "select" | "connect";

interface WorkflowHistory {
  undo: WorkflowDef[];
  redo: WorkflowDef[];
}

const historyLimit = 80;

/**
 * Which keystrokes survive focus landing in a text field. Canvas bindings defer
 * to the field — ⌘Z there means undo typing and V means the letter V — while
 * global ones still fire, because the browser's own ⌘S and ⌘Enter are no use to
 * anyone editing a workflow.
 */
type ShortcutScope = "global" | "canvas";

interface EditorShortcut {
  id: string;
  label: string;
  /** Rendered in the tooltip and the shortcut reference, hence a display string. */
  keys: string;
  scope: ShortcutScope;
  /** Escape leaves the key's default alone so it can still cancel a live drag. */
  preventDefault?: boolean;
  match: (event: KeyboardEvent) => boolean;
}

const isPrimaryModifier = (event: KeyboardEvent): boolean => event.metaKey || event.ctrlKey;
const isBareKey = (event: KeyboardEvent): boolean =>
  !event.metaKey && !event.ctrlKey && !event.altKey && !event.shiftKey;
const isModifiedKey = (event: KeyboardEvent, key: string): boolean =>
  isPrimaryModifier(event) && !event.altKey && !event.shiftKey && event.key.toLowerCase() === key;

/**
 * The single declaration of every binding: the window handler dispatches from
 * this list and the shortcut reference renders from it, so the advertised keys
 * and the live behavior cannot drift apart.
 */
const editorShortcuts: EditorShortcut[] = [
  {
    id: "undo",
    label: "撤销",
    keys: "⌘ / Ctrl + Z",
    scope: "canvas",
    match: (event) => isModifiedKey(event, "z")
  },
  {
    id: "redo",
    label: "重做",
    keys: "⌘ / Ctrl + ⇧ + Z 或 ⌘ / Ctrl + Y",
    scope: "canvas",
    match: (event) => isPrimaryModifier(event) && !event.altKey
      && (event.shiftKey ? event.key.toLowerCase() === "z" : event.key.toLowerCase() === "y")
  },
  {
    id: "save",
    label: "保存工作流",
    keys: "⌘ / Ctrl + S",
    scope: "global",
    match: (event) => isModifiedKey(event, "s")
  },
  {
    id: "run",
    label: "运行工作流",
    keys: "⌘ / Ctrl + Enter",
    scope: "global",
    match: (event) => isPrimaryModifier(event) && !event.altKey && event.key === "Enter"
  },
  {
    id: "duplicate",
    label: "复制选中节点",
    keys: "⌘ / Ctrl + D",
    scope: "canvas",
    match: (event) => isModifiedKey(event, "d")
  },
  {
    id: "findNode",
    label: "定位节点",
    keys: "⌘ / Ctrl + K",
    scope: "global",
    match: (event) => isModifiedKey(event, "k")
  },
  {
    id: "toggleLeftPanel",
    label: "收起 / 展开左栏",
    keys: "⌘ / Ctrl + B",
    scope: "canvas",
    match: (event) => isModifiedKey(event, "b")
  },
  {
    id: "toggleDiagnostics",
    label: "收起 / 展开诊断台",
    keys: "⌘ / Ctrl + J",
    // Global like ⌘K: J has no text-editing meaning, and the browser's own
    // ⌘J (downloads) is no use inside a workflow editor.
    scope: "global",
    // The physical key, so a layout that composes another character still hits.
    match: (event) => isPrimaryModifier(event) && !event.altKey && !event.shiftKey && event.code === "KeyJ"
  },
  {
    id: "toggleRightPanel",
    label: "收起 / 展开属性面板",
    keys: "⌥⌘B / Ctrl + Alt + B",
    scope: "canvas",
    // Option composes the produced character on macOS (⌥B reports "∫"), so the
    // physical key is the reliable discriminator; `key` stays as the fallback
    // for layouts that report "b" with Alt held.
    match: (event) =>
      isPrimaryModifier(event) && event.altKey && !event.shiftKey
      && (event.code === "KeyB" || event.key.toLowerCase() === "b")
  },
  {
    id: "selectTool",
    label: "选择工具",
    keys: "V",
    scope: "canvas",
    match: (event) => isBareKey(event) && event.key.toLowerCase() === "v"
  },
  {
    id: "connectTool",
    label: "连线工具",
    keys: "L",
    scope: "canvas",
    match: (event) => isBareKey(event) && event.key.toLowerCase() === "l"
  },
  {
    id: "addNode",
    label: "添加节点",
    keys: "A",
    scope: "canvas",
    match: (event) => isBareKey(event) && event.key.toLowerCase() === "a"
  },
  {
    id: "searchLibrary",
    label: "搜索节点库",
    keys: "/",
    scope: "canvas",
    match: (event) => isBareKey(event) && event.key === "/"
  },
  {
    id: "escape",
    label: "关闭抽屉 / 退出连线工具",
    keys: "Esc",
    scope: "canvas",
    preventDefault: false,
    match: (event) => event.key === "Escape"
  },
  {
    id: "shortcutsHelp",
    label: "快捷键速查",
    keys: "?",
    scope: "global",
    match: (event) => event.key === "?"
  }
];

/** Whether the key event came from inside an open popup that owns its own Esc. */
const isInsidePopup = (target: EventTarget | null): boolean =>
  target instanceof Element && target.closest('[role="menu"], [role="listbox"], [role="dialog"], [role="tooltip"]') !== null;

/** The shortcuts that stay live while an expanded diagnostics drawer covers the workbench. */
const expandedDockShortcuts: ReadonlySet<string> = new Set([
  "save",
  "run",
  "findNode",
  "toggleDiagnostics",
  "escape",
  "shortcutsHelp"
]);

/**
 * What `onSave` may resolve to besides the saved definition itself: the
 * definition plus the server's ParamSpec findings for it (`param_issues` of
 * POST/PUT /v1/workflows in warn mode). The editor shows each issue on its
 * field (Doc C §1 rule 3) until the next save or a new `value`.
 */
export interface XFlowEditorSaveResult {
  workflow: WorkflowDef;
  paramIssues?: ParamIssue[];
}

/**
 * A rejected `onSave` may carry the server's findings the same way: any
 * thrown value with a `paramIssues: ParamIssue[]` property (e.g. the
 * `XFlowApiError` of a 400 `workflow_param_invalid` in enforce mode) has
 * them placed on their fields. The editor does not depend on @xflow/api, so
 * this is duck-typed.
 */
export interface XFlowEditorSaveError {
  message?: string;
  paramIssues?: ParamIssue[];
}

/**
 * What `onRollback` resolves to: the version the server installed and whether
 * it was already the current one (the server's idempotent no-op).
 */
export interface XFlowRollbackResult {
  version: string;
  unchanged: boolean;
}

/**
 * The workbench's two views (§6.8). 编辑 is the definition editor; 运行记录
 * replays one execution's snapshot. The preview switch P2 carried here was
 * replaced by 运行记录 (user ruling 2026-10-19, default ruling 10): a read-only
 * definition is the editing canvas with the 只读 banner, not a third mode.
 */
export type XFlowEditorMode = "edit" | "runs";

export interface XFlowEditorProps {
  value: WorkflowDef;
  /** Optional host layout class; useful when the workbench lives below an app header. */
  className?: string;
  /**
   * Back to the workflow list. The top bar renders the ← button only when the
   * host supplies this handler; the editor does not own routing.
   */
  onBack?: () => void;
  runtime?: RuntimeSnapshot;
  onChange?: (value: WorkflowDef) => void;
  /**
   * Persists the draft. Resolve to the saved definition, or to
   * `{ workflow, paramIssues }` (XFlowEditorSaveResult) to also report the
   * server's param issues. Rejections may carry `paramIssues` too
   * (XFlowEditorSaveError).
   */
  onSave?: (value: WorkflowDef) => Promise<WorkflowDef | XFlowEditorSaveResult> | WorkflowDef | XFlowEditorSaveResult;
  onRun?: (value: WorkflowDef) => Promise<RuntimeSnapshot> | RuntimeSnapshot;
  /**
   * Runs one node's slice of the workflow (the canvas context menu's 从此节点
   * 运行 / 运行到此节点) and resolves to the resulting snapshot. Optional:
   * without it those menu items stay disabled rather than pretending the host
   * supports a partial run.
   */
  onRunNode?: (
    value: WorkflowDef,
    options: { node: string; mode: "from" | "to" }
  ) => Promise<RuntimeSnapshot> | RuntimeSnapshot;
  /**
   * GET /v1/node-types. Drives the Inspector's parameter form (Doc C) and the
   * save/run blocking mode (`param_validation_mode`). Omit (or pass while it
   * loads / after it fails) and every node gets the "no schema" form: common
   * fields only, parameters edited on the JSON tab.
   */
  nodeTypes?: NodeTypesResponse;
  /** Preferred color mode. Omit to use the editor's local setting (default `system`). */
  appearance?: XFlowEditorAppearance;
  /** Called whenever the user selects a color mode. Persistence belongs to the host. */
  onAppearanceChange?: (appearance: XFlowEditorAppearance) => void;
  /**
   * Restores the canvas pan/zoom saved in `WorkflowEditorMetadata.viewport`
   * (ADR-D4 §2.3), e.g. after `mergeEditorMetadata` on load. Omit it to keep
   * the canvas's own measured auto-fit. Changing it (a new workflow loaded)
   * re-applies the viewport.
   */
  defaultViewport?: Viewport;
  /**
   * Reports the canvas pan/zoom after the user stops panning or zooming, so
   * the host can fold it into `editor_metadata.viewport` on the next save
   * (`splitEditorMetadata` reads it from wherever the host keeps it, not from
   * `WorkflowDef`, since the viewport has no field there).
   */
  onViewportChange?: (viewport: Viewport) => void;
  /**
   * The host's GET /v1/node-types failed. Shows the 节点类型加载失败 banner;
   * the Inspector already falls back to JSON parameter editing when `nodeTypes`
   * is absent, and the canvas and connections are unaffected.
   */
  nodeTypesError?: { message?: string };
  /** Re-fetches node types. The banner's 重试 button exists only with it. */
  onRetryNodeTypes?: () => void;
  /**
   * Fetches the latest saved definition and replaces `value` with it, for the
   * 保存失败：工作流已被更新 banner (a rejected `onSave` with status 409 and
   * code `workflow_conflict`). The 重新加载 button exists only with it.
   */
  onReloadLatest?: () => void;
  /** Saves the current draft as a new workflow. The 另存为副本 button exists only with it. */
  onSaveAsCopy?: (definition: WorkflowDef) => void;
  /**
   * Publishes the draft under a new version label (§6.10): the host PUTs
   * `value` — whose `version` the publish dialog set — and resolves like
   * `onSave`. Omit and 发布 stays disabled; `capabilities.workflow === false`
   * hides it with the rest of the write actions.
   */
  onPublish?: (value: WorkflowDef) => Promise<WorkflowDef | XFlowEditorSaveResult> | WorkflowDef | XFlowEditorSaveResult;
  /**
   * GET /v1/workflows/{id}/versions. The version chip opens the history panel
   * only when this is present; the publish dialog also reads the online
   * version and hash from it. Called each time the panel or dialog opens.
   */
  onLoadVersions?: () => Promise<XFlowWorkflowVersions> | XFlowWorkflowVersions;
  /**
   * 查看 on a history row. The host replaces `value` with that version's
   * definition and sets `historicalVersion`; the editor turns read-only.
   */
  onViewVersion?: (version: string) => void;
  /**
   * 回滚到此, after the history panel's confirmation. Rejection with 409
   * `workflow_version_unavailable` disables that row and keeps the reason on
   * it. Omit and every row's rollback stays disabled.
   */
  onRollback?: (version: string) => Promise<XFlowRollbackResult> | XFlowRollbackResult;
  /**
   * The archived version the host loaded into `value`. A value makes the
   * editor read-only and switches the version chip to 「vX · 历史」; the
   * history banner carries the return entry.
   */
  historicalVersion?: string;
  /** Leaves the archived view. The history banner's 退出历史 exists only with it. */
  onExitHistory?: () => void;
  /**
   * Why the viewer may not edit (e.g. no write scope in the namespace). A value
   * shows the 只读 banner, closes every editing surface and hides 保存 / 发布 /
   * 运行 rather than disabling them (§6.5.1 / §6.10).
   */
  readOnlyReason?: string;
  /**
   * The capability bits the host derived from GET /v1/current-user's scopes
   * (`capabilitiesFromScopes` in `@xflow/core`). The editor never requests
   * them itself. Omit the prop to keep every control enabled, preserving the
   * pre-capabilities host contract; `workflow: false` makes the editor
   * read-only exactly like `readOnlyReason`, without needing a reason string.
   */
  capabilities?: XFlowEditorCapabilities;
  /**
   * The execution the host is tracking (structurally an `@xflow/api`
   * `ExecutionDetail`). A value floats the §6.3 run bar over the canvas; the
   * host polls GET /v1/executions/{id} and answers actions by replacing this
   * prop. Omit it and the bar does not exist.
   */
  execution?: XFlowExecution;
  /**
   * Asks the host to run one pause / resume / cancel on the tracked execution.
   * The host calls the API and feeds the refreshed detail back through
   * `execution`; a rejection surfaces as the editor's operation error (the host
   * owns the 403 toast and capability refresh). Omit and the bar's buttons are
   * disabled.
   */
  onExecutionAction?: (
    executionId: string,
    action: ExecutionControlAction
  ) => Promise<void> | void;
  /** Called after a run-bar count selects a node on the canvas (host-side locate). */
  onLocateNode?: (nodeName: string) => void;
  /** 查看运行记录. The host owns routing; omit and the link stays disabled. */
  onOpenRunHistory?: () => void;
  /** 打开子执行 from the Inspector's 运行 tab. Omit and the link is disabled. */
  onOpenExecution?: (executionId: string) => void;
  /**
   * The console user's verified subject (GET /v1/current-user). The approval
   * dialog uses it only to choose the signal name and to decide whether this
   * person may act; the server injects `_actor` into the payload, so the editor
   * never sends an identity.
   */
  currentUserSubject?: string;
  /**
   * Delivers an approval signal (POST /v1/executions/{id}/signals). Omit and
   * the dialog is read-only: the editor never fakes a submitted decision.
   */
  onSignal?: (executionId: string, signal: XFlowApprovalSignal) => Promise<void> | void;
  /**
   * Revokes a delivered-but-unconsumed signal (DELETE /signals/{name}).
   * Omit and the dialog has no 撤回 entry.
   */
  onRevokeSignal?: (executionId: string, name: string) => Promise<void> | void;
  /**
   * How long a terminal run bar stays before hiding itself, in ms. The editor
   * times it (default 5000); `null` keeps the bar until the user closes it.
   */
  runBarDismissMs?: number | null;
  /**
   * The active view. Omit to let the switch keep its own state; the host owns
   * `?exec=` and passes this so a reload or a shared URL lands in the same
   * view. `编辑` is the definition (read-only when the viewer may not write).
   */
  mode?: XFlowEditorMode;
  /** Reports a switch. The host may reject it by not changing `mode`. */
  onModeChange?: (mode: XFlowEditorMode) => void;
  /**
   * The accumulated GET /v1/executions pages for the current filter, keyed by
   * the host to this workflow (`workflow_id`). Page 1 replaces the rows; later
   * pages append. Omit it and 运行记录 shows its empty state.
   */
  executionList?: XFlowExecutionListState;
  /**
   * Asks the host for one execution-list page. Called on mount, on a filter
   * change (page 1) and when the scrolled list nears its end. The editor never
   * requests the API itself.
   */
  onLoadExecutions?: (query: XFlowExecutionListQuery) => void;
  /** The execution whose snapshot the run-record mode replays. */
  selectedExecution?: XFlowExecution;
  /** Row click in the execution list; the host loads the detail and owns `?exec=`. */
  onSelectExecution?: (executionId: string) => void;
  /**
   * The definition captured for {@link selectedExecution} — the host reads
   * `GET /v1/workflows/{id}/versions/{workflow_version}` and passes it here.
   * When the version is unavailable the host passes the current definition and
   * sets {@link executionDefinitionStale}.
   */
  executionSnapshotWorkflow?: WorkflowDef;
  /**
   * The requested version could not be resolved, so the canvas is replaying
   * the current definition: the status bar says 定义已变化，仅供参考.
   */
  executionDefinitionStale?: boolean;
  /**
   * The replay runtime the host derived with
   * `executionDetailToRuntimeSnapshot(selectedExecution)`. Kept separate from
   * `runtime` so switching back to 编辑 keeps the live run's colours.
   */
  executionSnapshotRuntime?: RuntimeSnapshot;
  /**
   * §6.9 重跑…. The host owns POST /v1/executions/{id}/rerun and the follow-up
   * (select the new execution, toast a queue position). Omit and every rerun
   * entry stays hidden — the editor never fakes a submitted rerun.
   */
  onRerun?: (request: XFlowRerunRequest) => Promise<XFlowRerunResult> | XFlowRerunResult;
}

function systemAppearance(): ResolvedXFlowEditorAppearance {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") {
    return "dark";
  }
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function useResolvedAppearance(
  appearance: XFlowEditorAppearance
): ResolvedXFlowEditorAppearance {
  const [systemMode, setSystemMode] = React.useState<ResolvedXFlowEditorAppearance>(systemAppearance);

  React.useEffect(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") {
      return undefined;
    }

    const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
    const sync = () => setSystemMode(mediaQuery.matches ? "dark" : "light");
    sync();

    if (typeof mediaQuery.addEventListener === "function") {
      mediaQuery.addEventListener("change", sync);
      return () => mediaQuery.removeEventListener("change", sync);
    }

    mediaQuery.addListener?.(sync);
    return () => mediaQuery.removeListener?.(sync);
  }, []);

  return resolveAppearance(appearance, systemMode === "dark");
}

/** Content-box inline size: the same box an `@container` size query reads. */
function contentInlineSize(element: HTMLElement): number {
  const rect = element.getBoundingClientRect();
  const style = window.getComputedStyle(element);
  const px = (value: string) => Number.parseFloat(value) || 0;
  return Math.max(
    0,
    rect.width -
      px(style.borderLeftWidth) -
      px(style.borderRightWidth) -
      px(style.paddingLeft) -
      px(style.paddingRight)
  );
}

/**
 * Inline size of the editor's own box (the `.xflow-editor` root, which is the
 * named inline-size container). The breakpoints are measured here rather than
 * on the window, so an editor embedded in a narrow host collapses with the box
 * it was given. The size is the content box, the same measurement `@container`
 * uses, so JS and CSS compare identical numbers (no rounding). 0 means "not
 * measured yet": SSR and hosts without ResizeObserver stay on the wide layout
 * instead of guessing.
 */
function useEditorWidth(ref: React.RefObject<HTMLElement | null>): number {
  const [width, setWidth] = React.useState(0);

  React.useLayoutEffect(() => {
    const element = ref.current;
    if (!element || typeof ResizeObserver === "undefined") {
      return undefined;
    }
    const commit = (next: number) => setWidth((current) => (current === next ? current : next));
    // Seed before the first paint: a ResizeObserver callback lands after it, so
    // a narrow editor would otherwise paint one frame of the wide layout.
    commit(contentInlineSize(element));
    const observer = new ResizeObserver(([entry]) => commit(entry.contentRect.width));
    observer.observe(element);
    return () => observer.disconnect();
  }, [ref]);

  return width;
}

/**
 * Layout tier from the container width; `xflow-core`'s `breakpoints` is the
 * single source shared with the stylesheet's `@container` rules. Below
 * `compact` only the left panel collapses (layout "b"); below `narrow` both do
 * (layout "c"). That band split mirrors the ui-preview baseline's
 * COMPACT_LEFT / COMPACT_RIGHT pair.
 */
function layoutTier(width: number): "wide" | "compact" | "narrow" {
  if (width <= 0) return "wide";
  if (width < breakpoints.narrow) return "narrow";
  if (width < breakpoints.compact) return "compact";
  return "wide";
}

/** The "Runner" badge on a runner-reported library tile; colored by --xflow-node-source-*. */
const editorNodeSourceTagClassNames = {
  content: "xflow-editor__node-tile-source-label",
  root: "xflow-editor__node-tile-source-root"
};

/** The library's always-on search box (baseline `.xflow-pv-search`): prefix icon, "/" key hint as suffix. */
const editorLibrarySearchClassNames = {
  input: "xflow-editor-field__control",
  prefix: "xflow-editor-library-search__prefix",
  root: "xflow-editor-library-search",
  suffix: "xflow-editor-library-search__suffix"
};

const editorCompactDrawerClassNames = {
  body: "xflow-editor-compact-drawer__body",
  close: "xflow-editor-compact-drawer__close",
  header: "xflow-editor-compact-drawer__header",
  section: "xflow-editor-compact-drawer__section",
  title: "xflow-editor-compact-drawer__title",
  wrapper: "xflow-editor-compact-drawer__wrapper"
};

// The side modifier carries the divider on the canvas side, as the baseline's
// floating panes draw it (border-right on the left pane, border-left on the right).
const editorCompactDrawerClassNamesStart = {
  ...editorCompactDrawerClassNames,
  wrapper: "xflow-editor-compact-drawer__wrapper xflow-editor-compact-drawer__wrapper--start"
};
const editorCompactDrawerClassNamesEnd = {
  ...editorCompactDrawerClassNames,
  wrapper: "xflow-editor-compact-drawer__wrapper xflow-editor-compact-drawer__wrapper--end"
};

function ThemeSettings({
  appearance,
  onAppearanceChange
}: {
  appearance: XFlowEditorAppearance;
  onAppearanceChange: (appearance: XFlowEditorAppearance) => void;
}): React.ReactElement {
  return (
    <div className="xflow-editor-theme-settings">
      <div className="xflow-editor-theme-settings__row">
        <div>
          <span className="xflow-editor-theme-settings__label">颜色模式</span>
          <span className="xflow-editor-theme-settings__hint">仅在当前编辑器会话生效</span>
        </div>
        <Segmented
          aria-label="颜色模式"
          classNames={{
            item: "xflow-editor-theme-segmented__item",
            label: "xflow-editor-theme-segmented__label",
            root: "xflow-editor-theme-segmented__root"
          }}
          rootClassName="xflow-editor-theme-segmented"
          options={[
            { icon: <SunOutlined />, label: "浅色", value: "light" },
            { icon: <MoonOutlined />, label: "深色", value: "dark" },
            { icon: <DesktopOutlined />, label: "跟随系统", value: "system" }
          ]}
          value={appearance}
          onChange={(nextAppearance) => onAppearanceChange(nextAppearance as XFlowEditorAppearance)}
        />
      </div>
    </div>
  );
}

/** Node-library groups in display order; "其他" collects types with no presentation entry. */
const nodeLibraryGroups = ["触发器", "流程控制", "动作与人工", "数据转换", "供应", "其他"] as const;
type NodeLibraryGroup = (typeof nodeLibraryGroups)[number];
type NodeTone = "blue" | "cyan" | "green" | "amber" | "violet";

/** One node-library tile: what the type is (from the schema) plus how it looks. */
interface NodeDescriptor {
  label: string;
  type: string;
  group: NodeLibraryGroup;
  tone: NodeTone;
  icon: React.ReactNode;
  kind?: NonNullable<WorkflowNode["kind"]>;
  description?: string;
  ports?: NodeFormPorts;
  /** The category its descriptor declares (spec §6.13); absent keeps the prefix fallback. */
  category?: string;
  /** "runner" for a type live runners reported; absent = server registry or offline list. */
  source?: NodeFormSource;
  /** Pool names reporting a runner type; empty when its runners have no pool. */
  pools?: string[];
}

/** Debounce of a parseable parameters-JSON edit before it commits (Doc C §6.2). */
const parameterDraftDebounceMs = 300;

function isSaveResult(value: unknown): value is XFlowEditorSaveResult {
  return Boolean(value) && typeof value === "object" && "workflow" in (value as object)
    && typeof (value as { workflow?: unknown }).workflow === "object" && (value as { workflow?: unknown }).workflow !== null;
}

function paramIssuesOf(error: unknown): ParamIssue[] | undefined {
  const issues = error && typeof error === "object" ? (error as XFlowEditorSaveError).paramIssues : undefined;
  return Array.isArray(issues) ? issues : undefined;
}

/** Ports the canvas adds to a node on its own; they stay valid next to the schema's ports. */
function canvasOutputPorts(node: WorkflowNode): string[] {
  const declared = declaredOutputPorts(node);
  return [...(declared.hasMain ? ["main"] : []), ...declared.ports];
}

/** Per-node issue counts of the node forms, for the save button and enforce blocking. */
interface FormIssueSummary {
  errors: number;
  nodes: string[];
}

/**
 * Offline node library, used only when the host passes no `nodeTypes` (it did
 * not fetch GET /v1/node-types, or the fetch failed). With `nodeTypes` the
 * library is derived from the server's schemas instead (Doc C §6.3), so a new
 * builtin shows up without a frontend change. This list is kept, not deleted,
 * because the editor must stay usable offline and in hosts that never call the
 * API; it is also the presentation source (group, tone, icon) for these types.
 * Custom types registered only on runners reach the library through the
 * response itself once the host requests it for a namespace (`source:
 * "runner"`); offline they are absent, and such nodes still load and edit as
 * no-schema nodes (Doc C §2.1).
 */
const fallbackNodeDescriptors: NodeDescriptor[] = ([
  { label: "Webhook", type: "xflow.trigger.webhook", group: "触发器", tone: "blue", icon: <LinkOutlined /> },
  { label: "Kafka", type: "xflow.trigger.kafka", group: "触发器", tone: "cyan", icon: <CloudServerOutlined /> },
  { label: "Cron", type: "xflow.trigger.cron", group: "触发器", tone: "amber", icon: <ClockCircleOutlined /> },
  { label: "Timer", type: "xflow.trigger.timer", group: "触发器", tone: "violet", icon: <ThunderboltOutlined /> },
  { label: "Start", type: "xflow.start", group: "流程控制", tone: "green", icon: <PlayCircleOutlined /> },
  { label: "End", type: "xflow.end", group: "流程控制", tone: "green", icon: <StepForwardOutlined /> },
  { label: "Switch", type: "xflow.switch", group: "流程控制", tone: "blue", icon: <BranchesOutlined /> },
  { label: "If", type: "xflow.if", group: "流程控制", tone: "blue", icon: <BranchesOutlined /> },
  { label: "Merge", type: "xflow.merge", group: "流程控制", tone: "cyan", icon: <MergeCellsOutlined /> },
  { label: "Wait", type: "xflow.wait", group: "流程控制", tone: "amber", icon: <ClockCircleOutlined /> },
  { label: "HTTP", type: "xflow.http", group: "动作与人工", tone: "blue", icon: <GlobalOutlined /> },
  { label: "gRPC", type: "xflow.grpc", group: "动作与人工", tone: "cyan", icon: <CloudServerOutlined /> },
  { label: "Database", type: "xflow.database", group: "动作与人工", tone: "green", icon: <DatabaseOutlined /> },
  { label: "Approval", type: "xflow.approval", group: "动作与人工", tone: "amber", icon: <AuditOutlined /> },
  { label: "Function", type: "xflow.function", group: "动作与人工", tone: "violet", icon: <CodeOutlined /> },
  { label: "Script", type: "xflow.script", group: "动作与人工", tone: "violet", icon: <CodeOutlined /> },
  { label: "Set", type: "xflow.transform.set", group: "数据转换", tone: "blue", icon: <CodeOutlined /> },
  { label: "Pick", type: "xflow.transform.pick", group: "数据转换", tone: "cyan", icon: <CodeOutlined /> },
  { label: "Filter", type: "xflow.transform.filter", group: "数据转换", tone: "amber", icon: <CodeOutlined /> },
  { label: "External Supply", type: "xflow.supply.external", group: "供应", tone: "cyan", icon: <DatabaseOutlined />, kind: "supply" },
  { label: "Static Supply", type: "xflow.supply.static", group: "供应", tone: "violet", icon: <DatabaseOutlined />, kind: "supply" }
] satisfies NodeDescriptor[]).map((descriptor) => ({ ...descriptor, label: translateNodeFormText(nodeFormCatalog, descriptor.label, descriptor.type) }));

type NodePresentation = Pick<NodeDescriptor, "group" | "tone" | "icon">;

/**
 * Look only (group, tone, icon), keyed by node type. Builtins that predate the
 * schema-driven library but were missing from the offline list get an entry
 * here so they land in their natural group. Anything else falls back to
 * {@link fallbackPresentation}: presentation is cosmetic, never a gate.
 */
const nodePresentation: ReadonlyMap<string, NodePresentation> = new Map<string, NodePresentation>([
  ...fallbackNodeDescriptors.map(({ type, group, tone, icon }): [string, NodePresentation] => [type, { group, tone, icon }]),
  ["xflow.trigger.redis", { group: "触发器", tone: "green", icon: <DatabaseOutlined /> }],
  ["xflow.map", { group: "流程控制", tone: "violet", icon: <ApartmentOutlined /> }],
  ["xflow.subworkflow", { group: "流程控制", tone: "cyan", icon: <ApartmentOutlined /> }],
  ["xflow.notification", { group: "动作与人工", tone: "amber", icon: <NotificationOutlined /> }],
  ["xflow.browser.cdp", { group: "动作与人工", tone: "cyan", icon: <DesktopOutlined /> }],
  ["xflow.transform.rename", { group: "数据转换", tone: "violet", icon: <CodeOutlined /> }],
  ["xflow.transform.sort", { group: "数据转换", tone: "green", icon: <CodeOutlined /> }],
  ["xflow.transform.limit", { group: "数据转换", tone: "amber", icon: <CodeOutlined /> }],
  ["xflow.transform.aggregate", { group: "数据转换", tone: "blue", icon: <CodeOutlined /> }],
  ["xflow.transform.remove_duplicates", { group: "数据转换", tone: "cyan", icon: <CodeOutlined /> }]
]);
/** Tile order inside a group: presentation-map order, then unknown types by label. */
const nodePresentationOrder = new Map([...nodePresentation.keys()].map((type, index) => [type, index]));

/** Presentation for a type with no entry: the group follows the schema's kind. */
function fallbackPresentation(kind: NodeFormKind): NodePresentation {
  if (kind === "trigger") return { group: "触发器", tone: "blue", icon: <AppstoreOutlined /> };
  if (kind === "supply") return { group: "供应", tone: "violet", icon: <AppstoreOutlined /> };
  return { group: "其他", tone: "blue", icon: <AppstoreOutlined /> };
}

/**
 * Supply declarations are resolved by the engine's supply subsystem, not the
 * handler registry, so GET /v1/node-types never lists them. They stay in the
 * library next to the schema-derived types.
 */
const engineDeclaredNodeDescriptors = fallbackNodeDescriptors.filter((descriptor) => descriptor.kind === "supply");

const derivedNodeDescriptors = new WeakMap<NodeTypesResponse, NodeDescriptor[]>();

/**
 * The node library for a `/v1/node-types` response (Doc C §6.3): one tile per
 * type (its latest version, which is what a node without `version` resolves
 * to), labelled by `display_name`, with look from {@link nodePresentation}.
 * Without a response it is the offline {@link fallbackNodeDescriptors}.
 */
function nodeLibraryDescriptors(nodeTypes: NodeTypesResponse | undefined): NodeDescriptor[] {
  if (!nodeTypes || !Array.isArray(nodeTypes.node_types)) return fallbackNodeDescriptors;
  const cached = derivedNodeDescriptors.get(nodeTypes);
  if (cached) return cached;

  const latest = new Map<string, NodeFormSchema>();
  for (const schema of nodeTypes.node_types) {
    if (!schema || typeof schema.node_type !== "string" || !schema.node_type) continue;
    const current = latest.get(schema.node_type);
    if (!current || (schema.node_version ?? 0) > (current.node_version ?? 0)) latest.set(schema.node_type, schema);
  }
  const derived: NodeDescriptor[] = [...latest.values()].map((schema) => {
    const kind: NodeFormKind = schema.kind === "trigger" || schema.kind === "supply" ? schema.kind : "action";
    const presentation = nodePresentation.get(schema.node_type) ?? fallbackPresentation(kind);
    return {
      ...presentation,
      label: schema.display_name?.trim() || schema.node_type,
      type: schema.node_type,
      kind,
      ...(schema.description ? { description: schema.description } : {}),
      ...(typeof schema.category === "string" && schema.category ? { category: schema.category } : {}),
      ...(schema.ports ? { ports: schema.ports } : {}),
      // Runner types usually have no presentation entry, so they keep the
      // kind-based fallback group; the tile badges where they came from.
      ...(schema.source === "runner"
        ? { source: schema.source, pools: Array.isArray(schema.runner_pools) ? schema.runner_pools : [] }
        : {})
    };
  });
  const descriptors = [...derived, ...engineDeclaredNodeDescriptors.filter((descriptor) => !latest.has(descriptor.type))].sort(
    (left, right) =>
      (nodePresentationOrder.get(left.type) ?? Number.MAX_SAFE_INTEGER) - (nodePresentationOrder.get(right.type) ?? Number.MAX_SAFE_INTEGER)
      || left.label.localeCompare(right.label)
  );
  derivedNodeDescriptors.set(nodeTypes, descriptors);
  return descriptors;
}

const derivedNodeCategories = new WeakMap<NodeTypesResponse, Readonly<Record<string, string>>>();

/**
 * The category each node type's descriptor declares (spec §6.13), from
 * GET /v1/node-types. The latest version of a type wins, like the library, and
 * an undeclared or empty category is left out so `categoryOf` keeps its prefix
 * fallback. Feeds the preview, the outline, the library and the diagnostics mix,
 * so all four name a type's category the same way.
 */
function nodeCategoriesFromTypes(nodeTypes: NodeTypesResponse | undefined): Readonly<Record<string, string>> {
  if (!nodeTypes || !Array.isArray(nodeTypes.node_types)) return {};
  const cached = derivedNodeCategories.get(nodeTypes);
  if (cached) return cached;
  const latestVersion = new Map<string, number>();
  // Null-prototype: a node type named `constructor` must not read an inherited
  // member back out of the map (the same hazard as a node named `constructor`).
  const categories: Record<string, string> = Object.create(null);
  for (const schema of nodeTypes.node_types) {
    if (!schema || typeof schema.node_type !== "string" || !schema.node_type) continue;
    const version = schema.node_version ?? 0;
    const seen = latestVersion.get(schema.node_type);
    if (seen !== undefined && seen >= version) continue;
    latestVersion.set(schema.node_type, version);
    const category = typeof schema.category === "string" ? schema.category.trim() : "";
    if (category) categories[schema.node_type] = category;
    else delete categories[schema.node_type];
  }
  derivedNodeCategories.set(nodeTypes, categories);
  return categories;
}

/** One-line port summary for a library tile's tooltip, from the schema's ports. */
function describePorts(ports: NodeFormPorts | undefined): string | undefined {
  if (!ports) return undefined;
  const names = (list: NodeFormPort[] | undefined) => (list ?? []).map((port) => port.name).join(", ");
  const parts = [
    ports.inputs?.length ? `输入 ${names(ports.inputs)}` : "",
    ports.dynamic_outputs ? "输出 动态" : ports.outputs?.length ? `输出 ${names(ports.outputs)}` : ""
  ].filter(Boolean);
  return parts.length ? parts.join(" · ") : undefined;
}

function defaultSelectedKey(nodes: WorkflowNode[]): string | undefined {
  const switchIndex = nodes.findIndex((node) => node.type?.includes("switch"));
  if (switchIndex >= 0) return nodeKey(nodes[switchIndex], switchIndex);
  return nodes[0] ? nodeKey(nodes[0], 0) : undefined;
}

/**
 * Every node upstream of `start`, following data connections backwards (§6.8).
 * These are the nodes a rerun copies instead of re-executing; the set is
 * walked from the graph, never listed by hand, so an edited connection changes
 * the annotations with it. Dependency ports are not dataflow and are skipped.
 */
function upstreamNodeNames(workflow: WorkflowDef, start: string): Set<string> {
  const incoming = new Map<string, string[]>();
  for (const [source, ports] of Object.entries(workflow.connections ?? {})) {
    for (const targets of Object.values(ports)) {
      const list = Array.isArray(targets) ? targets : targets?.targets ?? [];
      for (const target of list) {
        if (!target.node || target.node === source) continue;
        incoming.set(target.node, [...(incoming.get(target.node) ?? []), source]);
      }
    }
  }
  const upstream = new Set<string>();
  const queue = [start];
  while (queue.length > 0) {
    const current = queue.pop() as string;
    for (const source of incoming.get(current) ?? []) {
      if (source === start || upstream.has(source)) continue;
      upstream.add(source);
      queue.push(source);
    }
  }
  return upstream;
}

/**
 * Panel collapse is a workflow-level view preference (§6.0): remembered per
 * workflow id in localStorage. An unsaved workflow has no id and therefore no
 * stored state; a browser that denies storage simply starts expanded every time.
 */
const PANEL_STORAGE_PREFIX = "xflow-editor:panels:";

interface PanelCollapseState {
  leftCollapsed: boolean;
  rightCollapsed: boolean;
}

function readPanelCollapse(id: string | undefined): PanelCollapseState | undefined {
  if (!id || typeof window === "undefined") return undefined;
  try {
    const raw = window.localStorage.getItem(`${PANEL_STORAGE_PREFIX}${id}`);
    if (!raw) return undefined;
    const parsed = JSON.parse(raw) as Record<string, unknown> | null;
    if (!parsed || typeof parsed !== "object") return undefined;
    if (typeof parsed.leftCollapsed !== "boolean" || typeof parsed.rightCollapsed !== "boolean") return undefined;
    return { leftCollapsed: parsed.leftCollapsed, rightCollapsed: parsed.rightCollapsed };
  } catch {
    return undefined;
  }
}

function writePanelCollapse(id: string | undefined, state: PanelCollapseState): void {
  if (!id || typeof window === "undefined") return;
  try {
    window.localStorage.setItem(`${PANEL_STORAGE_PREFIX}${id}`, JSON.stringify(state));
  } catch {
    // Storage can be full or blocked; the in-memory state still works.
  }
}

/**
 * param_issues (Doc C §1 rule 3) as diagnostics rows: one per issue, on the
 * parent node, with the same pointer externalIssuesByNode places it on — so
 * the panel can offer 定位字段 next to every locatable finding.
 */
function paramIssueDiagnostics(issues: readonly ParamIssue[] | undefined): WorkflowDiagnostic[] {
  const rows: WorkflowDiagnostic[] = [];
  for (const [node, byPath] of externalIssuesByNode(issues)) {
    for (const [path, list] of Object.entries(byPath)) {
      for (const issue of list) {
        rows.push({
          status: issue.severity === "warning" ? "warn" : "error",
          area: "parameters",
          message: issue.message,
          nodeName: node,
          path
        });
      }
    }
  }
  return rows;
}

function validateWorkflow(
  workflow: WorkflowDef,
  operationError?: string,
  nodeTypes?: NodeTypesResponse
): WorkflowDiagnostic[] {
  // Doc C §4.3: edges left behind by a removed dynamic port are reported, never deleted.
  const dangling = new Set(danglingPorts(workflow, nodeTypes, canvasOutputPorts).map((edge: DanglingEdge) => `${edge.source}\u0000${edge.port}`));
  const registeredTypes = new Set((nodeTypes?.node_types ?? []).map((schema) => schema.node_type));
  const nodes = workflow.nodes ?? [];
  const names = nodes.map((node, index) => nodeName(node, index));
  const knownNames = new Set(names);
  const duplicateNames = names.filter((name, index) => names.indexOf(name) !== index);
  const diagnostics: WorkflowDiagnostic[] = [];

  diagnostics.push({
    status: workflow.name && workflow.name.trim() ? "pass" : "error",
    area: "DSL",
    message: workflow.name && workflow.name.trim() ? "spec/name/version 已通过基础校验" : "工作流名称不能为空"
  });
  diagnostics.push({
    status: nodes.length > 0 ? "pass" : "error",
    area: "nodes",
    message: nodes.length > 0 ? `${nodes.length} 个节点可用于编排` : "至少需要 1 个节点"
  });

  if (duplicateNames.length > 0) {
    diagnostics.push({
      status: "error",
      area: "nodes",
      message: `节点名称重复: ${Array.from(new Set(duplicateNames)).join(", ")}`,
      nodeName: duplicateNames[0]
    });
  }
  // A type outside the library may still be real (registered only on a
  // runner, Doc C §2.1): the node stays editable as a no-schema node, so this
  // is a warning, not an error.
  const libraryTypes = new Set(nodeLibraryDescriptors(nodeTypes).map((descriptor) => descriptor.type));
  const unsupportedTypes = nodes
    .map((node, index) => ({ node, index }))
    .filter(({ node }) => node.type && !libraryTypes.has(node.type) && !registeredTypes.has(node.type))
    .map(({ node, index }) => ({ name: nodeName(node, index), type: node.type }));
  if (unsupportedTypes.length > 0) {
    diagnostics.push({
      status: "warn",
      area: "nodes",
      message: `${nodeTypes ? "节点类型未在 server 注册" : "节点类型不在离线节点库中"}，参数按 JSON 编辑: ${unsupportedTypes.map(({ name, type }) => `${name}:${type}`).join(", ")}`,
      nodeName: unsupportedTypes[0].name
    });
  }
  if (workflow.runner_selector?.mode === "required" && Object.keys(workflow.runner_selector.match_labels ?? {}).length === 0) {
    diagnostics.push({
      status: "error",
      area: "runner",
      message: "Runner required 模式必须配置 matchLabels"
    });
  }
  const nodeRequiredSelectors = nodes
    .filter((node) => node.runner_selector?.mode === "required")
    .map((node, index) => nodeName(node, index));
  if (nodeRequiredSelectors.length > 0) {
    diagnostics.push({
      status: "error",
      area: "runner",
      message: `节点级 runnerSelector 不支持 required 模式: ${nodeRequiredSelectors.join(", ")}`,
      nodeName: nodeRequiredSelectors[0]
    });
  }

  const diagnosticsBeforeEdges = diagnostics.length;
  let dataEdgeCount = 0;
  let dependencyEdgeCount = 0;
  for (const [sourceName, ports] of Object.entries(workflow.connections ?? {})) {
    const sourceNode = workflowNodeByName(workflow, sourceName);
    if (!knownNames.has(sourceName)) {
      diagnostics.push({ status: "error", area: "edges", message: `连接源不存在: ${sourceName}` });
    }
    for (const [portName, portConnections] of Object.entries(ports)) {
      const type = connectionTypeFor(portConnections);
      const targets = targetsFor(portConnections);
      if (type === "invalid") {
        diagnostics.push({ status: "error", area: "edges", message: `${sourceName}.${portName} 使用了未知连接类型`, nodeName: sourceName });
        continue;
      }

      if (type === "dependency") {
        dependencyEdgeCount += targets.length;
        if (sourceNode && !isSupplyNode(sourceNode)) {
          diagnostics.push({ status: "error", area: "edges", message: `${sourceName}.${portName} 的 dependency 源必须是 supply 节点`, nodeName: sourceName });
        }
        for (const target of targets) {
          const targetName = target.node ?? "";
          const targetNode = targetName ? workflowNodeByName(workflow, targetName) : undefined;
          if (!targetNode) {
            diagnostics.push({
              status: "error",
              area: "edges",
              message: `${sourceName}.${portName} dependency 目标不存在: ${targetName || "unknown"}`,
              nodeName: sourceName
            });
            continue;
          }
          if (isSupplyNode(targetNode)) {
            diagnostics.push({ status: "error", area: "edges", message: `supply 节点不能依赖另一个 supply: ${targetName}`, nodeName: targetName });
          }
          if (Object.prototype.hasOwnProperty.call(target, "input")) {
            diagnostics.push({ status: "error", area: "edges", message: `${sourceName} → ${targetName} 的 dependency 不得声明 input`, nodeName: sourceName });
          }
        }
        continue;
      }

      dataEdgeCount += targets.length;
      if (dangling.has(`${sourceName}\u0000${portName}`)) {
        diagnostics.push({ status: "error", area: "edges", message: `${sourceName}.${portName} 端口已不存在，连线悬空；请删除或改接连线`, nodeName: sourceName });
        continue;
      }
      if (sourceNode && isSupplyNode(sourceNode)) {
        diagnostics.push({ status: "error", area: "edges", message: `supply 节点不能参与数据连接: ${sourceName}.${portName}`, nodeName: sourceName });
      } else if (sourceNode && !supportedOutputPorts(sourceNode, workflow).includes(portName)) {
        diagnostics.push({ status: "error", area: "edges", message: `${sourceName}.${portName} 不是有效输出端口`, nodeName: sourceName });
      }
      for (const target of targets) {
        const targetName = target.node ?? "";
        if (!targetName || !knownNames.has(targetName)) {
          diagnostics.push({
            status: "error",
            area: "edges",
            message: `${sourceName}.${portName} 连接目标不存在: ${targetName || "unknown"}`,
            nodeName: sourceName
          });
          continue;
        }
        const targetNode = workflowNodeByName(workflow, targetName);
        if (targetNode && isSupplyNode(targetNode)) {
          diagnostics.push({ status: "error", area: "edges", message: `supply 节点不能参与数据连接: ${targetName}`, nodeName: targetName });
          continue;
        }
        const targetInput = target.input ?? "main";
        if (targetNode && !supportedInputPorts(targetNode).includes(targetInput)) {
          diagnostics.push({
            status: "error",
            area: "edges",
            message: `${targetName}.${targetInput} 不是有效输入端口`,
            nodeName: targetName
          });
        }
      }
    }
  }

  for (const edge of workflow.dependency_edges ?? []) {
    dependencyEdgeCount += 1;
    const consumer = workflowNodeByName(workflow, edge.node);
    const supply = workflowNodeByName(workflow, edge.supply);
    if (!consumer) {
      diagnostics.push({ status: "error", area: "dependencies", message: `dependency consumer 不存在: ${edge.node}` });
    } else if (isSupplyNode(consumer)) {
      diagnostics.push({ status: "error", area: "dependencies", message: `supply 节点不能依赖另一个 supply: ${edge.node}`, nodeName: edge.node });
    }
    if (!supply) {
      diagnostics.push({ status: "error", area: "dependencies", message: `dependency supply 不存在: ${edge.supply}` });
    } else if (!isSupplyNode(supply)) {
      diagnostics.push({ status: "error", area: "dependencies", message: `dependency supply 不是 supply 节点: ${edge.supply}`, nodeName: edge.supply });
    }
  }

  // The edge check reads as passed only when none of its findings is an error;
  // the findings themselves are the errors, so a failing check adds no second row.
  if (!diagnostics.slice(diagnosticsBeforeEdges).some((item) => item.status === "error")) {
    diagnostics.push({
      status: "pass",
      area: "edges",
      message: `${dataEdgeCount} 条数据连接和 ${dependencyEdgeCount} 条依赖连接已解析`
    });
  }
  const credentialNames = Object.keys(workflow.credentials ?? {});
  diagnostics.push({
    status: credentialNames.length > 0 ? "pass" : "warn",
    area: "credentials",
    message: credentialNames.length > 0 ? `${credentialNames.length} 个凭证引用已声明` : "未声明凭证引用；涉及外部资源的节点运行前需要配置"
  });
  const pinDataNames = Object.keys(workflow.pin_data ?? {});
  const unknownPinDataNames = pinDataNames.filter((name) => !knownNames.has(name));
  if (pinDataNames.length > 0) {
    diagnostics.push({
      status: unknownPinDataNames.length > 0 ? "warn" : "pass",
      area: "pin_data",
      message: unknownPinDataNames.length > 0 ? `Pin Data 节点不存在: ${unknownPinDataNames.join(", ")}` : `${pinDataNames.length} 个节点配置了 Pin Data`
    });
  }
  if (operationError) {
    diagnostics.push({
      status: "error",
      area: "operation",
      message: operationError
    });
  }

  return diagnostics;
}

function removeNodeFromWorkflow(workflow: WorkflowDef, nodeToDelete: WorkflowNode): WorkflowDef {
  const deleteName = nodeToDelete.name ?? nodeToDelete.id;
  if (!deleteName) return workflow;

  const nextConnections: Connections = {};
  for (const [sourceName, ports] of Object.entries(workflow.connections ?? {})) {
    if (sourceName === deleteName) continue;
    nextConnections[sourceName] = {};
    for (const [portName, portConnections] of Object.entries(ports)) {
      nextConnections[sourceName][portName] = withTargets(
        portConnections,
        targetsFor(portConnections).filter((target) => target.node !== deleteName)
      );
    }
  }

  const nextPinData = workflow.pin_data
    ? Object.fromEntries(Object.entries(workflow.pin_data).filter(([name]) => name !== deleteName))
    : undefined;
  const nextGroups = workflow.groups
    ?.map((group) => ({
      ...group,
      ...(Array.isArray(group.members) ? { members: group.members.filter((member) => member !== deleteName) } : {})
    }))
    .filter((group) => !Array.isArray(group.members) || group.members.length > 0);
  const nextDependencyEdges = workflow.dependency_edges?.filter(
    (edge) => edge.node !== deleteName && edge.supply !== deleteName
  );

  return {
    ...workflow,
    nodes: (workflow.nodes ?? []).filter((node) => (node.name ?? node.id) !== deleteName),
    connections: nextConnections,
    ...(workflow.pin_data ? { pin_data: nextPinData } : {}),
    ...(workflow.groups ? { groups: nextGroups } : {}),
    ...(workflow.dependency_edges ? { dependency_edges: nextDependencyEdges } : {})
  };
}

function descriptorBaseName(descriptor: NodeDescriptor): string {
  return descriptor.type.replace(/^xflow\./, "").replace(/[^a-z0-9]+/gi, "_").toLowerCase();
}

function uniqueNodeName(workflow: WorkflowDef, descriptor: NodeDescriptor): string {
  const baseName = descriptorBaseName(descriptor);
  const usedNames = new Set((workflow.nodes ?? []).map((node, index) => nodeName(node, index)));
  let index = 1;
  while (usedNames.has(`${baseName}_${index}`)) {
    index += 1;
  }
  return `${baseName}_${index}`;
}

function kindForDescriptor(descriptor: NodeDescriptor): WorkflowNode["kind"] {
  return descriptor.kind ?? (descriptor.group === "触发器" ? "trigger" : "action");
}

/**
 * Clones a node under the first free `<base>_<n>` name derived from its own
 * name, so a duplicate of `http_2` becomes `http_3` rather than `http_1`. The
 * `id` is dropped because the name is the key connections and selection use; a
 * carried-over id would give two nodes the same identity.
 *
 * The clone is deliberately left unconnected: wiring it into the original's slot
 * would silently re-route the graph, which is not what "duplicate" promises.
 */
function duplicateNodeInWorkflow(workflow: WorkflowDef, source: WorkflowNode, sourceIndex: number): { workflow: WorkflowDef; node: WorkflowNode } {
  const sourceName = nodeName(source, sourceIndex);
  const baseName = sourceName.replace(/_\d+$/, "") || sourceName;
  const usedNames = new Set((workflow.nodes ?? []).map((node, index) => nodeName(node, index)));
  let suffix = 1;
  while (usedNames.has(`${baseName}_${suffix}`)) {
    suffix += 1;
  }

  const offset = 36;
  const position = source.position
    ? { x: (source.position.x ?? 0) + offset, y: (source.position.y ?? 0) + offset }
    : undefined;
  const { id: _droppedId, ...rest } = source;
  const node: WorkflowNode = {
    ...rest,
    name: `${baseName}_${suffix}`,
    ...(position ? { position } : {})
  };

  return {
    node,
    workflow: { ...workflow, nodes: [...(workflow.nodes ?? []), node] }
  };
}

/**
 * A new node carries identity and placement only: name, type, kind, position
 * and the tile label. It deliberately has no `parameters` (not even `{}`) and
 * no `version`: Doc C §5.2 keeps schema defaults as display hints, never
 * written values, so an untouched node is exactly what the handler's own
 * fallbacks run against, required parameters without a default stay absent
 * (the form reports them), and an omitted version resolves to the latest.
 */
function createNodeFromDescriptor(
  workflow: WorkflowDef,
  descriptor: NodeDescriptor,
  position?: { x: number; y: number }
): WorkflowNode {
  const nextIndex = (workflow.nodes ?? []).length + 1;
  return {
    name: uniqueNodeName(workflow, descriptor),
    type: descriptor.type,
    kind: kindForDescriptor(descriptor),
    position: position ?? { x: nextIndex * 155, y: 220 },
    ui: { label: descriptor.label }
  };
}

function createDraftWorkflow(): WorkflowDef {
  return {
    name: "未命名工作流",
    spec: "1.0",
    nodes: [],
    connections: {}
  };
}

function connectSelectedNode(
  workflow: WorkflowDef,
  selectedNode: WorkflowNode | undefined,
  nextNode: WorkflowNode
): WorkflowDef {
  const nextName = nextNode.name ?? nextNode.id;
  return nextName ? addConnection(workflow, selectedNode, nextName) : workflow;
}

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

/** Tooltip line naming where a runner-reported type came from, or undefined for server types. */
function describeSource(descriptor: NodeDescriptor): string | undefined {
  if (descriptor.source !== "runner") return undefined;
  return descriptor.pools?.length ? `Runner 资源池 ${descriptor.pools.join(", ")}` : "Runner 上报 · 未归属资源池";
}

function nodeTileTooltip(descriptor: NodeDescriptor): React.ReactNode {
  const ports = describePorts(descriptor.ports);
  const source = describeSource(descriptor);
  if (!ports && !source && !descriptor.description) return descriptor.type;
  return (
    <span className="xflow-editor-node-tile__tooltip">
      <span>{descriptor.type}</span>
      {ports ? <span>{ports}</span> : null}
      {source ? <span className="xflow-editor-node-tile__tooltip-source">{source}</span> : null}
      {descriptor.description ? <span>{descriptor.description}</span> : null}
    </span>
  );
}

function NodeLibrary({
  descriptors: libraryDescriptors,
  onAddNode,
  autoFocusSearch = false,
  searchInputRef,
  headerAction
}: {
  descriptors: NodeDescriptor[];
  onAddNode: (descriptor: NodeDescriptor) => void;
  autoFocusSearch?: boolean;
  searchInputRef?: React.Ref<InputRef>;
  headerAction?: React.ReactNode;
}): React.ReactElement {
  const [query, setQuery] = React.useState("");
  const [collapsedGroups, setCollapsedGroups] = React.useState<ReadonlySet<NodeDescriptor["group"]>>(
    () => new Set()
  );
  const normalizedQuery = query.trim().toLowerCase();
  const toggleGroup = React.useCallback((group: NodeDescriptor["group"]) => {
    setCollapsedGroups((current) => {
      const next = new Set(current);
      if (next.has(group)) next.delete(group);
      else next.add(group);
      return next;
    });
  }, []);
  const matchesQuery = React.useCallback(
    (descriptor: NodeDescriptor) =>
      !normalizedQuery ||
      descriptor.label.toLowerCase().includes(normalizedQuery) ||
      descriptor.type.toLowerCase().includes(normalizedQuery),
    [normalizedQuery]
  );
  const descriptorsForGroup = React.useCallback(
    (group: NodeDescriptor["group"]) =>
      libraryDescriptors.filter((descriptor) => descriptor.group === group && matchesQuery(descriptor)),
    [libraryDescriptors, matchesQuery]
  );

  return (
    <EditorPanel
      actions={headerAction}
      bodyClassName="xflow-editor-library__body"
      className="xflow-editor-library"
      count={libraryDescriptors.length}
      icon={<AppstoreOutlined />}
      title="节点"
    >
      {/* Always shown, as in the baseline; "/" focuses it (the shortcut list). */}
      <div className="xflow-editor-library__search">
        <Input
          autoFocus={autoFocusSearch}
          ref={searchInputRef}
          classNames={editorLibrarySearchClassNames}
          placeholder="搜索 Webhook / If / HTTP…"
          prefix={<SearchOutlined />}
          suffix={<kbd className="xflow-editor-kbd">/</kbd>}
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
      </div>
      <div className="xflow-editor-library__list">
      {nodeLibraryGroups.map((group) => {
        const descriptors = descriptorsForGroup(group);
        const collapsed = !normalizedQuery && collapsedGroups.has(group);
        // An empty group is hidden: always while searching, and for "其他",
        // which only exists to catch types without a presentation entry.
        if (descriptors.length === 0 && (normalizedQuery || group === "其他")) return null;

        return (
          <div className="xflow-editor-node-group" key={group}>
            <button
              aria-expanded={!collapsed}
              className="xflow-editor-node-group__title"
              type="button"
              onClick={() => toggleGroup(group)}
            >
              <span className="xflow-editor-node-group__heading">
                <span className="xflow-editor-block-title">{group}</span>
                <DownOutlined aria-hidden="true" className={collapsed ? "xflow-editor-node-group__chevron is-collapsed" : "xflow-editor-node-group__chevron"} />
                <span className="xflow-editor-node-group__count">{descriptors.length}</span>
              </span>
            </button>
            {collapsed ? null : (
              <div className="xflow-editor-node-list">
                {descriptors.map((descriptor) => (
                  <Tooltip classNames={editorTooltipClassNames} key={descriptor.type} title={nodeTileTooltip(descriptor)}>
                    <button
                      aria-label={descriptor.source === "runner" ? `${descriptor.label} (Runner)` : descriptor.label}
                      className="xflow-editor-node-tile"
                      data-node-type={descriptor.type}
                      data-source={descriptor.source}
                      draggable
                      type="button"
                      onClick={() => onAddNode(descriptor)}
                      onDragStart={(event) => {
                        // The canvas reads this payload back on drop. Click stays
                        // wired so the tile remains usable without a drag.
                        event.dataTransfer.setData(XFLOW_NODE_DRAG_MIME, descriptor.type);
                        event.dataTransfer.effectAllowed = "copy";
                      }}
                    >
                      <NodeCategoryIcon category={descriptor.category} type={descriptor.type} />
                      <span className="xflow-editor-node-tile__content">
                        <strong>{descriptor.label}</strong>
                        {/* The schema's description, or the type id when it has none; the id stays in the tooltip and the search. */}
                        <small
                          className="xflow-editor-node-tile__hint"
                          title={descriptor.description || descriptor.type}
                        >
                          {descriptor.description || descriptor.type}
                        </small>
                      </span>
                      {descriptor.source === "runner" ? (
                        <Tag
                          aria-hidden="true"
                          className="xflow-editor__node-tile-source"
                          classNames={editorNodeSourceTagClassNames}
                          variant="outlined"
                        >
                          Runner
                        </Tag>
                      ) : null}
                    </button>
                  </Tooltip>
                ))}
              </div>
            )}
          </div>
        );
      })}
      {normalizedQuery && !libraryDescriptors.some(matchesQuery) ? (
        <p className="xflow-editor-empty">没有匹配节点。</p>
      ) : null}
      </div>
    </EditorPanel>
  );
}

/**
 * §6.2 empty canvas: shown only while the workflow has no nodes, centred on the
 * canvas (the preview places it and keeps the pane a drop target around it).
 * 导入 DSL has no host hook yet, so it is offered disabled with the reason.
 */
function EmptyCanvasCard({ editable, onPickTrigger }: {
  editable: boolean;
  onPickTrigger: () => void;
}): React.ReactElement {
  return (
    <div className="xflow-editor-empty-canvas">
      <span className="xflow-editor-empty-canvas__icon" data-cat="trigger"><LinkOutlined /></span>
      <h2 className="xflow-editor-empty-canvas__title">添加触发器开始</h2>
      <p className="xflow-editor-empty-canvas__text">从 Webhook、Cron、Kafka 等入口开始，或导入已有 DSL。</p>
      {editable ? (
        <div className="xflow-editor-empty-canvas__actions">
          <Button size="small" type="primary" onClick={onPickTrigger}>选择触发器</Button>
          <Tooltip classNames={editorTooltipClassNames} title="未接入">
            <span className="xflow-editor-disabled-control">
              <Button disabled size="small">导入 DSL</Button>
            </span>
          </Tooltip>
        </div>
      ) : null}
    </div>
  );
}

/**
 * Renders the same `editorShortcuts` list the window handler dispatches from,
 * so this reference cannot advertise a key the editor does not honor.
 */
function ShortcutsHelp({ open, onClose }: { open: boolean; onClose: () => void }): React.ReactElement {
  const groups: { title: string; scope: ShortcutScope }[] = [
    { title: "画布", scope: "canvas" },
    { title: "全局", scope: "global" }
  ];

  return (
    <Modal
      className="xflow-editor-shortcuts-modal"
      classNames={{
        body: "xflow-editor-shortcuts",
        container: "xflow-editor-shortcuts__content",
        header: "xflow-editor-shortcuts__header"
      }}
      footer={null}
      getContainer={false}
      open={open}
      title="快捷键"
      onCancel={onClose}
    >
      {groups.map((group) => (
        <section className="xflow-editor-shortcuts__group" key={group.scope} aria-label={group.title}>
          <h3 className="xflow-editor-shortcuts__group-title">{group.title}</h3>
          <dl className="xflow-editor-shortcuts__list">
            {editorShortcuts.filter((shortcut) => shortcut.scope === group.scope).map((shortcut) => (
              <div className="xflow-editor-shortcuts__row" key={shortcut.id}>
                <dt className="xflow-editor-shortcuts__label">{shortcut.label}</dt>
                <dd className="xflow-editor-shortcuts__keys"><kbd>{shortcut.keys}</kbd></dd>
              </div>
            ))}
          </dl>
        </section>
      ))}
      <p className="xflow-editor-shortcuts__note">
        删除与退格键删除当前选中项：选中连线时删除连线，否则删除选中节点。
      </p>
    </Modal>
  );
}

export function XFlowEditor({
  value,
  className,
  onBack,
  runtime,
  onChange,
  onSave,
  onRun,
  onRunNode,
  nodeTypes: sourceNodeTypes,
  appearance: controlledAppearance,
  onAppearanceChange,
  defaultViewport,
  onViewportChange,
  nodeTypesError,
  onRetryNodeTypes,
  onReloadLatest,
  onSaveAsCopy,
  onPublish,
  onLoadVersions,
  onViewVersion,
  onRollback,
  historicalVersion,
  onExitHistory,
  readOnlyReason,
  capabilities,
  execution,
  onExecutionAction,
  onLocateNode,
  onOpenRunHistory,
  onOpenExecution,
  currentUserSubject,
  onSignal,
  onRevokeSignal,
  runBarDismissMs,
  mode: controlledMode,
  onModeChange,
  executionList,
  onLoadExecutions,
  selectedExecution,
  onSelectExecution,
  executionSnapshotWorkflow,
  executionDefinitionStale,
  executionSnapshotRuntime,
  onRerun
}: XFlowEditorProps): React.ReactElement {
  // Memoised per response inside localizeNodeTypes, so identity-keyed caches
  // downstream (node library, form compiler) keep hitting.
  const nodeTypes = localizeNodeTypes(sourceNodeTypes, nodeFormCatalog);
  const [uncontrolledAppearance, setUncontrolledAppearance] =
    React.useState<XFlowEditorAppearance>("system");
  const activeAppearance = controlledAppearance ?? uncontrolledAppearance;
  const resolvedAppearance = useResolvedAppearance(activeAppearance);
  const editorThemeConfig = React.useMemo(
    () => createEditorThemeConfig(resolvedAppearance),
    [resolvedAppearance]
  );
  const changeAppearance = React.useCallback(
    (nextAppearance: XFlowEditorAppearance) => {
      if (controlledAppearance === undefined) {
        setUncontrolledAppearance(nextAppearance);
      }
      onAppearanceChange?.(nextAppearance);
    },
    [controlledAppearance, onAppearanceChange]
  );
  const [draftWorkflow, setDraftWorkflow] = React.useState<WorkflowDef>(value);
  // The definition the version chip counts against: the last `value` the host
  // replaced, or the definition the last successful save persisted. Never
  // updated by ordinary edits, so undo back to it reports zero changes.
  const [baselineWorkflow, setBaselineWorkflow] = React.useState<WorkflowDef>(value);
  const draftRevisionRef = React.useRef(0);
  /** Counts the definitions the host has replaced `value` with, to tell a stale save answer from a current one. */
  const valueEpochRef = React.useRef(0);
  const latestDraftRef = React.useRef<WorkflowDef>(value);
  const historyRef = React.useRef<WorkflowHistory>({ undo: [], redo: [] });
  const [history, setHistory] = React.useState<WorkflowHistory>({ undo: [], redo: [] });
  const [canvasTool, setCanvasTool] = React.useState<CanvasTool>("select");
  const [localRuntime, setLocalRuntime] = React.useState<RuntimeSnapshot | undefined>(runtime);
  const editorRootRef = React.useRef<HTMLElement | null>(null);
  const editorWidth = useEditorWidth(editorRootRef);
  const widthTier = layoutTier(editorWidth);
  const [layoutPolicy, setLayoutPolicy] = React.useState<LayoutPolicy>("auto");
  const layout: EditorLayout =
    layoutPolicy === "compact" ? "c" : widthTier === "narrow" ? "c" : widthTier === "compact" ? "b" : "a";
  // §6.3: below 1280 (layout b/c, manual immersive included) the secondary
  // history / validate / theme actions fold into the ⋯ menu.
  const compactToolbar = layout !== "a";
  const [compactDrawer, setCompactDrawer] = React.useState<"left" | "right" | undefined>();
  const [leftDrawerTab, setLeftDrawerTab] = React.useState<"library" | "variables" | "executions">("library");
  const [desktopRailSection, setDesktopRailSection] = React.useState<"library" | "variables">("library");
  const nodeLibrarySearchRef = React.useRef<InputRef | null>(null);
  // Bumped by the "/" shortcut; the effect below focuses the search once the
  // library column (re)mounted by the same gesture is in the DOM.
  const [librarySearchFocusNonce, setLibrarySearchFocusNonce] = React.useState(0);
  const [nodeSearchOpen, setNodeSearchOpen] = React.useState(false);
  /** Bumped per open so the panel starts with an empty query (E2b review). */
  const [nodeSearchNonce, setNodeSearchNonce] = React.useState(0);
  const nodeSearchRef = React.useRef<InputRef | null>(null);
  /** The element to focus again when the locate panel closes. */
  const locateTriggerRef = React.useRef<HTMLElement | null>(null);
  const title = draftWorkflow.name ?? "Untitled workflow";
  const nodes = draftWorkflow.nodes ?? [];
  // §6.13: one declared-category map from the schemas, shared by the canvas, the
  // outline, the node library and the diagnostics mix.
  const nodeCategories = React.useMemo(() => nodeCategoriesFromTypes(nodeTypes), [nodeTypes]);
  // §6.1.1 card summaries, resolved once per definition: the preview only
  // draws rows it is handed, so field labels and formatting stay on this side
  // of the package boundary (and follow the localized schemas). The replay
  // resolves them against the snapshot's definition, not the live draft.
  const summariesFor = React.useCallback((workflow: WorkflowDef | undefined) => {
    if (!nodeTypes || !workflow) return undefined;
    // Null-prototype: a node keyed `constructor` would otherwise read an
    // inherited function where the card expects an array of rows.
    const byNode: Record<string, readonly CardSummaryRow[]> = Object.create(null);
    for (const node of workflow.nodes ?? []) {
      const schema = formSchemaForNode(nodeTypes, node);
      // An undeclared card_summary still draws the spec's required fallback.
      const rows = schema ? cardSummaryRows(schema, node.parameters) : [];
      if (rows.length > 0) byNode[nodeKey(node)] = rows;
    }
    return byNode;
  }, [nodeTypes]);
  const nodeSummaries = React.useMemo(
    () => summariesFor(draftWorkflow),
    [draftWorkflow, summariesFor]
  );
  // The publish diff's 摘要 row (§6.10): the same rows the card would draw, as
  // one line. A node whose card shows nothing has no summary to compare.
  const cardSummaryText = React.useCallback((node: WorkflowNode): string | undefined => {
    const schema = formSchemaForNode(nodeTypes, node);
    if (!schema) return undefined;
    const rows = cardSummaryRows(schema, node.parameters);
    return rows.length > 0 ? rows.map((row) => `${row.label} ${row.value}`).join(" · ") : undefined;
  }, [nodeTypes]);
  // The chip counts against the loaded/saved baseline, so an undo that returns
  // the draft to that baseline reports zero changes again (§6.3). The very same
  // array is the publish dialog's structured diff (§6.10 requires one source).
  const changes = React.useMemo(
    () => workflowChanges(baselineWorkflow, draftWorkflow, { cardSummary: cardSummaryText }),
    [baselineWorkflow, draftWorkflow, cardSummaryText]
  );
  const unsavedChanges = changes.length;
  // "基于 vX" names the definition the changes are measured against, so the
  // version comes from the baseline, not the draft: editing the version field
  // must not make the chip claim the edit is based on the new, unsaved value.
  const versionChip = versionChipState({
    persisted: Boolean(baselineWorkflow.id),
    version: baselineWorkflow.version,
    changes: unsavedChanges,
    historicalVersion
  });
  const titleTooltip = [draftWorkflow.id, draftWorkflow.version ? `v${draftWorkflow.version}` : undefined]
    .filter((part): part is string => Boolean(part))
    .join(" · ") || undefined;
  // §6.8: the switch is 编辑 | 运行记录. A controlled `mode` is the host's
  // (it owns `?exec=`); without one the editor keeps its own state.
  const [uncontrolledMode, setUncontrolledMode] = React.useState<XFlowEditorMode>("edit");
  const viewMode: XFlowEditorMode = controlledMode ?? uncontrolledMode;
  const runsMode = viewMode === "runs";
  /** The canvas instance is remounted per mode so a saved edit viewport can be restored. */
  const [canvasEpoch, setCanvasEpoch] = React.useState(0);
  const [restoreViewport, setRestoreViewport] = React.useState<Viewport | undefined>(defaultViewport);
  const editViewportRef = React.useRef<Viewport | undefined>(defaultViewport);
  const editSelectionRef = React.useRef<string | undefined>(undefined);
  const previousModeRef = React.useRef<XFlowEditorMode>(viewMode);
  // Panel collapse is restored per workflow (§6.0). `panelStateKey` tracks the
  // workflow the two flags belong to, so a rename does not write the previous
  // workflow's flags under the new id and an id swap cannot race the restore.
  const [panelStateKey, setPanelStateKey] = React.useState(value.id);
  const [leftCollapsed, setLeftCollapsed] = React.useState(() => readPanelCollapse(value.id)?.leftCollapsed ?? false);
  const [rightCollapsed, setRightCollapsed] = React.useState(() => readPanelCollapse(value.id)?.rightCollapsed ?? false);

  React.useEffect(() => {
    if (panelStateKey === value.id) return;
    // A workflow's first save adopts the id on the same mounted editor. That is
    // not a workflow swap: keep the flags the user just set and start
    // persisting them under the new id (the write effect below does that).
    if (panelStateKey === undefined && value.id !== undefined) {
      setPanelStateKey(value.id);
      return;
    }
    const restored = readPanelCollapse(value.id);
    setLeftCollapsed(restored?.leftCollapsed ?? false);
    setRightCollapsed(restored?.rightCollapsed ?? false);
    setPanelStateKey(value.id);
  }, [panelStateKey, value.id]);

  React.useEffect(() => {
    if (!value.id || panelStateKey !== value.id) return;
    writePanelCollapse(value.id, { leftCollapsed, rightCollapsed });
  }, [leftCollapsed, panelStateKey, rightCollapsed, value.id]);
  const [shortcutsOpen, setShortcutsOpen] = React.useState(false);
  // Keep the workbench canvas-first. Validation, execution, and failures open
  // this surface on demand so operational feedback is never hidden. In memory
  // only: it always starts collapsed.
  const [dock, setDock] = React.useState<DockState>("collapsed");
  const dockControllerRef = React.useRef<DiagnosticsDockController | null>(null);
  /** The canvas frame: the focus target for 定位 when the node's card is not rendered. */
  const canvasFrameRef = React.useRef<HTMLDivElement | null>(null);
  /** The run-bar row, measured against the preview's top floats (see the effect below). */
  const runBarRowRef = React.useRef<HTMLDivElement | null>(null);
  // A run bar wider than the gap between the canvas's top floats would cover
  // 添加节点 / 定位节点 (the baseline's float row has the same collision below
  // ~1400px). There is no allowed container breakpoint for "how wide is this
  // execution's bar", so the row is measured and drops one float row instead.
  const [runBarStacked, setRunBarStacked] = React.useState(false);
  /** The auto-open points: a closed dock docks; an expanded one is never pressed back down. */
  const revealDock = React.useCallback(() => setDock((current) => (current === "collapsed" ? "docked" : current)), []);
  /** Where focus goes when the banner holding it goes away (重试 succeeded, 重新加载 replaced the value). */
  const focusCanvasFrame = React.useCallback(() => canvasFrameRef.current?.focus({ preventScroll: true }), []);
  const [saving, setSaving] = React.useState(false);
  // Render state lags a same-tick second trigger (button + shortcut); the ref
  // is the in-flight guard, `saving` only drives the button's loading state.
  const savingRef = React.useRef(false);
  const [running, setRunning] = React.useState(false);
  // §6.10 publish / version history. The editor never requests: the host owns
  // `onLoadVersions` / `onPublish` / `onRollback`, these states only hold what
  // came back and which surface is open.
  const [publishOpen, setPublishOpen] = React.useState(false);
  const [publishVersion, setPublishVersion] = React.useState("");
  const [publishError, setPublishError] = React.useState<string>();
  const [publishing, setPublishing] = React.useState(false);
  const publishingRef = React.useRef(false);
  /** The suggested label the field last adopted; a user-typed label is never overwritten. */
  const publishSuggestedRef = React.useRef("");
  const [versionsOpen, setVersionsOpen] = React.useState(false);
  const [versionsState, setVersionsState] = React.useState<{
    data?: XFlowWorkflowVersions;
    error?: string;
    loading?: boolean;
  }>({});
  const [versionLoadNonce, setVersionLoadNonce] = React.useState(0);
  /** version → the reason the server refused its rollback, pinned to that row. */
  const [blockedVersions, setBlockedVersions] = React.useState<Record<string, string>>({});
  const [rollingBackVersion, setRollingBackVersion] = React.useState<string>();
  const rollingBackRef = React.useRef(false);
  const [operationStatus, setOperationStatus] = React.useState(() => value.id ? "已保存" : "未保存");
  const [operationError, setOperationError] = React.useState<string>();
  const [selectedKey, setSelectedKey] = React.useState<string | undefined>(() => defaultSelectedKey(nodes));
  const selectedIndex = nodes.findIndex((node, index) => nodeKey(node, index) === selectedKey);
  const selectedNode = selectedIndex >= 0 ? nodes[selectedIndex] : undefined;
  const selectedKeyRef = React.useRef(selectedKey);
  selectedKeyRef.current = selectedKey;
  /** §6.9: the execution and optional node the rerun dialog was opened from. */
  const [rerunTarget, setRerunTarget] = React.useState<{ executionId: string; node?: string }>();
  const openRerun = React.useCallback((executionId: string, node?: string) => {
    setRerunTarget({ executionId, node });
  }, []);
  /** The working view: the replay's definition while 运行记录 is active. */
  const canvasWorkflow: WorkflowDef = runsMode
    ? executionSnapshotWorkflow ?? {
      name: draftWorkflow.name,
      spec: draftWorkflow.spec,
      nodes: [],
      connections: {}
    }
    : draftWorkflow;
  const canvasNodeSummaries = React.useMemo(
    () => (runsMode ? summariesFor(executionSnapshotWorkflow) : nodeSummaries),
    [executionSnapshotWorkflow, nodeSummaries, runsMode, summariesFor]
  );
  // §6.8 rerun derivation: the start node says 重跑起点, everything upstream of
  // it says 沿用原执行结果. Both come from this run's lineage + snapshot graph.
  const replayMetaNotes = React.useMemo(() => {
    if (!runsMode || !selectedExecution?.rerunFrom) return undefined;
    if (!executionSnapshotWorkflow) return undefined;
    const notes: Record<string, string> = Object.create(null);
    for (const name of upstreamNodeNames(executionSnapshotWorkflow, selectedExecution.rerunFrom)) {
      notes[name] = "沿用原执行结果";
    }
    const nodes = executionSnapshotWorkflow.nodes ?? [];
    if (nodes.some((node, index) => nodeName(node, index) === selectedExecution.rerunFrom)) {
      notes[selectedExecution.rerunFrom] = "重跑起点";
    }
    return notes;
  }, [executionSnapshotWorkflow, runsMode, selectedExecution?.rerunFrom]);
  // The replay selects inside the snapshot definition, not the draft: the two
  // graphs can differ, so a key present in one may be absent from the other.
  const replayNodes = canvasWorkflow.nodes ?? [];
  const replaySelectedIndex = runsMode
    ? replayNodes.findIndex((node, index) => nodeKey(node, index) === selectedKey)
    : -1;
  const replaySelectedNode = replaySelectedIndex >= 0 ? replayNodes[replaySelectedIndex] : undefined;
  const replaySelectedName = replaySelectedNode
    ? nodeName(replaySelectedNode, replaySelectedIndex)
    : undefined;
  // §6.9 entry from the 运行 tab: only a rerunnable source execution offers it.
  const selectedRerunnable = selectedExecution
    ? workflowStatusMeta[selectedExecution.status].ops.includes("rerun")
    : false;
  const replayRerunFromNode =
    runsMode && selectedExecution && selectedRerunnable && onRerun && replaySelectedName
      ? () => openRerun(selectedExecution.executionId, replaySelectedName)
      : undefined;
  // Entering the replay keeps the edit selection and viewport for the way back;
  // leaving it restores both and remounts the canvas so `defaultViewport` takes.
  React.useEffect(() => {
    if (previousModeRef.current === viewMode) return;
    previousModeRef.current = viewMode;
    if (viewMode === "runs") {
      editSelectionRef.current = selectedKeyRef.current;
      setSelectedKey(undefined);
      // The replay renders its status bar in place of the diagnostics dock, so
      // an expanded dock would leave the workbench inert with no control left
      // on screen to collapse it. Converge to 停靠 before it is unmounted.
      setDock((current) => (current === "expanded" ? "docked" : current));
    } else {
      setRestoreViewport(editViewportRef.current);
      setSelectedKey(editSelectionRef.current);
    }
    setCanvasEpoch((epoch) => epoch + 1);
  }, [viewMode]);
  const changeMode = React.useCallback((next: XFlowEditorMode) => {
    if (next === viewMode) return;
    if (controlledMode === undefined) setUncontrolledMode(next);
    onModeChange?.(next);
  }, [controlledMode, onModeChange, viewMode]);
  // §6.11: 查看执行 needs the `execution` scope. Undefined keeps the
  // pre-capabilities host contract (everything enabled); only an explicit false
  // hides the entry. A host can still hand over `mode="runs"` (a shared ?exec=
  // URL, a scope refresh in flight), so the mode falls back as well: without
  // the entry there would be no way back to 编辑.
  const canViewExecutions = capabilities?.execution !== false;
  React.useEffect(() => {
    if (!canViewExecutions && viewMode === "runs") changeMode("edit");
  }, [canViewExecutions, changeMode, viewMode]);
  // Only the edit canvas's viewport is remembered and forwarded; a replay pans
  // its own snapshot without moving the view the user returns to.
  const handleViewportChange = React.useCallback((viewport: Viewport) => {
    editViewportRef.current = viewport;
    onViewportChange?.(viewport);
  }, [onViewportChange]);
  /**
   * The §6.12 dialog's target: one node of the tracked execution. Keyed by the
   * execution id too, so a new run can never leave the dialog over a node of
   * the previous one.
   */
  const [approvalTarget, setApprovalTarget] = React.useState<{ executionId: string; nodeName: string }>();
  const openApprovalFor = React.useCallback((approvalNodeName: string) => {
    if (!execution) return;
    setApprovalTarget({ executionId: execution.executionId, nodeName: approvalNodeName });
  }, [execution]);
  // A target whose node left the suspended state (a decision closed the gate,
  // the run moved on) closes the dialog: it would otherwise show a stale wait.
  React.useEffect(() => {
    if (!approvalTarget) return;
    if (!execution || execution.executionId !== approvalTarget.executionId) {
      setApprovalTarget(undefined);
      return;
    }
    const node = execution.nodes?.find((candidate) => candidate.name === approvalTarget.nodeName);
    if (!node || node.status !== "suspended") setApprovalTarget(undefined);
  }, [approvalTarget, execution]);
  /**
   * Suspended approval cards get the chain/progress line and a 处理 action
   * (§6.12); every other suspended card keeps the preview's 等待信号.
   */
  const approvalNotes = React.useMemo(() => {
    const notes: Record<string, { text: string; action: string }> = {};
    for (const execNode of execution?.nodes ?? []) {
      if (execNode.status !== "suspended") continue;
      const def = workflowNodeByName(draftWorkflow, execNode.name);
      if (!def || def.type !== APPROVAL_NODE_TYPE) continue;
      const params = approvalParamsFrom(def.parameters);
      const progress = approvalProgress(params.approvers ?? [], approvalDecisions(execNode.output));
      notes[execNode.name] = { text: approvalWaitText(params.mode, progress), action: "处理" };
    }
    return notes;
  }, [draftWorkflow, execution]);
  /** The first suspended approval of the tracked execution, if there is one. */
  const firstApprovalNode = React.useMemo(() => {
    for (const execNode of execution?.nodes ?? []) {
      if (execNode.status !== "suspended") continue;
      if (workflowNodeByName(draftWorkflow, execNode.name)?.type === APPROVAL_NODE_TYPE) return execNode.name;
    }
    return undefined;
  }, [draftWorkflow, execution]);
  // An archived version is opened read-only (§6.10): 查看 must never edit history.
  const readOnly = Boolean(readOnlyReason) || capabilities?.workflow === false || historicalVersion !== undefined;
  // Writes stay available while viewing history (回滚 is the one write this
  // state offers), so this is deliberately not `!readOnly`: it drops only the
  // reasons that remove the workflow scope itself.
  const canWriteWorkflow = !readOnlyReason && capabilities?.workflow !== false;
  // A read-only viewer and the replay both take the closed-editing path.
  const editMode = viewMode === "edit" && !readOnly;
  const [saveConflict, setSaveConflict] = React.useState(false);
  // The host's reason wins; without one, name the scopes the credential is
  // missing. The editor never asks the server — the host already passed the
  // bits, so this sentence is built from data already in the props.
  const readOnlyBannerReason = readOnlyReason || (
    capabilities?.workflow === false
      ? `缺少 ${missingCapabilityScopes(capabilities).join("、")} 权限，编辑器为只读。`
      : undefined
  );
  const editLockedTitle = runsMode
    ? "运行记录模式为只读，请切换到编辑模式。"
    : readOnlyBannerReason ?? "当前为只读模式。";
  // 节点 and 变量 are the two left-column sections in every mode. Preview mode
  // has no node library, so its 节点 section is the outline alone (spec §6.0.1);
  // 变量 stays reachable from the rail.
  const leftSection = desktopRailSection;
  const leftDrawerSection = leftDrawerTab;
  const hostNotices = [
    !readOnly && !onSave ? "保存不可用：宿主未提供 onSave 处理器" : undefined,
    !readOnly && !onRun ? "运行不可用：宿主未提供 onRun 处理器" : undefined,
    !readOnly && !onPublish ? "发布不可用：宿主未提供 onPublish 处理器" : undefined
  ].filter((notice): notice is string => Boolean(notice));

  React.useEffect(() => {
    if (serializeJson(latestDraftRef.current) === serializeJson(value)) {
      latestDraftRef.current = value;
      return;
    }
    draftRevisionRef.current += 1;
    latestDraftRef.current = value;
    setDraftWorkflow(value);
    historyRef.current = { undo: [], redo: [] };
    setHistory(historyRef.current);
    // Drafts and server findings belong to the replaced definition.
    clearDraftTimer();
    replaceParameterDrafts({});
    replaceWorkflowJsonDrafts({});
    setParamIssues(undefined);
    setBaselineWorkflow(value);
    setOperationError(undefined);
    // A replaced definition is the latest the host has: the stale-version warning is over.
    valueEpochRef.current += 1;
    setSaveConflict(false);
    setOperationStatus(value.id ? "已保存" : "未保存");
  }, [value]);

  React.useEffect(() => {
    setLocalRuntime(runtime);
  }, [runtime]);

  const commitWorkflow = React.useCallback(
    (nextWorkflow: WorkflowDef, keepRedo = false) => {
      const previousWorkflow = latestDraftRef.current;
      if (serializeJson(previousWorkflow) === serializeJson(nextWorkflow)) return;
      const nextHistory: WorkflowHistory = {
        undo: [...historyRef.current.undo, previousWorkflow].slice(-historyLimit),
        redo: keepRedo ? historyRef.current.redo : []
      };
      historyRef.current = nextHistory;
      setHistory(nextHistory);
      draftRevisionRef.current += 1;
      latestDraftRef.current = nextWorkflow;
      setDraftWorkflow(nextWorkflow);
      setOperationStatus("未保存");
      setOperationError(undefined);
      onChange?.(nextWorkflow);
    },
    [onChange]
  );

  // Every Inspector writer goes through this: the updater runs against the
  // latest committed draft, so writes in the same tick compose.
  const commitWorkflowWith = React.useCallback(
    (update: WorkflowUpdater, keepRedo = false) => {
      commitWorkflow(update(latestDraftRef.current), keepRedo);
    },
    [commitWorkflow]
  );

  // Parameters-JSON drafts. The ref is the source of truth (a blur or a save
  // in the same tick must see the keystroke before React re-renders); the
  // state mirrors it for rendering.
  const [parameterDrafts, setParameterDraftsState] = React.useState<Record<string, ParameterDraft>>({});
  const parameterDraftsRef = React.useRef(parameterDrafts);
  const replaceParameterDrafts = React.useCallback((next: Record<string, ParameterDraft>) => {
    if (next === parameterDraftsRef.current) return;
    parameterDraftsRef.current = next;
    setParameterDraftsState(next);
  }, []);
  const draftTimerRef = React.useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const clearDraftTimer = React.useCallback(() => {
    if (draftTimerRef.current !== undefined) clearTimeout(draftTimerRef.current);
    draftTimerRef.current = undefined;
  }, []);
  React.useEffect(() => clearDraftTimer, [clearDraftTimer]);
  const nodeTypesRef = React.useRef(nodeTypes);
  nodeTypesRef.current = nodeTypes;

  /** Commits one node's parseable draft now (blur, tab switch, debounce, save). */
  const commitParameterDraft = React.useCallback((key: string, keepRedo = false) => {
    const draft = parameterDraftsRef.current[key];
    if (!draft || draft.error) return;
    const parsed = parseParametersText(draft.text);
    const { [key]: _committed, ...rest } = parameterDraftsRef.current;
    replaceParameterDrafts(rest);
    if (!parsed.value) return;
    const parameters = parsed.value;
    commitWorkflowWith((latest) => updateNodeByKey(latest, key, (node) =>
      serializeJson(node.parameters ?? {}) === serializeJson(parameters) ? node : { ...node, parameters }
    ), keepRedo);
  }, [commitWorkflowWith, replaceParameterDrafts]);
  const flushParameterDrafts = React.useCallback((keepRedo = false) => {
    clearDraftTimer();
    for (const [key, draft] of Object.entries(parameterDraftsRef.current)) {
      if (!draft.error) commitParameterDraft(key, keepRedo);
    }
  }, [clearDraftTimer, commitParameterDraft]);
  /**
   * Every keystroke of the parameters JSON (Doc C §6.2): unparseable text
   * stays a blocking draft; parseable text is drafted and committed after
   * 300ms of quiet, or earlier on blur / tab switch / save / run.
   */
  const updateParameterText = React.useCallback((key: string, text: string) => {
    const parsed = parseParametersText(text);
    const current = parameterDraftsRef.current;
    const existing = current[key];
    if (!existing || existing.text !== text || existing.error !== parsed.error) {
      replaceParameterDrafts({ ...current, [key]: parsed.error ? { text, error: parsed.error } : { text } });
    }
    // One timer serves every node: re-arm it while ANY parseable draft is
    // pending, so an unparseable keystroke on one node never starves another
    // node's valid draft.
    clearDraftTimer();
    if (Object.values(parameterDraftsRef.current).some((draft) => !draft.error)) {
      draftTimerRef.current = setTimeout(() => flushParameterDrafts(), parameterDraftDebounceMs);
    }
  }, [clearDraftTimer, flushParameterDrafts, replaceParameterDrafts]);
  // Workflow-level JSON drafts: same ref-plus-state shape as the parameter
  // drafts, lifted here so an unparseable field blocks save/run and survives
  // the Inspector unmounting (compact drawer, collapsed panel).
  const [workflowJsonDrafts, setWorkflowJsonDraftsState] = React.useState<Partial<Record<WorkflowJsonField, WorkflowJsonDraft>>>({});
  const workflowJsonDraftsRef = React.useRef(workflowJsonDrafts);
  const replaceWorkflowJsonDrafts = React.useCallback((next: Partial<Record<WorkflowJsonField, WorkflowJsonDraft>>) => {
    if (next === workflowJsonDraftsRef.current) return;
    workflowJsonDraftsRef.current = next;
    setWorkflowJsonDraftsState(next);
  }, []);
  /** Every keystroke of a workflow-level JSON field: parseable text commits now, the rest stays a blocking draft. */
  const updateWorkflowJsonText = React.useCallback((field: WorkflowJsonField, text: string) => {
    let draft: WorkflowJsonDraft;
    try {
      const parsed = text.trim() ? (JSON.parse(text) as unknown) : {};
      draft = !parsed || typeof parsed !== "object" || Array.isArray(parsed)
        ? { text, error: "JSON 必须是对象" }
        : { text, value: parsed as Record<string, unknown> };
    } catch {
      draft = { text, error: "JSON 格式错误" };
    }
    replaceWorkflowJsonDrafts({ ...workflowJsonDraftsRef.current, [field]: draft });
    const value = draft.value;
    if (value) commitWorkflowWith((latest) => ({ ...latest, [field]: value }));
  }, [commitWorkflowWith, replaceWorkflowJsonDrafts]);
  const handleNodeRenamed = React.useCallback((previousKey: string, nextKey: string) => {
    setSelectedKey(nextKey);
    if (previousKey === nextKey) return;
    const current = parameterDraftsRef.current;
    if (!(previousKey in current)) return;
    const { [previousKey]: draft, ...rest } = current;
    replaceParameterDrafts({ ...rest, [nextKey]: draft });
  }, [replaceParameterDrafts]);
  // Drop drafts whose node no longer exists (deleted, or an undo/redo moved it
  // back to another key). Run as an effect, not inside commitWorkflow, so a
  // rename's commit and its draft rekey land in the same batch before pruning;
  // otherwise a later node reusing the key would inherit a stranger's draft.
  React.useEffect(() => {
    const liveKeys = new Set((draftWorkflow.nodes ?? []).map((node, index) => nodeKey(node, index)));
    const current = parameterDraftsRef.current;
    const orphaned = Object.keys(current).filter((key) => !liveKeys.has(key));
    if (orphaned.length === 0) return;
    const next = { ...current };
    for (const key of orphaned) delete next[key];
    replaceParameterDrafts(next);
  }, [draftWorkflow, replaceParameterDrafts]);

  /**
   * The C3 reducer (Doc C §6.1): one composer onChange batch for one node is
   * whitelisted, applied with core applyPatches (row identity kept), extended
   * with enum-invalidation unsets (§4.4) and committed in ONE synchronous
   * commitWorkflowWith call -- one undo entry. Dynamic ports (§4.3) are
   * derived from the committed value on the next render.
   */
  const applyNodePatches = React.useCallback((key: string, patches: Patch[]) => {
    // A debounced JSON draft of this node must not land on top of the form edit.
    commitParameterDraft(key);
    if (parameterDraftsRef.current[key]?.error) return; // read-only while the JSON is broken
    commitWorkflowWith((latest) => {
      const nodesNow = latest.nodes ?? [];
      const index = nodesNow.findIndex((node, nodeIndex) => nodeKey(node, nodeIndex) === key);
      if (index < 0) return latest;
      const node = nodesNow[index];
      const { node: next, applied } = reduceNodePatches(node, patches, formSchemaForNode(nodeTypesRef.current, node));
      if (applied.length === 0 || next === node) return latest;
      return { ...latest, nodes: nodesNow.map((candidate, candidateIndex) => (candidateIndex === index ? next : candidate)) };
    });
  }, [commitParameterDraft, commitWorkflowWith]);

  const [paramIssues, setParamIssues] = React.useState<ParamIssue[]>();
  const externalIssuesMap = React.useMemo(() => externalIssuesByNode(paramIssues), [paramIssues]);
  const [parameterTab, setParameterTab] = React.useState<ParameterTab>("form");
  const [inspectorRevealNonce, setInspectorRevealNonce] = React.useState(0);
  /** Latest 定位字段 request for the Inspector's parameter form (P1-c S5). */
  const [inspectorFocusRequest, setInspectorFocusRequest] = React.useState<FocusRequest>();
  const renameRef = React.useRef<((requestedName: string) => void) | undefined>(undefined);
  const formHost = React.useMemo<NodeFormHost>(() => {
    const registry = createNodeFormRegistry({ onRename: (name) => renameRef.current?.(name) });
    return { registry, compiler: new NodeFormCompiler(registry), renameRef };
  }, []);
  const validationMode = nodeTypes?.param_validation_mode ?? "warn";
  // Frontend node-form errors over the whole workflow (Doc C §1 rule 3): shown
  // on the save button in every mode, blocking only in enforce mode.
  const formIssueSummary = React.useMemo<FormIssueSummary>(() => {
    const summary: FormIssueSummary = { errors: 0, nodes: [] };
    if (!nodeTypes) return summary;
    const credentials = Object.keys(draftWorkflow.credentials ?? {});
    (draftWorkflow.nodes ?? []).forEach((node, index) => {
      const schema = formSchemaForNode(nodeTypes, node);
      if (!schema) return;
      const issues = formHost.compiler.issues(
        schema,
        node,
        { hasTemplate: nodeHasTemplate(draftWorkflow, node), parameters: node.parameters, schemasLoaded: true },
        { ports: portsForNode(schema, node), credentials }
      );
      const errors = issues.filter((issue) => issue.severity === "error").length;
      if (errors === 0) return;
      summary.errors += errors;
      summary.nodes.push(nodeName(node, index));
    });
    return summary;
  }, [draftWorkflow, formHost, nodeTypes]);
  const formIssueSummaryRef = React.useRef(formIssueSummary);
  formIssueSummaryRef.current = formIssueSummary;
  const [blockedDraftNodes, setBlockedDraftNodes] = React.useState<string[]>([]);
  const invalidDraftCount = Object.values(parameterDrafts).filter((draft) => draft.error).length;
  const invalidWorkflowJsonFields = (Object.keys(workflowJsonLabels) as WorkflowJsonField[])
    .filter((field) => workflowJsonDrafts[field]?.error);
  // 没有未保存修改时保存不可用 (baseline: `publish.tsx` clean state). A definition
  // that was never persisted reads 未保存 even at zero changes, and a pending
  // parameters draft (debounced, or unparseable) is an edit the count has not
  // seen yet, so neither counts as clean.
  const saveClean = versionChip.tone === "success"
    && Object.keys(parameterDrafts).length === 0
    && invalidWorkflowJsonFields.length === 0;
  const saveBlocked = invalidDraftCount > 0 || invalidWorkflowJsonFields.length > 0
    || (validationMode === "enforce" && formIssueSummary.errors > 0);
  const saveTooltip = !onSave
    ? "保存不可用：宿主未提供 onSave 处理器"
    : saveClean
      ? "没有未保存的修改"
    : invalidDraftCount > 0
      ? `${invalidDraftCount} 个节点的参数 JSON 未通过解析，修正后才能保存`
      : invalidWorkflowJsonFields.length > 0
        ? `工作流 ${invalidWorkflowJsonFields.map((field) => workflowJsonLabels[field]).join("、")} JSON 未通过解析，修正后才能保存`
      : formIssueSummary.errors > 0
        ? validationMode === "enforce"
          ? `${formIssueSummary.errors} 个参数错误（server 为 enforce 模式），修正后才能保存`
          : `${formIssueSummary.errors} 个参数问题（server 为 ${validationMode} 模式，不阻止保存）`
        : "保存工作流（⌘ / Ctrl + S）";

  /**
   * Save/run gate. Flushes parseable JSON drafts first, then blocks while any
   * live node holds an unparseable one (every mode: a local parse failure,
   * Doc C §6.2) or, in enforce mode, while node forms report errors (§1 rule 3).
   */
  const blockedByParameterDraft = React.useCallback((action: "保存" | "运行" | "发布"): boolean => {
    flushParameterDrafts();
    const nodesNow = latestDraftRef.current.nodes ?? [];
    const blocked = nodesNow
      .map((node, index) => ({ key: nodeKey(node, index), name: nodeName(node, index) }))
      .filter(({ key }) => parameterDraftsRef.current[key]?.error)
      .map(({ name }) => name);
    if (blocked.length > 0) {
      setBlockedDraftNodes(blocked);
      setOperationError(`节点 ${blocked.join("、")} 的参数 JSON 未通过解析，请修正后再${action}`);
      setOperationStatus(`${action}已阻止`);
      revealDock();
      return true;
    }
    setBlockedDraftNodes([]);
    // An unparseable workflow-level field was never committed: saving now
    // would silently persist its previous value.
    const blockedFields = (Object.keys(workflowJsonLabels) as WorkflowJsonField[])
      .filter((field) => workflowJsonDraftsRef.current[field]?.error)
      .map((field) => workflowJsonLabels[field]);
    if (blockedFields.length > 0) {
      setOperationError(`工作流 ${blockedFields.join("、")} JSON 未通过解析，请修正后再${action}`);
      setOperationStatus(`${action}已阻止`);
      revealDock();
      return true;
    }
    const summary = formIssueSummaryRef.current;
    if (nodeTypesRef.current?.param_validation_mode === "enforce" && summary.errors > 0) {
      setOperationError(`节点 ${summary.nodes.join("、")} 的参数未通过校验（${summary.errors} 个错误），请修正后再${action}`);
      setOperationStatus(`${action}已阻止`);
      revealDock();
      return true;
    }
    return false;
  }, [flushParameterDrafts]);
  const restoreHistory = React.useCallback((nextWorkflow: WorkflowDef, nextHistory: WorkflowHistory) => {
    historyRef.current = nextHistory;
    setHistory(nextHistory);
    draftRevisionRef.current += 1;
    latestDraftRef.current = nextWorkflow;
    setDraftWorkflow(nextWorkflow);
    setOperationStatus("未保存");
    setOperationError(undefined);
    onChange?.(nextWorkflow);
  }, [onChange]);

  // Undo/redo first land a pending parseable draft as its own undo entry, but
  // without clearing redo: the flush only records text the user already typed,
  // it is not a new branch point. Redo then still applies, and one undo after
  // it gets the drafted text back.
  const undoWorkflow = React.useCallback(() => {
    // Read-only (a missing workflow capability, or the preview view) must not
    // reach onChange through history: the controls are hidden, but a shortcut
    // or a stale toolbar click would still rewrite the definition.
    if (!editMode) return;
    flushParameterDrafts(true);
    const previousWorkflow = historyRef.current.undo.at(-1);
    if (!previousWorkflow) return;
    restoreHistory(previousWorkflow, {
      undo: historyRef.current.undo.slice(0, -1),
      redo: [...historyRef.current.redo, latestDraftRef.current].slice(-historyLimit)
    });
  }, [editMode, flushParameterDrafts, restoreHistory]);

  const redoWorkflow = React.useCallback(() => {
    if (!editMode) return;
    flushParameterDrafts(true);
    const nextWorkflow = historyRef.current.redo.at(-1);
    if (!nextWorkflow) return;
    restoreHistory(nextWorkflow, {
      undo: [...historyRef.current.undo, latestDraftRef.current].slice(-historyLimit),
      redo: historyRef.current.redo.slice(0, -1)
    });
  }, [editMode, flushParameterDrafts, restoreHistory]);

  const createWorkflow = React.useCallback(() => {
    const nextWorkflow = createDraftWorkflow();
    commitWorkflow(nextWorkflow);
    // A new draft is a new unsaved document (§6.3): its own definition is the
    // baseline, so the chip reads 未保存 (N=0) instead of counting the replaced
    // workflow's nodes as deletions.
    setBaselineWorkflow(nextWorkflow);
    setSelectedKey(undefined);
  }, [commitWorkflow]);

  // 运行 ▾ is a placeholder until P3: one disabled item, no behaviour to hang.
  const runOptionItems: MenuProps["items"] = [
    { key: "more-run", label: "更多运行方式（未接入）", disabled: true }
  ];

  // Computed once and shared by the Diagnostics panel and the validate button,
  // so the button's verdict always matches the panel's findings. param_issues
  // join as area="parameters" rows, each carrying its node and field pointer.
  const diagnostics = React.useMemo(
    () => [...validateWorkflow(draftWorkflow, operationError, nodeTypes), ...paramIssueDiagnostics(paramIssues)],
    [draftWorkflow, nodeTypes, operationError, paramIssues]
  );
  // §6.10 publish gate: unparseable drafts and diagnostics errors block; node
  // form findings are errors only where the server enforces them (everywhere
  // else they are the warnings the dialog says may be published).
  const publishDiagnostics = React.useMemo(() => {
    const enforce = validationMode === "enforce";
    // `area: "operation"` rows are the last failed save / run / rollback, not
    // findings about this definition (validateWorkflow appends them). Counting
    // them here would disable publish after an unrelated operation failed and
    // only an edit that clears `operationError` would re-enable it.
    const definitionErrors = diagnostics.filter(
      (entry) => entry.status === "error" && entry.area !== "operation"
    ).length;
    return {
      errors: invalidDraftCount
        + invalidWorkflowJsonFields.length
        + definitionErrors
        + (enforce ? formIssueSummary.errors : 0),
      warnings: diagnostics.filter((entry) => entry.status === "warn").length
        + (enforce ? 0 : formIssueSummary.errors)
    };
  }, [diagnostics, formIssueSummary, invalidDraftCount, invalidWorkflowJsonFields.length, validationMode]);
  const validateCurrentWorkflow = React.useCallback(() => {
    const hasErrors = diagnostics.some((item) => item.status === "error");
    setOperationStatus(hasErrors ? "校验失败" : "校验通过");
    revealDock();
  }, [diagnostics]);

  // §6.3 compact top bar (layout b/c): 撤销 / 重做 / 校验 / 主题 join the ⋯ menu
  // in that order, keeping each button's disabled state and shortcut hint. The
  // menu itself never moves: it sits just before 保存 at every width.
  // rc-menu owns the item role and exposes no aria-checked, so the active row
  // states the selection in its accessible name. Every row keeps an icon slot
  // (hidden when inactive) so the three labels share one text column.
  const themeMenuItem = (key: string, label: string, value: XFlowEditorAppearance) => {
    const active = activeAppearance === value;
    return {
      key,
      label: (
        <>
          {label}
          {active ? <span className="xflow-editor-sr-only">（当前）</span> : null}
        </>
      ),
      icon: (
        <CheckOutlined
          aria-hidden="true"
          className={active ? undefined : "xflow-editor-menu-check-inactive"}
        />
      )
    };
  };
  const banners = editorBanners({
    nodeTypesError,
    onRetryNodeTypes,
    saveConflict,
    onReloadLatest,
    onSaveAsCopy: onSaveAsCopy ? () => onSaveAsCopy(latestDraftRef.current) : undefined,
    readOnlyReason: readOnlyBannerReason,
    historicalVersion,
    onExitHistory
  });
  const moreMenuItems: MenuProps["items"] = compactToolbar
    ? [
        { key: "undo", label: "撤销", title: "撤销（⌘ / Ctrl + Z）", disabled: !editMode || history.undo.length === 0 },
        { key: "redo", label: "重做", title: "重做（⌘ / Ctrl + Shift + Z）", disabled: !editMode || history.redo.length === 0 },
        {
          key: "validate",
          label: "校验",
          title: runsMode ? "运行记录模式不提供诊断台" : "校验当前工作流",
          // A replay has no draft and no diagnostics dock: the action would
          // change hidden state with no visible result.
          disabled: runsMode
        },
        {
          type: "group",
          label: "主题",
          children: [
            themeMenuItem("theme-light", "浅色", "light"),
            themeMenuItem("theme-dark", "深色", "dark"),
            themeMenuItem("theme-system", "跟随系统", "system")
          ]
        },
        { type: "divider" },
        { key: "layout", label: layoutPolicy === "auto" ? "进入沉浸布局" : "恢复自动布局" },
        {
          key: "new",
          label: "新建工作流",
          disabled: !editMode,
          title: editMode ? undefined : editLockedTitle
        }
      ]
    : [
        { key: "layout", label: layoutPolicy === "auto" ? "进入沉浸布局" : "恢复自动布局" },
        {
          key: "new",
          label: "新建工作流",
          disabled: !editMode,
          title: editMode ? undefined : editLockedTitle
        }
      ];
  const handleMoreMenuClick: MenuProps["onClick"] = ({ key }) => {
    if (key === "undo") undoWorkflow();
    else if (key === "redo") redoWorkflow();
    else if (key === "validate") validateCurrentWorkflow();
    else if (key === "theme-light") changeAppearance("light");
    else if (key === "theme-dark") changeAppearance("dark");
    else if (key === "theme-system") changeAppearance("system");
    else if (key === "layout") {
      setLayoutPolicy((policy) => (policy === "auto" ? "compact" : "auto"));
    } else if (key === "new" && editMode) {
      createWorkflow();
    }
  };

  const libraryDescriptors = React.useMemo(() => nodeLibraryDescriptors(nodeTypes), [nodeTypes]);
  // Doc C §4.3: the canvas marks edges of removed dynamic ports; they are never deleted.
  const canvasDanglingPorts = React.useMemo(
    () => danglingPorts(draftWorkflow, nodeTypes, canvasOutputPorts).map((edge) => ({ source: edge.source, sourcePort: edge.port })),
    [draftWorkflow, nodeTypes]
  );

  const addNode = React.useCallback(
    (descriptor: NodeDescriptor, position?: { x: number; y: number }) => {
      const nextNode = createNodeFromDescriptor(draftWorkflow, descriptor, position);
      const withNode: WorkflowDef = {
        ...draftWorkflow,
        nodes: [...(draftWorkflow.nodes ?? []), nextNode]
      };
      const connectedWorkflow = connectSelectedNode(withNode, selectedNode, nextNode);
      commitWorkflow(connectedWorkflow);
      setSelectedKey(nodeKey(nextNode, (connectedWorkflow.nodes ?? []).length - 1));
    },
    [commitWorkflow, draftWorkflow, selectedNode]
  );

  const dropNode = React.useCallback(
    (type: string, position: { x: number; y: number }) => {
      // The payload crosses a drag boundary, so an unknown type is a real input
      // case rather than a programming error.
      const descriptor = libraryDescriptors.find((candidate) => candidate.type === type);
      if (!descriptor) return;
      addNode(descriptor, position);
    },
    [addNode, libraryDescriptors]
  );

  const updateNodePosition = React.useCallback(
    (nodeId: string, position: { x: number; y: number }) => {
      if (!editMode) return;
      const nextWorkflow: WorkflowDef = {
        ...draftWorkflow,
        nodes: (draftWorkflow.nodes ?? []).map((node, index) =>
          nodeKey(node, index) === nodeId ? { ...node, position } : node
        )
      };
      commitWorkflow(nextWorkflow);
    },
    [commitWorkflow, draftWorkflow, editMode]
  );

  const deleteSelectedNode = React.useCallback(() => {
    if (!selectedNode) return;
    const nextWorkflow = removeNodeFromWorkflow(draftWorkflow, selectedNode);
    commitWorkflow(nextWorkflow);
    setSelectedKey(defaultSelectedKey(nextWorkflow.nodes ?? []));
  }, [commitWorkflow, draftWorkflow, selectedNode]);

  const duplicateSelectedNode = React.useCallback(() => {
    if (!editMode || !selectedNode) return;
    const { workflow: nextWorkflow, node } = duplicateNodeInWorkflow(draftWorkflow, selectedNode, selectedIndex);
    commitWorkflow(nextWorkflow);
    setSelectedKey(nodeKey(node, (nextWorkflow.nodes ?? []).length - 1));
  }, [commitWorkflow, draftWorkflow, editMode, selectedIndex, selectedNode]);

  // A compact left panel is a rail, not a collapsible column, so the same
  // intent opens or closes the drawer instead.
  const toggleLeftPanel = React.useCallback(() => {
    if (layout === "a") {
      setLeftCollapsed((current) => !current);
      return;
    }
    setCompactDrawer((current) => (current === "left" ? undefined : "left"));
  }, [layout]);

  // The right panel collapses only on the desktop layout; below that it is a
  // docked column in "b" and a drawer in "c", so only "c" has a drawer to toggle.
  const toggleRightPanel = React.useCallback(() => {
    if (layout === "a") {
      setRightCollapsed((current) => !current);
      return;
    }
    if (layout === "c") {
      setCompactDrawer((current) => (current === "right" ? undefined : "right"));
    }
  }, [layout]);

  /**
   * Desktop rail section keys (§6.0): selecting the section that is already on
   * screen collapses the column; any other key expands it and switches section.
   */
  const selectLeftSection = React.useCallback(
    (section: "library" | "variables") => {
      if (layout !== "a") {
        setLeftDrawerTab(section);
        setCompactDrawer("left");
        return;
      }
      if (leftCollapsed) {
        setLeftCollapsed(false);
        setDesktopRailSection(section);
        return;
      }
      // Compare the section actually on screen.
      if (leftSection === section) {
        setLeftCollapsed(true);
        return;
      }
      setDesktopRailSection(section);
    },
    [layout, leftSection, leftCollapsed]
  );

  // Escape leaves a transient mode first (dialog, drawer, connect tool); with
  // none open it deselects, like a click on the empty canvas, which brings the
  // workflow settings back. Text entry never gets here (the shortcut is canvas-scoped).
  const leaveTransientMode = React.useCallback(() => {
    if (shortcutsOpen) {
      setShortcutsOpen(false);
      return;
    }
    if (compactDrawer) {
      setCompactDrawer(undefined);
      return;
    }
    if (canvasTool === "connect") {
      setCanvasTool("select");
      return;
    }
    if (nodeSearchOpen) return;
    setSelectedKey(undefined);
  }, [canvasTool, compactDrawer, nodeSearchOpen, shortcutsOpen]);

  const addCanvasConnection = React.useCallback(
    (connection: PreviewConnection) => {
      if (!editMode) return;
      const sourceNode = workflowNodeByName(draftWorkflow, connection.source);
      if (!sourceNode) return;
      const nextWorkflow = addConnection(
        draftWorkflow,
        sourceNode,
        connection.target,
        connection.sourcePort || "main",
        connection.targetPort || "main"
      );
      if (nextWorkflow !== draftWorkflow) {
        commitWorkflow(nextWorkflow);
      }
    },
    [commitWorkflow, draftWorkflow, editMode]
  );

  const removeCanvasConnection = React.useCallback(
    (connection: PreviewConnection) => {
      if (!editMode) return;
      const nextWorkflow = removeConnectionFromWorkflow(draftWorkflow, {
        sourceName: connection.source,
        sourcePort: connection.sourcePort || "main",
        targetName: connection.target,
        targetPort: connection.targetPort || "main"
      });
      if (nextWorkflow !== draftWorkflow) {
        commitWorkflow(nextWorkflow);
      }
    },
    [commitWorkflow, draftWorkflow, editMode]
  );

  const saveWorkflow = React.useCallback(async () => {
    if (readOnly) return;
    if (!onSave) {
      setOperationError("保存不可用：宿主未提供 onSave 处理器");
      setOperationStatus("保存不可用");
      revealDock();
      return;
    }
    if (savingRef.current) return;
    if (blockedByParameterDraft("保存")) return;
    // The ref, not render state: it already includes writes committed earlier
    // in this tick (the shortcut can fire from inside a text field).
    const workflowToSave = latestDraftRef.current;
    const revisionAtSave = draftRevisionRef.current;
    const valueEpochAtSave = valueEpochRef.current;
    savingRef.current = true;
    setSaving(true);
    setOperationError(undefined);
    try {
      const saved = await onSave(workflowToSave);
      const savedWorkflow = isSaveResult(saved) ? saved.workflow : saved;
      if (!savedWorkflow) throw new Error("保存处理器未返回工作流定义");
      // Server findings describe the snapshot just saved; show them even if
      // the user kept editing meanwhile (they are replaced on the next save).
      setParamIssues(isSaveResult(saved) && saved.paramIssues?.length ? saved.paramIssues : undefined);
      // A response for an older snapshot must never overwrite edits made while
      // the save was in flight, but the persisted definition is still the new
      // baseline those edits are measured against.
      setBaselineWorkflow(savedWorkflow);
      // The save went through, so the version race is over even when edits made
      // meanwhile keep the snapshot below from replacing the draft.
      setSaveConflict(false);
      if (draftRevisionRef.current !== revisionAtSave) return;
      latestDraftRef.current = savedWorkflow;
      setDraftWorkflow(savedWorkflow);
      setOperationStatus("已保存");
    } catch (error) {
      // Raised by a conflict and kept until a save succeeds or the host replaces
      // `value`: another kind of failure says nothing about the stale version.
      // A conflict answering a save the host has since replaced `value` under is
      // about a definition that is gone, so it raises nothing.
      if (isWorkflowConflictError(error) && valueEpochRef.current === valueEpochAtSave) {
        setSaveConflict(true);
        // The banner is the way out; an expanded drawer would cover and inert it.
        setDock((current) => (current === "expanded" ? "docked" : current));
      }
      const rejectedIssues = paramIssuesOf(error);
      if (rejectedIssues) setParamIssues(rejectedIssues.length > 0 ? rejectedIssues : undefined);
      if (draftRevisionRef.current === revisionAtSave) {
        // A conflict's server message is an internal string; the issue row
        // repeats the banner's wording instead.
        setOperationError(isWorkflowConflictError(error) ? "保存失败：工作流已被更新" : errorMessage(error, "保存失败"));
        setOperationStatus("保存失败");
        revealDock();
      }
      // The host application owns error presentation.
    } finally {
      savingRef.current = false;
      setSaving(false);
    }
  }, [blockedByParameterDraft, onSave, readOnly]);

  // §6.10 versions. The current row is the online version; before the list
  // answers, the baseline definition's own label is the fallback (the hash is
  // only ever readable from the list, never from GET /workflows/{id}).
  const onlineVersionRow = versionsState.data?.list.find((row) => row.current);
  const onlineVersion = onlineVersionRow?.version ?? baselineWorkflow.version;
  const suggestedVersion = React.useMemo(() => suggestNextVersion(onlineVersion), [onlineVersion]);
  const usedVersions = React.useMemo(() => {
    const labels = new Set<string>();
    (versionsState.data?.list ?? []).forEach((row) => labels.add(bareVersion(row.version)));
    if (baselineWorkflow.version) labels.add(bareVersion(baselineWorkflow.version));
    return Array.from(labels);
  }, [baselineWorkflow.version, versionsState.data]);
  const versionsVisible = versionsOpen || publishOpen;
  // A refused rollback is a property of a version label under one workflow id;
  // the same label under the next workflow must start clean. Only an id swap
  // resets these — a same-id value replacement (save / rollback refresh) keeps
  // the list the panel just loaded.
  const versionScopeRef = React.useRef(value.id);
  React.useEffect(() => {
    if (versionScopeRef.current === value.id) return;
    versionScopeRef.current = value.id;
    setBlockedVersions({});
    setVersionsState({});
  }, [value.id]);
  const loadVersionsRef = React.useRef(onLoadVersions);
  loadVersionsRef.current = onLoadVersions;
  React.useEffect(() => {
    const load = loadVersionsRef.current;
    if (!load || !versionsVisible) return undefined;
    let cancelled = false;
    setVersionsState((current) => ({ ...current, error: undefined, loading: true }));
    Promise.resolve(load()).then(
      (data) => {
        if (!cancelled) setVersionsState({ data });
      },
      (error: unknown) => {
        if (!cancelled) setVersionsState({ error: errorMessage(error, "版本历史加载失败") });
      }
    );
    return () => {
      cancelled = true;
    };
    // `value.id` joins the key so an open panel reloads for the new workflow and
    // the cleanup discards a response that belongs to the previous one.
  }, [value.id, versionLoadNonce, versionsVisible]);
  // The field starts at the patch+1 suggestion and follows a later load only
  // while it still shows the previous suggestion — never a user-typed label.
  React.useEffect(() => {
    if (!publishOpen) return;
    const previous = publishSuggestedRef.current;
    publishSuggestedRef.current = suggestedVersion;
    setPublishVersion((current) => (current === "" || current === previous ? suggestedVersion : current));
  }, [publishOpen, suggestedVersion]);

  const openPublishDialog = React.useCallback(() => {
    const initial = suggestNextVersion(onlineVersion);
    publishSuggestedRef.current = initial;
    setPublishVersion(initial);
    setPublishError(undefined);
    setPublishOpen(true);
  }, [onlineVersion]);

  const publishWorkflow = React.useCallback(async () => {
    if (readOnly || !onPublish || publishingRef.current) return;
    const version = publishVersion.trim();
    if (!version) {
      setPublishError("请填写新版本号。");
      return;
    }
    if (blockedByParameterDraft("发布")) {
      setPublishError("参数未通过校验，修正后再发布。");
      return;
    }
    const next = { ...latestDraftRef.current, version };
    const revisionAtPublish = draftRevisionRef.current;
    const valueEpochAtPublish = valueEpochRef.current;
    publishingRef.current = true;
    setPublishing(true);
    setPublishError(undefined);
    try {
      const saved = await onPublish(next);
      const savedWorkflow = isSaveResult(saved) ? saved.workflow : saved;
      if (!savedWorkflow) throw new Error("发布处理器未返回工作流定义");
      setParamIssues(isSaveResult(saved) && saved.paramIssues?.length ? saved.paramIssues : undefined);
      setBaselineWorkflow(savedWorkflow);
      setSaveConflict(false);
      if (draftRevisionRef.current === revisionAtPublish) {
        latestDraftRef.current = savedWorkflow;
        setDraftWorkflow(savedWorkflow);
      }
      setOperationError(undefined);
      setOperationStatus(`已发布 ${versionLabel(version)}`);
      setPublishOpen(false);
      // The list this session loaded predates the new version.
      setVersionsState({});
    } catch (error) {
      // §6.10: a publish conflict is the save conflict (§6.5.1), so the banner
      // with 重新加载 / 另存为副本 comes up behind the dialog — unless the host
      // has replaced `value` since, in which case that definition is gone.
      if (isWorkflowConflictError(error) && valueEpochRef.current === valueEpochAtPublish) {
        setSaveConflict(true);
        setDock((current) => (current === "expanded" ? "docked" : current));
      }
      if (draftRevisionRef.current === revisionAtPublish) {
        setOperationStatus("发布失败");
        revealDock();
      }
      const rejectedIssues = paramIssuesOf(error);
      if (rejectedIssues) setParamIssues(rejectedIssues.length > 0 ? rejectedIssues : undefined);
      setPublishError(publishErrorText(error));
    } finally {
      publishingRef.current = false;
      setPublishing(false);
    }
  }, [blockedByParameterDraft, onPublish, publishVersion, readOnly]);

  const rollbackToVersion = React.useCallback(async (version: string) => {
    if (!canWriteWorkflow || !onRollback || rollingBackRef.current) return;
    rollingBackRef.current = true;
    setRollingBackVersion(version);
    setOperationError(undefined);
    try {
      const result = await onRollback(version);
      const installed = result?.version ?? version;
      setOperationStatus(result?.unchanged ? `${versionLabel(installed)} 已是线上版本` : `已回滚到 ${versionLabel(installed)}`);
      setVersionsOpen(false);
      setVersionsState({});
      setVersionLoadNonce((nonce) => nonce + 1);
    } catch (error) {
      // A version the compiler cannot reproduce is a property of that row, so
      // the reason stays on it; every other failure is reported once.
      const reason = versionUnavailableReason(error);
      if (reason) {
        setBlockedVersions((current) => ({ ...current, [version]: reason }));
        setOperationError(reason);
      } else {
        setOperationError(rollbackErrorText(error));
      }
      setOperationStatus("回滚失败");
      revealDock();
    } finally {
      rollingBackRef.current = false;
      setRollingBackVersion(undefined);
    }
  }, [canWriteWorkflow, onRollback, revealDock]);

  const viewVersion = React.useCallback((version: string) => {
    setVersionsOpen(false);
    onViewVersion?.(version);
  }, [onViewVersion]);

  const runWorkflow = React.useCallback(async () => {
    if (readOnly) return;
    if (!onRun) {
      setOperationError("运行不可用：宿主未提供 onRun 处理器");
      setOperationStatus("运行不可用");
      revealDock();
      return;
    }
    if (blockedByParameterDraft("运行")) return;
    setRunning(true);
    setOperationError(undefined);
    try {
      const nextRuntime = await onRun(latestDraftRef.current);
      if (!nextRuntime) throw new Error("运行处理器未返回运行状态快照");
      setLocalRuntime(nextRuntime);
      setOperationStatus(operationStatusFromRuntime(nextRuntime));
      revealDock();
    } catch (error) {
      setOperationError(errorMessage(error, "运行失败"));
      setOperationStatus("运行失败");
      revealDock();
      // The host application owns error presentation.
    } finally {
      setRunning(false);
    }
  }, [blockedByParameterDraft, onRun, readOnly]);

  /**
   * §6.3 run-bar actions. The host owns the request; the editor only reports a
   * rejection in its dock (a host 403 toast and capability refresh happen on
   * the host side before this promise settles). The refreshed execution comes
   * back through the `execution` prop, never through a local guess.
   */
  const runExecutionAction = React.useCallback(
    async (executionId: string, action: ExecutionControlAction) => {
      if (!onExecutionAction) return;
      // Clear the previous attempt first: a rejected pause must not keep the
      // dock and status bar red after the next resume succeeds. The host owns
      // the refreshed detail, so the top bar reports the submission only.
      setOperationError(undefined);
      try {
        await onExecutionAction(executionId, action);
        setOperationStatus("操作已提交");
      } catch (error) {
        setOperationError(errorMessage(error, "执行操作失败"));
        setOperationStatus("操作失败");
        revealDock();
      }
    },
    [onExecutionAction, revealDock]
  );

  /**
   * The run bar's 等待审批 count opens the dialog (§6.12). The count names the
   * first suspended node, which need not be the approval: when it is not, the
   * first suspended approval of the same execution is the one to handle.
   */
  const handleRunBarApproval = React.useCallback((suspendedName: string) => {
    if (!firstApprovalNode) return;
    const def = workflowNodeByName(latestDraftRef.current, suspendedName);
    openApprovalFor(def?.type === APPROVAL_NODE_TYPE ? suspendedName : firstApprovalNode);
  }, [firstApprovalNode, openApprovalFor]);

  /** The canvas note's 处理 action; notes exist only for approval nodes. */
  const handlePreviewNodeAction = React.useCallback((approvalNodeName: string) => {
    openApprovalFor(approvalNodeName);
  }, [openApprovalFor]);

  // The bar's natural width (its id text ellipsizes when clamped, so the hidden
  // remainder is scrollWidth - clientWidth). If centring that width would reach
  // either of the preview's top floats, drop the row one float row: §6.3 keeps
  // the bar centred, and the plan forbids covering 添加节点 / 定位节点. The
  // editor width arrives through `editorWidth`, which re-renders on resize.
  React.useLayoutEffect(() => {
    const row = runBarRowRef.current;
    const bar = row?.querySelector<HTMLElement>(".xflow-editor-runbar");
    if (!execution || !row || !bar || editorWidth <= 0) {
      setRunBarStacked(false);
      return;
    }
    const canvas = canvasFrameRef.current;
    const canvasRect = canvas?.getBoundingClientRect();
    if (!canvas || !canvasRect || canvasRect.width === 0) return;
    const idText = bar.querySelector<HTMLElement>(".xflow-editor-runbar__id-text");
    const hiddenId = idText ? Math.max(0, idText.scrollWidth - idText.clientWidth) : 0;
    const natural = bar.clientWidth + hiddenId;
    const start = canvasRect.left + (canvasRect.width - natural) / 2;
    const end = start + natural;
    const root = editorRootRef.current;
    const leftFloat = root?.querySelector<HTMLElement>(
      ".xflow-preview-canvas-float:not(.xflow-preview-canvas-float--end)"
    );
    const rightFloat = root?.querySelector<HTMLElement>(".xflow-preview-canvas-float--end");
    setRunBarStacked(Boolean(
      (leftFloat && leftFloat.getBoundingClientRect().right + 8 > start) ||
      (rightFloat && rightFloat.getBoundingClientRect().left - 8 < end)
    ));
  }, [editorWidth, execution]);

  React.useEffect(() => {
    if (nodes.length === 0) {
      setSelectedKey(undefined);
      return;
    }
    // No selection is a real state (the Inspector shows the workflow settings);
    // only a key whose node is gone falls back to a default.
    if (selectedKey !== undefined && !nodes.some((node, index) => nodeKey(node, index) === selectedKey)) {
      setSelectedKey(defaultSelectedKey(nodes));
    }
  }, [nodes, selectedKey]);

  React.useEffect(() => {
    // Drop drawers the new layout cannot show: the left drawer exists in "b"
    // and "c", the right one only in "c".
    setCompactDrawer((current) => {
      if (current === "left" && layout === "a") return undefined;
      if (current === "right" && layout !== "c") return undefined;
      return current;
    });
  }, [layout]);

  const focusNodeLibrarySearch = React.useCallback(() => {
    nodeLibrarySearchRef.current?.focus({ preventScroll: true });
  }, []);

  React.useEffect(() => {
    if (layout !== "a" && compactDrawer === "left" && leftDrawerTab === "library" && editMode) {
      focusNodeLibrarySearch();
    }
  }, [compactDrawer, editMode, focusNodeLibrarySearch, layout, leftDrawerTab]);

  const selectNodeByName = React.useCallback((name: string) => {
    const index = nodes.findIndex((node, nodeIndex) => nodeName(node, nodeIndex) === name);
    if (index >= 0) setSelectedKey(nodeKey(nodes[index], index));
  }, [nodes]);

  /**
   * The diagnostics' 定位节点: select the node, pan the canvas to it and hand
   * the keyboard to the canvas. The inspector is left as the user has it (same
   * rule as the node search). The preview already follows a selection that is
   * off screen; `fitNode` also covers the one that is on screen, and both land
   * on the same viewport, so a node is never panned twice to two places.
   */
  const focusLocatedNode = React.useCallback((key: string) => {
    const card = document.querySelector<HTMLElement>(
      `.xflow-editor .react-flow__node[data-id="${key.replace(/["\\]/g, "\\$&")}"] .xflow-preview-node__select`
    );
    (card ?? canvasFrameRef.current)?.focus({ preventScroll: true });
  }, []);
  // The workbench is inert while the drawer is expanded, and inert elements
  // cannot take focus: a locate from an expanded drawer finishes the focus
  // hand-over in the commit that restores the drawer.
  const pendingLocateFocusRef = React.useRef<string | undefined>(undefined);
  React.useLayoutEffect(() => {
    const key = pendingLocateFocusRef.current;
    if (key === undefined) return;
    pendingLocateFocusRef.current = undefined;
    focusLocatedNode(key);
  }, [dock, focusLocatedNode]);
  const locateDiagnosticNode = React.useCallback((name: string) => {
    const index = nodes.findIndex((node, nodeIndex) => nodeName(node, nodeIndex) === name);
    if (index < 0) return;
    const key = nodeKey(nodes[index], index);
    setSelectedKey(key);
    previewHandleRef.current?.fitNode(key);
    if (dock === "expanded") pendingLocateFocusRef.current = key;
    else focusLocatedNode(key);
  }, [dock, focusLocatedNode, nodes]);

  /**
   * The run bar's 定位: the execution snapshot names nodes by name, while the
   * canvas selects by key (`node.id ?? node.name`). Resolve the name first,
   * then select and pan — the diagnostics' locate path without the keyboard
   * hand-over, because the click came from a top float, not from the drawer.
   */
  const locateRuntimeNode = React.useCallback((name: string) => {
    const index = nodes.findIndex((node, nodeIndex) => nodeName(node, nodeIndex) === name);
    if (index < 0) return;
    const key = nodeKey(nodes[index], index);
    setSelectedKey(key);
    previewHandleRef.current?.fitNode(key);
    setDock((current) => (current === "expanded" ? "docked" : current));
  }, [nodes]);

  const openCompactDrawer = React.useCallback((drawer: "left" | "right") => {
    setCompactDrawer(drawer);
  }, []);

  /**
   * §6.8: the execution list is the replay's only left-column content, so every
   * entry has to prepare the column for it — a collapsed desktop column would
   * hide the list with no surface left to reopen it, and a compact drawer left
   * on 节点与大纲 would show the draft instead.
   */
  const enterRuns = React.useCallback(() => {
    changeMode("runs");
    if (layout === "a") {
      setLeftCollapsed(false);
      return;
    }
    setLeftDrawerTab("executions");
    openCompactDrawer("left");
  }, [changeMode, layout, openCompactDrawer]);

  /** Doc C §6.2: from a blocked save/run, jump to the node's parameters JSON. */
  const revealParameterJson = React.useCallback((name: string) => {
    selectNodeByName(name);
    changeMode("edit");
    setParameterTab("json");
    setInspectorRevealNonce((nonce) => nonce + 1);
    // Only the desktop flag is persisted: "b" docks the inspector and "c"
    // opens its drawer, so neither may rewrite the desktop preference.
    if (layout === "a") setRightCollapsed(false);
    else if (layout === "c") openCompactDrawer("right");
  }, [changeMode, layout, openCompactDrawer, selectNodeByName]);

  // Nonces stay monotonic across handled requests: the editor clears the
  // request once the Composer reports it, and a next nonce derived from the
  // (cleared) state could repeat the last one and be de-duplicated away.
  const focusRequestNonceRef = React.useRef(0);
  const handleFocusRequestHandled = React.useCallback((nonce: number) => {
    setInspectorFocusRequest((current) => (current && current.nonce === nonce ? undefined : current));
  }, []);

  /** The diagnostics' 定位字段 (P1-c S5): node → parameters form → focus the field. */
  const locateParameterField = React.useCallback((name: string, path: string) => {
    selectNodeByName(name);
    changeMode("edit");
    setParameterTab("form");
    focusRequestNonceRef.current += 1;
    setInspectorFocusRequest({ path, nonce: focusRequestNonceRef.current });
    setInspectorRevealNonce((nonce) => nonce + 1);
    // Only the desktop flag is persisted: "b" docks the inspector and "c"
    // opens its drawer, so neither may rewrite the desktop preference.
    if (layout === "a") setRightCollapsed(false);
    else if (layout === "c") openCompactDrawer("right");
  }, [changeMode, layout, openCompactDrawer, selectNodeByName]);

  /** §6.1.1: pressing a card-summary row selects the node and reveals that field. */
  const locateSummaryField = React.useCallback((nodeId: string, path: string) => {
    const nodes = draftWorkflow.nodes ?? [];
    const index = nodes.findIndex((candidate, at) => nodeKey(candidate, at) === nodeId);
    if (index < 0) return;
    locateParameterField(nodeName(nodes[index], index), path);
  }, [draftWorkflow.nodes, locateParameterField]);

  const openNodeLibrary = React.useCallback(() => {
    changeMode("edit");
    setDesktopRailSection("library");
    if (layout === "a") {
      setLeftCollapsed(false);
      return;
    }
    // The compact drawer uses its own state; the persisted desktop flag stays
    // as the user left it.
    setLeftDrawerTab("library");
    openCompactDrawer("left");
  }, [changeMode, layout, openCompactDrawer]);

  const focusLibrarySearchFromKey = React.useCallback(() => {
    openNodeLibrary();
    setLibrarySearchFocusNonce((nonce) => nonce + 1);
  }, [openNodeLibrary]);
  React.useEffect(() => {
    if (librarySearchFocusNonce > 0) focusNodeLibrarySearch();
  }, [focusNodeLibrarySearch, librarySearchFocusNonce]);

  // The replay's ⌘K locates inside the snapshot: the draft's node set can
  // differ from the one on screen (§6.8), so searching it would list nodes that
  // are not drawn and miss nodes that are.
  const searchNodes = runsMode ? replayNodes : nodes;
  const nodeSearchOptions = React.useMemo<NodeSearchOption[]>(
    () => searchNodes.map((node, index) => ({
      key: nodeKey(node, index),
      name: nodeName(node, index),
      label: nodeLabel(node, index),
      type: node.type ?? "unknown"
    })),
    [searchNodes]
  );
  const openNodeSearch = React.useCallback((trigger?: HTMLElement) => {
    if (nodeSearchOpen || searchNodes.length === 0) return;
    // Remember who opened the panel so Esc (and a pick) returns focus there.
    locateTriggerRef.current = trigger ?? (document.activeElement instanceof HTMLElement ? document.activeElement : null);
    // Remount the search so a reopen never keeps the previous filter.
    setNodeSearchNonce((nonce) => nonce + 1);
    setNodeSearchOpen(true);
  }, [nodeSearchOpen, searchNodes.length]);
  const closeNodeSearch = React.useCallback(() => {
    setNodeSearchOpen(false);
    const trigger = locateTriggerRef.current;
    locateTriggerRef.current = null;
    // The dialog is still mounted while it animates closed; restore afterwards.
    window.setTimeout(() => trigger?.focus(), 0);
  }, []);
  // Node search is a canvas locate, not an intent to edit: selecting the node
  // updates the reopen chip's name but leaves a collapsed inspector closed.
  // An expanded drawer would cover the node it just found, so it steps down.
  const locateNode = React.useCallback((key: string) => {
    setSelectedKey(key);
    setDock((current) => (current === "expanded" ? "docked" : current));
  }, []);
  const clearSelection = React.useCallback(() => setSelectedKey(undefined), []);

  // ---- Canvas context menu -------------------------------------------------

  const [contextMenu, setContextMenu] = React.useState<ContextMenuRequest>();
  /**
   * Bumped to ask the Inspector to start its header rename. Kept until a
   * mounted Inspector consumes it: the same gesture may mount the panel first
   * (desktop re-expand, compact drawer), so a mount must still see the request.
   */
  const [nameFocusNonce, setNameFocusNonce] = React.useState<number | undefined>(undefined);
  const closeContextMenu = React.useCallback(() => setContextMenu(undefined), []);
  const handleNameFocusHandled = React.useCallback((nonce: number) => {
    setNameFocusNonce((current) => (current === nonce ? undefined : current));
  }, []);

  /** Right-click on a node: select it (the menu always acts on the node under the pointer). */
  const openNodeContextMenu = React.useCallback((nodeId: string, position: { clientX: number; clientY: number }) => {
    setSelectedKey(nodeId);
    setContextMenu({ kind: "node", nodeId, clientX: position.clientX, clientY: position.clientY });
  }, []);
  const openPaneContextMenu = React.useCallback(
    (position: { clientX: number; clientY: number; flowX: number; flowY: number }) => {
      setContextMenu({
        kind: "pane",
        clientX: position.clientX,
        clientY: position.clientY,
        flowX: position.flowX,
        flowY: position.flowY
      });
    },
    []
  );

  /** 打开属性: bring the Inspector forward for a node (drawer in the compact layout). */
  const openInspectorFor = React.useCallback((key: string) => {
    setSelectedKey(key);
    changeMode("edit");
    setInspectorRevealNonce((nonce) => nonce + 1);
    // Only the desktop flag is persisted: "b" docks the inspector and "c"
    // opens its drawer, so neither may rewrite the desktop preference.
    if (layout === "a") setRightCollapsed(false);
    else if (layout === "c") openCompactDrawer("right");
  }, [changeMode, layout, openCompactDrawer]);

  /** 重命名: select the node, reveal the Inspector and start its header rename. */
  const renameNodeFromMenu = React.useCallback((key: string) => {
    setSelectedKey(key);
    changeMode("edit");
    setNameFocusNonce((nonce) => (nonce ?? 0) + 1);
    // Only the desktop flag is persisted: "b" docks the inspector and "c"
    // opens its drawer, so neither may rewrite the desktop preference.
    if (layout === "a") setRightCollapsed(false);
    else if (layout === "c") openCompactDrawer("right");
  }, [changeMode, layout, openCompactDrawer]);

  /** 启用 / 禁用节点 goes through the node form's own patch path, so it is undoable. */
  const toggleNodeDisabled = React.useCallback((key: string) => {
    const nodesNow = latestDraftRef.current.nodes ?? [];
    const index = nodesNow.findIndex((node, nodeIndex) => nodeKey(node, nodeIndex) === key);
    if (index < 0) return;
    applyNodePatches(key, [{ op: "set", path: "/disabled", value: !nodesNow[index].disabled }]);
  }, [applyNodePatches]);

  /** 连接到: this node's first output port to the target's first input port. */
  const connectNodeTo = React.useCallback((key: string, targetName: string) => {
    commitWorkflowWith((latest) => {
      const source = (latest.nodes ?? []).find((node, index) => nodeKey(node, index) === key);
      if (!source) return latest;
      // A node with no output port (xflow.end, xflow.start's inputs, ...) has
      // nowhere to connect from: adding the edge under a made-up "main" port
      // would write a connection the runtime cannot route.
      const outputPort = supportedOutputPorts(source, latest)[0];
      if (!outputPort) return latest;
      const target = workflowNodeByName(latest, targetName);
      const inputPort = target ? supportedInputPorts(target)[0] : undefined;
      if (!inputPort) return latest;
      return addConnection(latest, source, targetName, outputPort, inputPort);
    });
  }, [commitWorkflowWith]);

  /** 断开所有连接: every edge touching the node, in ONE updater (one undo entry). */
  const disconnectNode = React.useCallback((key: string) => {
    commitWorkflowWith((latest) => {
      const node = (latest.nodes ?? []).find((candidate, index) => nodeKey(candidate, index) === key);
      const name = node?.name ?? node?.id;
      if (!name) return latest;
      return listConnectionReferences(latest)
        .filter((reference) => reference.sourceName === name || reference.targetName === name)
        .reduce((workflow, reference) => removeConnectionFromWorkflow(workflow, reference), latest);
    });
  }, [commitWorkflowWith]);

  /** The view commands the preview hands over once React Flow is live. */
  const previewHandleRef = React.useRef<XFlowPreviewHandle | undefined>(undefined);
  const handlePreviewReady = React.useCallback((handle: XFlowPreviewHandle) => {
    previewHandleRef.current = handle;
  }, []);

  /** The rendered card of a node, by the id the preview puts on it. */
  const canvasNodeElement = React.useCallback(
    (key: string) =>
      document.querySelector<HTMLElement>(`.xflow-editor .react-flow__node[data-id="${key.replace(/["\\]/g, "\\$&")}"]`),
    []
  );

  /** The measured canvas box of a node, so auto layout spaces real cards apart. */
  const nodeBox = React.useCallback(
    (key: string) => {
      const element = canvasNodeElement(key);
      const width = element?.offsetWidth ?? 0;
      const height = element?.offsetHeight ?? 0;
      return width > 0 && height > 0 ? { width, height } : defaultNodeBox;
    },
    [canvasNodeElement]
  );

  /**
   * Fits the view once the moved cards are actually on the canvas. The new
   * positions reach React Flow through the owner (which echoes the updated
   * workflow back down), so they land a few frames after the commit; framing
   * before that would fit the OLD bounds and leave the new graph half
   * off-canvas. The attempt cap keeps a card that never renders (or is not
   * where we put it) from holding the fit back forever.
   */
  const layoutFitFrameRef = React.useRef<number | undefined>(undefined);
  React.useEffect(
    () => () => {
      if (layoutFitFrameRef.current !== undefined) cancelAnimationFrame(layoutFitFrameRef.current);
    },
    []
  );
  const fitWhenNodesMoved = React.useCallback(
    (positions: Map<string, LayoutPoint>, attempt = 0) => {
      const landed = Array.from(positions).every(([key, point]) => {
        const style = canvasNodeElement(key)?.style.transform ?? "";
        const placed = /translate\(\s*(-?[\d.]+)px,\s*(-?[\d.]+)px/.exec(style);
        return placed ? Math.abs(Number(placed[1]) - point.x) < 0.5 && Math.abs(Number(placed[2]) - point.y) < 0.5 : false;
      });
      if (landed || attempt >= 20) {
        layoutFitFrameRef.current = undefined;
        previewHandleRef.current?.fitView();
        return;
      }
      layoutFitFrameRef.current = requestAnimationFrame(() => fitWhenNodesMoved(positions, attempt + 1));
    },
    [canvasNodeElement]
  );

  /** 自动布局: every position in ONE updater (one undo entry), then fit the view. */
  const autoLayoutWorkflow = React.useCallback(() => {
    const positions = layoutWorkflowPositions(latestDraftRef.current, nodeBox);
    if (positions.size === 0) return;
    commitWorkflowWith((latest) => ({
      ...latest,
      nodes: (latest.nodes ?? []).map((node, index) => {
        const point = positions.get(nodeKey(node, index));
        return point ? { ...node, position: point } : node;
      })
    }));
    fitWhenNodesMoved(positions);
  }, [commitWorkflowWith, fitWhenNodesMoved, nodeBox]);

  /** The partial-run items share runWorkflow's gate; without a host handler they are disabled. */
  const runNode = React.useCallback(async (key: string, mode: "from" | "to") => {
    if (!onRunNode) return;
    const nodesNow = latestDraftRef.current.nodes ?? [];
    const index = nodesNow.findIndex((node, nodeIndex) => nodeKey(node, nodeIndex) === key);
    const name = index >= 0 ? nodeName(nodesNow[index], index) : undefined;
    if (!name) return;
    if (blockedByParameterDraft("运行")) return;
    setRunning(true);
    setOperationError(undefined);
    try {
      const nextRuntime = await onRunNode(latestDraftRef.current, { node: name, mode });
      if (!nextRuntime) throw new Error("运行处理器未返回运行状态快照");
      setLocalRuntime(nextRuntime);
      setOperationStatus(operationStatusFromRuntime(nextRuntime));
      revealDock();
    } catch (error) {
      setOperationError(errorMessage(error, "运行失败"));
      setOperationStatus("运行失败");
      revealDock();
      // The host application owns error presentation.
    } finally {
      setRunning(false);
    }
  }, [blockedByParameterDraft, onRunNode]);

  const contextMenuNode = React.useMemo(() => {
    const nodeId = contextMenu?.kind === "node" ? contextMenu.nodeId : undefined;
    if (!nodeId) return undefined;
    const index = nodes.findIndex((node, nodeIndex) => nodeKey(node, nodeIndex) === nodeId);
    return index >= 0 ? { index, node: nodes[index] } : undefined;
  }, [contextMenu, nodes]);

  // An undo (or any other writer) can take the node away while its menu is
  // open; the menu closes instead of floating over an empty canvas.
  React.useEffect(() => {
    if (contextMenu?.kind !== "node" || contextMenuNode) return;
    setContextMenu(undefined);
  }, [contextMenu, contextMenuNode]);

  const nodeContextMenuItems = React.useMemo<MenuProps["items"]>(() => {
    if (!contextMenuNode) return [];
    const { index, node } = contextMenuNode;
    const key = nodeKey(node, index);
    const name = nodeName(node, index);
    // While the node's parameters JSON does not parse the form is read-only, and
    // so is the disable toggle that writes through it.
    const patchable = !parameterDrafts[key]?.error;
    // A node without a single output port has nothing to connect from, so the
    // whole 连接到 entry is unavailable rather than silently doing nothing.
    const outputPort = supportedOutputPorts(node, draftWorkflow)[0];
    const connectable = Boolean(outputPort);
    const existingTargets = new Set(
      outputPort
        ? listConnectionReferences(draftWorkflow)
            .filter((reference) => reference.sourceName === name && reference.sourcePort === outputPort)
            .map((reference) => reference.targetName)
        : []
    );
    const connectTargets = (draftWorkflow.nodes ?? [])
      .map((candidate, candidateIndex) => ({ name: nodeName(candidate, candidateIndex), node: candidate }))
      .filter(({ name: candidateName, node: candidate }) =>
        candidateName !== name && !isSupplyNode(candidate) && supportedInputPorts(candidate).length > 0
      )
      .map(({ name: candidateName }) => ({
        disabled: existingTargets.has(candidateName),
        key: `connect:${candidateName}`,
        label: candidateName
      }));
    const hasConnections = listConnectionReferences(draftWorkflow)
      .some((reference) => reference.sourceName === name || reference.targetName === name);
    const runNote = onRunNode ? undefined : "宿主未提供 onRunNode";
    return [
      { icon: <FileSearchOutlined />, key: "open", label: <ContextMenuLabel>打开属性</ContextMenuLabel> },
      { icon: <EditOutlined />, key: "rename", label: <ContextMenuLabel>重命名</ContextMenuLabel> },
      {
        disabled: !patchable,
        icon: <ExportOutlined />,
        key: "duplicate",
        label: <ContextMenuLabel hint="⌘D">复制节点</ContextMenuLabel>
      },
      {
        disabled: !patchable,
        icon: node.disabled ? <CheckCircleOutlined /> : <WarningOutlined />,
        key: "toggle-disabled",
        label: <ContextMenuLabel>{node.disabled ? "启用节点" : "禁用节点"}</ContextMenuLabel>
      },
      { type: "divider" },
      {
        children: connectTargets.length > 0 ? connectTargets : [{ disabled: true, key: "connect:none", label: "没有可连接的目标" }],
        disabled: !connectable,
        icon: <BranchesOutlined />,
        key: "connect",
        label: <ContextMenuLabel note={connectable ? undefined : "该节点没有输出端口"}>连接到</ContextMenuLabel>
      },
      {
        disabled: !hasConnections,
        icon: <ApartmentOutlined />,
        key: "disconnect-all",
        label: <ContextMenuLabel>断开所有连接</ContextMenuLabel>
      },
      { type: "divider" },
      {
        disabled: !onRunNode,
        icon: <PlayCircleOutlined />,
        key: "run-from",
        label: <ContextMenuLabel note={runNote}>从此节点运行</ContextMenuLabel>
      },
      {
        disabled: !onRunNode,
        icon: <StepForwardOutlined />,
        key: "run-to",
        label: <ContextMenuLabel note={runNote}>运行到此节点</ContextMenuLabel>
      },
      { type: "divider" },
      {
        danger: true,
        icon: <DeleteOutlined />,
        key: "delete",
        label: <ContextMenuLabel hint="Del">删除节点</ContextMenuLabel>
      }
    ];
  }, [contextMenuNode, draftWorkflow, onRunNode, parameterDrafts]);

  const paneContextMenuItems = React.useMemo<MenuProps["items"]>(() => {
    const groups = nodeLibraryGroups
      .map((group) => {
        const descriptors = libraryDescriptors.filter((descriptor) => descriptor.group === group);
        if (descriptors.length === 0) return null;
        return {
          children: descriptors.map((descriptor) => ({
            icon: descriptor.icon,
            key: `add:${descriptor.type}`,
            label: descriptor.label
          })),
          key: `group:${group}`,
          label: group,
          type: "group" as const
        };
      })
      .filter((group): group is NonNullable<typeof group> => group !== null);
    return [
      {
        children: groups,
        icon: <AppstoreAddOutlined />,
        key: "add-node",
        label: <ContextMenuLabel>添加节点</ContextMenuLabel>
      },
      { type: "divider" },
      {
        icon: <CompressOutlined />,
        key: "fit-view",
        label: <ContextMenuLabel>适应视图</ContextMenuLabel>
      },
      {
        disabled: (draftWorkflow.nodes ?? []).length === 0,
        icon: <PartitionOutlined />,
        key: "auto-layout",
        label: <ContextMenuLabel>自动布局</ContextMenuLabel>
      }
    ];
  }, [draftWorkflow.nodes, libraryDescriptors]);

  const handleContextMenuSelect = React.useCallback((itemKey: string) => {
    const request = contextMenu;
    if (!request) return;
    if (request.kind === "pane") {
      if (itemKey === "fit-view") {
        previewHandleRef.current?.fitView();
        return;
      }
      if (itemKey === "auto-layout") {
        autoLayoutWorkflow();
        return;
      }
      if (!itemKey.startsWith("add:") || request.flowX === undefined || request.flowY === undefined) return;
      dropNode(itemKey.slice("add:".length), { x: request.flowX, y: request.flowY });
      return;
    }
    const nodeId = request.nodeId;
    if (!nodeId) return;
    if (itemKey === "open") openInspectorFor(nodeId);
    else if (itemKey === "rename") renameNodeFromMenu(nodeId);
    else if (itemKey === "duplicate") duplicateSelectedNode();
    else if (itemKey === "toggle-disabled") toggleNodeDisabled(nodeId);
    else if (itemKey === "disconnect-all") disconnectNode(nodeId);
    else if (itemKey.startsWith("connect:")) connectNodeTo(nodeId, itemKey.slice("connect:".length));
    else if (itemKey === "run-from") void runNode(nodeId, "from");
    else if (itemKey === "run-to") void runNode(nodeId, "to");
    else if (itemKey === "delete") deleteSelectedNode();
  }, [
    autoLayoutWorkflow,
    connectNodeTo,
    contextMenu,
    deleteSelectedNode,
    disconnectNode,
    dropNode,
    duplicateSelectedNode,
    openInspectorFor,
    renameNodeFromMenu,
    runNode,
    toggleNodeDisabled
  ]);

  React.useEffect(() => {
    const runShortcut = (id: string) => {
      switch (id) {
        case "undo":
          undoWorkflow();
          return;
        case "redo":
          redoWorkflow();
          return;
        case "save":
          if (onSave && editMode && !saveClean) void saveWorkflow();
          return;
        case "run":
          if (onRun && editMode && !running) void runWorkflow();
          return;
        case "duplicate":
          duplicateSelectedNode();
          return;
        case "findNode":
          openNodeSearch();
          return;
        case "toggleLeftPanel":
          toggleLeftPanel();
          return;
        case "toggleRightPanel":
          toggleRightPanel();
          return;
        case "toggleDiagnostics":
          dockControllerRef.current?.toggle();
          return;
        case "selectTool":
          setCanvasTool("select");
          return;
        case "connectTool":
          setCanvasTool("connect");
          return;
        case "addNode":
          // Advertised by the canvas float, which preview mode does not show.
          if (editMode) openNodeLibrary();
          return;
        case "searchLibrary":
          // The library only exists in edit mode; preview has nothing to search.
          if (editMode) focusLibrarySearchFromKey();
          return;
        case "escape":
          leaveTransientMode();
          return;
        case "shortcutsHelp":
          setShortcutsOpen(true);
          return;
        default:
          return;
      }
    };

    const handleCanvasShortcut = (event: KeyboardEvent) => {
      // An open canvas menu owns the keyboard: its own capture-phase handler
      // takes Escape, and Delete / ⌘D must not act on the node behind it (that
      // would delete the node and leave the menu pointing at nothing).
      if (contextMenu) return;
      const inTextEntry = isTextEntryTarget(event.target);
      for (const shortcut of editorShortcuts) {
        if (inTextEntry && shortcut.scope === "canvas") continue;
        if (!shortcut.match(event)) continue;
        // Esc inside a popup (menu, listbox, dialog) closes that popup only.
        if (shortcut.id === "escape" && isInsidePopup(event.target)) continue;
        // The workbench behind an expanded drawer is inert: shortcuts that act on
        // it (undo, duplicate, panels, tools...) must not reach what cannot be seen.
        if (dock === "expanded" && !expandedDockShortcuts.has(shortcut.id)) continue;
        if (shortcut.preventDefault !== false) event.preventDefault();
        // Holding the chord must not flicker the dock, and a dialog in front
        // owns the keyboard: the browser default is still swallowed above.
        if (shortcut.id === "toggleDiagnostics" && (event.repeat || shortcutsOpen || nodeSearchOpen)) return;
        runShortcut(shortcut.id);
        return;
      }
    };
    window.addEventListener("keydown", handleCanvasShortcut);
    return () => window.removeEventListener("keydown", handleCanvasShortcut);
  }, [
    contextMenu,
    dock,
    duplicateSelectedNode,
    editMode,
    leaveTransientMode,
    nodeSearchOpen,
    onRun,
    onSave,
    focusLibrarySearchFromKey,
    openNodeLibrary,
    openNodeSearch,
    readOnly,
    redoWorkflow,
    running,
    runWorkflow,
    saveClean,
    saveWorkflow,
    shortcutsOpen,
    toggleLeftPanel,
    toggleRightPanel,
    undoWorkflow
  ]);

  // §6.0: desktop collapse flags. The compact bands are rails/drawers, so a
  // persisted desktop preference never shrinks a compact pane.
  const desktopLeftCollapsed = layout === "a" && leftCollapsed;
  const desktopRightCollapsed = layout === "a" && rightCollapsed;
  // Collapsing the column unmounts whatever held focus inside it (the 收起左栏
  // key, a library row), which drops focus to <body>; the rail stays, so hand
  // focus to its 节点 key. Focus that is anywhere else is left alone.
  const wasLeftCollapsedRef = React.useRef(desktopLeftCollapsed);
  React.useEffect(() => {
    const was = wasLeftCollapsedRef.current;
    wasLeftCollapsedRef.current = desktopLeftCollapsed;
    if (!desktopLeftCollapsed || was) return;
    if (document.activeElement && document.activeElement !== document.body) return;
    editorRootRef.current
      ?.querySelector<HTMLButtonElement>(".xflow-editor-desktop-rail button[aria-label='节点与大纲']")
      ?.focus();
  }, [desktopLeftCollapsed]);
  // Leaving the desktop layout swaps the left column for the compact rail and
  // closes the drawer, which unmounts a focused library row or outline item the
  // same way; hand focus to the rail's 节点 key instead of <body>.
  const wasDesktopLayoutRef = React.useRef(layout === "a");
  React.useEffect(() => {
    const was = wasDesktopLayoutRef.current;
    wasDesktopLayoutRef.current = layout === "a";
    if (layout === "a" || !was) return;
    if (document.activeElement && document.activeElement !== document.body) return;
    editorRootRef.current
      ?.querySelector<HTMLButtonElement>(".xflow-editor-compact-rail--left button[aria-label='节点与大纲']")
      ?.focus();
  }, [layout]);
  // The mirror: widening to the desktop layout closes the compact drawer and
  // unmounts what held focus inside it (the drawer may still be animating out,
  // so focus inside it counts as lost too); the desktop rail's 节点 key takes it.
  const wasCompactLayoutRef = React.useRef(layout !== "a");
  React.useEffect(() => {
    const was = wasCompactLayoutRef.current;
    wasCompactLayoutRef.current = layout !== "a";
    if (layout !== "a" || !was) return;
    const active = document.activeElement;
    const inDrawer = active instanceof Element && active.closest(".xflow-editor-compact-drawer") !== null;
    if (active && active !== document.body && !inDrawer) return;
    editorRootRef.current
      ?.querySelector<HTMLButtonElement>(".xflow-editor-desktop-rail button[aria-label='节点与大纲']")
      ?.focus();
  }, [layout]);
  const collapseLeftAction = layout === "a" ? (
    <Tooltip classNames={editorTooltipClassNames} title="收起左栏（⌘ / Ctrl + B）">
      <Button
        aria-label="收起左栏"
        aria-pressed
        className="xflow-editor-panel-collapse"
        color="default"
        icon={<MenuFoldOutlined />}
        size="small"
        variant="text"
        onClick={() => setLeftCollapsed(true)}
      />
    </Tooltip>
  ) : undefined;
  const locateSelectedNode = React.useCallback(() => {
    if (selectedKey !== undefined) previewHandleRef.current?.fitNode(selectedKey);
  }, [selectedKey]);
  /**
   * An outline row: select the node and centre it, the same pair the
   * diagnostics' 定位节点 runs. The inspector is left as the user has it
   * (E1b), and focus stays on the row so the tree keeps its keyboard.
   */
  const selectOutlineNode = React.useCallback((key: string) => {
    setSelectedKey(key);
    previewHandleRef.current?.fitNode(key);
  }, []);
  // The last outline row was deleted while it held focus: nothing in the tree
  // can take it (the empty note is not focusable), so hand it to the library
  // search, else (preview, or no library on screen) the rail's 节点与大纲 key.
  const handleOutlineFocusLost = React.useCallback(() => {
    if (document.activeElement && document.activeElement !== document.body) return;
    // Try the search for real and judge by where focus ended up, not by a
    // pre-check of the ref: a library that is on screen takes focus, one that is
    // not (preview, unmounted) leaves it on <body> and the rail key gets it.
    nodeLibrarySearchRef.current?.focus({ preventScroll: true });
    if (document.activeElement && document.activeElement !== document.body) return;
    editorRootRef.current
      ?.querySelector<HTMLButtonElement>(".xflow-editor-desktop-rail button[aria-label='节点与大纲'], .xflow-editor-compact-rail--left button[aria-label='节点与大纲']")
      ?.focus();
  }, []);
  /**
   * The 节点 state of the left column (§6.0.1): the node library over the
   * outline, one region each. Preview has no library, so the outline fills the
   * column alone and carries the collapse action itself. `inDrawer` is the
   * compact drawer, which focuses the search on open.
   */
  const renderNodeZones = (headerAction: React.ReactNode, inDrawer = false): React.ReactElement => (
    <aside aria-label="节点与大纲" className="xflow-editor-lib" data-zones={editMode ? "dual" : "single"}>
      {editMode ? (
        <NodeLibrary
          autoFocusSearch={inDrawer}
          descriptors={libraryDescriptors}
          headerAction={headerAction}
          searchInputRef={nodeLibrarySearchRef}
          onAddNode={addNode}
        />
      ) : null}
      <OutlinePanel
        canLocate={selectedNode !== undefined}
        headerAction={editMode ? undefined : headerAction}
        nodeCategories={nodeCategories}
        runtime={localRuntime}
        selectedKey={selectedKey}
        workflow={draftWorkflow}
        onFocusLost={handleOutlineFocusLost}
        onLocate={locateSelectedNode}
        onSearch={openNodeSearch}
        onSelectNode={selectOutlineNode}
      />
    </aside>
  );
  const collapseRightAction = layout === "a" ? (
    <Tooltip classNames={editorTooltipClassNames} title="收起属性面板（⌥⌘B / Ctrl + Alt + B）">
      <Button
        aria-label="收起属性面板"
        aria-pressed
        className="xflow-editor-panel-collapse"
        color="default"
        icon={<MenuUnfoldOutlined />}
        size="small"
        variant="text"
        onClick={() => setRightCollapsed(true)}
      />
    </Tooltip>
  ) : undefined;
  // §6.0: a double-click on a node is the "edit this node" gesture; a single
  // click only selects, so a collapsed inspector stays out of the way.
  const handleCanvasDoubleClick = React.useCallback((event: React.MouseEvent<HTMLDivElement>) => {
    const target = event.target as Element | null;
    const nodeId = target?.closest?.("[data-node-id]")?.getAttribute("data-node-id");
    if (!nodeId) return;
    setSelectedKey(nodeId);
    if (layout === "a") setRightCollapsed(false);
    else if (layout === "c") setCompactDrawer("right");
  }, [layout]);

  // The Inspector header's ⋯ menu toggles the selected node (header ≠ canvas:
  // it names the node by key, the same way toggleNodeDisabled does).
  const toggleSelectedNodeDisabled = React.useCallback(() => {
    if (selectedKey) toggleNodeDisabled(selectedKey);
  }, [selectedKey, toggleNodeDisabled]);

  // §6.12 运行 tab: a suspended approval node offers 处理审批 for the tracked run.
  const selectedApprovalName =
    selectedNode?.type === APPROVAL_NODE_TYPE ? nodeName(selectedNode, selectedIndex) : undefined;
  const selectedApprovalSuspended = Boolean(
    selectedApprovalName &&
      execution?.nodes?.some((candidate) => candidate.name === selectedApprovalName && candidate.status === "suspended")
  );
  const handleSelectedApproval = selectedApprovalSuspended && selectedApprovalName
    ? () => openApprovalFor(selectedApprovalName)
    : undefined;

  const inspectorElement = (
    <Inspector
      defaultTab={runsMode ? "run" : undefined}
      editable={editMode}
      headerAction={collapseRightAction}
      externalIssues={
        !runsMode && selectedNode ? externalIssuesMap.get(nodeName(selectedNode, selectedIndex)) : undefined
      }
      focusRequest={inspectorFocusRequest}
      formHost={formHost}
      nameFocusNonce={nameFocusNonce}
      nodeTypes={nodeTypes}
      nodeCategories={nodeCategories}
      parameterDrafts={parameterDrafts}
      parameterTab={parameterTab}
      revealNonce={inspectorRevealNonce}
      runtime={runsMode ? executionSnapshotRuntime : localRuntime}
      selectedIndex={runsMode ? replaySelectedIndex : selectedIndex}
      selectedNode={runsMode ? replaySelectedNode : selectedNode}
      workflow={canvasWorkflow}
      workflowJsonDrafts={workflowJsonDrafts}
      onApplyNodePatches={applyNodePatches}
      onChangeWith={commitWorkflowWith}
      running={running}
      onOpenExecution={onOpenExecution}
      onHandleApproval={runsMode ? undefined : handleSelectedApproval}
      onRerunFromNode={replayRerunFromNode}
      onRunNode={!runsMode && onRunNode && selectedKey ? () => void runNode(selectedKey, "to") : undefined}
      onDeleteNode={deleteSelectedNode}
      onDuplicateNode={duplicateSelectedNode}
      onToggleNodeDisabled={toggleSelectedNodeDisabled}
      onFocusRequestHandled={handleFocusRequestHandled}
      onNameFocusHandled={handleNameFocusHandled}
      onNodeRenamed={handleNodeRenamed}
      onParameterCommit={commitParameterDraft}
      onParameterTabChange={setParameterTab}
      onParameterText={updateParameterText}
      onWorkflowJsonText={updateWorkflowJsonText}
    />
  );

  // §6.12 dialog inputs: the definition node (parameters, label) and the
  // execution record (public ledger) of the same node name.
  const approvalDefinitionNode = approvalTarget
    ? workflowNodeByName(draftWorkflow, approvalTarget.nodeName)
    : undefined;
  const approvalExecutionNode = approvalTarget && execution?.executionId === approvalTarget.executionId
    ? execution.nodes?.find((candidate) => candidate.name === approvalTarget.nodeName)
    : undefined;

  const moreMenuButton = (
    <Dropdown
      classNames={{ root: "xflow-editor-more-menu" }}
      menu={{ items: moreMenuItems, onClick: handleMoreMenuClick }}
      placement="bottomRight"
      trigger={["click"]}
    >
      <Button
        aria-label="更多操作"
        className="xflow-editor-icon-button"
        icon={<MoreOutlined />}
        title="更多操作"
        type="text"
      />
    </Dropdown>
  );

  // §6.8 top bar: execution id + status + lineage + the status table's control
  // ops + 重跑…. The replay has no editing actions; the ⋯ menu stays for the
  // view-level entries (布局 / 帮助) but 保存 / 运行 / 发布 are absent.
  const runsStatusMeta = selectedExecution ? workflowStatusMeta[selectedExecution.status] : undefined;
  const runsOps = runsStatusMeta
    ? runsStatusMeta.ops.filter((op): op is ExecutionControlAction => op !== "rerun" && (
      op === "cancel" ? capabilities?.execution !== false : capabilities?.executionControl !== false
    ))
    : [];
  const runsCanRerun = Boolean(
    selectedExecution &&
      runsStatusMeta?.ops.includes("rerun") &&
      capabilities?.executionRerun !== false &&
      onRerun
  );
  const runsLineage = selectedExecution?.parentExecutionId
    ? selectedExecution.rerunFrom
      ? `重跑自 ${selectedExecution.parentExecutionId} · 起点 ${selectedExecution.rerunFrom}`
      : selectedExecution.parentNode
        ? `子流程 · 父执行 ${selectedExecution.parentExecutionId}`
        : undefined
    : undefined;
  // §6.8 status bar counts, derived from this execution's node records so a
  // status can only appear once and every count names a real node.
  const runsCounts = React.useMemo(() => {
    const counts: Partial<Record<string, number>> = {};
    for (const node of selectedExecution?.nodes ?? []) {
      counts[node.status] = (counts[node.status] ?? 0) + 1;
    }
    return (Object.keys(nodeStatusMeta) as (keyof typeof nodeStatusMeta)[])
      .filter((status) => counts[status])
      .map((status) => `${counts[status]} ${nodeStatusMeta[status].label}`)
      .join(" · ");
  }, [selectedExecution?.nodes]);
  const rerunExecution = rerunTarget === undefined
    ? undefined
    : [selectedExecution, execution].find((candidate) => candidate?.executionId === rerunTarget.executionId);
  const runsToolbar = (
    <div className="xflow-editor-runmeta" aria-label="当前执行">
      {selectedExecution ? (
        <>
          <span className="xflow-editor-runmeta__id" title={selectedExecution.executionId}>
            {selectedExecution.executionId}
          </span>
          <span
            className="xflow-editor-chip"
            data-status={selectedExecution.status}
            data-tone={runsStatusMeta?.tone}
            role="status"
          >
            <span aria-hidden="true" className="xflow-editor-chip__icon">
              {runsStatusMeta ? statusGlyph[runsStatusMeta.icon] : null}
            </span>
            <span>{runsStatusMeta?.label}</span>
          </span>
          {runsLineage ? (
            selectedExecution?.rerunFrom && selectedExecution.parentExecutionId && onSelectExecution ? (
              // §6.8: the rerun lineage jumps to the source execution it names.
              // A subflow's parent belongs to another workflow's record, so it
              // stays a plain label here.
              <button
                className="xflow-editor-runmeta__lineage xflow-editor-runmeta__lineage-link"
                title={`查看来源执行 ${selectedExecution.parentExecutionId}`}
                type="button"
                onClick={() => onSelectExecution(selectedExecution.parentExecutionId!)}
              >
                {runsLineage}
              </button>
            ) : (
              <span className="xflow-editor-runmeta__lineage" title={runsLineage}>
                {runsLineage}
              </span>
            )
          ) : null}
          <Divider className="xflow-editor-sep" orientation="vertical" />
          {runsOps.map((op) =>
            op === "cancel" ? (
              <Popconfirm
                key={op}
                cancelText="返回"
                classNames={{ root: "xflow-editor-confirm" }}
                description="在途节点提交后停止，已完成的结果保留。"
                disabled={!onExecutionAction}
                getPopupContainer={() => editorRootRef.current ?? document.body}
                okButtonProps={{ danger: true }}
                okText="取消执行"
                onConfirm={() => void runExecutionAction(selectedExecution.executionId, "cancel")}
                title={`取消执行 ${selectedExecution.executionId}？`}
              >
                <Button danger disabled={!onExecutionAction} size="small">
                  {executionOpLabel[op]}
                </Button>
              </Popconfirm>
            ) : (
              <Button
                disabled={!onExecutionAction}
                key={op}
                size="small"
                onClick={() => void runExecutionAction(selectedExecution.executionId, op)}
              >
                {executionOpLabel[op]}
              </Button>
            )
          )}
          {runsCanRerun ? (
            <Button
              className="xflow-editor-rerun-button"
              icon={<StepForwardOutlined />}
              size="small"
              type="primary"
              onClick={() => openRerun(selectedExecution.executionId)}
            >
              重跑…
            </Button>
          ) : null}
        </>
      ) : (
        <span className="xflow-editor-runmeta__empty">未选择执行</span>
      )}
      {moreMenuButton}
    </div>
  );

  return (
    <ConfigProvider
      // AntD splits a two-character label into "发 布"; the baseline draws it
      // tight, and the space is a rendering rule, so it is off for the whole
      // editor instead of patched per button.
      button={{ autoInsertSpace: false }}
      getPopupContainer={(trigger) => {
        // Inside a compact drawer, popups must live in the drawer: its focus
        // trap pulls focus back from anything outside and closes the popup.
        const container = trigger?.closest(".xflow-editor-compact-drawer__section") ?? trigger?.closest(".xflow-editor");
        return container instanceof HTMLElement ? container : document.body;
      }}
      theme={editorThemeConfig}
    >
      <section
        className={`xflow-editor ${className ?? ""}`}
        aria-label={`${title} editor`}
        ref={editorRootRef}
        data-appearance={activeAppearance}
        data-canvas-tool={canvasTool}
        data-layout={layout}
        data-layout-policy={layoutPolicy}
        data-theme={resolvedAppearance}
        data-view-mode={viewMode}
      >
      <header className="xflow-editor-toolbar" role="toolbar" aria-label="编辑器工具栏">
        {onBack ? (
          <Tooltip classNames={editorTooltipClassNames} title="返回工作流列表">
            <Button
              aria-label="返回工作流列表"
              className="xflow-editor-icon-button"
              icon={<ArrowLeftOutlined />}
              type="text"
              onClick={onBack}
            />
          </Tooltip>
        ) : null}
        <div className="xflow-editor-title">
          <strong title={titleTooltip}>{title}</strong>
          {runsMode ? (
            <span className="xflow-editor-chip" data-tone="neutral">
              <span aria-hidden="true" className="xflow-editor-chip__icon">
                <ClockCircleOutlined />
              </span>
              <span>
                运行版本{" "}
                {selectedExecution?.workflowVersion
                  ? versionLabel(selectedExecution.workflowVersion)
                  : "-"}
              </span>
            </span>
          ) : onLoadVersions ? (
            <Popover
              classNames={{
                container: "xflow-editor-version-popover__container",
                root: "xflow-editor-version-popover",
                content: "xflow-editor-version-popover__content"
              }}
              content={
                <VersionHistory
                  blocked={blockedVersions}
                  error={versionsState.error}
                  loading={versionsState.loading}
                  rollingBackVersion={rollingBackVersion}
                  versions={versionsState.data}
                  onReload={() => setVersionLoadNonce((nonce) => nonce + 1)}
                  onRollback={onRollback && canWriteWorkflow ? (version) => void rollbackToVersion(version) : undefined}
                  onViewVersion={onViewVersion ? viewVersion : undefined}
                />
              }
              open={versionsOpen}
              placement="bottomLeft"
              trigger="click"
              onOpenChange={setVersionsOpen}
            >
              <button
                aria-label="版本历史"
                className="xflow-editor-chip xflow-editor-version-chip"
                data-tone={versionChip.tone}
                type="button"
              >
                <span aria-hidden="true" className="xflow-editor-chip__icon">
                  {versionChip.tone === "warning" ? <WarningOutlined /> : versionChip.tone === "neutral" ? <HistoryOutlined /> : <CheckOutlined />}
                </span>
                <span>{versionChip.label}</span>
              </button>
            </Popover>
          ) : (
            <span className="xflow-editor-chip" data-tone={versionChip.tone}>
              <span aria-hidden="true" className="xflow-editor-chip__icon">
                {versionChip.tone === "warning" ? <WarningOutlined /> : versionChip.tone === "neutral" ? <HistoryOutlined /> : <CheckOutlined />}
              </span>
              <span>{versionChip.label}</span>
            </span>
          )}
        </div>
        {canViewExecutions ? (
          <Segmented
            classNames={{
              item: "xflow-editor-mode-segmented__item",
              label: "xflow-editor-mode-segmented__label",
              root: "xflow-editor-mode-segmented__root"
            }}
            rootClassName="xflow-editor-mode-segmented"
            value={viewMode}
            options={[
              { label: "编辑", value: "edit" },
              { label: "运行记录", value: "runs" }
            ]}
            onChange={(nextView) => {
              if (nextView === "runs") enterRuns();
              else changeMode("edit");
            }}
          />
        ) : null}
        <div className="xflow-editor-toolbar-actions">
          {runsMode ? runsToolbar : compactToolbar ? moreMenuButton : (
            <>
          <span aria-label="常用画布与历史工具" className="xflow-editor-history-group" role="group">
            <Tooltip classNames={editorTooltipClassNames} title="撤销（⌘ / Ctrl + Z）">
              <span>
                <Button
                  aria-label="撤销"
                  className="xflow-editor-icon-button"
                  disabled={!editMode || history.undo.length === 0}
                  icon={<UndoOutlined />}
                  type="text"
                  onClick={undoWorkflow}
                />
              </span>
            </Tooltip>
            <Tooltip classNames={editorTooltipClassNames} title="重做（⌘ / Ctrl + Shift + Z）">
              <span>
                <Button
                  aria-label="重做"
                  className="xflow-editor-icon-button"
                  disabled={!editMode || history.redo.length === 0}
                  icon={<RedoOutlined />}
                  type="text"
                  onClick={redoWorkflow}
                />
              </span>
            </Tooltip>
          </span>
          <Divider className="xflow-editor-sep" orientation="vertical" />
          <Tooltip classNames={editorTooltipClassNames} title="校验当前工作流">
            <Button
              aria-label="校验"
              className="xflow-editor-validate-button"
              icon={<SafetyCertificateOutlined />}
              type="text"
              onClick={validateCurrentWorkflow}
            >
              校验
            </Button>
          </Tooltip>
          <Popover
            classNames={{
              container: "xflow-editor-theme-popover__container",
              root: "xflow-editor-theme-popover",
              content: "xflow-editor-theme-popover__content",
              title: "xflow-editor-theme-popover__title"
            }}
            content={
              <ThemeSettings
                appearance={activeAppearance}
                onAppearanceChange={changeAppearance}
              />
            }
            placement="bottomRight"
            title="主题设置"
            trigger="click"
          >
            <Button
              aria-label="主题设置"
              className="xflow-editor-icon-button xflow-editor-theme-trigger"
              icon={<BgColorsOutlined />}
              type="text"
            />
          </Popover>
              {moreMenuButton}
              <Divider className="xflow-editor-sep" orientation="vertical" />
            </>
          )}
          {/* §6.5.1 / §6.8 / §6.10: a read-only viewer and the run-record replay
              both drop the editing actions instead of showing disabled copies. */}
          {editMode ? (
            <>
            <Tooltip classNames={editorTooltipClassNames} title={saveTooltip}>
              <span>
                <Button
                  aria-label="保存"
                  className="xflow-editor-save-button"
                  data-blocked={saveBlocked || undefined}
                  disabled={!onSave || saveClean}
                  icon={saveBlocked ? <WarningOutlined className="xflow-editor-save-button__icon" /> : undefined}
                  loading={saving}
                  onClick={() => void saveWorkflow()}
                >
                  保存
                  <kbd className="xflow-editor-kbd">⌘S</kbd>
                  {formIssueSummary.errors > 0 ? (
                    <span className="xflow-editor-save-issues" data-testid="save-issue-count">
                      {formIssueSummary.errors}
                    </span>
                  ) : null}
                </Button>
              </span>
            </Tooltip>
            <span className="xflow-editor-run-split">
              <Tooltip classNames={editorTooltipClassNames} title={onRun ? "运行工作流（⌘ / Ctrl + Enter）" : "运行不可用：宿主未提供 onRun 处理器"}>
                <span>
                  <Button
                    aria-label="运行工作流"
                    className="xflow-editor-run-split__main"
                    disabled={running || !onRun}
                    icon={<CaretRightOutlined />}
                    loading={running}
                    type="primary"
                    onClick={() => void runWorkflow()}
                  >
                    运行
                  </Button>
                </span>
              </Tooltip>
              <Tooltip classNames={editorTooltipClassNames} title="运行选项尚未接入">
                <span>
                  <Dropdown
                    classNames={{ root: "xflow-editor-run-menu" }}
                    menu={{ items: runOptionItems }}
                    placement="bottomRight"
                    trigger={["click"]}
                  >
                    <Button
                      aria-label="运行选项"
                      className="xflow-editor-run-split__options"
                      icon={<DownOutlined />}
                      type="primary"
                    />
                  </Dropdown>
                </span>
              </Tooltip>
            </span>
            <Tooltip
              classNames={editorTooltipClassNames}
              title={onPublish
                ? "发布新版本：归档当前线上版本，此后新触发的执行使用新版本"
                : "发布不可用：宿主未提供 onPublish 处理器"}
            >
              <span>
                <Button
                  aria-label="发布"
                  className="xflow-editor-publish-button"
                  disabled={!onPublish}
                  onClick={openPublishDialog}
                >
                  发布
                </Button>
              </span>
            </Tooltip>
            </>
          ) : null}
          <span aria-live="polite" className="xflow-editor-sr-only" role="status">
            <span>{operationStatus}</span>
            {hostNotices.map((notice) => <span key={notice}>{notice}</span>)}
          </span>
        </div>
      </header>

      {/* An expanded drawer is drawn over this whole region, so it leaves the tab
          order and the accessibility tree until the drawer is restored (§6.5). */}
      <div
        className="xflow-editor-main"
        data-left-collapsed={desktopLeftCollapsed}
        data-right-collapsed={desktopRightCollapsed}
        inert={dock === "expanded"}
      >
        {layout === "a" ? (
          <div className="xflow-editor-left">
            <nav className="xflow-editor-rail xflow-editor-desktop-rail" aria-label="工作台导航">
              <RailKey
                icon={<AppstoreOutlined />}
                label="节点与大纲"
                pressed={!runsMode && !leftCollapsed && leftSection === "library"}
                text="节点"
                onClick={() => {
                  // Coming back from the replay the column shows the execution
                  // list, so 节点 must expand it: "same section on screen" is
                  // not true even when the remembered edit section is 节点.
                  if (runsMode) {
                    changeMode("edit");
                    setLeftCollapsed(false);
                    setDesktopRailSection("library");
                    return;
                  }
                  selectLeftSection("library");
                }}
              />
              {canViewExecutions ? (
                <RailKey
                  icon={<HistoryOutlined />}
                  label="执行记录"
                  pressed={runsMode}
                  text="执行"
                  onClick={enterRuns}
                />
              ) : null}
              <RailKey
                icon={<FunctionOutlined />}
                label="变量"
                pressed={!runsMode && !leftCollapsed && leftSection === "variables"}
                text="变量"
                onClick={() => {
                  if (runsMode) {
                    changeMode("edit");
                    setLeftCollapsed(false);
                    setDesktopRailSection("variables");
                    return;
                  }
                  selectLeftSection("variables");
                }}
              />
              <span className="xflow-editor-rail__spacer" />
              <RailKey
                icon={<QuestionCircleOutlined />}
                label="快捷键与帮助"
                text="帮助"
                title="快捷键（?）"
                onClick={() => setShortcutsOpen(true)}
              />
            </nav>
            {desktopLeftCollapsed ? null : (
              <div className="xflow-editor-left__content">
                {runsMode ? (
                  <ExecutionListPanel
                    selectedId={selectedExecution?.executionId}
                    state={executionList}
                    onLoad={onLoadExecutions}
                    onSelect={onSelectExecution}
                  />
                ) : leftSection === "library" ? (
                  renderNodeZones(collapseLeftAction)
                ) : (
                  <VariablesPanel
                    headerAction={collapseLeftAction}
                    readOnly={!editMode}
                    workflow={draftWorkflow}
                    workflowJsonDrafts={workflowJsonDrafts}
                    onChangeWith={commitWorkflowWith}
                    onWorkflowJsonText={updateWorkflowJsonText}
                  />
                )}
              </div>
            )}
          </div>
        ) : (
          <aside className="xflow-editor-rail xflow-editor-compact-rail xflow-editor-compact-rail--left" aria-label="紧凑导航">
            <RailKey
              icon={<AppstoreOutlined />}
              label="节点与大纲"
              pressed={!runsMode && compactDrawer === "left" && leftDrawerSection === "library"}
              text="节点"
              onClick={() => {
                if (runsMode) changeMode("edit");
                if (compactDrawer === "left" && leftDrawerSection === "library") {
                  setCompactDrawer(undefined);
                  return;
                }
                setLeftDrawerTab("library");
                openCompactDrawer("left");
              }}
            />
            {canViewExecutions ? (
              <RailKey
                icon={<HistoryOutlined />}
                label="执行记录"
                pressed={runsMode}
                text="执行"
                onClick={enterRuns}
              />
            ) : null}
            <RailKey
              icon={<FunctionOutlined />}
              label="变量"
              pressed={!runsMode && compactDrawer === "left" && leftDrawerSection === "variables"}
              text="变量"
              onClick={() => {
                if (runsMode) changeMode("edit");
                if (compactDrawer === "left" && leftDrawerSection === "variables") {
                  setCompactDrawer(undefined);
                  return;
                }
                setLeftDrawerTab("variables");
                openCompactDrawer("left");
              }}
            />
            <span className="xflow-editor-rail__spacer" />
            {/* Below `narrow` the inspector has no column left; the rail opens it as an overlay. */}
            {layout === "c" ? (
              <RailKey
                icon={<SlidersOutlined />}
                label="属性面板"
                pressed={compactDrawer === "right"}
                text="属性"
                onClick={() => setCompactDrawer((current) => (current === "right" ? undefined : "right"))}
              />
            ) : null}
            <RailKey
              icon={<QuestionCircleOutlined />}
              label="快捷键与帮助"
              text="帮助"
              title="快捷键（?）"
              onClick={() => setShortcutsOpen(true)}
            />
          </aside>
        )}

        <section className="xflow-editor-canvas" aria-label="画布">
          {/* §6.3: the run bar owns the centre column of the canvas's top float
              row; the banner stack starts below the shared float height. */}
          {!runsMode && execution ? (
            <div
              className="xflow-editor-runbar-row"
              data-stacked={runBarStacked ? "true" : undefined}
              ref={runBarRowRef}
            >
              <RunBar
                capabilities={capabilities}
                dismissMs={runBarDismissMs}
                execution={execution}
                getPopupContainer={() => editorRootRef.current ?? document.body}
                onAction={onExecutionAction ? runExecutionAction : undefined}
                onRerun={onRerun ? (executionId) => openRerun(executionId) : undefined}
                onHandleApproval={firstApprovalNode ? handleRunBarApproval : undefined}
                onLocateNode={(name) => {
                  // The snapshot names nodes; the canvas keys them by id ?? name.
                  locateRuntimeNode(name);
                  onLocateNode?.(name);
                }}
                onOpenRunHistory={onOpenRunHistory}
              />
            </div>
          ) : null}
          {runsMode ? null : <BannerStack banners={banners} onFocusLost={focusCanvasFrame} />}
          <div
            className="xflow-editor-preview-frame"
            ref={canvasFrameRef}
            tabIndex={-1}
            onDoubleClick={handleCanvasDoubleClick}
          >
            <XFlowPreview
              key={`${viewMode}-${canvasEpoch}`}
              workflow={canvasWorkflow}
              runtime={runsMode ? executionSnapshotRuntime : localRuntime}
              className="xflow-editor-preview"
              editable={editMode}
              interactionMode={runsMode ? "select" : canvasTool}
              // "" rather than undefined: an empty editor selection must not
              // fall back to the preview's own uncontrolled selection.
              selectedNodeId={selectedKey ?? ""}
              onSelectNode={setSelectedKey}
              onPaneClick={clearSelection}
              onNodePositionChange={runsMode ? undefined : updateNodePosition}
              onConnect={runsMode ? undefined : addCanvasConnection}
              onDeleteConnection={runsMode ? undefined : removeCanvasConnection}
              // The preview's Delete / Backspace listener is on the window: with the
              // workbench inert behind an expanded drawer it must not delete a node.
              onDeleteNode={runsMode || dock === "expanded" ? undefined : deleteSelectedNode}
              onDropNode={runsMode ? undefined : dropNode}
              danglingPorts={runsMode ? undefined : canvasDanglingPorts}
              // A remount takes `defaultViewport`; the way back reuses the
              // viewport captured from the edit canvas before entering the replay.
              defaultViewport={runsMode ? undefined : restoreViewport ?? defaultViewport}
              onViewportChange={runsMode ? undefined : handleViewportChange}
              nodeNotes={runsMode ? undefined : approvalNotes}
              nodeCategories={nodeCategories}
              nodeSummaries={canvasNodeSummaries}
              nodeMetaNotes={runsMode ? replayMetaNotes : undefined}
              onLocateParameter={runsMode ? undefined : locateSummaryField}
              onNodeAction={runsMode || !firstApprovalNode ? undefined : handlePreviewNodeAction}
              // Preview mode keeps the browser's own menu: the editor has
              // nothing to offer there.
              onNodeContextMenu={editMode ? openNodeContextMenu : undefined}
              onPaneContextMenu={editMode ? openPaneContextMenu : undefined}
              onReady={handlePreviewReady}
              // §6.2 / §7: the read-only canvas offers neither adding nor laying out.
              onAutoLayout={editMode ? autoLayoutWorkflow : undefined}
              canvasFloat={runsMode ? (
                <div className="xflow-editor-canvas-mode" data-replay="true" role="status">
                  <EyeOutlined />
                  <strong>只读回放</strong>
                  <span>· 节点不可拖动、不可连线</span>
                </div>
              ) : editMode ? (
                <>
                  <Tooltip classNames={editorTooltipClassNames} title="添加节点 (A)">
                    <Button
                      aria-label="添加节点 (A)"
                      className="xflow-editor-canvas-add"
                      icon={<PlusOutlined />}
                      size="small"
                      type="text"
                      onClick={openNodeLibrary}
                    >
                      添加节点
                    </Button>
                  </Tooltip>
                  <Tooltip classNames={editorTooltipClassNames} title="定位节点 ⌘K">
                    <Button
                      aria-label="定位节点 ⌘K"
                      className="xflow-editor-canvas-locate"
                      icon={<SearchOutlined />}
                      size="small"
                      type="text"
                      onClick={(event) => openNodeSearch(event.currentTarget)}
                    />
                  </Tooltip>
                </>
              ) : (
                <>
                  {/* The 只读 banner already says it; the preview notice would repeat it. */}
                  {readOnly ? null : (
                    <div className="xflow-editor-canvas-mode" role="status">
                      <EyeOutlined />
                      <strong>预览模式</strong>
                      <span>画布为只读；切换到编辑模式后才能修改工作流</span>
                    </div>
                  )}
                  {/* Read-only still needs locate: the magnifier follows the notice. */}
                  <Tooltip classNames={editorTooltipClassNames} title="定位节点 ⌘K">
                    <Button
                      aria-label="定位节点 ⌘K"
                      className="xflow-editor-canvas-locate"
                      icon={<SearchOutlined />}
                      size="small"
                      type="text"
                      onClick={(event) => openNodeSearch(event.currentTarget)}
                    />
                  </Tooltip>
                </>
              )}
              canvasFloatEnd={desktopRightCollapsed ? (
                <Tooltip classNames={editorTooltipClassNames} title="展开属性面板（⌥⌘B / Ctrl + Alt + B）">
                  <Button
                    aria-label="展开属性面板"
                    className="xflow-editor-canvas-reopen"
                    icon={<MenuUnfoldOutlined />}
                    size="small"
                    type="text"
                    onClick={() => setRightCollapsed(false)}
                  >
                    {selectedNode ? `属性 · ${nodeLabel(selectedNode, selectedIndex)}` : "属性"}
                  </Button>
                </Tooltip>
              ) : undefined}
              emptyState={runsMode ? (
                <div className="xflow-editor-replay-empty">
                  <h2 className="xflow-editor-empty-canvas__title">从左侧选择一条执行</h2>
                  <p className="xflow-editor-empty-canvas__text">选中后在这里回放当次运行的画布与节点状态。</p>
                </div>
              ) : (
                <EmptyCanvasCard editable={editMode} onPickTrigger={openNodeLibrary} />
              )}
            />
          </div>
        </section>

        {layout !== "c" ? (
          <aside className="xflow-editor-right">
            {desktopRightCollapsed ? null : editMode || runsMode ? (
              inspectorElement
            ) : (
              <section className="xflow-editor-preview-inspector" aria-label="预览模式说明">
                {/* §6.0: the collapse entry lives in the panel title bar in
                    every mode, so the preview notice keeps one too. */}
                <div className="xflow-editor-preview-inspector__header">
                  <EyeOutlined />
                  <strong>{readOnly ? "只读" : "预览模式"}</strong>
                  {collapseRightAction}
                </div>
                <p>{readOnly ? "画布、节点属性和工作流定义均保持只读。" : "画布、节点属性和工作流定义均保持只读。切换到编辑模式后可继续修改。"}</p>
              </section>
            )}
          </aside>
        ) : null}

        {/* Inside the workbench body, so it starts under the top bar and ends above the status bar (baseline). */}
        {layout !== "a" ? (
          <Drawer
            destroyOnHidden
            focusable={{ trap: true, focusTriggerAfterClose: true }}
            afterOpenChange={(open) => {
              if (open && leftDrawerTab === "library" && editMode) {
                focusNodeLibrarySearch();
              }
            }}
            getContainer={false}
            keyboard
            mask={false}
            // No visible title (the panel's own title bar names it), but the dialog
            // still needs an accessible name.
            aria-label={leftDrawerSection === "library" ? "节点与大纲" : leftDrawerSection === "executions" ? "执行列表" : "变量"}
            open={compactDrawer === "left"}
            placement="left"
            classNames={editorCompactDrawerClassNamesStart}
            rootClassName="xflow-editor-compact-drawer"
            closable={false}
            rootStyle={{ left: 60, position: "absolute" }}
            size={280}
            onClose={() => setCompactDrawer(undefined)}
          >
            {leftDrawerSection === "executions" ? (
              <ExecutionListPanel
                selectedId={selectedExecution?.executionId}
                state={executionList}
                onLoad={onLoadExecutions}
                onSelect={onSelectExecution}
              />
            ) : leftDrawerSection === "library" ? (
              renderNodeZones(undefined, true)
            ) : (
              <VariablesPanel
                readOnly={!editMode}
                workflow={draftWorkflow}
                workflowJsonDrafts={workflowJsonDrafts}
                onChangeWith={commitWorkflowWith}
                onWorkflowJsonText={updateWorkflowJsonText}
              />
            )}
          </Drawer>
        ) : null}
      </div>

      {layout === "c" ? (
        <Drawer
          destroyOnHidden
          focusable={{ trap: true, focusTriggerAfterClose: true }}
          getContainer={false}
          keyboard
          mask={false}
          aria-label="属性"
          open={compactDrawer === "right"}
          placement="right"
          classNames={editorCompactDrawerClassNamesEnd}
          rootClassName="xflow-editor-compact-drawer"
          rootStyle={{ position: "absolute" }}
          size={360}
          onClose={() => setCompactDrawer(undefined)}
        >
          {editMode || runsMode ? (
            inspectorElement
          ) : (
            <section className="xflow-editor-preview-inspector" aria-label="预览模式说明">
              <EyeOutlined />
              <strong>{readOnly ? "只读" : "预览模式"}</strong>
              <p>{readOnly ? "画布、节点属性和工作流定义均保持只读。" : "画布、节点属性和工作流定义均保持只读。切换到编辑模式后可继续修改。"}</p>
            </section>
          )}
        </Drawer>
      ) : null}

      <CanvasContextMenu
        items={contextMenu?.kind === "pane" ? paneContextMenuItems : nodeContextMenuItems}
        request={contextMenu}
        onClose={closeContextMenu}
        onSelect={handleContextMenuSelect}
      />
      {runsMode ? (
        <div aria-label="执行状态栏" className="xflow-editor-runs-statusbar" role="group">
          <span className="xflow-editor-runs-statusbar__item">
            执行{" "}
            <span className="xflow-editor-runs-statusbar__id">
              {selectedExecution?.executionId ?? "未选择"}
            </span>
          </span>
          {selectedExecution ? (
            <>
              <span aria-hidden="true" className="xflow-editor-statusbar__sep" />
              <span className="xflow-editor-runs-statusbar__item" data-tone="faint">
                创建 {selectedExecution.createdAt ? executionClock(selectedExecution.createdAt) : "—"}
                {" · "}更新 {selectedExecution.updatedAt ? executionClock(selectedExecution.updatedAt) : "—"}
              </span>
            </>
          ) : null}
          <span className="xflow-editor-statusbar__spacer" />
          {selectedExecution ? (
            <span className="xflow-editor-runs-statusbar__item">
              {(selectedExecution.nodes ?? []).length} 节点{runsCounts ? ` · ${runsCounts}` : ""}
            </span>
          ) : null}
          <span className="xflow-editor-runs-statusbar__item" data-tone="faint">
            {executionDefinitionStale ? "定义已变化，仅供参考" : "只读回放 · 定义为当次运行版本"}
          </span>
        </div>
      ) : (
        <DiagnosticsDock
          workflow={draftWorkflow}
          runtime={localRuntime}
          diagnostics={diagnostics}
          blockedDraftNodes={operationError ? blockedDraftNodes : undefined}
          nodeCategories={nodeCategories}
          onRevealJson={revealParameterJson}
          onLocateField={locateParameterField}
          operationError={operationError}
          versionLabel={versionChip.label}
          controllerRef={dockControllerRef}
          dock={dock}
          onDockChange={setDock}
          onLocateNode={locateDiagnosticNode}
        />
      )}
      <Modal
        centered={false}
        className="xflow-editor-locate"
        classNames={{
          body: "xflow-editor-locate__body",
          container: "xflow-editor-locate__content",
          header: "xflow-editor-locate__header"
        }}
        footer={null}
        focusable={{ focusTriggerAfterClose: false }}
        getContainer={() => editorRootRef.current ?? document.body}
        open={nodeSearchOpen}
        style={{ top: 64 }}
        title="定位节点"
        width={480}
        afterOpenChange={(open) => {
          if (open) nodeSearchRef.current?.focus();
        }}
        onCancel={closeNodeSearch}
      >
        <NodeSearch
          key={nodeSearchNonce}
          nodes={nodeSearchOptions}
          ref={nodeSearchRef}
          onSelect={(key) => {
            locateNode(key);
            closeNodeSearch();
          }}
        />
      </Modal>
      <PublishDialog
        changes={changes}
        error={publishError}
        errorCount={publishDiagnostics.errors}
        getContainer={() => editorRootRef.current ?? document.body}
        onlineHash={onlineVersionRow?.definitionHash}
        onlineVersion={onlineVersion}
        open={publishOpen}
        publishing={publishing}
        suggestedVersion={suggestedVersion}
        usedVersions={usedVersions}
        version={publishVersion}
        versionsError={versionsState.error}
        warningCount={publishDiagnostics.warnings}
        workflow={draftWorkflow}
        onCancel={() => setPublishOpen(false)}
        onPublish={() => void publishWorkflow()}
        onReloadVersions={() => setVersionLoadNonce((nonce) => nonce + 1)}
        onVersionChange={(next) => {
          setPublishVersion(next);
          setPublishError(undefined);
        }}
      />
      <ShortcutsHelp open={shortcutsOpen} onClose={() => setShortcutsOpen(false)} />
      {/* Keyed remount: closing or retargeting discards the previous node's
          pending signal, choice and comment instead of carrying them across. */}
      <ApprovalDialog
        capabilities={capabilities}
        currentUserSubject={currentUserSubject}
        executionId={approvalTarget?.executionId ?? ""}
        executionNode={approvalExecutionNode}
        executionStatus={execution?.status ?? "running"}
        getContainer={() => editorRootRef.current ?? document.body}
        key={`${approvalTarget?.executionId ?? ""}:${approvalTarget?.nodeName ?? ""}`}
        node={approvalDefinitionNode}
        open={Boolean(approvalTarget && execution && approvalTarget.executionId === execution.executionId)}
        onClose={() => setApprovalTarget(undefined)}
        onRevokeSignal={onRevokeSignal}
        onSignal={onSignal}
      />
      {/* §6.9 rerun dialog, opened from the run bar, the 运行 tab footer and the
          run-record top bar. The host owns the API call and the new execution. */}
      <RerunDialog
        execution={rerunExecution}
        getContainer={() => editorRootRef.current ?? document.body}
        initialNode={rerunTarget?.node}
        open={rerunTarget !== undefined}
        onCancel={() => setRerunTarget(undefined)}
        onSubmit={onRerun}
      />
      </section>
    </ConfigProvider>
  );
}

export { XFlowPreview };
