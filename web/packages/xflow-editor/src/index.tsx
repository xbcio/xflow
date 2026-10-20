import {
  AimOutlined,
  ApartmentOutlined,
  AppstoreAddOutlined,
  AppstoreOutlined,
  AuditOutlined,
  BgColorsOutlined,
  BranchesOutlined,
  CheckCircleOutlined,
  CloudServerOutlined,
  CloudUploadOutlined,
  ClockCircleOutlined,
  CodeOutlined,
  CompressOutlined,
  ConsoleSqlOutlined,
  DatabaseOutlined,
  DeleteOutlined,
  DesktopOutlined,
  DownOutlined,
  EditOutlined,
  ExportOutlined,
  EyeOutlined,
  FileAddOutlined,
  FileSearchOutlined,
  FunctionOutlined,
  InfoCircleOutlined,
  GlobalOutlined,
  ImportOutlined,
  LeftOutlined,
  LinkOutlined,
  MergeCellsOutlined,
  MoonOutlined,
  NotificationOutlined,
  PartitionOutlined,
  PlayCircleOutlined,
  PlaySquareOutlined,
  QuestionCircleOutlined,
  RedoOutlined,
  RightOutlined,
  SaveOutlined,
  SearchOutlined,
  SettingOutlined,
  StepForwardOutlined,
  SunOutlined,
  ThunderboltOutlined,
  UndoOutlined,
  UpOutlined,
  WarningOutlined
} from "@ant-design/icons";
import * as React from "react";
import {
  Button,
  ConfigProvider,
  Drawer,
  Input,
  Modal,
  Popover,
  Segmented,
  Space,
  Tag,
  Tooltip,
  theme,
  type InputRef,
  type MenuProps,
  type RefSelectProps,
  type ThemeConfig
} from "antd";
import type {
  Connections,
  NodeTypesResponse,
  ParamIssue,
  RuntimeSnapshot,
  Viewport,
  WorkflowDef,
  WorkflowNode
} from "@xflow/core";
import { declaredOutputPorts, isSupplyNode, isTextEntryTarget } from "@xflow/core";
import type { Patch } from "@xflow/composer/core";
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
import { VariablesPanel } from "./panels/variables";
import { NodeSearch, type NodeSearchOption } from "./panels/nodeSearch";
import { Inspector, type NodeFormHost, type NodePresentationHint } from "./panels/inspector";
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
  editorInputClassNames,
  editorTooltipClassNames,
  serializeJson,
  workflowJsonLabels,
  type WorkflowJsonDraft,
  type WorkflowJsonField,
  type WorkflowUpdater
} from "./panels/shared";
import {
  editorThemePalettes,
  type ResolvedXFlowEditorAppearance,
  type XFlowEditorAppearance,
  type XFlowEditorThemeVariant
} from "./theme";
// Composer form styles first so the editor's scoped overrides win on equal specificity.
import "@xflow/composer/form/styles.css";
import "./styles.css";

/**
 * Node Descriptor text arrives in English (GET /v1/node-types); the editor UI
 * is Chinese, so the response is localized once where it enters the editor
 * (Doc C §8 item 1). Custom types without catalog entries stay English.
 */
const nodeFormCatalog = nodeFormCatalogs["zh-CN"];

export type { XFlowEditorAppearance, XFlowEditorThemeVariant } from "./theme";
type LayoutPolicy = "auto" | "compact";
type EditorLayout = "a" | "c";
type CanvasTool = "select" | "connect";

interface WorkflowHistory {
  undo: WorkflowDef[];
  redo: WorkflowDef[];
}

const compactViewportQuery = "(max-width: 1399px)";
const historyLimit = 80;
const rulerMarks = [0, 100, 200, 300, 400, 500, 600, 700];

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
    label: "收起 / 展开左侧面板",
    keys: "⌘ / Ctrl + B",
    scope: "canvas",
    match: (event) => isModifiedKey(event, "b")
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

const rulerStepPx = 100;

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

export interface XFlowEditorProps {
  value: WorkflowDef;
  /** Optional host layout class; useful when the workbench lives below an app header. */
  className?: string;
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
  /** Preferred color mode. Omit to use the editor's local setting. */
  appearance?: XFlowEditorAppearance;
  /** Preferred workbench visual language. Omit to use the editor's local setting. */
  themeVariant?: XFlowEditorThemeVariant;
  /** Called whenever the user selects a color mode. Persistence belongs to the host. */
  onAppearanceChange?: (appearance: XFlowEditorAppearance) => void;
  /** Called whenever the user selects a workbench visual language. */
  onThemeVariantChange?: (themeVariant: XFlowEditorThemeVariant) => void;
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

  return appearance === "system" ? systemMode : appearance;
}

function compactViewport(): boolean {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") {
    return false;
  }
  return window.matchMedia(compactViewportQuery).matches;
}

function useCompactViewport(): boolean {
  const [isCompact, setIsCompact] = React.useState(compactViewport);

  React.useEffect(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") {
      return undefined;
    }

    const mediaQuery = window.matchMedia(compactViewportQuery);
    const sync = () => setIsCompact(mediaQuery.matches);
    sync();

    if (typeof mediaQuery.addEventListener === "function") {
      mediaQuery.addEventListener("change", sync);
      return () => mediaQuery.removeEventListener("change", sync);
    }

    mediaQuery.addListener?.(sync);
    return () => mediaQuery.removeListener?.(sync);
  }, []);

  return isCompact;
}

function createEditorThemeConfig(
  themeVariant: XFlowEditorThemeVariant,
  appearance: ResolvedXFlowEditorAppearance
): ThemeConfig {
  // `editorThemePalettes` mirrors the --xflow-* custom properties declared in
  // styles.css. ConfigProvider owns AntD's interaction states; the root CSS
  // variables own editor-only surfaces such as rulers and the React Flow canvas.
  const palette = editorThemePalettes[themeVariant][appearance];
  const focusOutline = `${palette.primary}33`;

  return {
    algorithm: appearance === "dark" ? theme.darkAlgorithm : theme.defaultAlgorithm,
    cssVar: {
      prefix: "xflow-ant",
      key: `xflow-editor-${themeVariant}-${appearance}`
    },
    token: {
      borderRadius: 6,
      borderRadiusSM: 4,
      colorBgBase: palette.app,
      colorBgContainer: palette.panel,
      colorBgContainerDisabled: palette.panelRaised,
      colorBgElevated: palette.panelRaised,
      colorBgLayout: palette.app,
      colorBgTextActive: palette.hover,
      colorBgTextHover: palette.hover,
      colorBorder: palette.border,
      colorBorderBg: palette.border,
      colorBorderSecondary: palette.border,
      colorError: palette.error,
      colorFillAlter: palette.field,
      colorFillContent: palette.field,
      colorFillContentHover: palette.hover,
      colorFillSecondary: palette.field,
      colorIcon: palette.textTertiary,
      colorIconHover: palette.text,
      colorInfo: palette.primary,
      colorLink: palette.primary,
      colorPrimary: palette.primary,
      colorSplit: palette.border,
      colorSuccess: palette.success,
      colorText: palette.text,
      colorTextBase: palette.text,
      colorTextDescription: palette.textTertiary,
      colorTextLabel: palette.textSecondary,
      colorTextPlaceholder: palette.textTertiary,
      colorTextSecondary: palette.textSecondary,
      colorTextTertiary: palette.textTertiary,
      colorWarning: palette.warning,
      controlHeight: 32,
      controlItemBgActive: palette.hover,
      controlItemBgActiveHover: palette.hover,
      controlItemBgHover: palette.hover,
      controlOutline: focusOutline,
      controlOutlineWidth: 2,
      fontSize: 12,
      fontSizeSM: 11
    },
    components: {
      Button: {
        dangerColor: palette.error,
        dangerShadow: "none",
        defaultActiveBg: palette.hover,
        defaultActiveBorderColor: palette.primary,
        defaultActiveColor: palette.text,
        defaultBg: palette.field,
        defaultBorderColor: palette.border,
        defaultColor: palette.text,
        defaultHoverBg: palette.hover,
        defaultHoverBorderColor: palette.primary,
        defaultHoverColor: palette.primary,
        defaultShadow: "none",
        fontWeight: 600,
        iconGap: 4,
        onlyIconSizeSM: 14,
        paddingInlineSM: 9,
        primaryColor: "#ffffff",
        primaryShadow: "none",
        solidTextColor: "#ffffff"
      },
      Card: {
        bodyPaddingSM: 10,
        headerBg: palette.panelRaised,
        headerHeightSM: 42,
        headerPaddingSM: 10
      },
      Input: {
        activeBg: palette.field,
        activeBorderColor: palette.primary,
        activeShadow: `0 0 0 2px ${focusOutline}`,
        hoverBg: palette.field,
        hoverBorderColor: palette.primary,
        inputFontSizeSM: 11,
        paddingBlockSM: 0,
        paddingInlineSM: 7
      },
      Segmented: {
        itemActiveBg: palette.hover,
        itemColor: palette.textSecondary,
        itemHoverBg: palette.hover,
        itemHoverColor: palette.text,
        itemSelectedBg: palette.field,
        itemSelectedColor: palette.text,
        trackBg: palette.panelRaised,
        trackPadding: 2
      },
      Select: {
        activeBorderColor: palette.primary,
        activeOutlineColor: focusOutline,
        hoverBorderColor: palette.primary,
        optionActiveBg: palette.hover,
        optionFontSize: 11,
        optionHeight: 28,
        optionLineHeight: "20px",
        optionPadding: "4px 8px",
        optionSelectedBg: palette.hover,
        optionSelectedColor: palette.text,
        optionSelectedFontWeight: 600,
        selectorBg: palette.field
      },
      Switch: {
        handleBg: palette.field,
        handleShadow: "none",
        handleSizeSM: 12,
        trackHeightSM: 16,
        trackMinWidthSM: 28,
        trackPadding: 2
      },
      Tag: {
        defaultBg: palette.panelRaised,
        defaultColor: palette.textSecondary,
        solidTextColor: "#ffffff"
      }
    }
  };
}



/** The "Runner" badge on a runner-reported library tile; colored by --xflow-node-source-*. */
const editorNodeSourceTagClassNames = {
  content: "xflow-editor__node-tile-source-label",
  root: "xflow-editor__node-tile-source-root"
};

const editorCompactDrawerClassNames = {
  body: "xflow-editor-compact-drawer__body",
  close: "xflow-editor-compact-drawer__close",
  header: "xflow-editor-compact-drawer__header",
  section: "xflow-editor-compact-drawer__section",
  title: "xflow-editor-compact-drawer__title",
  wrapper: "xflow-editor-compact-drawer__wrapper"
};

function ThemeSettings({
  appearance,
  onAppearanceChange,
  onThemeVariantChange,
  themeVariant
}: {
  appearance: XFlowEditorAppearance;
  onAppearanceChange: (appearance: XFlowEditorAppearance) => void;
  onThemeVariantChange: (themeVariant: XFlowEditorThemeVariant) => void;
  themeVariant: XFlowEditorThemeVariant;
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
          size="small"
          value={appearance}
          onChange={(nextAppearance) => onAppearanceChange(nextAppearance as XFlowEditorAppearance)}
        />
      </div>
      <div className="xflow-editor-theme-settings__row">
        <div>
          <span className="xflow-editor-theme-settings__label">工作台风格</span>
          <span className="xflow-editor-theme-settings__hint">Graphite 注重层级，Blueprint 强化连线</span>
        </div>
        <Segmented
          aria-label="工作台风格"
          classNames={{
            item: "xflow-editor-theme-segmented__item",
            label: "xflow-editor-theme-segmented__label",
            root: "xflow-editor-theme-segmented__root"
          }}
          rootClassName="xflow-editor-theme-segmented"
          options={[
            { label: "石墨", value: "graphite" },
            { label: "蓝图", value: "blueprint" }
          ]}
          size="small"
          value={themeVariant}
          onChange={(nextThemeVariant) =>
            onThemeVariantChange(nextThemeVariant as XFlowEditorThemeVariant)
          }
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
  ["xflow.subworkflow", { group: "流程控制", tone: "violet", icon: <ApartmentOutlined /> }],
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

function groupIcon(group: NodeDescriptor["group"]): React.ReactNode {
  if (group === "触发器") return <ThunderboltOutlined />;
  if (group === "流程控制") return <BranchesOutlined />;
  if (group === "数据转换") return <CodeOutlined />;
  if (group === "供应") return <DatabaseOutlined />;
  if (group === "其他") return <AppstoreOutlined />;
  return <AuditOutlined />;
}

function defaultSelectedKey(nodes: WorkflowNode[]): string | undefined {
  const switchIndex = nodes.findIndex((node) => node.type?.includes("switch"));
  if (switchIndex >= 0) return nodeKey(nodes[switchIndex], switchIndex);
  return nodes[0] ? nodeKey(nodes[0], 0) : undefined;
}

interface WorkflowDiagnostic {
  status: "pass" | "warn" | "error";
  area: string;
  message: string;
  /** The node the finding is about, set where it is created (never inferred from the message). */
  nodeName?: string;
}

function validateWorkflow(
  workflow: WorkflowDef,
  runtime?: RuntimeSnapshot,
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

  const edgeDiagnostics = diagnostics.slice(diagnosticsBeforeEdges);
  diagnostics.push({
    status: edgeDiagnostics.some((item) => item.status === "error") ? "error" : "pass",
    area: "edges",
    message: edgeDiagnostics.some((item) => item.status === "error")
      ? `${dataEdgeCount} 条数据连接和 ${dependencyEdgeCount} 条依赖连接存在错误`
      : `${dataEdgeCount} 条数据连接和 ${dependencyEdgeCount} 条依赖连接已解析`
  });
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
  diagnostics.push({
    status: "warn",
    area: "runtime",
    message: runtime ? "运行态数据来自最近一次执行快照" : "尚未运行，运行态数据为空"
  });
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
  searchInputRef
}: {
  descriptors: NodeDescriptor[];
  onAddNode: (descriptor: NodeDescriptor) => void;
  autoFocusSearch?: boolean;
  searchInputRef?: React.Ref<InputRef>;
}): React.ReactElement {
  const [searchVisible, setSearchVisible] = React.useState(autoFocusSearch);
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
      ariaLabel="节点"
      bodyClassName="xflow-editor-library__body"
      className="xflow-editor-library"
      icon={<AppstoreAddOutlined />}
      iconAriaLabel="添加节点图标"
      subtitle="拖入画布或点击添加"
      title="节点"
      extra={
        <Button
          aria-label="搜索节点"
          className="xflow-editor-library-search-toggle"
          color="default"
          data-active={searchVisible}
          size="small"
          variant="text"
          onClick={() => setSearchVisible((visible) => !visible)}
        >
          <SearchOutlined />
        </Button>
      }
    >
      {searchVisible ? (
        <Input
          autoFocus={autoFocusSearch}
          ref={searchInputRef}
          className="xflow-editor-library-search"
          classNames={editorInputClassNames}
          placeholder="搜索 Start / Switch / HTTP"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
      ) : null}
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
                <span className="xflow-editor-block-title">
                  {groupIcon(group)}
                  {group}
                </span>
                <span className="xflow-editor-node-group__count">{descriptors.length}</span>
              </span>
              <DownOutlined className={collapsed ? "xflow-editor-node-group__chevron is-collapsed" : "xflow-editor-node-group__chevron"} />
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
                      data-tone={descriptor.tone}
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
                      <span className="xflow-editor-node-tile__icon">{descriptor.icon}</span>
                      <span className="xflow-editor-node-tile__content">
                        <strong>{descriptor.label}</strong>
                        <small>{descriptor.type}</small>
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
                      <span aria-hidden="true" className="xflow-editor-node-tile__add">＋</span>
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
    </EditorPanel>
  );
}

function UnavailableControl({
  ariaLabel,
  children,
  className,
  reason
}: {
  ariaLabel: string;
  children: React.ReactNode;
  className?: string;
  reason: string;
}): React.ReactElement {
  return (
    <Tooltip classNames={editorTooltipClassNames} title={reason}>
      <span className="xflow-editor-disabled-control" title={reason}>
        <button aria-label={ariaLabel} className={className} disabled title={reason} type="button">
          {children}
        </button>
      </span>
    </Tooltip>
  );
}

function Diagnostics({
  workflow,
  runtime,
  diagnostics: allDiagnostics,
  operationError,
  blockedDraftNodes,
  collapsed,
  onToggle,
  onSelectNode,
  onRevealJson
}: {
  workflow: WorkflowDef;
  runtime?: RuntimeSnapshot;
  diagnostics: WorkflowDiagnostic[];
  operationError?: string;
  /** Nodes whose unparseable parameters JSON blocked the last save/run. */
  blockedDraftNodes?: string[];
  collapsed: boolean;
  onToggle: () => void;
  onSelectNode?: (nodeName: string) => void;
  onRevealJson?: (nodeName: string) => void;
}): React.ReactElement {
  const nodes = workflow.nodes ?? [];
  const connectionsCount = Object.values(workflow.connections ?? {}).reduce(
    (count, ports) => count + Object.values(ports).reduce((inner, portConnections) => inner + targetsFor(portConnections).length, 0),
    0
  );
  const runtimeNodes = Object.entries(runtime?.nodes ?? {});
  const runningNode = runtimeNodes.find(([, snapshot]) => snapshot.status === "running")?.[0];
  const failedCount = runtimeNodes.filter(([, snapshot]) => snapshot.status === "failed").length;
  const isWaitingForDefinition = nodes.length === 0 && !operationError;
  const diagnostics = isWaitingForDefinition ? [] : allDiagnostics;
  const errorCount = diagnostics.filter((item) => item.status === "error").length;
  const warningCount = diagnostics.filter((item) => item.status === "warn").length;
  const workflowValid = !isWaitingForDefinition && errorCount === 0;
  const lastDuration = runtimeNodes.reduce(
    (maxDuration, [, snapshot]) => Math.max(maxDuration, snapshot.durationMs ?? 0),
    0
  );

  return (
    <footer
      className="xflow-editor-diagnostics"
      role="region"
      aria-label="诊断台"
      data-collapsed={collapsed}
    >
      <div className="xflow-editor-diagnostics__bar">
        <div className="xflow-editor-diagnostics__tabs" role="toolbar" aria-label="诊断视图">
          <strong className="xflow-editor-block-title">
            <ConsoleSqlOutlined />
            诊断台
          </strong>
          <span className="xflow-editor-diagnostics__tab active">
            <WarningOutlined />
            问题
          </span>
          <UnavailableControl ariaLabel="运行日志" reason="运行日志尚未接入执行记录。">
            <FileSearchOutlined />运行日志
          </UnavailableControl>
          <UnavailableControl ariaLabel="输入" reason="运行输入快照尚未接入。">
            <ImportOutlined />输入
          </UnavailableControl>
          <UnavailableControl ariaLabel="输出" reason="运行输出快照尚未接入。">
            <ExportOutlined />输出
          </UnavailableControl>
        </div>
        <div className="xflow-editor-diagnostics__status">
          {isWaitingForDefinition ? (
            <>
              <span className="waiting"><ClockCircleOutlined /> 等待工作流数据</span>
              <span>0 nodes · 0 connections</span>
              <span>尚未运行</span>
            </>
          ) : (
            <>
              <span className={workflowValid ? "ok" : "warn"}>
                {workflowValid ? <CheckCircleOutlined /> : <WarningOutlined />} DSL {workflowValid ? "valid" : "invalid"}
              </span>
              <span>{errorCount} errors</span>
              <span>{warningCount} warnings</span>
              <span>last run {lastDuration > 0 ? `${(lastDuration / 1000).toFixed(1)}s` : "-"}</span>
            </>
          )}
          <button
            aria-label={collapsed ? "展开诊断台" : "收起诊断台"}
            className="xflow-editor-diagnostics-toggle"
            title={collapsed ? "展开诊断台" : "收起诊断台"}
            type="button"
            onClick={onToggle}
          >
            {collapsed ? <UpOutlined /> : <DownOutlined />}
          </button>
        </div>
      </div>
      {collapsed ? null : (
        <div className="xflow-editor-diagnostics__body">
          <section className="xflow-editor-diagnostics-log" aria-label="诊断问题">
            <div className="xflow-editor-diagnostics-section-title">
              <span className="xflow-editor-block-title">
                <CheckCircleOutlined />
                诊断问题
              </span>
              <span>{isWaitingForDefinition ? "等待节点" : `${errorCount} errors / ${warningCount} warnings`}</span>
            </div>
            <div className="xflow-editor-diagnostics-log__list">
              {isWaitingForDefinition ? (
                <p className="xflow-editor-diagnostics-waiting">
                  <span>waiting</span>
                  <span>载入或编辑真实工作流后，这里将列出 DSL、节点和连接的校验结果。</span>
                </p>
              ) : diagnostics.map((item, index) => (
                  <p
                    className="xflow-editor-diagnostics-log__row"
                    key={`${item.area}-${index}`}
                  >
                    <span className={item.status === "pass" ? "ok" : item.status === "warn" ? "warn" : "error"}>
                      {item.status === "pass" ? "pass" : item.status}
                    </span>
                    <span>{item.area}</span>
                    <span>
                      {item.message}
                      {item.area === "operation" && blockedDraftNodes?.length ? (
                        blockedDraftNodes.map((blockedName) => (
                          <button
                            className="xflow-editor-diagnostics-locate"
                            key={blockedName}
                            type="button"
                            onClick={() => onRevealJson?.(blockedName)}
                          >
                            修正 {blockedName} 的参数 JSON
                          </button>
                        ))
                      ) : item.nodeName && nodes.some((node, index) => nodeName(node, index) === item.nodeName) ? (
                        <button
                          className="xflow-editor-diagnostics-locate"
                          type="button"
                          onClick={() => onSelectNode?.(item.nodeName!)}
                        >
                          定位节点 {item.nodeName}
                        </button>
                      ) : null}
                    </span>
                  </p>
                ))}
            </div>
          </section>
          <section className="xflow-editor-diagnostics-summary">
            <div className="xflow-editor-diagnostics-section-title">
              <span className="xflow-editor-block-title">
                <CodeOutlined />
                DSL 摘要
              </span>
              <span>{isWaitingForDefinition ? "等待" : workflow.version ?? "spec 1.0"}</span>
            </div>
            <div className="xflow-editor-diagnostics-meter">
              <div><span>节点</span><span>{nodes.length}</span></div>
              <i style={{ width: `${isWaitingForDefinition ? 0 : Math.min(Math.max(nodes.length * 10, 16), 100)}%` }} />
            </div>
            <div className="xflow-editor-diagnostics-meter">
              <div><span>连接</span><span>{isWaitingForDefinition ? "0 / 0" : connectionsCount > 0 ? "完整" : "待连接"}</span></div>
              <i className="green" style={{ width: isWaitingForDefinition ? "0%" : connectionsCount > 0 ? "100%" : "18%" }} />
            </div>
            <div className="xflow-editor-diagnostics-kv">
              <span>凭据</span>
              <strong>{isWaitingForDefinition ? "等待真实数据" : nodes.length === 0 ? "未配置" : "运行时注入"}</strong>
            </div>
            <pre className="xflow-editor-diagnostics-dsl" aria-label="DSL 预览">
              {serializeJson(workflow)}
            </pre>
          </section>
          <section className="xflow-editor-diagnostics-run">
            <div className="xflow-editor-diagnostics-section-title">
              <span className="xflow-editor-block-title">
                <ClockCircleOutlined />
                最近运行
              </span>
              <span>{runtime ? "execution-local" : "尚未运行"}</span>
            </div>
            <div><span>状态</span><strong data-status={runtime?.status ?? "pending"}>{runtime?.status ?? "尚未运行"}</strong></div>
            <div><span>当前节点</span><code>{runningNode ?? "-"}</code></div>
            <div><span>失败节点</span><code>{failedCount}</code></div>
            <div><span>跟踪节点</span><code>{runtimeNodes.length}</code></div>
            <p><InfoCircleOutlined /> {runtime ? "后续接运行日志、输入和输出快照。" : "运行真实工作流后，这里显示执行摘要和输出入口。"}</p>
          </section>
        </div>
      )}
    </footer>
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
  runtime,
  onChange,
  onSave,
  onRun,
  onRunNode,
  nodeTypes: sourceNodeTypes,
  appearance: controlledAppearance,
  themeVariant: controlledThemeVariant,
  onAppearanceChange,
  onThemeVariantChange,
  defaultViewport,
  onViewportChange
}: XFlowEditorProps): React.ReactElement {
  // Memoised per response inside localizeNodeTypes, so identity-keyed caches
  // downstream (node library, form compiler) keep hitting.
  const nodeTypes = localizeNodeTypes(sourceNodeTypes, nodeFormCatalog);
  const [uncontrolledAppearance, setUncontrolledAppearance] =
    React.useState<XFlowEditorAppearance>("dark");
  const [uncontrolledThemeVariant, setUncontrolledThemeVariant] =
    React.useState<XFlowEditorThemeVariant>("graphite");
  const activeAppearance = controlledAppearance ?? uncontrolledAppearance;
  const activeThemeVariant = controlledThemeVariant ?? uncontrolledThemeVariant;
  const resolvedAppearance = useResolvedAppearance(activeAppearance);
  const editorThemeConfig = React.useMemo(
    () => createEditorThemeConfig(activeThemeVariant, resolvedAppearance),
    [activeThemeVariant, resolvedAppearance]
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
  const changeThemeVariant = React.useCallback(
    (nextThemeVariant: XFlowEditorThemeVariant) => {
      if (controlledThemeVariant === undefined) {
        setUncontrolledThemeVariant(nextThemeVariant);
      }
      onThemeVariantChange?.(nextThemeVariant);
    },
    [controlledThemeVariant, onThemeVariantChange]
  );
  const [draftWorkflow, setDraftWorkflow] = React.useState<WorkflowDef>(value);
  const draftRevisionRef = React.useRef(0);
  const latestDraftRef = React.useRef<WorkflowDef>(value);
  const historyRef = React.useRef<WorkflowHistory>({ undo: [], redo: [] });
  const [history, setHistory] = React.useState<WorkflowHistory>({ undo: [], redo: [] });
  const [canvasTool, setCanvasTool] = React.useState<CanvasTool>("select");
  const [localRuntime, setLocalRuntime] = React.useState<RuntimeSnapshot | undefined>(runtime);
  const compactViewport = useCompactViewport();
  const [layoutPolicy, setLayoutPolicy] = React.useState<LayoutPolicy>("auto");
  const layout: EditorLayout = layoutPolicy === "compact" || compactViewport ? "c" : "a";
  const [compactDrawer, setCompactDrawer] = React.useState<"left" | "right" | undefined>();
  const [leftDrawerTab, setLeftDrawerTab] = React.useState<"library" | "variables">("library");
  const [desktopRailSection, setDesktopRailSection] = React.useState<"library" | "variables">("library");
  const nodeLibrarySearchRef = React.useRef<InputRef | null>(null);
  const [nodeSearchOpen, setNodeSearchOpen] = React.useState(false);
  const nodeSearchRef = React.useRef<RefSelectProps | null>(null);
  const title = draftWorkflow.name ?? "Untitled workflow";
  const nodes = draftWorkflow.nodes ?? [];
  const [viewMode, setViewMode] = React.useState<"edit" | "preview">("edit");
  const [leftCollapsed, setLeftCollapsed] = React.useState(false);
  const [rightCollapsed, setRightCollapsed] = React.useState(false);
  const [shortcutsOpen, setShortcutsOpen] = React.useState(false);
  // Keep the workbench canvas-first. Validation, execution, and failures expand
  // this surface on demand so operational feedback is never hidden.
  const [bottomCollapsed, setBottomCollapsed] = React.useState(true);
  const [saving, setSaving] = React.useState(false);
  // Render state lags a same-tick second trigger (button + shortcut); the ref
  // is the in-flight guard, `saving` only drives the button's loading state.
  const savingRef = React.useRef(false);
  const [running, setRunning] = React.useState(false);
  const [operationStatus, setOperationStatus] = React.useState(() => value.id ? "已保存" : "未保存");
  const [isDirty, setIsDirty] = React.useState(() => !value.id);
  const [operationError, setOperationError] = React.useState<string>();
  const [selectedKey, setSelectedKey] = React.useState<string | undefined>(() => defaultSelectedKey(nodes));
  const selectedIndex = nodes.findIndex((node, index) => nodeKey(node, index) === selectedKey);
  const selectedNode = selectedIndex >= 0 ? nodes[selectedIndex] : undefined;
  const editMode = viewMode === "edit";
  // The node library only exists in edit mode; preview falls back to the read-only variables.
  const leftSection = editMode ? desktopRailSection : "variables";
  const leftDrawerSection = editMode ? leftDrawerTab : "variables";
  const hostNotices = [
    !onSave ? "保存不可用：宿主未提供 onSave 处理器" : undefined,
    !onRun ? "运行不可用：宿主未提供 onRun 处理器" : undefined
  ].filter((notice): notice is string => Boolean(notice));
  const workflowSummary = [
    `${nodes.length} 个节点`,
    draftWorkflow.settings?.timezone ? `时区 ${draftWorkflow.settings.timezone}` : undefined,
    draftWorkflow.settings?.timeout !== undefined ? `超时 ${draftWorkflow.settings.timeout}` : undefined
  ]
    .filter((detail): detail is string => Boolean(detail))
    .join(" / ");

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
    setIsDirty(!value.id);
    setOperationError(undefined);
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
      setIsDirty(true);
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
  const saveBlocked = invalidDraftCount > 0 || invalidWorkflowJsonFields.length > 0
    || (validationMode === "enforce" && formIssueSummary.errors > 0);
  const saveTooltip = !onSave
    ? "保存不可用：宿主未提供 onSave 处理器"
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
  const blockedByParameterDraft = React.useCallback((action: "保存" | "运行"): boolean => {
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
      setBottomCollapsed(false);
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
      setBottomCollapsed(false);
      return true;
    }
    const summary = formIssueSummaryRef.current;
    if (nodeTypesRef.current?.param_validation_mode === "enforce" && summary.errors > 0) {
      setOperationError(`节点 ${summary.nodes.join("、")} 的参数未通过校验（${summary.errors} 个错误），请修正后再${action}`);
      setOperationStatus(`${action}已阻止`);
      setBottomCollapsed(false);
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
    setIsDirty(true);
    setOperationStatus("未保存");
    setOperationError(undefined);
    onChange?.(nextWorkflow);
  }, [onChange]);

  // Undo/redo first land a pending parseable draft as its own undo entry, but
  // without clearing redo: the flush only records text the user already typed,
  // it is not a new branch point. Redo then still applies, and one undo after
  // it gets the drafted text back.
  const undoWorkflow = React.useCallback(() => {
    flushParameterDrafts(true);
    const previousWorkflow = historyRef.current.undo.at(-1);
    if (!previousWorkflow) return;
    restoreHistory(previousWorkflow, {
      undo: historyRef.current.undo.slice(0, -1),
      redo: [...historyRef.current.redo, latestDraftRef.current].slice(-historyLimit)
    });
  }, [flushParameterDrafts, restoreHistory]);

  const redoWorkflow = React.useCallback(() => {
    flushParameterDrafts(true);
    const nextWorkflow = historyRef.current.redo.at(-1);
    if (!nextWorkflow) return;
    restoreHistory(nextWorkflow, {
      undo: [...historyRef.current.undo, latestDraftRef.current].slice(-historyLimit),
      redo: historyRef.current.redo.slice(0, -1)
    });
  }, [flushParameterDrafts, restoreHistory]);

  const createWorkflow = React.useCallback(() => {
    const nextWorkflow = createDraftWorkflow();
    commitWorkflow(nextWorkflow);
    setSelectedKey(undefined);
  }, [commitWorkflow]);

  // Computed once and shared by the Diagnostics panel and the validate button,
  // so the button's verdict always matches the panel's findings.
  const diagnostics = React.useMemo(
    () => validateWorkflow(draftWorkflow, localRuntime, operationError, nodeTypes),
    [draftWorkflow, localRuntime, nodeTypes, operationError]
  );
  const validateCurrentWorkflow = React.useCallback(() => {
    const hasErrors = diagnostics.some((item) => item.status === "error");
    setOperationStatus(hasErrors ? "校验失败" : "校验通过");
    setBottomCollapsed(false);
  }, [diagnostics]);

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

  // Compact layout has no collapsed rail, so the same intent opens or closes the
  // drawer instead of collapsing a panel that is not there.
  const toggleLeftPanel = React.useCallback(() => {
    if (layout === "a") {
      setLeftCollapsed((current) => !current);
      return;
    }
    setCompactDrawer((current) => (current === "left" ? undefined : "left"));
  }, [layout]);

  // Escape never clears the selection: it leaves a transient mode. Deselecting
  // (to reach the workflow settings) is the Inspector header's explicit action.
  const leaveTransientMode = React.useCallback(() => {
    if (shortcutsOpen) {
      setShortcutsOpen(false);
      return;
    }
    if (compactDrawer) {
      setCompactDrawer(undefined);
      return;
    }
    setCanvasTool((current) => (current === "connect" ? "select" : current));
  }, [compactDrawer, shortcutsOpen]);

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
    if (!onSave) {
      setOperationError("保存不可用：宿主未提供 onSave 处理器");
      setOperationStatus("保存不可用");
      setBottomCollapsed(false);
      return;
    }
    if (savingRef.current) return;
    if (blockedByParameterDraft("保存")) return;
    // The ref, not render state: it already includes writes committed earlier
    // in this tick (the shortcut can fire from inside a text field).
    const workflowToSave = latestDraftRef.current;
    const revisionAtSave = draftRevisionRef.current;
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
      // the save was in flight. Those edits remain dirty and can be saved next.
      if (draftRevisionRef.current !== revisionAtSave) return;
      latestDraftRef.current = savedWorkflow;
      setDraftWorkflow(savedWorkflow);
      setIsDirty(false);
      setOperationStatus("已保存");
    } catch (error) {
      const rejectedIssues = paramIssuesOf(error);
      if (rejectedIssues) setParamIssues(rejectedIssues.length > 0 ? rejectedIssues : undefined);
      if (draftRevisionRef.current === revisionAtSave) {
        setOperationError(errorMessage(error, "保存失败"));
        setOperationStatus("保存失败");
        setBottomCollapsed(false);
      }
      // The host application owns error presentation.
    } finally {
      savingRef.current = false;
      setSaving(false);
    }
  }, [blockedByParameterDraft, onSave]);

  const runWorkflow = React.useCallback(async () => {
    if (!onRun) {
      setOperationError("运行不可用：宿主未提供 onRun 处理器");
      setOperationStatus("运行不可用");
      setBottomCollapsed(false);
      return;
    }
    if (blockedByParameterDraft("运行")) return;
    setRunning(true);
    setOperationError(undefined);
    try {
      const nextRuntime = await onRun(latestDraftRef.current);
      if (!nextRuntime) throw new Error("运行处理器未返回运行状态快照");
      setLocalRuntime(nextRuntime);
      setOperationStatus("运行完成");
      setBottomCollapsed(false);
    } catch (error) {
      setOperationError(errorMessage(error, "运行失败"));
      setOperationStatus("运行失败");
      setBottomCollapsed(false);
      // The host application owns error presentation.
    } finally {
      setRunning(false);
    }
  }, [blockedByParameterDraft, onRun]);

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
    if (layout === "a") setCompactDrawer(undefined);
  }, [layout]);

  const focusNodeLibrarySearch = React.useCallback(() => {
    nodeLibrarySearchRef.current?.focus({ preventScroll: true });
  }, []);

  React.useEffect(() => {
    if (layout === "c" && compactDrawer === "left" && leftDrawerTab === "library" && editMode) {
      focusNodeLibrarySearch();
    }
  }, [compactDrawer, editMode, focusNodeLibrarySearch, layout, leftDrawerTab]);

  const selectNodeByName = React.useCallback((name: string) => {
    const index = nodes.findIndex((node, nodeIndex) => nodeName(node, nodeIndex) === name);
    if (index >= 0) setSelectedKey(nodeKey(nodes[index], index));
  }, [nodes]);

  const openCompactDrawer = React.useCallback((drawer: "left" | "right") => {
    setCompactDrawer(drawer);
  }, []);

  /** Doc C §6.2: from a blocked save/run, jump to the node's parameters JSON. */
  const revealParameterJson = React.useCallback((name: string) => {
    selectNodeByName(name);
    setViewMode("edit");
    setParameterTab("json");
    setInspectorRevealNonce((nonce) => nonce + 1);
    if (layout === "a") setRightCollapsed(false);
    else openCompactDrawer("right");
  }, [layout, openCompactDrawer, selectNodeByName]);

  const openNodeLibrary = React.useCallback(() => {
    setViewMode("edit");
    setLeftCollapsed(false);
    setDesktopRailSection("library");
    if (layout === "c") {
      setLeftDrawerTab("library");
      openCompactDrawer("left");
    }
  }, [layout, openCompactDrawer]);

  const nodeSearchOptions = React.useMemo<NodeSearchOption[]>(
    () => nodes.map((node, index) => ({
      key: nodeKey(node, index),
      name: nodeName(node, index),
      label: nodeLabel(node, index),
      type: node.type ?? "unknown"
    })),
    [nodes]
  );
  const openNodeSearch = React.useCallback(() => {
    if (nodes.length === 0) return;
    setNodeSearchOpen(true);
    nodeSearchRef.current?.focus();
  }, [nodes.length]);
  const locateNode = React.useCallback((key: string) => {
    setSelectedKey(key);
    if (layout === "a") setRightCollapsed(false);
  }, [layout]);
  const clearSelection = React.useCallback(() => setSelectedKey(undefined), []);

  /** Look (icon, tone) of the selected node's type, from the node library. */
  const selectedPresentation = React.useMemo<NodePresentationHint | undefined>(() => {
    const type = selectedNode?.type;
    if (!type) return undefined;
    const descriptor = libraryDescriptors.find((candidate) => candidate.type === type);
    return descriptor ? { icon: descriptor.icon, tone: descriptor.tone } : undefined;
  }, [libraryDescriptors, selectedNode?.type]);

  // ---- Canvas context menu -------------------------------------------------

  const [contextMenu, setContextMenu] = React.useState<ContextMenuRequest>();
  /** Bumped to ask the Inspector to put the caret in the node-name field. */
  const [nameFocusNonce, setNameFocusNonce] = React.useState(0);
  const closeContextMenu = React.useCallback(() => setContextMenu(undefined), []);

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
    setViewMode("edit");
    setInspectorRevealNonce((nonce) => nonce + 1);
    if (layout === "a") setRightCollapsed(false);
    else openCompactDrawer("right");
  }, [layout, openCompactDrawer]);

  /** 重命名: the same, on the parameters tab, with the caret in 节点名称. */
  const renameNodeFromMenu = React.useCallback((key: string) => {
    setSelectedKey(key);
    setViewMode("edit");
    setParameterTab("form");
    setInspectorRevealNonce((nonce) => nonce + 1);
    setNameFocusNonce((nonce) => nonce + 1);
    if (layout === "a") setRightCollapsed(false);
    else openCompactDrawer("right");
  }, [layout, openCompactDrawer]);

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
      setOperationStatus("运行完成");
      setBottomCollapsed(false);
    } catch (error) {
      setOperationError(errorMessage(error, "运行失败"));
      setOperationStatus("运行失败");
      setBottomCollapsed(false);
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
          if (onSave) void saveWorkflow();
          return;
        case "run":
          if (onRun && !running) void runWorkflow();
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
        case "selectTool":
          setCanvasTool("select");
          return;
        case "connectTool":
          setCanvasTool("connect");
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
        if (shortcut.preventDefault !== false) event.preventDefault();
        runShortcut(shortcut.id);
        return;
      }
    };
    window.addEventListener("keydown", handleCanvasShortcut);
    return () => window.removeEventListener("keydown", handleCanvasShortcut);
  }, [
    contextMenu,
    duplicateSelectedNode,
    leaveTransientMode,
    onRun,
    onSave,
    openNodeSearch,
    redoWorkflow,
    running,
    runWorkflow,
    saveWorkflow,
    toggleLeftPanel,
    undoWorkflow
  ]);

  const inspectorElement = (
    <Inspector
      externalIssues={selectedNode ? externalIssuesMap.get(nodeName(selectedNode, selectedIndex)) : undefined}
      formHost={formHost}
      nameFocusNonce={nameFocusNonce}
      nodeTypes={nodeTypes}
      parameterDrafts={parameterDrafts}
      parameterTab={parameterTab}
      presentation={selectedPresentation}
      revealNonce={inspectorRevealNonce}
      runtime={localRuntime}
      selectedIndex={selectedIndex}
      selectedNode={selectedNode}
      workflow={draftWorkflow}
      workflowJsonDrafts={workflowJsonDrafts}
      onApplyNodePatches={applyNodePatches}
      onChangeWith={commitWorkflowWith}
      onClearSelection={clearSelection}
      onDeleteNode={deleteSelectedNode}
      onNodeRenamed={handleNodeRenamed}
      onParameterCommit={commitParameterDraft}
      onParameterTabChange={setParameterTab}
      onParameterText={updateParameterText}
      onWorkflowJsonText={updateWorkflowJsonText}
    />
  );

  return (
    <ConfigProvider
      componentSize="small"
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
        data-appearance={activeAppearance}
        data-canvas-tool={canvasTool}
        data-layout={layout}
        data-layout-policy={layoutPolicy}
        data-theme={resolvedAppearance}
        data-theme-variant={activeThemeVariant}
        data-view-mode={viewMode}
      >
      <header className="xflow-editor-toolbar" role="toolbar" aria-label="编辑器工具栏">
        <div className="xflow-editor-title">
          <span>X</span>
          <div>
            <div className="xflow-editor-title__heading">
              <strong>{title}</strong>
              <em>spec {draftWorkflow.spec ?? "未指定"}</em>
              {draftWorkflow.version ? <em className="blue">v{draftWorkflow.version}</em> : null}
              <em className="amber">草稿</em>
              <em className={isDirty ? "amber" : "blue"}>{operationStatus}</em>
            </div>
            <p>{workflowSummary}</p>
          </div>
        </div>
        <div
          className="xflow-editor-tool-strip"
          role="group"
          aria-label="常用画布与历史工具"
        >
          <div className="xflow-editor-tool-group">
            <button
              aria-label="选择工具"
              aria-pressed={canvasTool === "select"}
              className={`xflow-editor-tool-button ${canvasTool === "select" ? "active" : ""}`}
              title="选择工具（V）"
              type="button"
              onClick={() => setCanvasTool("select")}
            >
              <AimOutlined />
            </button>
            <button
              aria-label="连线工具"
              aria-pressed={canvasTool === "connect"}
              className={`xflow-editor-tool-button ${canvasTool === "connect" ? "active" : ""}`}
              title="连线工具（L）"
              type="button"
              onClick={() => setCanvasTool("connect")}
            >
              <ApartmentOutlined />
            </button>
            <span className="xflow-editor-tool-divider xflow-editor-tool-divider--history" aria-hidden="true" />
            <button
              aria-label="撤销"
              className="xflow-editor-tool-button xflow-editor-tool-button--history"
              disabled={history.undo.length === 0}
              title="撤销（⌘ / Ctrl + Z）"
              type="button"
              onClick={undoWorkflow}
            >
              <UndoOutlined />
            </button>
            <button
              aria-label="重做"
              className="xflow-editor-tool-button xflow-editor-tool-button--history"
              disabled={history.redo.length === 0}
              title="重做（⌘ / Ctrl + Shift + Z）"
              type="button"
              onClick={redoWorkflow}
            >
              <RedoOutlined />
            </button>
          </div>
          <div className="xflow-editor-tool-group xflow-editor-tool-group--search">
            <NodeSearch
              nodes={nodeSearchOptions}
              open={nodeSearchOpen}
              ref={nodeSearchRef}
              onOpenChange={setNodeSearchOpen}
              onSelect={locateNode}
            />
          </div>
        </div>
        <div className="xflow-editor-toolbar-actions">
          {hostNotices.length > 0 ? <span className="xflow-editor-integration-notice" role="status">{hostNotices.join("；")}</span> : null}
          <Popover
            classNames={{
              root: "xflow-editor-theme-popover",
              content: "xflow-editor-theme-popover__content",
              title: "xflow-editor-theme-popover__title"
            }}
            content={
              <ThemeSettings
                appearance={activeAppearance}
                themeVariant={activeThemeVariant}
                onAppearanceChange={changeAppearance}
                onThemeVariantChange={changeThemeVariant}
              />
            }
            placement="bottomRight"
            title="主题设置"
            trigger="click"
          >
            <Button
              aria-label="主题设置"
              className="xflow-editor-theme-trigger"
              icon={<BgColorsOutlined />}
            />
          </Popover>
          <Segmented
            classNames={{
              item: "xflow-editor-mode-segmented__item",
              label: "xflow-editor-mode-segmented__label",
              root: "xflow-editor-mode-segmented__root"
            }}
            rootClassName="xflow-editor-mode-segmented"
            value={viewMode}
            options={[
              { icon: <EditOutlined />, label: "编辑", value: "edit" },
              { icon: <EyeOutlined />, label: "预览", value: "preview" }
            ]}
            onChange={(nextView) => setViewMode(nextView as "edit" | "preview")}
          />
          <Space className="xflow-editor-workflow-actions">
            <Tooltip classNames={editorTooltipClassNames} title={layoutPolicy === "auto" ? "进入沉浸布局" : "恢复自动布局"}>
              <Button
                aria-label={layoutPolicy === "auto" ? "进入沉浸布局" : "恢复自动布局"}
                className="xflow-editor-toolbar-icon-action xflow-editor-layout-toggle"
                icon={<DesktopOutlined />}
                onClick={() => setLayoutPolicy((policy) => policy === "auto" ? "compact" : "auto")}
              />
            </Tooltip>
            <Tooltip classNames={editorTooltipClassNames} title={editMode ? "新建工作流" : "预览模式为只读，请切换到编辑模式。"}>
              <span>
                <Button aria-label="新建工作流" className="xflow-editor-toolbar-icon-action xflow-editor-new-button" disabled={!editMode} icon={<FileAddOutlined />} onClick={createWorkflow} />
              </span>
            </Tooltip>
            <Button aria-label="校验" className="xflow-editor-validate-button" icon={<CheckCircleOutlined />} onClick={validateCurrentWorkflow}>校验</Button>
            <Tooltip classNames={editorTooltipClassNames} title={saveTooltip}>
              <span>
                <Button
                  aria-label="保存"
                  className="xflow-editor-save-button"
                  color={saveBlocked ? "danger" : isDirty ? "primary" : "default"}
                  data-blocked={saveBlocked || undefined}
                  disabled={!onSave}
                  icon={saveBlocked ? <WarningOutlined /> : <SaveOutlined />}
                  loading={saving}
                  variant={isDirty || saveBlocked ? "solid" : "outlined"}
                  onClick={() => void saveWorkflow()}
                >
                  保存
                  {formIssueSummary.errors > 0 ? (
                    <span className="xflow-editor-save-issues" data-testid="save-issue-count">
                      {formIssueSummary.errors}
                    </span>
                  ) : null}
                </Button>
              </span>
            </Tooltip>
            <Tooltip classNames={editorTooltipClassNames} title={onRun ? "运行工作流（⌘ / Ctrl + Enter）" : "运行不可用：宿主未提供 onRun 处理器"}>
              <span>
                <Button
                  aria-label="运行工作流"
                  className="xflow-editor-run-button"
                  color="primary"
                  disabled={running || !onRun}
                  icon={<PlaySquareOutlined />}
                  loading={running}
                  variant="solid"
                  onClick={() => void runWorkflow()}
                >
                  运行
                </Button>
              </span>
            </Tooltip>
            <Tooltip classNames={editorTooltipClassNames} title="发布流程尚未接入。">
              <span>
                <Button aria-label="发布" className="xflow-editor-publish-button" color="primary" disabled icon={<CloudUploadOutlined />} variant="solid">发布</Button>
              </span>
            </Tooltip>
          </Space>
        </div>
      </header>

      <div className="xflow-editor-main" data-left-collapsed={leftCollapsed} data-right-collapsed={rightCollapsed}>
        {layout === "a" ? (
          <aside className="xflow-editor-left">
            <Button
              aria-label={leftCollapsed ? "展开左侧面板" : "收起左侧面板"}
              className="xflow-editor-panel-handle xflow-editor-panel-handle--left"
              size="small"
              title="收起 / 展开左侧面板（⌘ / Ctrl + B）"
              onClick={() => setLeftCollapsed((current) => !current)}
            >
              {leftCollapsed ? <RightOutlined /> : <LeftOutlined />}
            </Button>
            {leftCollapsed ? null : (
              <>
                <nav className="xflow-editor-rail xflow-editor-desktop-rail" aria-label="工作台导航">
                  <button
                    aria-label="节点"
                    aria-pressed={leftSection === "library"}
                    className={leftSection === "library" ? "active" : ""}
                    disabled={!editMode}
                    title={editMode ? "节点库" : "预览模式为只读，请切换到编辑模式。"}
                    type="button"
                    onClick={() => setDesktopRailSection("library")}
                  >
                    <AppstoreAddOutlined />
                    <span>节点</span>
                  </button>
                  <button
                    aria-label="变量"
                    aria-pressed={leftSection === "variables"}
                    className={leftSection === "variables" ? "active" : ""}
                    type="button"
                    onClick={() => setDesktopRailSection("variables")}
                  >
                    <FunctionOutlined />
                    <span>变量</span>
                  </button>
                  <button
                    aria-label="快捷键帮助"
                    className="xflow-editor-rail-help"
                    title="快捷键（?）"
                    type="button"
                    onClick={() => setShortcutsOpen(true)}
                  >
                    <QuestionCircleOutlined />
                    <span>帮助</span>
                  </button>
                </nav>
                <div className={`xflow-editor-left__content ${editMode ? "" : "xflow-editor-left__content--preview"}`}>
                  {leftSection === "library" ? (
                    <NodeLibrary descriptors={libraryDescriptors} onAddNode={addNode} />
                  ) : (
                    <VariablesPanel
                      readOnly={!editMode}
                      workflow={draftWorkflow}
                      workflowJsonDrafts={workflowJsonDrafts}
                      onChangeWith={commitWorkflowWith}
                      onWorkflowJsonText={updateWorkflowJsonText}
                    />
                  )}
                </div>
              </>
            )}
          </aside>
        ) : (
          <aside className="xflow-editor-rail xflow-editor-compact-rail xflow-editor-compact-rail--left" aria-label="紧凑导航">
            <button
              aria-label="节点"
              className={compactDrawer === "left" && leftDrawerSection === "library" ? "active" : ""}
              disabled={!editMode}
              title={editMode ? "节点库" : "预览模式为只读，请切换到编辑模式。"}
              type="button"
              onClick={() => {
                setLeftDrawerTab("library");
                openCompactDrawer("left");
              }}
            >
              <AppstoreAddOutlined />
              <span>节点</span>
            </button>
            <button
              aria-label="变量"
              className={compactDrawer === "left" && leftDrawerSection === "variables" ? "active" : ""}
              type="button"
              onClick={() => {
                setLeftDrawerTab("variables");
                openCompactDrawer("left");
              }}
            >
              <FunctionOutlined />
              <span>变量</span>
            </button>
            <button
              aria-label="快捷键帮助"
              className="xflow-editor-rail-help"
              title="快捷键（?）"
              type="button"
              onClick={() => setShortcutsOpen(true)}
            >
              <QuestionCircleOutlined />
              <span>帮助</span>
            </button>
          </aside>
        )}

        <section className="xflow-editor-canvas" aria-label="画布">
          <div className="xflow-editor-canvas__frame">
            <div className="xflow-editor-canvas__ruler-corner" aria-hidden="true" />
            <div className="xflow-editor-canvas__ruler xflow-editor-canvas__ruler--x" aria-hidden="true">
            {rulerMarks.map((mark, index) => (
              <span key={mark} style={{ left: `${index * rulerStepPx + 5}px` }}>{mark}</span>
            ))}
            </div>
            <div className="xflow-editor-canvas__ruler xflow-editor-canvas__ruler--y" aria-hidden="true">
              {rulerMarks.slice(0, 7).map((mark, index) => (
                <span key={mark} style={{ top: `${index * rulerStepPx + 6}px` }}>{mark}</span>
              ))}
            </div>
            <div className="xflow-editor-preview-frame">
              {!editMode ? (
                <div className="xflow-editor-canvas-mode" role="status">
                  <strong>预览模式</strong>
                  <span>画布为只读；切换到编辑模式后才能修改工作流</span>
                </div>
              ) : (
                <div className="xflow-editor-canvas-context">
                  <i />
                  <strong>{canvasTool === "connect" ? "连线工具" : "编辑工作台"}</strong>
                  <span>{canvasTool === "connect" ? "从输出端口拖至目标输入端口" : "从节点库开始构建真实工作流"}</span>
                </div>
              )}
              {nodes.length === 0 ? (
                <div className="xflow-editor-empty-canvas">
                  <span className="xflow-editor-empty-canvas__icon"><AppstoreAddOutlined /></span>
                  <h1>等待真实工作流数据</h1>
                  <p>这里不会预置任何业务节点或连接。创建或载入工作流后，画布将按照真实定义呈现。</p>
                  <button type="button" onClick={openNodeLibrary}>打开节点库</button>
                </div>
              ) : null}
              <XFlowPreview
                workflow={draftWorkflow}
                runtime={localRuntime}
                className="xflow-editor-preview"
                editable={editMode}
                interactionMode={canvasTool}
                // "" rather than undefined: an empty editor selection must not
                // fall back to the preview's own uncontrolled selection.
                selectedNodeId={selectedKey ?? ""}
                onSelectNode={setSelectedKey}
                onNodePositionChange={updateNodePosition}
                onConnect={addCanvasConnection}
                onDeleteConnection={removeCanvasConnection}
                onDeleteNode={deleteSelectedNode}
                onDropNode={dropNode}
                danglingPorts={canvasDanglingPorts}
                defaultViewport={defaultViewport}
                onViewportChange={onViewportChange}
                // Preview mode keeps the browser's own menu: the editor has
                // nothing to offer there.
                onNodeContextMenu={editMode ? openNodeContextMenu : undefined}
                onPaneContextMenu={editMode ? openPaneContextMenu : undefined}
                onReady={handlePreviewReady}
              />
            </div>
          </div>
        </section>

        {layout === "a" ? (
          <aside className="xflow-editor-right">
            <Button
              aria-label={rightCollapsed ? "展开属性面板" : "收起属性面板"}
              className="xflow-editor-panel-handle xflow-editor-panel-handle--right"
              size="small"
              onClick={() => setRightCollapsed((current) => !current)}
            >
              {rightCollapsed ? <LeftOutlined /> : <RightOutlined />}
            </Button>
            {rightCollapsed ? (
              <div className="xflow-editor-right__strip">属性</div>
            ) : !editMode ? (
              <section className="xflow-editor-preview-inspector" aria-label="预览模式说明">
                <EyeOutlined />
                <strong>预览模式</strong>
                <p>画布、节点属性和工作流定义均保持只读。切换到编辑模式后可继续修改。</p>
              </section>
            ) : (
              inspectorElement
            )}
          </aside>
        ) : (
          <aside className="xflow-editor-rail xflow-editor-compact-rail xflow-editor-compact-rail--right" aria-label="紧凑属性">
            <button
              aria-label="属性"
              className={compactDrawer === "right" ? "active" : ""}
              type="button"
              onClick={() => openCompactDrawer("right")}
            >
              <SettingOutlined />
              <span>属性</span>
            </button>
          </aside>
        )}
      </div>

      {layout === "c" ? (
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
          open={compactDrawer === "left"}
          placement="left"
          classNames={editorCompactDrawerClassNames}
          rootClassName="xflow-editor-compact-drawer"
          rootStyle={{ left: 52, position: "absolute" }}
          title={leftDrawerSection === "library" ? "节点" : "变量"}
          size={340}
          onClose={() => setCompactDrawer(undefined)}
        >
          {leftDrawerSection === "library" ? (
            <NodeLibrary autoFocusSearch descriptors={libraryDescriptors} onAddNode={addNode} searchInputRef={nodeLibrarySearchRef} />
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

      {layout === "c" ? (
        <Drawer
          destroyOnHidden
          focusable={{ trap: true, focusTriggerAfterClose: true }}
          getContainer={false}
          keyboard
          mask={false}
          open={compactDrawer === "right"}
          placement="right"
          classNames={editorCompactDrawerClassNames}
          rootClassName="xflow-editor-compact-drawer"
          rootStyle={{ position: "absolute" }}
          title="属性"
          size={360}
          onClose={() => setCompactDrawer(undefined)}
        >
          {editMode ? (
            inspectorElement
          ) : (
            <section className="xflow-editor-preview-inspector" aria-label="预览模式说明">
              <EyeOutlined />
              <strong>预览模式</strong>
              <p>画布、节点属性和工作流定义均保持只读。切换到编辑模式后可继续修改。</p>
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
      <Diagnostics
        workflow={draftWorkflow}
        runtime={localRuntime}
        diagnostics={diagnostics}
        blockedDraftNodes={operationError ? blockedDraftNodes : undefined}
        onRevealJson={revealParameterJson}
        operationError={operationError}
        collapsed={bottomCollapsed}
        onToggle={() => setBottomCollapsed((current) => !current)}
        onSelectNode={selectNodeByName}
      />
      <ShortcutsHelp open={shortcutsOpen} onClose={() => setShortcutsOpen(false)} />
      </section>
    </ConfigProvider>
  );
}

export { XFlowPreview };
