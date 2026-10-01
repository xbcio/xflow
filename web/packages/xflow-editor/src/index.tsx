import {
  AimOutlined,
  ApartmentOutlined,
  AppstoreAddOutlined,
  AppstoreOutlined,
  AuditOutlined,
  BarsOutlined,
  BgColorsOutlined,
  BranchesOutlined,
  CheckCircleOutlined,
  CloudServerOutlined,
  CloudUploadOutlined,
  ClockCircleOutlined,
  CodeOutlined,
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
  ForkOutlined,
  InfoCircleOutlined,
  GlobalOutlined,
  ImportOutlined,
  LeftOutlined,
  LinkOutlined,
  MergeCellsOutlined,
  MoonOutlined,
  NotificationOutlined,
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
  Card,
  ConfigProvider,
  Drawer,
  Input,
  Modal,
  Popover,
  Select,
  Segmented,
  Space,
  Tabs,
  Tag,
  Tooltip,
  theme,
  type InputRef,
  type ThemeConfig
} from "antd";
import type {
  Connection,
  ConnectionTargets,
  ConnectionType,
  Connections,
  GroupDef,
  NodeTypesResponse,
  ParamIssue,
  RuntimeNodeSnapshot,
  RuntimeSnapshot,
  WorkflowDef,
  WorkflowNode
} from "@xflow/core";
import { declaredOutputPorts } from "@xflow/core";
import type { Issue, Patch } from "@xflow/composer/core";
import { Composer, type Registry } from "@xflow/composer/react";
import { XFlowPreview, XFLOW_NODE_DRAG_MIME, type PreviewConnection } from "@xflow/preview";
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
  schemaForNode,
  translateNodeFormText,
  withDynamicPortsFallback,
  type DanglingEdge
} from "./node-form";
import type { NodeFormKind, NodeFormPort, NodeFormPorts, NodeFormSchema, NodeFormSource } from "./node-form/schema";
// Composer form styles first so the editor's scoped overrides win on equal specificity.
import "@xflow/composer/form/styles.css";
import "./styles.css";

/**
 * Node Descriptor text arrives in English (GET /v1/node-types); the editor UI
 * is Chinese, so the response is localized once where it enters the editor
 * (Doc C §8 item 1). Custom types without catalog entries stay English.
 */
const nodeFormCatalog = nodeFormCatalogs["zh-CN"];

export type XFlowEditorAppearance = "light" | "dark" | "system";
export type XFlowEditorThemeVariant = "graphite" | "blueprint";
type ResolvedXFlowEditorAppearance = Exclude<XFlowEditorAppearance, "system">;
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

/**
 * Whether a key event belongs to a text field that owns the keystroke. React
 * Flow skips its own Delete handling in exactly this case, so matching it keeps
 * a backspace while renaming a node from acting on the canvas behind it.
 */
function isTextEntryTarget(target: EventTarget | null): boolean {
  return target instanceof HTMLElement
    && (target.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName));
}
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
}

interface EditorThemePalette {
  app: string;
  border: string;
  borderStrong: string;
  field: string;
  hover: string;
  panel: string;
  panelRaised: string;
  primary: string;
  text: string;
  textSecondary: string;
  textTertiary: string;
  toolbar: string;
  success: string;
  warning: string;
  error: string;
}

const editorThemePalettes: Record<
  XFlowEditorThemeVariant,
  Record<ResolvedXFlowEditorAppearance, EditorThemePalette>
> = {
  graphite: {
    light: {
      app: "#f5f6f8",
      border: "#d9dee7",
      borderStrong: "#bcc5d0",
      field: "#ffffff",
      hover: "#edf3ff",
      panel: "#ffffff",
      panelRaised: "#f7f8fa",
      primary: "#2f7cff",
      text: "#1f2329",
      textSecondary: "#4e5969",
      textTertiary: "#86909c",
      toolbar: "#ffffff",
      success: "#1a9b67",
      warning: "#c98600",
      error: "#d9363e"
    },
    dark: {
      app: "#151617",
      border: "#3a3d43",
      borderStrong: "#4b5058",
      field: "#1e2023",
      hover: "#30343a",
      panel: "#24262a",
      panelRaised: "#2b2e33",
      primary: "#4c8dff",
      text: "#f2f4f7",
      textSecondary: "#c8cdd5",
      textTertiary: "#a4abb6",
      toolbar: "#1e1f21",
      success: "#35c98b",
      warning: "#f7b955",
      error: "#ff5f57"
    }
  },
  blueprint: {
    light: {
      app: "#f6f8fc",
      border: "#d5dfeb",
      borderStrong: "#b7c7d9",
      field: "#ffffff",
      hover: "#ecf3ff",
      panel: "#ffffff",
      panelRaised: "#f1f5fb",
      primary: "#316dff",
      text: "#1b2b3f",
      textSecondary: "#50647d",
      textTertiary: "#8191a5",
      toolbar: "#ffffff",
      success: "#168b63",
      warning: "#b87700",
      error: "#cf3c45"
    },
    dark: {
      app: "#111827",
      border: "#314158",
      borderStrong: "#465b76",
      field: "#121b28",
      hover: "#24344a",
      panel: "#17202d",
      panelRaised: "#1e2a3a",
      primary: "#5b8cff",
      text: "#e6edf7",
      textSecondary: "#c0cddc",
      textTertiary: "#8fa0b5",
      toolbar: "#141e2d",
      success: "#48c78e",
      warning: "#eab85b",
      error: "#ff6b6b"
    }
  }
};

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

const editorInputClassNames = {
  input: "xflow-editor-field__control"
};

const editorTextAreaClassNames = {
  textarea: "xflow-editor-textarea__control"
};

const editorSelectClassNames = {
  content: "xflow-editor-select__content",
  popup: {
    list: "xflow-editor-select__popup-list",
    listItem: "xflow-editor-select__popup-item",
    root: "xflow-editor-select__popup"
  },
  root: "xflow-editor-select__root"
};


const editorTagClassNames = {
  content: "xflow-editor-runtime-tag__content",
  root: "xflow-editor-runtime-tag__root"
};

/** The "Runner" badge on a runner-reported library tile; colored by --xflow-node-source-*. */
const editorNodeSourceTagClassNames = {
  content: "xflow-editor__node-tile-source-label",
  root: "xflow-editor__node-tile-source-root"
};

const editorTooltipClassNames = {
  container: "xflow-editor-tooltip__container",
  root: "xflow-editor-tooltip"
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

interface EditorPanelProps {
  ariaLabel: string;
  bodyClassName?: string;
  children: React.ReactNode;
  className?: string;
  extra?: React.ReactNode;
  icon: React.ReactNode;
  iconAriaLabel?: string;
  subtitle?: React.ReactNode;
  title: React.ReactNode;
}

interface RenameResult {
  workflow?: WorkflowDef;
  error?: string;
  hasPotentialFreeFormReference: boolean;
}

interface RenameFeedback {
  nodeKey: string;
  kind: "error" | "warning";
  message: string;
}

/**
 * An uncommitted parameters-JSON draft, held by the editor (keyed by node)
 * rather than the Inspector, so it survives the Inspector unmounting (drawers
 * and tabs use destroyOnHidden) and can block save/run.
 * - `error` set: the text does not parse; never committed, blocks save/run
 *   and makes the node form read-only.
 * - `error` unset: parseable text waiting for the 300ms debounce or a blur
 *   (Doc C §6.2); save, run, undo and form edits flush it first.
 */
interface ParameterDraft {
  text: string;
  error?: string;
}

/** Debounce of a parseable parameters-JSON edit before it commits (Doc C §6.2). */
const parameterDraftDebounceMs = 300;

function parseParametersText(text: string): { value?: Record<string, unknown>; error?: string } {
  try {
    const parsed = text.trim() ? (JSON.parse(text) as unknown) : {};
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return { error: "参数 JSON 必须是对象" };
    return { value: parsed as Record<string, unknown> };
  } catch {
    return { error: "参数 JSON 格式错误" };
  }
}

function isSaveResult(value: unknown): value is XFlowEditorSaveResult {
  return Boolean(value) && typeof value === "object" && "workflow" in (value as object)
    && typeof (value as { workflow?: unknown }).workflow === "object" && (value as { workflow?: unknown }).workflow !== null;
}

function paramIssuesOf(error: unknown): ParamIssue[] | undefined {
  const issues = error && typeof error === "object" ? (error as XFlowEditorSaveError).paramIssues : undefined;
  return Array.isArray(issues) ? issues : undefined;
}

type ParameterTab = "form" | "json";

/** The node's schema for the form, with the dynamic-ports fallback applied. */
function formSchemaForNode(nodeTypes: NodeTypesResponse | undefined, node: WorkflowNode | undefined): NodeFormSchema | null {
  const schema = schemaForNode(nodeTypes, node);
  return schema ? withDynamicPortsFallback(schema) : null;
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
 * Computes the next workflow from the latest committed draft. Writers pass an
 * updater instead of a whole workflow so two writes in the same tick compose
 * instead of the second silently discarding the first.
 */
type WorkflowUpdater = (latest: WorkflowDef) => WorkflowDef;

function updateNodeByKey(
  workflow: WorkflowDef,
  key: string,
  update: (node: WorkflowNode) => WorkflowNode
): WorkflowDef {
  const nodes = workflow.nodes ?? [];
  const index = nodes.findIndex((node, nodeIndex) => nodeKey(node, nodeIndex) === key);
  if (index < 0) return workflow;
  return { ...workflow, nodes: nodes.map((node, nodeIndex) => (nodeIndex === index ? update(node) : node)) };
}

interface ConnectionReference {
  sourceName: string;
  sourcePort: string;
  targetName: string;
  targetPort: string;
  type: ConnectionType | "invalid";
  legacyDependency?: boolean;
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
  ["xflow.split", { group: "流程控制", tone: "cyan", icon: <ForkOutlined /> }],
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

function nodeKey(node: WorkflowNode, index: number): string {
  return node.id ?? node.name ?? `node-${index}`;
}

function nodeName(node: WorkflowNode, index: number): string {
  return node.name ?? node.id ?? `node-${index}`;
}

function nodeDisplayName(node: WorkflowNode, index: number): string {
  const name = nodeName(node, index);
  const label = typeof node.ui?.label === "string" && node.ui.label.trim() ? node.ui.label : name;
  return label === name ? name : `${label} / ${name}`;
}

function nodeLabel(node: WorkflowNode, index: number): string {
  const name = nodeName(node, index);
  return typeof node.ui?.label === "string" && node.ui.label.trim() ? node.ui.label : name;
}

function runtimeForNode(runtime: RuntimeSnapshot | undefined, node: WorkflowNode): RuntimeNodeSnapshot {
  const name = node.name ?? node.id;
  return (name ? runtime?.nodes?.[name] : undefined) ?? { status: "pending" };
}

function hasOwnKey(value: object | undefined, key: string): boolean {
  return Boolean(value && Object.prototype.hasOwnProperty.call(value, key));
}

function isSupplyNode(node?: WorkflowNode): boolean {
  return node?.kind === "supply" || node?.type?.startsWith("xflow.supply.") === true;
}

function targetsFor(value: ConnectionTargets | undefined): Connection[] {
  if (Array.isArray(value)) return value;
  return value?.targets ?? [];
}

function connectionTypeFor(value: ConnectionTargets | undefined): ConnectionType | "invalid" {
  if (Array.isArray(value) || value === undefined) return "data";
  const type = (value as { type?: unknown }).type;
  if (type === undefined || type === "data") return "data";
  if (type === "dependency") return "dependency";
  return "invalid";
}

function withTargets(original: ConnectionTargets | undefined, targets: Connection[]): ConnectionTargets {
  if (Array.isArray(original) || original === undefined) return targets;
  return { ...original, targets };
}

function workflowNodeByName(workflow: WorkflowDef, name: string): WorkflowNode | undefined {
  return (workflow.nodes ?? []).find((node, index) => nodeName(node, index) === name);
}

function dependencyReferenceKey(sourceName: string, targetName: string): string {
  return JSON.stringify([sourceName, targetName]);
}

function listConnectionReferences(workflow: WorkflowDef): ConnectionReference[] {
  const references: ConnectionReference[] = [];
  const typedDependencies = new Set<string>();
  for (const [sourceName, ports] of Object.entries(workflow.connections ?? {})) {
    for (const [sourcePort, portConnections] of Object.entries(ports)) {
      const type = connectionTypeFor(portConnections);
      for (const target of targetsFor(portConnections)) {
        const targetName = target.node ?? "unknown";
        if (type === "dependency") {
          typedDependencies.add(dependencyReferenceKey(sourceName, targetName));
        }
        references.push({
          sourceName,
          sourcePort,
          targetName,
          targetPort: target.input ?? "main",
          type
        });
      }
    }
  }

  // Legacy dependency_edges remain readable during migration. A matching typed
  // dependency is one semantic edge, so it is shown once and deletion removes
  // both representations below.
  for (const edge of workflow.dependency_edges ?? []) {
    if (typedDependencies.has(dependencyReferenceKey(edge.supply, edge.node))) continue;
    references.push({
      sourceName: edge.supply,
      sourcePort: "supply",
      targetName: edge.node,
      targetPort: "main",
      type: "dependency",
      legacyDependency: true
    });
  }
  return references;
}

function renameConnectionTargets(
  portConnections: ConnectionTargets,
  previousName: string,
  nextName: string
): ConnectionTargets {
  return withTargets(
    portConnections,
    targetsFor(portConnections).map((target) => ({
      ...target,
      node: target.node === previousName ? nextName : target.node
    }))
  );
}

function updateConnectionNodeNames(workflow: WorkflowDef, previousName: string, nextName: string): WorkflowDef {
  const connections = workflow.connections;
  if (!connections || previousName === nextName) return workflow;

  const nextConnections: Connections = {};
  for (const [sourceName, ports] of Object.entries(connections)) {
    const nextSourceName = sourceName === previousName ? nextName : sourceName;
    nextConnections[nextSourceName] = {};

    for (const [portName, portConnections] of Object.entries(ports)) {
      nextConnections[nextSourceName][portName] = renameConnectionTargets(portConnections, previousName, nextName);
    }
  }

  return { ...workflow, connections: nextConnections };
}

function renamePinDataNodeName(
  pinData: WorkflowDef["pin_data"],
  previousName: string,
  nextName: string
): WorkflowDef["pin_data"] {
  if (!pinData || !hasOwnKey(pinData, previousName)) return pinData;

  const { [previousName]: value, ...remainingPinData } = pinData;
  return { ...remainingPinData, [nextName]: value };
}

function renameGroupMembers(
  groups: GroupDef[] | undefined,
  previousName: string,
  nextName: string
): GroupDef[] | undefined {
  return groups?.map((group) => ({
    ...group,
    ...(Array.isArray(group.members)
      ? { members: group.members.map((member) => (member === previousName ? nextName : member)) }
      : {})
  }));
}

function renameDependencyEdges(
  dependencyEdges: WorkflowDef["dependency_edges"],
  previousName: string,
  nextName: string
): WorkflowDef["dependency_edges"] {
  return dependencyEdges?.map((edge) => ({
    ...edge,
    node: edge.node === previousName ? nextName : edge.node,
    supply: edge.supply === previousName ? nextName : edge.supply
  }));
}

function containsStringReference(value: unknown, referenceName: string, seen = new WeakSet<object>()): boolean {
  if (typeof value === "string") return value.includes(referenceName);
  if (!value || typeof value !== "object") return false;
  if (seen.has(value)) return false;
  seen.add(value);

  if (Array.isArray(value)) {
    return value.some((item) => containsStringReference(item, referenceName, seen));
  }

  return Object.values(value).some((item) => containsStringReference(item, referenceName, seen));
}

function hasPotentialFreeFormReference(workflow: WorkflowDef, previousName: string): boolean {
  const candidates: unknown[] = [
    workflow.context,
    workflow.description,
    workflow.node_templates,
    workflow.outputs,
    workflow.settings,
    ...(workflow.nodes ?? []).flatMap((node) => [node.notes, node.parameters, node.output_schema, node.ui])
  ];

  return candidates.some((candidate) => containsStringReference(candidate, previousName));
}

function renameNodeInWorkflow(workflow: WorkflowDef, selectedIndex: number, requestedName: string): RenameResult {
  const nodes = workflow.nodes ?? [];
  const selectedNode = nodes[selectedIndex];
  const previousName = selectedNode?.name ?? selectedNode?.id;
  const nextName = requestedName.trim();

  if (!selectedNode || !previousName) {
    return { error: "无法重命名未命名节点", hasPotentialFreeFormReference: false };
  }
  if (!nextName) {
    return { error: "节点名称不能为空", hasPotentialFreeFormReference: false };
  }
  if (nextName === previousName) {
    return { workflow, hasPotentialFreeFormReference: false };
  }
  if (nodes.some((node, index) => index !== selectedIndex && nodeName(node, index) === nextName)) {
    return { error: `节点名称已存在: ${nextName}`, hasPotentialFreeFormReference: false };
  }

  const connections = workflow.connections;
  if (connections && hasOwnKey(connections, nextName)) {
    return {
      error: `连接中已存在源节点 ${nextName}；为避免覆盖连接，未重命名`,
      hasPotentialFreeFormReference: false
    };
  }
  if (workflow.pin_data && hasOwnKey(workflow.pin_data, previousName) && hasOwnKey(workflow.pin_data, nextName)) {
    return {
      error: `Pin Data 中已存在节点 ${nextName}；为避免覆盖数据，未重命名`,
      hasPotentialFreeFormReference: false
    };
  }

  let nextWorkflow = updateConnectionNodeNames(
    {
      ...workflow,
      nodes: nodes.map((node, index) => (index === selectedIndex ? { ...node, name: nextName } : node))
    },
    previousName,
    nextName
  );

  if (workflow.pin_data && hasOwnKey(workflow.pin_data, previousName)) {
    nextWorkflow = { ...nextWorkflow, pin_data: renamePinDataNodeName(workflow.pin_data, previousName, nextName) };
  }
  if (nextWorkflow.groups) {
    nextWorkflow = { ...nextWorkflow, groups: renameGroupMembers(nextWorkflow.groups, previousName, nextName) };
  }
  if (nextWorkflow.dependency_edges) {
    nextWorkflow = {
      ...nextWorkflow,
      dependency_edges: renameDependencyEdges(nextWorkflow.dependency_edges, previousName, nextName)
    };
  }

  return {
    workflow: nextWorkflow,
    hasPotentialFreeFormReference: hasPotentialFreeFormReference(workflow, previousName)
  };
}

function groupIcon(group: NodeDescriptor["group"]): React.ReactNode {
  if (group === "触发器") return <ThunderboltOutlined />;
  if (group === "流程控制") return <BranchesOutlined />;
  if (group === "数据转换") return <CodeOutlined />;
  if (group === "供应") return <DatabaseOutlined />;
  if (group === "其他") return <AppstoreOutlined />;
  return <AuditOutlined />;
}

function statusColor(status: string | undefined): string {
  if (status === "success") return "green";
  if (status === "running" || status === "waiting") return "blue";
  if (status === "failed" || status === "canceled") return "red";
  if (status === "pinned" || status === "continued") return "gold";
  if (status === "suspended") return "gold";
  return "default";
}

function EditorPanel({
  ariaLabel,
  bodyClassName,
  children,
  className,
  extra,
  icon,
  iconAriaLabel,
  subtitle,
  title
}: EditorPanelProps): React.ReactElement {
  return (
    <Card
      aria-label={ariaLabel}
      className={`xflow-editor-panel-card ${className ?? ""}`}
      rootClassName="xflow-editor-panel-card__root"
      classNames={{
        body: `xflow-editor-panel-card__body ${bodyClassName ?? ""}`,
        extra: "xflow-editor-panel-card__extra",
        header: "xflow-editor-panel-card__header",
        title: "xflow-editor-panel-card__title"
      }}
      extra={extra}
      role="region"
      size="small"
      title={
        <div className="xflow-editor-panel-title">
          <span className="xflow-editor-panel-title__icon" aria-label={iconAriaLabel}>
            {icon}
          </span>
          <div className="xflow-editor-panel-title__text">
            <strong>{title}</strong>
            {subtitle ? <span>{subtitle}</span> : null}
          </div>
        </div>
      }
    >
      {children}
    </Card>
  );
}

function defaultSelectedKey(nodes: WorkflowNode[]): string | undefined {
  const switchIndex = nodes.findIndex((node) => node.type?.includes("switch"));
  if (switchIndex >= 0) return nodeKey(nodes[switchIndex], switchIndex);
  return nodes[0] ? nodeKey(nodes[0], 0) : undefined;
}

function selectedPorts(workflow: WorkflowDef, selectedNode?: WorkflowNode): string[] {
  const selectedName = selectedNode?.name ?? selectedNode?.id;
  if (!selectedName) return [];
  return Object.entries(workflow.connections?.[selectedName] ?? {})
    .filter(([, portConnections]) => connectionTypeFor(portConnections) === "data")
    .map(([port]) => port);
}

function supportedOutputPorts(node?: WorkflowNode, workflow?: WorkflowDef): string[] {
  if (!node || isSupplyNode(node)) return [];
  const type = node.type ?? "";
  const nodeNameValue = node.name ?? node.id;
  const existingPorts = nodeNameValue && workflow
    ? Object.entries(workflow.connections?.[nodeNameValue] ?? {})
        .filter(([, portConnections]) => connectionTypeFor(portConnections) === "data")
        .map(([port]) => port)
    : [];
  if (type === "xflow.end") {
    return existingPorts;
  }

  const declared = declaredOutputPorts(node);
  const ports = new Set<string>(declared.hasMain ? ["main"] : []);
  for (const port of [...existingPorts, ...declared.ports]) {
    ports.add(port);
  }
  return Array.from(ports);
}

function supportedInputPorts(node?: WorkflowNode): string[] {
  if (!node || isSupplyNode(node)) return [];
  const declared = (node.inputs ?? [])
    .map((input) => input.name)
    .filter((name): name is string => typeof name === "string" && name.trim().length > 0);
  if (declared.length > 0) return declared;
  if (node.type === "xflow.start" || node.type?.startsWith("xflow.trigger.")) return [];
  return ["main"];
}

interface WorkflowDiagnostic {
  status: "pass" | "warn" | "error";
  area: string;
  message: string;
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
      message: `节点名称重复: ${Array.from(new Set(duplicateNames)).join(", ")}`
    });
  }
  // A type outside the library may still be real (registered only on a
  // runner, Doc C §2.1): the node stays editable as a no-schema node, so this
  // is a warning, not an error.
  const libraryTypes = new Set(nodeLibraryDescriptors(nodeTypes).map((descriptor) => descriptor.type));
  const unsupportedTypes = nodes
    .map((node, index) => ({ node, index }))
    .filter(({ node }) => node.type && !libraryTypes.has(node.type) && !registeredTypes.has(node.type))
    .map(({ node, index }) => `${nodeName(node, index)}:${node.type}`);
  if (unsupportedTypes.length > 0) {
    diagnostics.push({
      status: "warn",
      area: "nodes",
      message: `${nodeTypes ? "节点类型未在 server 注册" : "节点类型不在离线节点库中"}，参数按 JSON 编辑: ${unsupportedTypes.join(", ")}`
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
      message: `节点级 runnerSelector 不支持 required 模式: ${nodeRequiredSelectors.join(", ")}`
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
        diagnostics.push({ status: "error", area: "edges", message: `${sourceName}.${portName} 使用了未知连接类型` });
        continue;
      }

      if (type === "dependency") {
        dependencyEdgeCount += targets.length;
        if (sourceNode && !isSupplyNode(sourceNode)) {
          diagnostics.push({ status: "error", area: "edges", message: `${sourceName}.${portName} 的 dependency 源必须是 supply 节点` });
        }
        for (const target of targets) {
          const targetName = target.node ?? "";
          const targetNode = targetName ? workflowNodeByName(workflow, targetName) : undefined;
          if (!targetNode) {
            diagnostics.push({
              status: "error",
              area: "edges",
              message: `${sourceName}.${portName} dependency 目标不存在: ${targetName || "unknown"}`
            });
            continue;
          }
          if (isSupplyNode(targetNode)) {
            diagnostics.push({ status: "error", area: "edges", message: `supply 节点不能依赖另一个 supply: ${targetName}` });
          }
          if (Object.prototype.hasOwnProperty.call(target, "input")) {
            diagnostics.push({ status: "error", area: "edges", message: `${sourceName} → ${targetName} 的 dependency 不得声明 input` });
          }
        }
        continue;
      }

      dataEdgeCount += targets.length;
      if (dangling.has(`${sourceName}\u0000${portName}`)) {
        diagnostics.push({ status: "error", area: "edges", message: `${sourceName}.${portName} 端口已不存在，连线悬空；请删除或改接连线` });
        continue;
      }
      if (sourceNode && isSupplyNode(sourceNode)) {
        diagnostics.push({ status: "error", area: "edges", message: `supply 节点不能参与数据连接: ${sourceName}.${portName}` });
      } else if (sourceNode && !supportedOutputPorts(sourceNode, workflow).includes(portName)) {
        diagnostics.push({ status: "error", area: "edges", message: `${sourceName}.${portName} 不是有效输出端口` });
      }
      for (const target of targets) {
        const targetName = target.node ?? "";
        if (!targetName || !knownNames.has(targetName)) {
          diagnostics.push({
            status: "error",
            area: "edges",
            message: `${sourceName}.${portName} 连接目标不存在: ${targetName || "unknown"}`
          });
          continue;
        }
        const targetNode = workflowNodeByName(workflow, targetName);
        if (targetNode && isSupplyNode(targetNode)) {
          diagnostics.push({ status: "error", area: "edges", message: `supply 节点不能参与数据连接: ${targetName}` });
          continue;
        }
        const targetInput = target.input ?? "main";
        if (targetNode && !supportedInputPorts(targetNode).includes(targetInput)) {
          diagnostics.push({
            status: "error",
            area: "edges",
            message: `${targetName}.${targetInput} 不是有效输入端口`
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
      diagnostics.push({ status: "error", area: "dependencies", message: `supply 节点不能依赖另一个 supply: ${edge.node}` });
    }
    if (!supply) {
      diagnostics.push({ status: "error", area: "dependencies", message: `dependency supply 不存在: ${edge.supply}` });
    } else if (!isSupplyNode(supply)) {
      diagnostics.push({ status: "error", area: "dependencies", message: `dependency supply 不是 supply 节点: ${edge.supply}` });
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

  const namesByLength = Array.from(knownNames).sort((left, right) => right.length - left.length);
  return diagnostics.map((diagnostic) => {
    const diagnosticNodeName = namesByLength.find((name) => name && diagnostic.message.includes(name));
    return diagnosticNodeName ? { ...diagnostic, nodeName: diagnosticNodeName } : diagnostic;
  });
}

function addConnection(
  workflow: WorkflowDef,
  selectedNode: WorkflowNode | undefined,
  targetName: string,
  sourcePort = "main",
  targetInput = "main"
): WorkflowDef {
  const selectedName = selectedNode?.name ?? selectedNode?.id;
  const targetNode = workflowNodeByName(workflow, targetName);
  if (!selectedName || selectedName === targetName || !targetNode) return workflow;
  if (isSupplyNode(selectedNode) || isSupplyNode(targetNode)) return workflow;
  if (!supportedOutputPorts(selectedNode, workflow).includes(sourcePort)) return workflow;
  if (!supportedInputPorts(targetNode).includes(targetInput)) return workflow;

  const connections = workflow.connections ?? {};
  const sourcePorts = connections[selectedName] ?? {};
  const portConnections = sourcePorts[sourcePort];
  if (portConnections && connectionTypeFor(portConnections) !== "data") return workflow;

  const targets = targetsFor(portConnections);
  if (targets.some((target) => target.node === targetName && (target.input ?? "main") === targetInput)) return workflow;

  return {
    ...workflow,
    connections: {
      ...connections,
      [selectedName]: {
        ...sourcePorts,
        [sourcePort]: withTargets(portConnections, [
          ...targets,
          targetInput === "main" ? { node: targetName } : { node: targetName, input: targetInput }
        ])
      }
    }
  };
}

function removeConnectionFromWorkflow(
  workflow: WorkflowDef,
  connection: Pick<ConnectionReference, "sourceName" | "sourcePort" | "targetName" | "targetPort"> & {
    type?: ConnectionReference["type"];
  }
): WorkflowDef {
  const connections = workflow.connections ?? {};
  const sourcePorts = connections[connection.sourceName];
  const portConnections = sourcePorts?.[connection.sourcePort];
  let removedConnection = false;
  let nextConnections = connections;

  if (sourcePorts && portConnections !== undefined) {
    const nextTargets = targetsFor(portConnections).filter((target) => {
      const matches = target.node === connection.targetName && (target.input ?? "main") === connection.targetPort;
      if (matches && !removedConnection) {
        removedConnection = true;
        return false;
      }
      return true;
    });
    if (removedConnection) {
      nextConnections = {
        ...connections,
        [connection.sourceName]: {
          ...sourcePorts,
          [connection.sourcePort]: withTargets(portConnections, nextTargets)
        }
      };
    }
  }

  const isDependency = connection.type === "dependency";
  const nextDependencyEdges = isDependency
    ? workflow.dependency_edges?.filter(
      (edge) => edge.node !== connection.targetName || edge.supply !== connection.sourceName
    )
    : workflow.dependency_edges;
  const removedLegacyDependency = isDependency && nextDependencyEdges?.length !== workflow.dependency_edges?.length;
  if (!removedConnection && !removedLegacyDependency) return workflow;

  return {
    ...workflow,
    ...(removedConnection ? { connections: nextConnections } : {}),
    ...(removedLegacyDependency ? { dependency_edges: nextDependencyEdges } : {})
  };
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

function serializeJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
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

function Outline({
  workflow,
  selectedKey,
  runtime,
  onSelect
}: {
  workflow: WorkflowDef;
  selectedKey: string | undefined;
  runtime?: RuntimeSnapshot;
  onSelect: (key: string) => void;
}): React.ReactElement {
  const nodes = workflow.nodes ?? [];
  const buttonRefs = React.useRef(new Map<string, HTMLButtonElement>());
  const typeahead = React.useRef({ query: "", timeout: undefined as ReturnType<typeof setTimeout> | undefined });
  const connections = React.useMemo(() => listConnectionReferences(workflow), [workflow]);
  const diagnostics = React.useMemo(() => validateWorkflow(workflow, runtime), [runtime, workflow]);

  React.useEffect(() => () => {
    if (typeahead.current.timeout !== undefined) {
      clearTimeout(typeahead.current.timeout);
    }
  }, []);

  const moveSelection = React.useCallback((index: number) => {
    const node = nodes[index];
    if (!node) return;
    const key = nodeKey(node, index);
    buttonRefs.current.get(key)?.focus();
    onSelect(key);
  }, [nodes, onSelect]);

  const handleKeyDown = React.useCallback((event: React.KeyboardEvent<HTMLButtonElement>, currentIndex: number) => {
    if (nodes.length === 0) return;
    let nextIndex: number | undefined;
    if (event.key === "ArrowDown") {
      nextIndex = (currentIndex + 1) % nodes.length;
    } else if (event.key === "ArrowUp") {
      nextIndex = (currentIndex - 1 + nodes.length) % nodes.length;
    } else if (event.key === "Home") {
      nextIndex = 0;
    } else if (event.key === "End") {
      nextIndex = nodes.length - 1;
    } else if (
      event.key.length === 1
      && !event.altKey
      && !event.ctrlKey
      && !event.metaKey
    ) {
      const query = `${typeahead.current.query}${event.key.toLocaleLowerCase()}`;
      typeahead.current.query = query;
      if (typeahead.current.timeout !== undefined) clearTimeout(typeahead.current.timeout);
      typeahead.current.timeout = setTimeout(() => {
        typeahead.current.query = "";
        typeahead.current.timeout = undefined;
      }, 500);
      const startIndex = (currentIndex + 1) % nodes.length;
      nextIndex = Array.from({ length: nodes.length }, (_, offset) => (startIndex + offset) % nodes.length)
        .find((index) => {
          const node = nodes[index];
          return nodeName(node, index).toLocaleLowerCase().startsWith(query)
            || nodeLabel(node, index).toLocaleLowerCase().startsWith(query);
        });
    } else {
      return;
    }

    event.preventDefault();
    if (nextIndex !== undefined) moveSelection(nextIndex);
  }, [moveSelection, nodes]);

  return (
    <EditorPanel
      ariaLabel="大纲"
      bodyClassName="xflow-editor-outline__body"
      className="xflow-editor-outline"
      icon={<BarsOutlined />}
      subtitle={`${nodes.length} 个节点`}
      title="大纲"
    >
      <div className="xflow-editor-outline-list !gap-0.5">
        {nodes.map((node, index) => {
          const key = nodeKey(node, index);
          const runtimeNode = runtimeForNode(runtime, node);
          const incomingConnections = connections.filter((connection) => connection.targetName === nodeName(node, index)).length;
          const outgoingConnections = connections.filter((connection) => connection.sourceName === nodeName(node, index)).length;
          const diagnosticCount = diagnostics.filter((diagnostic) => diagnostic.nodeName === nodeName(node, index)).length;
          return (
            <button
              aria-label={`选择节点 ${nodeName(node, index)}`}
              className="xflow-editor-outline-row !min-h-6 !grid-cols-[6px_minmax(0,1fr)] !gap-1.5 !rounded !px-1.5 !py-1 !text-[10.5px]"
              data-selected={selectedKey === key}
              key={key}
              ref={(element) => {
                if (element) buttonRefs.current.set(key, element);
                else buttonRefs.current.delete(key);
              }}
              tabIndex={selectedKey === key || (!selectedKey && index === 0) ? 0 : -1}
              type="button"
              onClick={() => onSelect(key)}
              onKeyDown={(event) => handleKeyDown(event, index)}
            >
              <span className="xflow-editor-status-dot" data-status={runtimeNode.status} />
              <span className="xflow-editor-outline-node">
                <strong>{nodeDisplayName(node, index)}</strong>
                <span className="xflow-editor-outline-meta">
                  <span>{node.type ?? "unknown"}</span>
                  <span>{runtimeNode.status}</span>
                  <span>入 {incomingConnections} / 出 {outgoingConnections}</span>
                  <span>诊断 {diagnosticCount}</span>
                </span>
              </span>
            </button>
          );
        })}
      </div>
    </EditorPanel>
  );
}

function ConnectionsPanel({
  workflow,
  selectedNode,
  onAddConnection,
  onRemoveConnection
}: {
  workflow: WorkflowDef;
  selectedNode?: WorkflowNode;
  onAddConnection: (targetName: string, sourcePort: string, targetInput: string) => void;
  onRemoveConnection: (connection: ConnectionReference) => void;
}): React.ReactElement {
  const [sourcePort, setSourcePort] = React.useState("main");
  const [targetInput, setTargetInput] = React.useState("main");
  const selectedName = selectedNode?.name ?? selectedNode?.id;
  const connections = React.useMemo(() => listConnectionReferences(workflow), [workflow]);
  const visibleConnections = connections.filter(
    (connection) => connection.sourceName === selectedName || connection.targetName === selectedName
  );
  const sourcePorts = supportedOutputPorts(selectedNode, workflow);
  const targetCandidates = (workflow.nodes ?? []).filter((node, index) => {
    const name = nodeName(node, index);
    return name !== selectedName && !isSupplyNode(node) && supportedInputPorts(node).length > 0;
  });

  React.useEffect(() => {
    const nextSourcePort = sourcePorts.includes(sourcePort) ? sourcePort : (sourcePorts[0] ?? "main");
    setSourcePort(nextSourcePort);
  }, [sourcePort, sourcePorts]);

  if (!selectedName) {
    return <p className="xflow-editor-empty">选择节点后查看连接。</p>;
  }

  return (
    <div className="xflow-editor-connection-list">
      <div className="xflow-editor-connection-actions">
        <span>添加数据连接</span>
        <div className="xflow-editor-connection-selectors">
          <Select
            aria-label="输出端口"
            className="xflow-editor-connection-select"
            classNames={editorSelectClassNames}
            disabled={sourcePorts.length === 0}
            options={sourcePorts.map((port) => ({ label: port, value: port }))}
            popupMatchSelectWidth={false}
            size="small"
            value={sourcePort}
            onChange={setSourcePort}
          />
          <Select
            aria-label="输入端口"
            className="xflow-editor-connection-select"
            classNames={editorSelectClassNames}
            options={[{ label: "main", value: "main" }]}
            popupMatchSelectWidth={false}
            size="small"
            value={targetInput}
            onChange={setTargetInput}
          />
        </div>
        <div>
          {targetCandidates.map((targetNode, index) => {
            const targetName = nodeName(targetNode, index);
            const inputPorts = supportedInputPorts(targetNode);
            const resolvedTargetInput = inputPorts.includes(targetInput) ? targetInput : (inputPorts[0] ?? "main");
            return (
              <button
                aria-label={`连接到 ${targetName}`}
                disabled={sourcePorts.length === 0 || inputPorts.length === 0}
                key={targetName}
                type="button"
                onClick={() => onAddConnection(targetName, sourcePort, resolvedTargetInput)}
              >
                {targetName}
              </button>
            );
          })}
        </div>
      </div>
      {visibleConnections.length === 0 ? <p className="xflow-editor-empty">当前节点暂无连接。</p> : null}
      {visibleConnections.map((connection, index) => (
        <div className="xflow-editor-connection-row" key={`${connection.sourceName}:${connection.sourcePort}:${connection.targetName}:${connection.targetPort}:${connection.type}:${index}`}>
          <code>{connection.sourceName}</code>
          <span>{connection.sourcePort}</span>
          <span>{connection.legacyDependency ? "dependency (legacy)" : connection.type}</span>
          <span>→</span>
          <code>{connection.targetName}</code>
          <span>{connection.type === "dependency" ? "—" : connection.targetPort}</span>
          <Tooltip classNames={editorTooltipClassNames} title="删除连接">
            <button
              aria-label={`删除连接 ${connection.sourceName} ${connection.sourcePort} 到 ${connection.targetName} ${connection.targetPort}`}
              className="xflow-editor-connection-delete"
              type="button"
              onClick={() => onRemoveConnection(connection)}
            >
              <DeleteOutlined />
            </button>
          </Tooltip>
        </div>
      ))}
    </div>
  );
}

function RunPanel({
  selectedNode,
  runtime
}: {
  selectedNode?: WorkflowNode;
  runtime?: RuntimeSnapshot;
}): React.ReactElement {
  if (!selectedNode) {
    return <p className="xflow-editor-empty">选择节点后查看运行状态。</p>;
  }

  const snapshot = runtimeForNode(runtime, selectedNode);
  return (
    <div className="xflow-editor-run-list">
      <div>
        <span>状态</span>
        <Tag className="xflow-editor-runtime-tag" classNames={editorTagClassNames} color={statusColor(snapshot.status)}>{snapshot.status}</Tag>
      </div>
      <div>
        <span>尝试次数</span>
        <strong>{snapshot.attempts ?? 1}</strong>
      </div>
      <div>
        <span>耗时</span>
        <strong>{snapshot.durationMs === undefined ? "-" : `${snapshot.durationMs} ms`}</strong>
      </div>
      {snapshot.error ? <p className="xflow-editor-error">{snapshot.error}</p> : null}
    </div>
  );
}

/** Shared, stable node-form machinery the editor creates once and hands to the Inspector. */
interface NodeFormHost {
  registry: Registry;
  compiler: NodeFormCompiler;
  /** Written by the mounted Inspector; read by the registry's NodeNameInput. */
  renameRef: React.MutableRefObject<((requestedName: string) => void) | undefined>;
}

function NodeParametersForm({
  workflow,
  node,
  nodeKeyValue,
  nodeTypes,
  host,
  externalIssues,
  readOnly,
  invalidDraft,
  onApplyPatches
}: {
  workflow: WorkflowDef;
  node: WorkflowNode;
  nodeKeyValue: string;
  nodeTypes?: NodeTypesResponse;
  host: NodeFormHost;
  externalIssues?: Record<string, Issue[]>;
  readOnly: boolean;
  invalidDraft: boolean;
  onApplyPatches?: (key: string, patches: Patch[]) => void;
}): React.ReactElement {
  const schema = formSchemaForNode(nodeTypes, node);
  const hasTemplate = nodeHasTemplate(workflow, node);
  // Memoised per schema + flags inside the compiler: typing never recompiles.
  const spec = host.compiler.spec(schema, { hasTemplate, parameters: node.parameters, schemasLoaded: Boolean(nodeTypes) });
  const ports = portsForNode(schema, node);
  const portsKey = JSON.stringify(ports);
  const credentialsKey = Object.keys(workflow.credentials ?? {}).join("\u0000");
  const context = React.useMemo(
    () => ({
      ports: JSON.parse(portsKey) as unknown,
      credentials: credentialsKey ? credentialsKey.split("\u0000") : []
    }),
    [credentialsKey, portsKey]
  );
  const handleChange = React.useCallback(
    (patches: Patch[]) => onApplyPatches?.(nodeKeyValue, patches),
    [nodeKeyValue, onApplyPatches]
  );

  return (
    <div className="xflow-editor-node-form" data-node-form-schema={schema ? `${schema.node_type}@${schema.node_version}` : "none"}>
      {!nodeTypes ? (
        <p className="xflow-editor-node-form__notice" role="status">
          <InfoCircleOutlined />
          节点类型 schema 未加载：参数请在 JSON 页签编辑，通用字段仍可在此修改。
        </p>
      ) : null}
      {invalidDraft ? (
        <p className="xflow-editor-node-form__notice xflow-editor-node-form__notice--error" role="status">
          <WarningOutlined />
          参数 JSON 未通过解析，表单暂为只读；请在 JSON 页签修正。
        </p>
      ) : null}
      <Composer
        // A node switch remounts the form, so local drafts never cross nodes.
        key={nodeKeyValue}
        context={context}
        externalIssues={externalIssues}
        readOnly={readOnly}
        registry={host.registry}
        spec={spec}
        value={node}
        onChange={handleChange}
      />
    </div>
  );
}

function Inspector({
  workflow,
  selectedNode,
  selectedIndex,
  runtime,
  editable = true,
  nodeTypes,
  formHost,
  externalIssues,
  parameterTab,
  revealNonce,
  onChangeWith,
  onApplyNodePatches,
  onDeleteNode,
  onNodeRenamed,
  parameterDrafts,
  onParameterText,
  onParameterCommit,
  onParameterTabChange
}: {
  workflow: WorkflowDef;
  selectedNode?: WorkflowNode;
  selectedIndex: number;
  runtime?: RuntimeSnapshot;
  editable?: boolean;
  nodeTypes?: NodeTypesResponse;
  formHost: NodeFormHost;
  externalIssues?: Record<string, Issue[]>;
  parameterTab: ParameterTab;
  /** Bumped by the editor to bring the configuration view to the front. */
  revealNonce?: number;
  onChangeWith?: (update: WorkflowUpdater) => void;
  onApplyNodePatches?: (key: string, patches: Patch[]) => void;
  onDeleteNode?: () => void;
  onNodeRenamed?: (previousKey: string, nextKey: string) => void;
  parameterDrafts?: Record<string, ParameterDraft>;
  /** Every keystroke of the parameters JSON; the editor parses, drafts and debounces. */
  onParameterText?: (key: string, text: string) => void;
  /** Blur / leaving the JSON tab: commit a parseable draft now. */
  onParameterCommit?: (key: string) => void;
  onParameterTabChange?: (tab: ParameterTab) => void;
}): React.ReactElement {
  const [activeTab, setActiveTab] = React.useState<"config" | "connections" | "run">("config");
  const [renameFeedback, setRenameFeedback] = React.useState<RenameFeedback>();
  const [parametersText, setParametersText] = React.useState("{}");
  const [workflowJsonText, setWorkflowJsonText] = React.useState<Record<string, string>>({});
  const [workflowJsonErrors, setWorkflowJsonErrors] = React.useState<Record<string, string | undefined>>({});
  const selectedNodeIdentity = selectedNode ? nodeKey(selectedNode, selectedIndex) : undefined;
  const activeRenameFeedback =
    renameFeedback && renameFeedback.nodeKey === selectedNodeIdentity ? renameFeedback : undefined;
  const selectedDraft = selectedNodeIdentity ? parameterDrafts?.[selectedNodeIdentity] : undefined;
  const invalidDraft = Boolean(selectedDraft?.error);
  // Read through refs so a draft edit does not re-run the resync effect below
  // on every keystroke.
  const parameterDraftsRef = React.useRef(parameterDrafts);
  parameterDraftsRef.current = parameterDrafts;
  const parametersTextRef = React.useRef(parametersText);
  parametersTextRef.current = parametersText;

  React.useEffect(() => {
    if (revealNonce) setActiveTab("config");
  }, [revealNonce]);

  React.useEffect(() => {
    setRenameFeedback((current) =>
      current?.nodeKey === selectedNodeIdentity ? current : undefined
    );
    // A draft (unparseable, or parseable but not yet committed) is never
    // overwritten by a re-serialization of the committed parameters -- not by
    // an undo, not by another writer, and not by the Inspector remounting.
    const draft = selectedNodeIdentity ? parameterDraftsRef.current?.[selectedNodeIdentity] : undefined;
    if (draft) {
      setParametersText(draft.text);
      return;
    }
    // The user's own text that just committed stays as typed (no reformat,
    // no caret jump) as long as it still means the committed parameters.
    const committed = serializeJson(selectedNode?.parameters ?? {});
    const current = parseParametersText(parametersTextRef.current);
    setParametersText(current.value && serializeJson(current.value) === committed ? parametersTextRef.current : committed);
  }, [selectedIndex, selectedNodeIdentity, selectedNode?.name, selectedNode?.id, selectedNode?.parameters]);

  React.useEffect(() => {
    setWorkflowJsonText({
      runner_selector: serializeJson(workflow.runner_selector ?? { mode: "default", match_labels: {} }),
      params: serializeJson(workflow.params ?? {}),
      context: serializeJson(workflow.context ?? { vars: {}, config: {} }),
      settings: serializeJson(workflow.settings ?? {}),
      credentials: serializeJson(workflow.credentials ?? {}),
      pin_data: serializeJson(workflow.pin_data ?? {})
    });
    setWorkflowJsonErrors({});
  }, [workflow.runner_selector, workflow.params, workflow.context, workflow.settings, workflow.credentials, workflow.pin_data]);

  const updateWorkflowMeta = React.useCallback(
    (patch: Partial<WorkflowDef>) => {
      onChangeWith?.((latest) => ({ ...latest, ...patch }));
    },
    [onChangeWith]
  );
  const updateWorkflowJson = React.useCallback(
    (
      field: "runner_selector" | "params" | "context" | "settings" | "credentials" | "pin_data",
      nextText: string
    ) => {
      setWorkflowJsonText((current) => ({ ...current, [field]: nextText }));
      try {
        const nextValue = nextText.trim() ? (JSON.parse(nextText) as unknown) : {};
        if (!nextValue || typeof nextValue !== "object" || Array.isArray(nextValue)) {
          setWorkflowJsonErrors((current) => ({ ...current, [field]: "JSON 必须是对象" }));
          return;
        }
        setWorkflowJsonErrors((current) => ({ ...current, [field]: undefined }));
        updateWorkflowMeta({ [field]: nextValue } as Partial<WorkflowDef>);
      } catch {
        setWorkflowJsonErrors((current) => ({ ...current, [field]: "JSON 格式错误" }));
      }
    },
    [updateWorkflowMeta]
  );
  const updateSelectedNode = React.useCallback(
    (patch: Partial<WorkflowNode>) => {
      if (!selectedNodeIdentity) return;
      const key = selectedNodeIdentity;
      onChangeWith?.((latest) => updateNodeByKey(latest, key, (node) => ({ ...node, ...patch })));
    },
    [onChangeWith, selectedNodeIdentity]
  );
  // Called by the form's NodeNameInput (Doc C §4.2): `/name` is never a patch.
  const commitNodeRename = React.useCallback((requestedName: string) => {
    if (!selectedNodeIdentity) return;
    const previousKey = selectedNodeIdentity;
    // Held in an object: TypeScript does not see assignments made inside the
    // updater callback, so a plain `let` would narrow to `undefined` here.
    // Relies on commitWorkflowWith running the updater synchronously (Doc C §6.1).
    const outcome: { result?: RenameResult; nextKey: string } = { nextKey: previousKey };
    onChangeWith?.((latest) => {
      const index = (latest.nodes ?? []).findIndex((node, nodeIndex) => nodeKey(node, nodeIndex) === previousKey);
      if (index < 0) return latest;
      const result = renameNodeInWorkflow(latest, index, requestedName);
      outcome.result = result;
      if (result.error || !result.workflow) return latest;
      const renamedNode = result.workflow.nodes?.[index];
      outcome.nextKey = renamedNode ? nodeKey(renamedNode, index) : previousKey;
      return result.workflow;
    });
    const { result, nextKey } = outcome;
    if (!result) return;
    if (result.error) {
      setRenameFeedback({ nodeKey: previousKey, kind: "error", message: result.error });
      return;
    }
    if (!result.workflow) return;
    setRenameFeedback(
      result.hasPotentialFreeFormReference
        ? {
            nodeKey: nextKey,
            kind: "warning",
            message: "已更新结构化引用；备注、参数和表达式中的自由文本引用未自动替换。"
          }
        : undefined
    );
    // Selection is keyed by id ?? name, so an id-less node must be re-selected
    // under its new name or the Inspector loses it.
    onNodeRenamed?.(previousKey, nextKey);
  }, [onChangeWith, onNodeRenamed, selectedNodeIdentity]);
  // The registry is created once per editor; its NodeNameInput calls through
  // this ref, so it always reaches the mounted Inspector's current node.
  const renameRef = formHost.renameRef;
  React.useLayoutEffect(() => {
    renameRef.current = commitNodeRename;
    return () => {
      if (renameRef.current === commitNodeRename) renameRef.current = undefined;
    };
  }, [commitNodeRename, renameRef]);
  const addSelectedConnection = React.useCallback(
    (targetName: string, sourcePort: string, targetInput: string) => {
      const key = selectedNodeIdentity;
      onChangeWith?.((latest) => {
        const node = key
          ? (latest.nodes ?? []).find((candidate, index) => nodeKey(candidate, index) === key)
          : undefined;
        return addConnection(latest, node, targetName, sourcePort, targetInput);
      });
    },
    [onChangeWith, selectedNodeIdentity]
  );
  const removeSelectedConnection = React.useCallback(
    (connection: ConnectionReference) => {
      onChangeWith?.((latest) => removeConnectionFromWorkflow(latest, connection));
    },
    [onChangeWith]
  );
  const updateParameters = React.useCallback(
    (nextText: string) => {
      setParametersText(nextText);
      if (selectedNodeIdentity) onParameterText?.(selectedNodeIdentity, nextText);
    },
    [onParameterText, selectedNodeIdentity]
  );
  const commitParameters = React.useCallback(() => {
    if (selectedNodeIdentity) onParameterCommit?.(selectedNodeIdentity);
  }, [onParameterCommit, selectedNodeIdentity]);
  const changeParameterTab = React.useCallback(
    (tab: string) => {
      // Leaving the JSON tab unmounts the textarea without a blur: commit first
      // so a pending debounced draft never lands on top of form edits.
      commitParameters();
      onParameterTabChange?.(tab === "json" ? "json" : "form");
    },
    [commitParameters, onParameterTabChange]
  );
  const configPanel = selectedNode ? (
    <div className="xflow-editor-config">
    <div className="xflow-editor-form">
      <div className="xflow-editor-form-section">
        <div className="xflow-editor-form-section__title xflow-editor-form-section__title--spaced">
          <span className="xflow-editor-block-title">
            <InfoCircleOutlined />
            基础信息
          </span>
        </div>
        {/* Doc C §4.2: /type stays on the editor's own control; name, notes,
            disabled, on_error, timeout, retry and runner_selector are owned by
            the node form below (one writer per path). */}
        <label className="xflow-editor-form-row xflow-editor-form-row--node">
          <span>类型</span>
          <Input
            aria-label="类型"
            className="xflow-editor-field"
            classNames={editorInputClassNames}
            value={selectedNode.type ?? ""}
            onChange={(event) => updateSelectedNode({ type: event.target.value })}
          />
        </label>
        <label className="xflow-editor-form-row xflow-editor-form-row--node">
          <span>模板</span>
          <Input aria-label="模板" className="xflow-editor-field"
            classNames={editorInputClassNames} readOnly value={selectedNode.template ?? "无"} />
        </label>
      </div>
      {selectedNode.type?.includes("switch") ? (
        <div className="xflow-editor-form-section">
          <div className="xflow-editor-form-section__title xflow-editor-form-section__title--spaced">
            <span className="xflow-editor-block-title">
              <BranchesOutlined />
              路由配置
            </span>
          </div>
          <label className="xflow-editor-form-row xflow-editor-form-row--node">
            <span>输出端口</span>
            <Input
              aria-label="输出端口"
              className="xflow-editor-field"
            classNames={editorInputClassNames}
              readOnly
              value={selectedPorts(workflow, selectedNode).join(", ") || "未配置"}
            />
          </label>
          <p className="xflow-editor-empty">路由条件在下方节点配置中编辑；这里仅展示当前数据输出端口。</p>
        </div>
      ) : null}
      </div>
      <section className="xflow-editor-node-params" aria-label="节点配置">
        <div className="xflow-editor-form-section__title xflow-editor-form-section__title--spaced">
          <span className="xflow-editor-block-title">
            <SettingOutlined />
            节点配置
          </span>
        </div>
        {activeRenameFeedback ? (
          <span className={activeRenameFeedback.kind === "error" ? "xflow-editor-error" : "xflow-editor-warning"} role="status">
            {activeRenameFeedback.message}
          </span>
        ) : null}
        <Tabs
          activeKey={parameterTab}
          classNames={{
            header: "xflow-editor-param-tabs__header",
            item: "xflow-editor-param-tabs__item",
            content: "xflow-editor-param-tabs__content"
          }}
          destroyOnHidden
          items={[
            {
              key: "form",
              label: "表单",
              children: (
                <NodeParametersForm
                  externalIssues={externalIssues}
                  host={formHost}
                  invalidDraft={invalidDraft}
                  node={selectedNode}
                  nodeKeyValue={selectedNodeIdentity ?? ""}
                  nodeTypes={nodeTypes}
                  readOnly={!editable || invalidDraft}
                  workflow={workflow}
                  onApplyPatches={onApplyNodePatches}
                />
              )
            },
            {
              key: "json",
              label: "JSON",
              children: (
                <div className="xflow-editor-json-field">
                  <Input.TextArea
                    aria-label="参数 JSON"
                    autoSize={{ minRows: 4, maxRows: 16 }}
                    className="xflow-editor-textarea xflow-editor-textarea--code"
                    classNames={editorTextAreaClassNames}
                    readOnly={!editable}
                    status={selectedDraft?.error ? "error" : undefined}
                    value={parametersText}
                    onBlur={commitParameters}
                    onChange={(event) => updateParameters(event.target.value)}
                  />
                  {selectedDraft?.error ? <span>{selectedDraft.error}</span> : null}
                </div>
              )
            }
          ]}
          rootClassName="xflow-editor-param-tabs"
          size="small"
          onChange={changeParameterTab}
        />
      </section>
      {/* Workflow-level fields after the selected node: the node is what the user just picked. */}
      <div className="xflow-editor-form">
      <div className="xflow-editor-form-section">
        <div className="xflow-editor-form-section__title">
          <span className="xflow-editor-block-title">
            <FileSearchOutlined />
            工作流
          </span>
        </div>
        <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span>名称</span>
          <Input
            aria-label="工作流名称"
            className="xflow-editor-field"
            classNames={editorInputClassNames}
            value={workflow.name ?? ""}
            onChange={(event) => updateWorkflowMeta({ name: event.target.value })}
          />
        </label>
        <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span>版本</span>
          <Input
            aria-label="工作流版本"
            className="xflow-editor-field"
            classNames={editorInputClassNames}
            value={workflow.version ?? ""}
            onChange={(event) => updateWorkflowMeta({ version: event.target.value })}
          />
        </label>
        <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span>描述</span>
          <Input
            aria-label="工作流描述"
            className="xflow-editor-field"
            classNames={editorInputClassNames}
            value={workflow.description ?? ""}
            onChange={(event) => updateWorkflowMeta({ description: event.target.value })}
          />
        </label>
        <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span>Runner</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="工作流 Runner 选择 JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="xflow-editor-textarea xflow-editor-textarea--code"
              classNames={editorTextAreaClassNames}
              status={workflowJsonErrors.runner_selector ? "error" : undefined}
              value={workflowJsonText.runner_selector ?? "{}"}
              onChange={(event) => updateWorkflowJson("runner_selector", event.target.value)}
            />
            {workflowJsonErrors.runner_selector ? <span>{workflowJsonErrors.runner_selector}</span> : null}
          </div>
        </label>
        <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span>输入参数</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="工作流输入参数 JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="xflow-editor-textarea xflow-editor-textarea--code"
              classNames={editorTextAreaClassNames}
              status={workflowJsonErrors.params ? "error" : undefined}
              value={workflowJsonText.params ?? "{}"}
              onChange={(event) => updateWorkflowJson("params", event.target.value)}
            />
            {workflowJsonErrors.params ? <span>{workflowJsonErrors.params}</span> : null}
          </div>
        </label>
        <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span>变量配置</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="工作流变量配置 JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="xflow-editor-textarea xflow-editor-textarea--code"
              classNames={editorTextAreaClassNames}
              status={workflowJsonErrors.context ? "error" : undefined}
              value={workflowJsonText.context ?? "{}"}
              onChange={(event) => updateWorkflowJson("context", event.target.value)}
            />
            {workflowJsonErrors.context ? <span>{workflowJsonErrors.context}</span> : null}
          </div>
        </label>
        <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span>执行设置</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="工作流执行设置 JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="xflow-editor-textarea xflow-editor-textarea--code"
              classNames={editorTextAreaClassNames}
              status={workflowJsonErrors.settings ? "error" : undefined}
              value={workflowJsonText.settings ?? "{}"}
              onChange={(event) => updateWorkflowJson("settings", event.target.value)}
            />
            {workflowJsonErrors.settings ? <span>{workflowJsonErrors.settings}</span> : null}
          </div>
        </label>
        <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span>密钥</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="工作流密钥 JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="xflow-editor-textarea xflow-editor-textarea--code"
              classNames={editorTextAreaClassNames}
              status={workflowJsonErrors.credentials ? "error" : undefined}
              value={workflowJsonText.credentials ?? "{}"}
              onChange={(event) => updateWorkflowJson("credentials", event.target.value)}
            />
            {workflowJsonErrors.credentials ? <span>{workflowJsonErrors.credentials}</span> : null}
          </div>
        </label>
        <label className="xflow-editor-form-row xflow-editor-form-row--workflow">
          <span>Pin Data</span>
          <div className="xflow-editor-json-field">
            <Input.TextArea
              aria-label="工作流 Pin Data JSON"
              autoSize={{ minRows: 2, maxRows: 5 }}
              className="xflow-editor-textarea xflow-editor-textarea--code"
              classNames={editorTextAreaClassNames}
              status={workflowJsonErrors.pin_data ? "error" : undefined}
              value={workflowJsonText.pin_data ?? "{}"}
              onChange={(event) => updateWorkflowJson("pin_data", event.target.value)}
            />
            {workflowJsonErrors.pin_data ? <span>{workflowJsonErrors.pin_data}</span> : null}
          </div>
        </label>
      </div>
      </div>
    </div>
  ) : (
    <p className="xflow-editor-empty xflow-editor-inspector-empty">选择节点后编辑配置。</p>
  );
  const inspectorTabs = [
    { key: "config" as const, icon: <SettingOutlined />, label: "配置", panel: configPanel },
    {
      key: "connections" as const,
      icon: <BranchesOutlined />,
      label: "连接",
      panel: (
        <ConnectionsPanel
          workflow={workflow}
          selectedNode={selectedNode}
          onAddConnection={addSelectedConnection}
          onRemoveConnection={removeSelectedConnection}
        />
      )
    },
    { key: "run" as const, icon: <PlaySquareOutlined />, label: "运行", panel: <RunPanel selectedNode={selectedNode} runtime={runtime} /> }
  ];
  const activePanel = inspectorTabs.find((tab) => tab.key === activeTab)?.panel ?? configPanel;
  const activeTabIndex = inspectorTabs.findIndex((tab) => tab.key === activeTab);
  const handleTabKeyDown = (event: React.KeyboardEvent<HTMLButtonElement>, currentIndex: number) => {
    let nextIndex: number | undefined;
    if (event.key === "ArrowLeft" || event.key === "ArrowUp") {
      nextIndex = (currentIndex - 1 + inspectorTabs.length) % inspectorTabs.length;
    } else if (event.key === "ArrowRight" || event.key === "ArrowDown") {
      nextIndex = (currentIndex + 1) % inspectorTabs.length;
    } else if (event.key === "Home") {
      nextIndex = 0;
    } else if (event.key === "End") {
      nextIndex = inspectorTabs.length - 1;
    } else {
      return;
    }

    event.preventDefault();
    const nextTab = inspectorTabs[nextIndex];
    setActiveTab(nextTab.key);
    document.getElementById(`xflow-editor-inspector-tab-${nextTab.key}`)?.focus();
  };

  const inspectorHeaderMeta = selectedNode
    ? `${nodeLabel(selectedNode, selectedIndex)} · ${selectedNode.type ?? "unknown"}`
    : "选择节点以查看配置";

  return (
    <EditorPanel
      ariaLabel="属性"
      bodyClassName="xflow-editor-inspector__body"
      className="xflow-editor-inspector"
      icon={<SettingOutlined />}
      title="属性"
      extra={
        <div className="xflow-editor-inspector-header-actions">
          <span className="xflow-editor-inspector-header-meta" title={inspectorHeaderMeta}>{inspectorHeaderMeta}</span>
          {selectedNode ? (
            <Tooltip classNames={editorTooltipClassNames} title="删除节点（Delete）">
              <Button
                aria-label="删除节点"
                className="xflow-editor-inspector-delete"
                color="danger"
                danger
                icon={<DeleteOutlined />}
                size="small"
                variant="text"
                onClick={onDeleteNode}
              />
            </Tooltip>
          ) : null}
        </div>
      }
    >
      <div className="xflow-editor-inspector-tabs" role="tablist" aria-label="属性面板视图">
        {inspectorTabs.map((tab) => (
          <button
            aria-label={tab.label}
            aria-controls={`xflow-editor-inspector-panel-${tab.key}`}
            aria-selected={activeTab === tab.key}
            className={activeTab === tab.key ? "active" : ""}
            id={`xflow-editor-inspector-tab-${tab.key}`}
            key={tab.key}
            role="tab"
            tabIndex={activeTab === tab.key ? 0 : -1}
            type="button"
            onClick={() => setActiveTab(tab.key)}
            onKeyDown={(event) => handleTabKeyDown(event, inspectorTabs.findIndex((candidate) => candidate.key === tab.key))}
          >
            <span className="xflow-editor-inspector-tab-label">{tab.label}</span>
          </button>
        ))}
      </div>
      <div
        aria-labelledby={`xflow-editor-inspector-tab-${inspectorTabs[activeTabIndex]?.key ?? "config"}`}
        className="xflow-editor-inspector-panel"
        id={`xflow-editor-inspector-panel-${inspectorTabs[activeTabIndex]?.key ?? "config"}`}
        role="tabpanel"
      >
        {activePanel}
      </div>
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
  nodeTypes,
  operationError,
  blockedDraftNodes,
  collapsed,
  onToggle,
  onSelectNode,
  onRevealJson
}: {
  workflow: WorkflowDef;
  runtime?: RuntimeSnapshot;
  nodeTypes?: NodeTypesResponse;
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
  const diagnostics = React.useMemo(
    () => (isWaitingForDefinition ? [] : validateWorkflow(workflow, runtime, operationError, nodeTypes)),
    [isWaitingForDefinition, nodeTypes, operationError, runtime, workflow]
  );
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
        <div className="xflow-editor-diagnostics__tabs !gap-1.5" role="toolbar" aria-label="诊断视图">
          <strong className="xflow-editor-block-title !mr-2 !gap-1.5 !text-[11px] !font-semibold">
            <ConsoleSqlOutlined />
            诊断台
          </strong>
          <span className="active !inline-flex !h-5 !items-center !gap-1 !rounded !px-2 !text-[11px]">
            <WarningOutlined />
            问题
          </span>
          <UnavailableControl ariaLabel="运行日志" className="!h-5 !rounded !px-2 !text-[11px]" reason="运行日志尚未接入执行记录。">
            <FileSearchOutlined />运行日志
          </UnavailableControl>
          <UnavailableControl ariaLabel="输入" className="!h-5 !rounded !px-2 !text-[11px]" reason="运行输入快照尚未接入。">
            <ImportOutlined />输入
          </UnavailableControl>
          <UnavailableControl ariaLabel="输出" className="!h-5 !rounded !px-2 !text-[11px]" reason="运行输出快照尚未接入。">
            <ExportOutlined />输出
          </UnavailableControl>
        </div>
        <div className="xflow-editor-diagnostics__status !gap-3 !text-[10.5px]">
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
        <div className="xflow-editor-diagnostics__body !text-[11px]">
          <section className="xflow-editor-diagnostics-log !gap-0 !border-l-0 !px-0 !py-0" aria-label="诊断问题">
            <div className="xflow-editor-diagnostics-section-title !mb-0 !h-7 !border-b !px-3 !text-[10.5px]">
              <span className="xflow-editor-block-title">
                <CheckCircleOutlined />
                诊断问题
              </span>
              <span>{isWaitingForDefinition ? "等待节点" : `${errorCount} errors / ${warningCount} warnings`}</span>
            </div>
            <div className="!px-3 !py-2">
              {isWaitingForDefinition ? (
                <p className="xflow-editor-diagnostics-waiting !border-b-0 !py-1">
                  <span>waiting</span>
                  <span>载入或编辑真实工作流后，这里将列出 DSL、节点和连接的校验结果。</span>
                </p>
              ) : diagnostics.map((item, index) => (
                  <p
                    className="!grid !grid-cols-[74px_74px_minmax(0,1fr)] !gap-2 !border-b !py-1 !font-mono !text-[10.5px] !leading-4 last:!border-b-0"
                    key={`${item.area}-${index}`}
                  >
                    <span className={`${item.status === "pass" ? "ok" : item.status === "warn" ? "warn" : "error"} !min-w-0`}>
                      {item.status === "pass" ? "pass" : item.status}
                    </span>
                    <span className="!min-w-0">{item.area}</span>
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
          <section className="xflow-editor-diagnostics-summary !px-3 !py-2">
            <div className="xflow-editor-diagnostics-section-title !mb-2 !text-[10.5px]">
              <span className="xflow-editor-block-title">
                <CodeOutlined />
                DSL 摘要
              </span>
              <span>{isWaitingForDefinition ? "等待" : workflow.version ?? "spec 1.0"}</span>
            </div>
            <div className="xflow-editor-diagnostics-meter !mb-2">
              <div><span>节点</span><span>{nodes.length}</span></div>
              <i style={{ width: `${isWaitingForDefinition ? 0 : Math.min(Math.max(nodes.length * 10, 16), 100)}%` }} />
            </div>
            <div className="xflow-editor-diagnostics-meter !mb-2">
              <div><span>连接</span><span>{isWaitingForDefinition ? "0 / 0" : connectionsCount > 0 ? "完整" : "待连接"}</span></div>
              <i className="green" style={{ width: isWaitingForDefinition ? "0%" : connectionsCount > 0 ? "100%" : "18%" }} />
            </div>
            <div className="xflow-editor-diagnostics-kv !mt-2 !pt-2 !text-[10.5px]">
              <span>凭据</span>
              <strong>{isWaitingForDefinition ? "等待真实数据" : nodes.length === 0 ? "未配置" : "运行时注入"}</strong>
            </div>
            <pre className="xflow-editor-diagnostics-dsl" aria-label="DSL 预览">
              {serializeJson(workflow)}
            </pre>
          </section>
          <section className="xflow-editor-diagnostics-run !gap-1.5 !px-3 !py-2">
            <div className="xflow-editor-diagnostics-section-title !mb-1 !text-[10.5px]">
              <span className="xflow-editor-block-title">
                <ClockCircleOutlined />
                最近运行
              </span>
              <span>{runtime ? "execution-local" : "尚未运行"}</span>
            </div>
            <div className="!text-[10.5px]"><span>状态</span><strong data-status={runtime?.status ?? "pending"}>{runtime?.status ?? "尚未运行"}</strong></div>
            <div className="!text-[10.5px]"><span>当前节点</span><code>{runningNode ?? "-"}</code></div>
            <div className="!text-[10.5px]"><span>失败节点</span><code>{failedCount}</code></div>
            <div className="!text-[10.5px]"><span>跟踪节点</span><code>{runtimeNodes.length}</code></div>
            <p className="!mt-1 !pt-2 !text-[10px]"><InfoCircleOutlined /> {runtime ? "后续接运行日志、输入和输出快照。" : "运行真实工作流后，这里显示执行摘要和输出入口。"}</p>
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
  nodeTypes: sourceNodeTypes,
  appearance: controlledAppearance,
  themeVariant: controlledThemeVariant,
  onAppearanceChange,
  onThemeVariantChange
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
  const [leftDrawerTab, setLeftDrawerTab] = React.useState<"library" | "outline">("library");
  const [desktopRailSection, setDesktopRailSection] = React.useState<"library" | "outline">("library");
  const nodeLibrarySearchRef = React.useRef<InputRef | null>(null);
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
  const [running, setRunning] = React.useState(false);
  const [operationStatus, setOperationStatus] = React.useState(() => value.id ? "已保存" : "未保存");
  const [isDirty, setIsDirty] = React.useState(() => !value.id);
  const [operationError, setOperationError] = React.useState<string>();
  const [selectedKey, setSelectedKey] = React.useState<string | undefined>(() => defaultSelectedKey(nodes));
  const selectedIndex = nodes.findIndex((node, index) => nodeKey(node, index) === selectedKey);
  const selectedNode = selectedIndex >= 0 ? nodes[selectedIndex] : undefined;
  const editMode = viewMode === "edit";
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
    setParamIssues(undefined);
    setIsDirty(!value.id);
    setOperationError(undefined);
    setOperationStatus(value.id ? "已保存" : "未保存");
  }, [value]);

  React.useEffect(() => {
    setLocalRuntime(runtime);
  }, [runtime]);

  const commitWorkflow = React.useCallback(
    (nextWorkflow: WorkflowDef) => {
      const previousWorkflow = latestDraftRef.current;
      if (serializeJson(previousWorkflow) === serializeJson(nextWorkflow)) return;
      const nextHistory: WorkflowHistory = {
        undo: [...historyRef.current.undo, previousWorkflow].slice(-historyLimit),
        redo: []
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
    (update: WorkflowUpdater) => {
      commitWorkflow(update(latestDraftRef.current));
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
  const commitParameterDraft = React.useCallback((key: string) => {
    const draft = parameterDraftsRef.current[key];
    if (!draft || draft.error) return;
    const parsed = parseParametersText(draft.text);
    const { [key]: _committed, ...rest } = parameterDraftsRef.current;
    replaceParameterDrafts(rest);
    if (!parsed.value) return;
    const parameters = parsed.value;
    commitWorkflowWith((latest) => updateNodeByKey(latest, key, (node) =>
      serializeJson(node.parameters ?? {}) === serializeJson(parameters) ? node : { ...node, parameters }
    ));
  }, [commitWorkflowWith, replaceParameterDrafts]);
  const flushParameterDrafts = React.useCallback(() => {
    clearDraftTimer();
    for (const [key, draft] of Object.entries(parameterDraftsRef.current)) {
      if (!draft.error) commitParameterDraft(key);
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
    clearDraftTimer();
    if (!parsed.error) draftTimerRef.current = setTimeout(flushParameterDrafts, parameterDraftDebounceMs);
  }, [clearDraftTimer, flushParameterDrafts, replaceParameterDrafts]);
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
  const saveBlocked = invalidDraftCount > 0 || (validationMode === "enforce" && formIssueSummary.errors > 0);
  const saveTooltip = !onSave
    ? "保存不可用：宿主未提供 onSave 处理器"
    : invalidDraftCount > 0
      ? `${invalidDraftCount} 个节点的参数 JSON 未通过解析，修正后才能保存`
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

  const undoWorkflow = React.useCallback(() => {
    flushParameterDrafts();
    const previousWorkflow = historyRef.current.undo.at(-1);
    if (!previousWorkflow) return;
    restoreHistory(previousWorkflow, {
      undo: historyRef.current.undo.slice(0, -1),
      redo: [...historyRef.current.redo, latestDraftRef.current].slice(-historyLimit)
    });
  }, [flushParameterDrafts, restoreHistory]);

  const redoWorkflow = React.useCallback(() => {
    flushParameterDrafts();
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

  const validateCurrentWorkflow = React.useCallback(() => {
    const diagnostics = validateWorkflow(draftWorkflow, localRuntime, operationError, nodeTypes);
    const hasErrors = diagnostics.some((item) => item.status === "error");
    setOperationStatus(hasErrors ? "校验失败" : "校验通过");
    setBottomCollapsed(false);
  }, [draftWorkflow, localRuntime, nodeTypes, operationError]);

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

  // Escape never clears the selection: the effect below falls back to a default
  // node whenever the selected key is missing, so clearing would be undone on the
  // next render. Leaving a transient mode is the behavior that sticks.
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
    if (blockedByParameterDraft("保存")) return;
    // The ref, not render state: it already includes writes committed earlier
    // in this tick (the shortcut can fire from inside a text field).
    const workflowToSave = latestDraftRef.current;
    const revisionAtSave = draftRevisionRef.current;
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
    if (!nodes.some((node, index) => nodeKey(node, index) === selectedKey)) {
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
    if (layout === "c") {
      setLeftDrawerTab("library");
      openCompactDrawer("left");
    }
  }, [layout, openCompactDrawer]);

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
    duplicateSelectedNode,
    leaveTransientMode,
    onRun,
    onSave,
    redoWorkflow,
    running,
    runWorkflow,
    saveWorkflow,
    toggleLeftPanel,
    undoWorkflow
  ]);

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
                    aria-pressed={desktopRailSection === "library"}
                    className={desktopRailSection === "library" ? "active" : ""}
                    disabled={!editMode}
                    type="button"
                    onClick={() => setDesktopRailSection("library")}
                  >
                    <AppstoreAddOutlined />
                    <span>节点</span>
                  </button>
                  <button
                    aria-label="大纲"
                    aria-pressed={desktopRailSection === "outline"}
                    className={desktopRailSection === "outline" ? "active" : ""}
                    type="button"
                    onClick={() => setDesktopRailSection("outline")}
                  >
                    <BarsOutlined />
                    <span>大纲</span>
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
                  {editMode ? <NodeLibrary descriptors={libraryDescriptors} onAddNode={addNode} /> : null}
                  <Outline workflow={draftWorkflow} selectedKey={selectedKey} runtime={localRuntime} onSelect={setSelectedKey} />
                </div>
              </>
            )}
          </aside>
        ) : (
          <aside className="xflow-editor-rail xflow-editor-compact-rail xflow-editor-compact-rail--left" aria-label="紧凑导航">
            <button
              aria-label="节点"
              className={compactDrawer === "left" && leftDrawerTab === "library" ? "active" : ""}
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
              aria-label="大纲"
              className={compactDrawer === "left" && leftDrawerTab === "outline" ? "active" : ""}
              type="button"
              onClick={() => {
                setLeftDrawerTab("outline");
                openCompactDrawer("left");
              }}
            >
              <BarsOutlined />
              <span>大纲</span>
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
                selectedNodeId={selectedKey}
                onSelectNode={setSelectedKey}
                onNodePositionChange={updateNodePosition}
                onConnect={addCanvasConnection}
                onDeleteConnection={removeCanvasConnection}
                onDeleteNode={deleteSelectedNode}
                onDropNode={dropNode}
                danglingPorts={canvasDanglingPorts}
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
              <Inspector
                workflow={draftWorkflow}
                selectedNode={selectedNode}
                selectedIndex={selectedIndex}
                runtime={localRuntime}
                onChangeWith={commitWorkflowWith}
                onApplyNodePatches={applyNodePatches}
                onDeleteNode={deleteSelectedNode}
                onNodeRenamed={handleNodeRenamed}
                parameterDrafts={parameterDrafts}
                parameterTab={parameterTab}
                revealNonce={inspectorRevealNonce}
                nodeTypes={nodeTypes}
                formHost={formHost}
                externalIssues={selectedNode ? externalIssuesMap.get(nodeName(selectedNode, selectedIndex)) : undefined}
                onParameterText={updateParameterText}
                onParameterCommit={commitParameterDraft}
                onParameterTabChange={setParameterTab}
              />
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
          title={leftDrawerTab === "library" ? "节点" : "大纲"}
          size={340}
          onClose={() => setCompactDrawer(undefined)}
        >
          {leftDrawerTab === "library" ? (
            editMode ? <NodeLibrary autoFocusSearch descriptors={libraryDescriptors} onAddNode={addNode} searchInputRef={nodeLibrarySearchRef} /> : <p className="xflow-editor-empty">预览模式为只读。</p>
          ) : (
            <Outline workflow={draftWorkflow} selectedKey={selectedKey} runtime={localRuntime} onSelect={(key) => setSelectedKey(key)} />
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
            <Inspector
              workflow={draftWorkflow}
              selectedNode={selectedNode}
              selectedIndex={selectedIndex}
              runtime={localRuntime}
              onChangeWith={commitWorkflowWith}
              onApplyNodePatches={applyNodePatches}
              onDeleteNode={deleteSelectedNode}
              onNodeRenamed={handleNodeRenamed}
              parameterDrafts={parameterDrafts}
              parameterTab={parameterTab}
              revealNonce={inspectorRevealNonce}
              nodeTypes={nodeTypes}
              formHost={formHost}
              externalIssues={selectedNode ? externalIssuesMap.get(nodeName(selectedNode, selectedIndex)) : undefined}
              onParameterText={updateParameterText}
              onParameterCommit={commitParameterDraft}
              onParameterTabChange={setParameterTab}
            />
          ) : (
            <section className="xflow-editor-preview-inspector" aria-label="预览模式说明">
              <EyeOutlined />
              <strong>预览模式</strong>
              <p>画布、节点属性和工作流定义均保持只读。切换到编辑模式后可继续修改。</p>
            </section>
          )}
        </Drawer>
      ) : null}

      <Diagnostics
        workflow={draftWorkflow}
        runtime={localRuntime}
        nodeTypes={nodeTypes}
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
