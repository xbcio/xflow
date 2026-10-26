import * as React from "react";
import {
  BanIcon,
  BranchIcon,
  ClockIcon,
  KeyIcon,
  PinIcon,
  SkipIcon,
  SpinIcon,
  WarnIcon,
  XMarkIcon
} from "@xflow/icons";
import {
  Background,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  SelectionMode,
  type Connection as ReactFlowConnection,
  type BuiltInEdge,
  type Edge,
  type EdgeChange,
  type NodeChange,
  type IsValidConnection,
  type Node,
  type NodeProps,
  type OnBeforeDelete,
  type ReactFlowInstance,
  type ReactFlowState,
  type Viewport as ReactFlowViewport,
  useStore,
  useStoreApi,
  ViewportPortal
} from "@xyflow/react";
import {
  autoLayoutPositions,
  categoryOf,
  edgeVariant,
  foundationTokens,
  hasNodePosition,
  isSupplyNode,
  isTextEntryTarget,
  nodeCategoryMeta,
  nodeKey,
  nodeStatusMeta,
  runtimeForNode,
  toGraphModel,
  workflowStatusMeta,
  type AutoLayoutBox,
  type EdgeVariant,
  type ErrorPolicy,
  type GraphEdge,
  type GraphNode,
  type NodeCategory,
  type NodeStatusKey,
  type Position as NodePosition,
  type CardSummaryRow,
  type RuntimeNodeSnapshot,
  type RuntimeSnapshot,
  type Viewport,
  type WorkflowEditorSection,
  type WorkflowDef
} from "@xflow/core";
import { CanvasRulers, FLOAT_ATTRIBUTE, MAX_ZOOM, MINIMAP_SIZE, MIN_ZOOM, ZoomBar, measureSurface } from "./canvasChrome";
import { fitViewportClearOf, locateViewportClearOf } from "./fit";
import { categoryGlyph, statusGlyph } from "./icons";
import { READABLE_FIT_ZOOM, hitInverseZoom, lodLevel, lodScale, type LodLevel } from "./lod";
export { READABLE_FIT_ZOOM } from "./lod";
import "./styles.css";

/**
 * Payload key the node library writes when a node tile starts a drag. It lives
 * here, not in the editor, because the preview owns the drop target and the
 * editor already depends on this package — the reverse import is not allowed.
 */
export const XFLOW_NODE_DRAG_MIME = "application/xflow-node-type";

/**
 * §6.2's canvas dot grid, in flow pixels: the same 16px pitch the background
 * paints. Drags quantize to it (xyflow's `snapToGrid`/`snapGrid`), and a
 * section move or resize does too, so nothing lands between two dots. It only
 * affects gestures -- loading a definition keeps every stored coordinate as
 * authored, so an existing layout is never reflowed behind the user's back.
 */
const CANVAS_GRID = 16;
/** React Flow reads `snapGrid` as a tuple; keep one stable reference. */
const CANVAS_SNAP_GRID: [number, number] = [CANVAS_GRID, CANVAS_GRID];

/** Round one flow-coordinate value onto the canvas grid. */
const snapToCanvasGrid = (value: number): number => Math.round(value / CANVAS_GRID) * CANVAS_GRID;

/** One node's canvas position, as a completed drag left it. */
export interface PreviewNodePosition {
  id: string;
  position: Required<NodePosition>;
}

/** A data-flow connection expressed with workflow node names and port names. */
export interface PreviewConnection {
  source: string;
  sourcePort: string;
  target: string;
  targetPort: string;
}

export type XFlowPreviewInteractionMode = "select" | "connect";

/**
 * One node's wait note (§6.12). `text` replaces the generic 等待信号 line on a
 * suspended card; `action` renders a button beside it and requires
 * {@link XFlowPreviewProps.onNodeAction}.
 */
export interface XFlowPreviewNodeNote {
  text: string;
  action?: string;
}

/**
 * One card's local node-form findings (CP-01): the counts the badge speaks and
 * the first message it shows as a title. The owner builds this from the same
 * rows its diagnostics use, so the card and the drawer can never disagree.
 */
export interface XFlowPreviewNodeIssues {
  errors: number;
  warnings: number;
  /**
   * The first finding's message, already safe to display. The owner must not
   * put a parameter value in it: a title is visible text, not a log.
   */
  message?: string;
}

export interface XFlowPreviewProps {
  workflow: WorkflowDef;
  runtime?: RuntimeSnapshot;
  className?: string;
  editable?: boolean;
  /**
   * Restricts an editable canvas to one intentional interaction at a time.
   * Omit it to retain the package's legacy editable behavior (drag + connect).
   */
  interactionMode?: XFlowPreviewInteractionMode;
  /**
   * Draws the §6.2 minimap in the canvas's bottom-right corner at 200x124. Off by
   * default. The zoom bar's toggle (and ⇧M) flips it from there; a new value of
   * this prop wins over the toggle again. Every flip re-fits the graph, so the
   * minimap never lands on top of content.
   */
  minimap?: boolean;
  /**
   * Draws the §6.2 rulers along the canvas's top and left edges. On by default,
   * toggled like `minimap` (⇧R), and never drawn while the workflow has no nodes.
   */
  ruler?: boolean;
  /** Shows the §6.2 bottom-left zoom bar, which also carries the two toggles. On by default. */
  zoomBar?: boolean;
  /**
   * Adds 自动布局 to the zoom bar. The preview never moves a node itself: the owner
   * lays the graph out and echoes the new positions back through `workflow`.
   */
  onAutoLayout?: () => void;
  /**
   * Replaces the "No nodes to preview" note shown on a canvas with no nodes. The
   * canvas stays a drop target underneath; only the content itself takes the pointer.
   */
  emptyState?: React.ReactNode;
  /**
   * The §6.2 top-left float: the editor's 添加节点, or its read-only notice. Drawn clear
   * of the rulers in the zoom bar's float chrome, and kept clear by every fit like
   * the other floating layers.
   */
  canvasFloat?: React.ReactNode;
  /**
   * The §6.2 top-right float: the editor's inspector reopen chip. Same chrome as
   * `canvasFloat`, parked 12px inside the rulers (or the canvas edge without them),
   * so the two slots cannot drift apart.
   */
  canvasFloatEnd?: React.ReactNode;
  /**
   * Draws the end float as the baseline's compact chip instead of the 34px
   * floating-bar chrome: 28px tall, 6px radius, 12px in from the edge.
   */
  canvasFloatEndCompact?: boolean;
  selectedNodeId?: string;
  onSelectNode?: (nodeId: string) => void;
  /**
   * The owner-managed selection set. Supplying it turns the canvas into a
   * multi-select surface: nodes become selectable, a left drag draws a marquee,
   * and every change is reported through {@link onSelectionChange}. Omit it to
   * keep the legacy single-selection contract.
   */
  selectedNodeIds?: readonly string[];
  /**
   * Reports the full selection whenever the canvas changes it (a click, a
   * multi-key toggle, a marquee, or the deselection a drag start performs).
   * `primaryId` names the node a single-node change just picked; a marquee
   * leaves it undefined so the owner can keep its current primary when that
   * node is still selected.
   */
  onSelectionChange?: (nodeIds: readonly string[], primaryId?: string) => void;
  /**
   * Per-node wait notes for suspended cards, keyed by node name. The approval
   * flow uses it to show the chain and progress instead of the generic
   * 等待信号 line (§6.12). A missing entry keeps the generic line.
   */
  nodeNotes?: Readonly<Record<string, XFlowPreviewNodeNote>>;
  /**
   * Per-node run-record annotations for the card's meta line, keyed by node
   * name (重跑起点 / 沿用原执行结果, §6.8). A value takes the meta line's left
   * slot; without one the slot keeps the node duration (or stays empty).
   */
  nodeMetaNotes?: Readonly<Record<string, string>>;
  /**
   * Local node-form findings per node, drawn as the card's issue badge (CP-01):
   * the error count in the danger tone, or the warning count when there is no
   * error. The callback receives the canvas's `GraphNode` and must be
   * referentially stable (a `useCallback`/`useMemo`), like `nodeIcon`; without
   * it, or when it returns nothing for a node, no badge is drawn.
   */
  nodeIssues?: (node: GraphNode) => XFlowPreviewNodeIssues | undefined;
  /**
   * A press on a wait note's action button (e.g. 处理 on an approval card).
   * Without it the action button is not drawn: the preview never pretends a
   * gesture it cannot deliver.
   */
  onNodeAction?: (nodeName: string, action: string) => void;
  /**
   * The category each node type's descriptor declares (spec §6.13), from the
   * host's `/v1/node-types` response. A type missing here keeps the prefix
   * fallback; a declared value wins over it in `categoryOf`.
   */
  nodeCategories?: Readonly<Record<string, string>>;
  /**
   * The human name of each node type (the descriptor's `display_name`), keyed
   * by type. The card prints this instead of the raw id; a type missing here
   * falls back to {@link readableNodeType}. The raw id stays in the inspector.
   */
  nodeTypeLabels?: Readonly<Record<string, string>>;
  /**
   * Per-node-type glyph for the card's category block, replacing the category
   * table's icon when it returns a node. The callback receives the canvas's
   * `GraphNode` and must be referentially stable (a `useCallback`, or a value
   * that never changes) so cards keep their `React.memo` identity. Omit it to
   * keep the category icon.
   */
  nodeIcon?: (node: GraphNode) => React.ReactNode;
  /**
   * Card-summary rows per node key (`id ?? name`), already resolved from the
   * type's `card_summary` and the node's parameters by core's
   * `cardSummaryRows` (spec §6.1.1). The preview only draws them: formatting and
   * the localized display names belong to the owner holding the schemas.
   */
  nodeSummaries?: Readonly<Record<string, readonly CardSummaryRow[]>>;
  /**
   * A press on one card-summary row (spec §6.1.1): the owner selects the node
   * and reveals that parameter in its editor. Without it the rows stay plain
   * text — the preview never pretends a gesture it cannot deliver.
   */
  onLocateParameter?: (nodeId: string, fieldPath: string) => void;
  /** A click on the empty canvas (not a card or a connection): the owner clears its selection. */
  onPaneClick?: () => void;
  onNodePositionChange?: (nodeId: string, position: { x: number; y: number }) => void;
  /**
   * Reports every node a completed drag moved, in one call. A multi-selection
   * drag moves the whole selected set, so the owner receives all of their final
   * positions together and can commit them as one undoable change. When
   * supplied it replaces {@link onNodePositionChange}; without it the legacy
   * single-node callback keeps its contract.
   */
  onNodePositionsChange?: (positions: readonly PreviewNodePosition[]) => void;
  /** Requests that the owner add a data-flow connection. The preview never mutates the workflow. */
  onConnect?: (connection: PreviewConnection) => void;
  /** Requests that the owner remove one data-flow connection. The preview never mutates the workflow. */
  onDeleteConnection?: (connection: PreviewConnection) => void;
  /**
   * Requests that the owner remove a node. Supplying it enables Delete and
   * Backspace on the canvas. The preview never mutates the workflow, and it
   * stands down when a connection is selected so one press cannot both remove a
   * connection and the node behind it.
   */
  onDeleteNode?: (nodeId: string) => void;
  /**
   * Requests that the owner remove a whole selection. Supplying it makes Delete
   * and Backspace a batch action: the owner receives every selected node in one
   * call, so it can commit the removal as a single undoable change. Without it,
   * the single-node `onDeleteNode` contract still applies.
   */
  onDeleteNodes?: (nodeIds: readonly string[]) => void;
  /**
   * Requests that the owner add a node of `type` at a canvas position. A drop on
   * an empty workflow is admitted like any other, so the very first node can be
   * dragged in rather than clicked. The preview never mutates the workflow.
   */
  onDropNode?: (type: string, position: Required<NodePosition>) => void;
  /**
   * Output ports, by source node name, whose edges no longer lead anywhere
   * valid (e.g. a removed dynamic branch). Their edges are drawn dashed in the
   * warning color and labeled; the preview never removes them.
   */
  danglingPorts?: readonly DanglingPort[];
  /**
   * Restores the canvas pan/zoom saved in `WorkflowEditorMetadata.viewport`
   * (ADR-D4 §2.3). When supplied, the preview applies it on mount instead of
   * {@link InitialViewFitter}'s measured auto-fit, so a reload lands where the
   * author left the canvas rather than re-fitting to the graph bounds.
   */
  defaultViewport?: Viewport;
  /**
   * Visual canvas sections (spec §6.2 / ADR-D4 §2.3), from the workflow's
   * `editor_metadata.sections`. Each section draws one purely visual box behind
   * the cards -- dashed border plus a top-left title label -- at its canvas
   * coordinates; the accent colour is derived from the dominant category of its
   * member nodes, because no colour is stored. Sections are not `workflow.groups`:
   * groups are scheduling semantics and the preview never draws them.
   *
   * Between gestures the boxes sit below the cards and take no pointer events,
   * so they never interfere with a node drag or a canvas pan; only a press on
   * a box's own margin, title chip or resize handle (never on a card) starts a
   * section gesture. With none of the section callbacks they are read-only
   * decoration; the editor supplies {@link onSectionTitleChange} /
   * {@link onSectionDelete} / {@link onSectionGeometryChange} to expose the
   * title editor, the delete button and the move/resize handles on each box. A
   * section whose `node_keys` resolve to no node, or whose box has no positive
   * size, is not drawn (the server drops those too, but a hand-written
   * definition can still carry one).
   */
  sections?: readonly WorkflowEditorSection[];
  /**
   * Renames one visual section from its title chip (double-click or Enter).
   * Supplying this and/or {@link onSectionDelete} turns the boxes' title chips
   * into editing handles; without either the boxes stay the read-only
   * decoration every other host gets. The preview never renames anything
   * itself: the owner applies the change to `editor_metadata.sections` and
   * echoes the new value back through `sections`.
   *
   * The title is passed as typed (trimmed only); an empty title is the owner's
   * call to resolve, because only it knows the default naming rule.
   */
  onSectionTitleChange?: (sectionId: string, title: string) => void;
  /**
   * Requests the removal of one visual section (the title chip's × button).
   * The preview never removes anything itself; the owner confirms and applies
   * the change. A section without an id gets no handles, since the server
   * drops id-less sections anyway.
   */
  onSectionDelete?: (sectionId: string) => void;
  /**
   * Commits one move or resize gesture on a visual section. While the gesture
   * is live the box follows the pointer locally -- no state leaves this frame
   * on pointermove -- and this fires once, on release, with the final rectangle
   * in flow coordinates (screen deltas are divided by the current zoom).
   *
   * "Move" means a press on the box's empty interior or its title chip; a press
   * on a card inside the box still belongs to the card. The bottom-right
   * handle resizes, clamped to the minimum box. Supplying this makes the boxes
   * draggable; without it they stay the read-only decoration every other host
   * gets. The preview never moves anything itself: the owner applies the
   * rectangle to `editor_metadata.sections` and echoes it back through
   * `sections`.
   */
  onSectionGeometryChange?: (sectionId: string, geometry: PreviewSectionGeometry) => void;
  /**
   * Reports the canvas pan/zoom after the user stops panning or zooming
   * (React Flow's `onMoveEnd`). The preview never persists this itself; the
   * owner decides when/whether to save it as part of `editor_metadata`.
   */
  onViewportChange?: (viewport: Viewport) => void;
  /**
   * Requests a context menu for a node, at viewport (client) coordinates. Fired
   * by a right click on the node, or by Shift+F10 / the ContextMenu key while a
   * node is focused or selected (anchored at the node's center). The node is
   * selected first, so the owner's inspector already follows it. Omit it to
   * leave the browser's native menu untouched.
   */
  onNodeContextMenu?: (nodeId: string, position: PreviewClientPosition) => void;
  /**
   * Requests a context menu for the empty canvas. `flowX`/`flowY` are the same
   * point in canvas coordinates (pan and zoom folded in), suitable as a drop
   * position for a node added from that menu. Omit it to leave the browser's
   * native menu untouched.
   */
  onPaneContextMenu?: (position: PreviewClientPosition & { flowX: number; flowY: number }) => void;
  /**
   * Reports a handle once React Flow is live, for view commands the owner's own
   * chrome has to trigger (a "fit view" or "center node" item in its context
   * menu). The preview's built-in fit control keeps working unchanged, and
   * nothing happens until the owner calls the handle.
   */
  onReady?: (handle: XFlowPreviewHandle) => void;
}

export interface PreviewClientPosition {
  clientX: number;
  clientY: number;
}

/** A visual section's rectangle in flow coordinates, as a move/resize left it. */
export interface PreviewSectionGeometry {
  x: number;
  y: number;
  width: number;
  height: number;
}

/** View commands the preview exposes to its owner; read-only, never mutating. */
export interface XFlowPreviewHandle {
  /**
   * Fits the whole graph, exactly like the canvas's own fit control. `minZoom`
   * lifts the automatic fit's floor; the callers that lay the graph out pass
   * {@link READABLE_FIT_ZOOM} so a huge graph is anchored, not shrunk to a
   * sliver. Omitted, it fits everything like the zoom bar's 适应 does.
   */
  fitView: (options?: { padding?: number; minZoom?: number }) => void;
  /** Brings one node into view; an unknown node id is ignored. */
  fitNode: (nodeId: string, options?: { padding?: number }) => void;
}

/** One output port of a node that the owner reports as no longer existing. */
export interface DanglingPort {
  source: string;
  sourcePort: string;
}

const danglingKey = (source: string, port: string) => `${source}\u0000${port}`;

/**
 * §6.1.1: a card shows at most three summary lines. The rows past the second
 * fold into the third line's "+n 项", so a five-field node still keeps the
 * promise instead of growing without bound.
 */
const SUMMARY_ROWS = 3;

/** Duration the way the card prints it: "86 ms" under a second, "1.2 s" above. */
function formatDuration(ms: number): string {
  return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(1)} s`;
}

/** Letters that must stay upper-case in a type name read off the raw id. */
const TYPE_ACRONYMS = new Set([
  "ai", "api", "csv", "db", "http", "https", "id", "jwt", "llm", "rpc", "sse", "sql", "url", "uuid", "xml"
]);

/**
 * The readable type name a card shows when the owner supplied no descriptor
 * label: the type's tail, capitalized word by word, with acronyms left
 * upper-case (`xflow.http` → "HTTP", `xflow.trigger.timer` → "Timer"). The raw
 * id stays in the inspector and in the name's `title`.
 */
export function readableNodeType(type: string): string {
  const tail = type.split(/[.:/\\]/).filter(Boolean).pop() ?? type;
  const words = tail.split(/[-_]+/).filter(Boolean);
  const label = words
    .map((word) => TYPE_ACRONYMS.has(word.toLowerCase()) ? word.toUpperCase() : word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
  return label || type;
}

/** Node timeouts are nanoseconds; the badge shows whole h/m/s when it can. */
function formatNodeTimeout(ns: number): string {
  const seconds = ns / 1_000_000_000;
  if (seconds >= 3600 && Number.isInteger(seconds / 3600)) return `${seconds / 3600}h`;
  if (seconds >= 60 && Number.isInteger(seconds / 60)) return `${seconds / 60}m`;
  if (Number.isInteger(seconds)) return `${seconds}s`;
  return `${ns}ns`;
}

/**
 * A node's own configured run policy, drawn as the card's configuration
 * badges. Workflow-level defaults are deliberately not repeated on every card:
 * a badge means this node overrides them.
 */
interface PreviewNodePolicy {
  /** Effective retry bound: the node's own `retry.max_attempts` when enabled. */
  retryAttempts?: number;
  /** The node's own `timeout`, in nanoseconds. */
  timeoutNs?: number;
  /** The node's own failure route (`on_error`), never the workflow default. */
  onError?: ErrorPolicy;
}

/**
 * What a card knows about its run. `undefined` means the preview was given no
 * `runtime` at all (edit mode): the card then draws no status chip. A passed
 * `runtime` gives every node a snapshot — one that the run has not reached is
 * `{ status: "pending" }` — so "no run" and "run, but this node is queued" stay
 * distinguishable, which core's `runtimeForNode` alone cannot express.
 *
 * `runtimeForNode` pads a node the run never reported with `pending`, so it cannot
 * tell that from a snapshot that says `pending`; presence is read from the keys
 * (name first, then id, the same order). A disabled node is never queued: the
 * engine commits it as skipped when its turn comes, so while the run has not
 * reported it, it keeps its edit-mode `disabled` face instead of turning pending.
 */
function nodeSnapshot(runtime: RuntimeSnapshot | undefined, node: GraphNode): RuntimeNodeSnapshot | undefined {
  if (!runtime) return undefined;
  const nodes = runtime.nodes;
  const own = (key: string | undefined) => (key && nodes && Object.hasOwn(nodes, key) ? nodes[key] : undefined);
  const reported = own(node.name) ?? own(node.id);
  if (!reported && node.disabled) return undefined;
  return reported ?? runtimeForNode(runtime, node);
}

/**
 * The status a card, its chip and its minimap swatch speak: the run's status when
 * there is one, otherwise `disabled` for a disabled node, otherwise nothing.
 */
function displayStatus(node: GraphNode, snapshot: RuntimeNodeSnapshot | undefined): NodeStatusKey | undefined {
  return snapshot?.status ?? (node.disabled ? "disabled" : undefined);
}

/** One drawable §6.2 canvas section: resolved members, box, and accent category. */
interface PreviewSectionBox {
  key: string;
  id?: string;
  title?: string;
  x: number;
  y: number;
  width: number;
  height: number;
  category: NodeCategory;
}

function finiteOr(value: number | undefined, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

/**
 * Resolves the owner's visual sections against the canvas graph (§6.2). A
 * section is drawn only when it still covers a node and has a positive box:
 * the server drops those, but a hand-written definition can carry one, and a
 * zero-size box would paint an orphan label over the canvas.
 *
 * `node_keys` uses the same stable identity as `positions`/`ui` (`id ?? name`),
 * which is exactly a `GraphNode.id`, so the lookup is direct. The accent
 * category is the dominant category of the section's member nodes (first seen
 * wins a tie), read through core's `categoryOf` so a box and the cards inside
 * it cannot disagree; no colour is stored in the definition.
 */
function toSectionBoxes(
  sections: readonly WorkflowEditorSection[] | undefined,
  graphNodes: GraphNode[],
  nodeCategories: Readonly<Record<string, string>> | undefined
): PreviewSectionBox[] {
  if (!sections || sections.length === 0) return [];
  const nodesById = new Map(graphNodes.map((node) => [node.id, node]));
  const boxes: PreviewSectionBox[] = [];

  sections.forEach((section, index) => {
    const keys = Array.isArray(section.node_keys) ? section.node_keys : [];
    const members: GraphNode[] = [];
    const seen = new Set<string>();
    for (const key of keys) {
      if (typeof key !== "string" || seen.has(key)) continue;
      seen.add(key);
      const node = nodesById.get(key);
      if (node) members.push(node);
    }
    // Empty, or every key points at a node that no longer exists.
    if (members.length === 0) return;

    const width = finiteOr(section.width, 0);
    const height = finiteOr(section.height, 0);
    // A box with no extent has nothing to frame.
    if (width <= 0 || height <= 0) return;

    const tally = new Map<NodeCategory, number>();
    for (const member of members) {
      const category = categoryOf(member.type, nodeCategories?.[member.type]);
      tally.set(category, (tally.get(category) ?? 0) + 1);
    }
    let category: NodeCategory = "unknown";
    let best = 0;
    for (const [candidate, count] of tally) {
      if (count > best) {
        best = count;
        category = candidate;
      }
    }

    const title = typeof section.title === "string" ? section.title.trim() : "";
    boxes.push({
      // A duplicate/absent id is dropped by the server; the index still gives
      // the React key a stable identity across renders.
      key: `${section.id ?? "section"}:${index}`,
      id: section.id,
      title: title || undefined,
      x: finiteOr(section.x, 0),
      y: finiteOr(section.y, 0),
      width,
      height,
      category
    });
  });

  return boxes;
}

/** Smallest box a resize may leave, in flow pixels; below this the chip dominates. */
const SECTION_MIN_WIDTH = 120;
const SECTION_MIN_HEIGHT = 80;
/**
 * Screen pixels a press must travel before it is a drag rather than a click.
 * Four is the usual double-click slop, so double-clicking the title chip still
 * opens the rename editor instead of nudging the box by a pixel or two.
 */
const SECTION_DRAG_THRESHOLD = 4;

/** One pointer gesture in flight on a section box, between press and release. */
interface SectionGesture {
  mode: "move" | "resize";
  pointerId: number | undefined;
  startClientX: number;
  startClientY: number;
  origin: PreviewSectionGeometry;
  moved: boolean;
  /** The box owns the pointer; without it, moves outside the box are lost. */
  captured: boolean;
}

/**
 * One §6.2 section box. Read-only by default; when the owner supplies the
 * section callbacks, the title chip becomes an inline editor (double-click or
 * Enter to open, Enter/blur to commit, Escape to cancel) and grows a × button.
 * With {@link XFlowPreviewProps.onSectionGeometryChange} the box itself is a
 * move handle and its bottom-right corner resizes; the live rectangle stays
 * local to this frame and only the final one is reported, so a drag never
 * rerenders the editor. Editing state lives here, per box, so a gesture
 * rerenders only this frame.
 */
function PreviewSectionFrame({
  box,
  editable,
  onTitleChange,
  onDelete,
  onGeometryChange
}: {
  box: PreviewSectionBox;
  editable: boolean;
  onTitleChange?: (sectionId: string, title: string) => void;
  onDelete?: (sectionId: string) => void;
  onGeometryChange?: (sectionId: string, geometry: PreviewSectionGeometry) => void;
}): React.ReactElement {
  const [editing, setEditing] = React.useState(false);
  const [draft, setDraft] = React.useState("");
  // Escape must close without an `onBlur` commit racing it, and Enter must not
  // report twice if the unmounting input also blurs.
  const cancelRef = React.useRef(false);
  const editingRef = React.useRef(false);
  const canRename = editable && box.id !== undefined && onTitleChange !== undefined;
  const canDelete = editable && box.id !== undefined && onDelete !== undefined;
  const canMove = editable && box.id !== undefined && onGeometryChange !== undefined;
  // Canvas deltas are screen pixels; dividing by the viewport scale is what
  // makes a drag track the pointer at any zoom.
  const zoom = useStore((state) => state.transform[2]);
  const [live, setLive] = React.useState<PreviewSectionGeometry | null>(null);
  const gestureRef = React.useRef<SectionGesture | null>(null);
  // Between gestures the owner's prop is the rectangle; during one, this frame
  // follows the pointer without touching the owner's state.
  const geometry: PreviewSectionGeometry = live ?? { x: box.x, y: box.y, width: box.width, height: box.height };

  const geometryAt = (gesture: SectionGesture, clientX: number, clientY: number): PreviewSectionGeometry => {
    const scale = zoom > 0 ? zoom : 1;
    const dx = (clientX - gesture.startClientX) / scale;
    const dy = (clientY - gesture.startClientY) / scale;
    if (gesture.mode === "move") {
      return {
        x: snapToCanvasGrid(gesture.origin.x + dx),
        y: snapToCanvasGrid(gesture.origin.y + dy),
        width: gesture.origin.width,
        height: gesture.origin.height
      };
    }
    return {
      x: gesture.origin.x,
      y: gesture.origin.y,
      width: Math.max(SECTION_MIN_WIDTH, snapToCanvasGrid(gesture.origin.width + dx)),
      height: Math.max(SECTION_MIN_HEIGHT, snapToCanvasGrid(gesture.origin.height + dy))
    };
  };

  const samePointer = (gesture: SectionGesture, event: React.PointerEvent<HTMLElement>): boolean =>
    gesture.pointerId === undefined || event.pointerId === undefined || gesture.pointerId === event.pointerId;

  /** Capture so a fast drag that crosses a card keeps following this box. */
  const capturePointer = (element: HTMLElement, gesture: SectionGesture): void => {
    if (gesture.captured || gesture.pointerId === undefined) return;
    try {
      element.setPointerCapture?.(gesture.pointerId);
      gesture.captured = true;
    } catch {
      // jsdom has no active pointer to capture; the box still gets the moves.
    }
  };

  const beginGesture =
    (mode: SectionGesture["mode"]) =>
    (event: React.PointerEvent<HTMLElement>): void => {
      if (!canMove || event.button !== 0 || gestureRef.current !== null) return;
      // A press on a control inside the box belongs to that control, not to
      // the move handle. The exception is the title chip's rename button: the
      // chip IS the drag handle, and its double-click rename still works
      // because the pointer is only captured once the press becomes a drag.
      let deferCapture = false;
      if (mode === "move") {
        const control = (event.target as HTMLElement).closest("button, input, textarea, select, a");
        if (control !== null) {
          if (!control.classList.contains("xflow-preview-section__title-action")) return;
          // Capturing on pointerdown would retarget the click/dblclick that
          // follows to this box, so the chip's own rename handler would never
          // see them.
          deferCapture = true;
        }
      }
      event.preventDefault();
      event.stopPropagation();
      const gesture: SectionGesture = {
        mode,
        pointerId: event.pointerId,
        startClientX: event.clientX,
        startClientY: event.clientY,
        origin: geometry,
        moved: false,
        captured: false
      };
      gestureRef.current = gesture;
      if (!deferCapture) capturePointer(event.currentTarget, gesture);
    };

  const handlePointerMove = (event: React.PointerEvent<HTMLElement>): void => {
    const gesture = gestureRef.current;
    if (gesture === null || !samePointer(gesture, event)) return;
    if (!gesture.moved) {
      const travelled = Math.hypot(event.clientX - gesture.startClientX, event.clientY - gesture.startClientY);
      if (travelled < SECTION_DRAG_THRESHOLD) return;
      gesture.moved = true;
      // A press on the title chip waits for this moment: capturing on
      // pointerdown would retarget the click/dblclick that opens the renamer.
      capturePointer(event.currentTarget, gesture);
    }
    event.preventDefault();
    event.stopPropagation();
    setLive(geometryAt(gesture, event.clientX, event.clientY));
  };

  const endGesture =
    (commit: boolean) =>
    (event: React.PointerEvent<HTMLElement>): void => {
      const gesture = gestureRef.current;
      if (gesture === null || !samePointer(gesture, event)) return;
      gestureRef.current = null;
      try {
        // Only a drag ever captured the pointer.
        if (gesture.captured && gesture.pointerId !== undefined) event.currentTarget.releasePointerCapture?.(gesture.pointerId);
      } catch {
        // Nothing was captured, or jsdom has no active pointer.
      }
      // A press that never crossed the threshold is a click (or half of the
      // double-click that opens the title editor), not a geometry change.
      if (!gesture.moved) {
        setLive(null);
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      const final = geometryAt(gesture, event.clientX, event.clientY);
      setLive(null);
      if (commit) onGeometryChange?.(box.id as string, final);
    };

  /** A captured pointer that never comes back leaves no gesture behind. */
  const cancelGesture = (): void => {
    gestureRef.current = null;
    setLive(null);
  };

  /**
   * A press on the title chip has not captured the pointer yet. If it leaves
   * the box before crossing the threshold, its moves would never reach this
   * frame again, so the pending gesture is dropped instead of sticking.
   */
  const handlePointerLeave = (event: React.PointerEvent<HTMLElement>): void => {
    const gesture = gestureRef.current;
    if (gesture === null || gesture.captured || !samePointer(gesture, event)) return;
    cancelGesture();
  };

  const beginRename = (): void => {
    if (!canRename) return;
    cancelRef.current = false;
    editingRef.current = true;
    setDraft(box.title ?? "");
    setEditing(true);
  };
  const commitRename = (): void => {
    if (!editingRef.current) return;
    editingRef.current = false;
    setEditing(false);
    if (cancelRef.current) {
      cancelRef.current = false;
      return;
    }
    onTitleChange?.(box.id as string, draft.trim());
  };
  const cancelRename = (): void => {
    cancelRef.current = true;
    editingRef.current = false;
    setEditing(false);
  };

  return (
    <div
      className={`xflow-preview-section${canMove ? " xflow-preview-section--interactive" : ""}`}
      data-section-id={box.id}
      style={{
        left: geometry.x,
        top: geometry.y,
        width: geometry.width,
        height: geometry.height,
        "--xflow-preview-section-category": `var(--xflow-${nodeCategoryMeta[box.category].color})`
      } as React.CSSProperties}
      onLostPointerCapture={canMove ? cancelGesture : undefined}
      onPointerCancel={canMove ? endGesture(false) : undefined}
      onPointerDown={canMove ? beginGesture("move") : undefined}
      onPointerLeave={canMove ? handlePointerLeave : undefined}
      onPointerMove={canMove ? handlePointerMove : undefined}
      onPointerUp={canMove ? endGesture(true) : undefined}
    >
      {editing ? (
        <input
          autoFocus
          aria-label={`分区标题 ${box.title ?? ""}`.trim()}
          className="xflow-preview-section__title xflow-preview-section__title--input"
          value={draft}
          onBlur={commitRename}
          onChange={(event) => setDraft(event.target.value)}
          onDoubleClick={(event) => event.stopPropagation()}
          onKeyDown={(event) => {
            event.stopPropagation();
            if (event.key === "Enter") {
              event.preventDefault();
              commitRename();
            } else if (event.key === "Escape") {
              event.preventDefault();
              cancelRename();
            }
          }}
        />
      ) : box.title || canRename || canDelete ? (
        <span className="xflow-preview-section__title">
          {canRename ? (
            <button
              className="xflow-preview-section__title-action"
              type="button"
              aria-label={`重命名分区 ${box.title ?? ""}`.trim()}
              onDoubleClick={(event) => {
                event.stopPropagation();
                beginRename();
              }}
              onKeyDown={(event) => {
                if (event.key !== "Enter") return;
                event.preventDefault();
                beginRename();
              }}
            >
              {box.title ?? ""}
            </button>
          ) : (
            box.title
          )}
          {canDelete ? (
            <button
              className="xflow-preview-section__delete"
              type="button"
              aria-label={`删除分区 ${box.title ?? ""}`.trim()}
              title="删除分区"
              onClick={(event) => {
                event.stopPropagation();
                onDelete?.(box.id as string);
              }}
              onDoubleClick={(event) => event.stopPropagation()}
            >
              <XMarkIcon />
            </button>
          ) : null}
        </span>
      ) : null}
      {canMove ? (
        // The one handle the design leaves room for: the bottom-right corner.
        // Presentation only -- the same gesture has no keyboard route, so it
        // is not exposed as a control.
        <span
          aria-hidden="true"
          className="xflow-preview-section__resize"
          onPointerDown={beginGesture("resize")}
        />
      ) : null}
    </div>
  );
}

/** Shift+F10 and the ContextMenu key are the platform keyboard routes to a context menu. */
function isContextMenuKey(event: React.KeyboardEvent): boolean {
  return event.key === "ContextMenu" || (event.shiftKey && event.key === "F10");
}

/**
 * Browsers that map the ContextMenu key to a native `contextmenu` event fire it
 * right after the keydown the preview already answered; this window swallows
 * that echo so one press cannot open the menu twice.
 */
const KEYBOARD_MENU_ECHO_MS = 500;

/**
 * How long after a pointer gesture its trailing `click` may still arrive. A
 * browser dispatches the click immediately after `pointerup`; this window only
 * has to outlast an event loop stalled by load. It exists because React Flow
 * suppresses the pane's own `onClick` while `selectionOnDrag` owns the left
 * button and answers a real click from `pointerup` instead: the click that
 * trails that gesture must not be replayed, and the click that trails a marquee
 * must not undo the selection the marquee just made.
 */
const POINTER_CLICK_ECHO_MS = 2000;

function elementCenter(element: Element): PreviewClientPosition {
  const rect = element.getBoundingClientRect();
  return { clientX: rect.left + rect.width / 2, clientY: rect.top + rect.height / 2 };
}

/**
 * Minimap swatch: the node's status tone when it has a status, else its category
 * colour. Both come from core's tables, so no status or type name is guessed here.
 * The 55% mix is the one the preview page paints its minimap nodes with.
 */
function miniMapNodeColor(node: Node<PreviewNodeData>): string {
  const status = displayStatus(node.data.graphNode, node.data.runtime);
  if (status) {
    return `color-mix(in srgb, var(--xflow-${nodeStatusMeta[status].tone}) 55%, var(--xflow-border-strong))`;
  }
  return `var(--xflow-${nodeCategoryMeta[categoryOf(node.data.graphNode.type, node.data.category)].color})`;
}

/**
 * Everything a card needs, and nothing that changes with selection: `selected`
 * rides on the xyflow node itself (`NodeProps.selected`), so picking another node
 * leaves every other card's `data` reference — and with it React.memo — intact.
 */
interface PreviewNodeData extends Record<string, unknown> {
  graphNode: GraphNode;
  /** Absent when the preview got no `runtime`; see {@link nodeSnapshot}. */
  runtime?: RuntimeNodeSnapshot;
  sourcePorts: string[];
  targetPorts: string[];
  canConnect: boolean;
  /** Stable for the life of the preview. */
  onSelect: (nodeId: string) => void;
  /** Owner-managed multi-selection: a click toggles instead of replacing. */
  multiSelect: boolean;
  /** Set only while the owner manages a selection set; stable while set. */
  onToggle?: (nodeId: string) => void;
  /** Set only while the owner listens for node context menus; stable while set. */
  onContextMenuKey?: (nodeId: string, anchor: Element) => void;
  /** The owner's wait-note override for this node, if any. */
  note?: XFlowPreviewNodeNote;
  /**
   * A run-record annotation for the card's meta line (重跑起点 / 沿用原执行
   * 结果). It takes the left-hand slot ahead of a duration: the replay explains
   * the derivation, which is what that line is for in §6.8.
   */
  metaNote?: string;
  /** This node's local form findings (CP-01); absent means no badge. */
  issues?: XFlowPreviewNodeIssues;
  /** The type's declared descriptor category, when the host knows one. */
  category?: string;
  /** The type's readable name (`nodeTypeLabels`, else the id's tail). */
  typeLabel: string;
  /** This node's own configured retry/timeout/error route; absent means none. */
  policy?: PreviewNodePolicy;
  /** The owner's per-node glyph; see {@link XFlowPreviewProps.nodeIcon}. */
  nodeIcon?: (node: GraphNode) => React.ReactNode;
  /** Resolved card-summary rows for this node (spec §6.1.1); absent means none. */
  summary?: readonly CardSummaryRow[];
  /** Set only while the owner listens for note actions; stable while set. */
  onNodeAction?: (nodeName: string, action: string) => void;
  /** Set only while the owner locates a summary row's field; stable while set. */
  onLocateParameter?: (nodeId: string, fieldPath: string) => void;
}

type PreviewFlowNode = Node<PreviewNodeData, "xflowPreviewNode">;

const initialFitViewOptions = { padding: 0.18 };

/** Padding for fitting a single node, looser than the whole-graph fit. */
const nodeFitPadding = 0.28;

/** How long a locate glides the viewport; reduced motion turns the glide off. */
const LOCATE_TRANSITION_MS = 200;

function locateTransitionDuration(): number {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") return LOCATE_TRANSITION_MS;
  return window.matchMedia("(prefers-reduced-motion: reduce)").matches ? 0 : LOCATE_TRANSITION_MS;
}

/**
 * Brings one node into view (⌘K, the outline, a diagnostic). The readable band
 * is [READABLE_FIT_ZOOM, 1]: an off-screen node at 80% is only panned, while a
 * node small enough to fit at 158% is capped at 100% instead of filling the
 * canvas with a close-up.
 */
function locateBoundsClear(
  flow: ReactFlowInstance<PreviewFlowNode, Edge>,
  bounds: { x: number; y: number; width: number; height: number },
  canvas: HTMLElement | null | undefined,
  padding = nodeFitPadding
): void {
  const surface = measureSurface(canvas ?? null);
  const viewport = surface
    ? locateViewportClearOf(bounds, surface.area, surface.layers, {
        currentZoom: flow.getViewport().zoom,
        minZoom: READABLE_FIT_ZOOM,
        maxZoom: 1,
        padding
      })
    : undefined;
  if (viewport) {
    void flow.setViewport(viewport, { duration: locateTransitionDuration() });
    return;
  }
  // No measurable canvas (before layout, or a test DOM): xyflow's own fit.
  void flow.fitBounds(bounds, { padding });
}

/** Frames a fit may retry while its subject is still unmeasured. */
const fitRetryLimit = 12;

/**
 * Fits `bounds` the way fitBounds would, but clear of the canvas's floating layers
 * (rulers, zoom bar, minimap) by at least FLOAT_GAP. Without a measurable canvas
 * — before layout, or in a test DOM — it is exactly fitBounds.
 */
function fitBoundsClear(
  flow: ReactFlowInstance<PreviewFlowNode, Edge>,
  bounds: { x: number; y: number; width: number; height: number },
  padding: number,
  canvas: HTMLElement | null | undefined,
  minZoom = MIN_ZOOM
): void {
  const surface = measureSurface(canvas ?? null);
  const viewport = surface
    ? fitViewportClearOf(bounds, surface.area, surface.layers, { minZoom, maxZoom: MAX_ZOOM, padding })
    : undefined;
  if (viewport) {
    void flow.setViewport(viewport);
    return;
  }
  void flow.fitBounds(bounds, { padding });
}

function fitMeasuredNodes(
  flow: ReactFlowInstance<PreviewFlowNode, Edge>,
  padding = initialFitViewOptions.padding,
  canvas?: HTMLElement | null,
  minZoom = MIN_ZOOM
): boolean {
  const nodes = flow.getNodes();
  if (nodes.length === 0) return false;

  const bounds = flow.getNodesBounds(nodes);
  if (bounds.width <= 0 || bounds.height <= 0) return false;

  fitBoundsClear(flow, bounds, padding, canvas, minZoom);
  return true;
}

const sameBounds = (a: { x: number; y: number; width: number; height: number }, b: typeof a) =>
  Math.abs(a.x - b.x) < 0.5 && Math.abs(a.y - b.y) < 0.5
  && Math.abs(a.width - b.width) < 0.5 && Math.abs(a.height - b.height) < 0.5;

/** Frames without a bounds change after which a fit counts as settled. */
const fitSettleFrames = 4;

/**
 * fitMeasuredNodes, then follows the cards as they re-measure: a fit that crosses
 * LOD_ZOOM resizes them (low detail scales the header by the capped inverse), so
 * the bounds it used are stale a frame later and the content would run under the
 * chrome. Refits while the bounds keep changing, for at most fitRetryLimit frames.
 * Returns a cancel function for the follow-up still in flight; false when nothing
 * was fitted.
 */
function fitMeasuredNodesSettled(
  flow: ReactFlowInstance<PreviewFlowNode, Edge>,
  padding: number | undefined,
  canvas: HTMLElement | null | undefined,
  minZoom = MIN_ZOOM
): (() => void) | false {
  if (!fitMeasuredNodes(flow, padding, canvas, minZoom)) return false;
  let fitted = flow.getNodesBounds(flow.getNodes());
  let frame: number | undefined;
  let quiet = 0;
  let frames = 0;
  const check = () => {
    frame = undefined;
    const bounds = flow.getNodesBounds(flow.getNodes());
    if (bounds.width > 0 && bounds.height > 0 && !sameBounds(bounds, fitted)) {
      fitBoundsClear(flow, bounds, padding ?? initialFitViewOptions.padding, canvas, minZoom);
      fitted = bounds;
      quiet = 0;
    } else {
      quiet += 1;
    }
    frames += 1;
    if (quiet < fitSettleFrames && frames < fitRetryLimit) frame = requestAnimationFrame(check);
  };
  frame = requestAnimationFrame(check);
  return () => {
    if (frame !== undefined) cancelAnimationFrame(frame);
  };
}

/**
 * Fits whatever `boundsOf` reports, waiting out a 0×0 box: a just-appended node
 * has no measured size yet, and fitting that box would clamp the zoom to
 * maxZoom. The first attempt is synchronous; later ones ride animation frames.
 * Returns a cancel function for the retry still in flight, if any.
 */
function fitWhenMeasured(
  boundsOf: () => { x: number; y: number; width: number; height: number },
  fit: (bounds: { x: number; y: number; width: number; height: number }) => void
): () => void {
  let frame: number | undefined;
  let attempts = 0;
  const tryFit = () => {
    frame = undefined;
    const bounds = boundsOf();
    if (bounds.width > 0 && bounds.height > 0) {
      fit(bounds);
      return;
    }
    attempts += 1;
    if (attempts < fitRetryLimit) {
      frame = requestAnimationFrame(tryFit);
    }
  };
  tryFit();

  return () => {
    if (frame !== undefined) cancelAnimationFrame(frame);
  };
}

/**
 * Whether a node is on screen at all, judged by its own anchor point. Nodes are
 * top-level, so their position is already absolute and no parent offset applies.
 * The box is deliberately not consulted: a freshly added node has no measured
 * size yet, and a node the user dropped near an edge is still where they put it,
 * so re-centering on anything short of "off screen" would move the graph out
 * from under them.
 */
function isNodeAnchorOnScreen(
  flow: ReactFlowInstance<PreviewFlowNode, Edge>,
  node: PreviewFlowNode,
  canvas: DOMRect
): boolean {
  const position = node.position as { x: number; y: number } | undefined;
  if (!position) return false;

  const { x, y, zoom } = flow.getViewport();
  const left = canvas.left + x + position.x * zoom;
  const top = canvas.top + y + position.y * zoom;
  return left >= canvas.left && left <= canvas.right && top >= canvas.top && top <= canvas.bottom;
}

/**
 * Fits once after the controlled nodes have received measured dimensions.
 * Disabled entirely when the owner supplied a `defaultViewport` to restore
 * (ADR-D4 §2.3): a measured auto-fit would immediately overwrite the
 * author's saved pan/zoom.
 */
function InitialViewFitter({
  flow,
  disabled,
  canvas,
  onMeasured
}: {
  flow: ReactFlowInstance<PreviewFlowNode, Edge> | null;
  disabled?: boolean;
  canvas: React.RefObject<HTMLDivElement | null>;
  /** Applies the first measured layout, before the cards are framed. */
  onMeasured?: (flow: ReactFlowInstance<PreviewFlowNode, Edge>) => void;
}): null {
  const hasFittedInitialView = React.useRef(false);
  const fitViewFrame = React.useRef<number | undefined>(undefined);
  const settleCancel = React.useRef<(() => void) | undefined>(undefined);
  // The callback may change with the definition; it must not re-run this effect.
  const onMeasuredRef = React.useRef(onMeasured);
  React.useEffect(() => {
    onMeasuredRef.current = onMeasured;
  }, [onMeasured]);

  // `disabled` is a dependency so a viewport restored after mount cancels a
  // pending retry; hasFittedInitialView keeps a re-run from fitting twice.
  React.useEffect(() => {
    if (!flow || disabled || hasFittedInitialView.current) return;

    let attempts = 0;
    const tryFit = () => {
      fitViewFrame.current = undefined;
      if (hasFittedInitialView.current) return;

      onMeasuredRef.current?.(flow);
      const settling = fitMeasuredNodesSettled(flow, undefined, canvas.current, READABLE_FIT_ZOOM);
      if (settling) {
        hasFittedInitialView.current = true;
        settleCancel.current = settling;
        return;
      }

      attempts += 1;
      if (attempts < 12) {
        fitViewFrame.current = requestAnimationFrame(tryFit);
      }
    };

    fitViewFrame.current = requestAnimationFrame(tryFit);
    return () => {
      if (fitViewFrame.current !== undefined) {
        cancelAnimationFrame(fitViewFrame.current);
        fitViewFrame.current = undefined;
      }
      settleCancel.current?.();
      settleCancel.current = undefined;
    };
  }, [flow, disabled, canvas]);

  return null;
}

function addPort(portsByNode: Map<string, string[]>, nodeId: string, port: string): void {
  const ports = portsByNode.get(nodeId) ?? [];
  if (!ports.includes(port)) {
    ports.push(port);
    portsByNode.set(nodeId, ports);
  }
}

function portsWithMain(...portGroups: string[][]): string[] {
  const ports = ["main"];
  for (const portGroup of portGroups) {
    for (const port of portGroup) {
      if (port && !ports.includes(port)) {
        ports.push(port);
      }
    }
  }
  return ports;
}

/**
 * Declared ports lead so a branch or error handle exists before anything is
 * wired to it. Edge ports follow: React Flow needs a handle for every edge it
 * has to draw, including one the node type does not declare (a hand-written
 * definition, or an edge kept from an older version of the node).
 */
function mergeDeclaredPorts(declaredPorts: string[], edgePorts: string[]): string[] {
  const ports: string[] = [];
  for (const port of [...declaredPorts, ...edgePorts]) {
    if (port && !ports.includes(port)) {
      ports.push(port);
    }
  }
  return ports;
}

/**
 * Own-property read. Node keys are user data: a node named `constructor` must
 * not inherit `Object.prototype.constructor` from a plain record (the card
 * would then call `.map` on a function and the whole canvas would fail).
 */
function own<T>(map: Readonly<Record<string, T>> | undefined, key: string): T | undefined {
  return map && Object.hasOwn(map, key) ? map[key] : undefined;
}

function toFlowNodes(
  graphNodes: GraphNode[],
  graphEdges: GraphEdge[],
  runtime: RuntimeSnapshot | undefined,
  selectedNodeIds: ReadonlySet<string>,
  multiSelect: boolean,
  editable: boolean,
  canConnect: boolean,
  onSelect: (nodeId: string) => void,
  onToggle: ((nodeId: string) => void) | undefined,
  onContextMenuKey: ((nodeId: string, anchor: Element) => void) | undefined,
  nodeNotes: Readonly<Record<string, XFlowPreviewNodeNote>> | undefined,
  onNodeAction: ((nodeName: string, action: string) => void) | undefined,
  nodeCategories: Readonly<Record<string, string>> | undefined,
  nodeTypeLabels: Readonly<Record<string, string>> | undefined,
  nodePolicies: Readonly<Record<string, PreviewNodePolicy>> | undefined,
  nodeIcon: ((node: GraphNode) => React.ReactNode) | undefined,
  nodeSummaries: Readonly<Record<string, readonly CardSummaryRow[]>> | undefined,
  onLocateParameter: ((nodeId: string, fieldPath: string) => void) | undefined,
  nodeMetaNotes: Readonly<Record<string, string>> | undefined,
  nodeIssues: ((node: GraphNode) => XFlowPreviewNodeIssues | undefined) | undefined
): PreviewFlowNode[] {
  const sourcePortsByNode = new Map<string, string[]>();
  const targetPortsByNode = new Map<string, string[]>();
  for (const edge of graphEdges) {
    addPort(sourcePortsByNode, edge.source, edge.sourcePort);
    addPort(targetPortsByNode, edge.target, edge.targetPort);
  }

  return graphNodes.map((node) => {
    const snapshot = nodeSnapshot(runtime, node);
    const isSupply = isSupplyNode(node);
    const nodeCanConnect = canConnect && !isSupply;
    const summaryRows = own(nodeSummaries, node.id);
    return {
      id: node.id,
      type: "xflowPreviewNode",
      position: node.position,
      draggable: editable,
      // The owner still renders selection from `selected`, but React Flow has
      // to own clicks and the marquee for multi-select: `selectable` is what
      // includes a node in `getNodesInside` and lets a click toggle it.
      selectable: multiSelect,
      selected: selectedNodeIds.has(node.id),
      connectable: nodeCanConnect,
      deletable: false,
      ariaLabel: `${node.name} node ${displayStatus(node, snapshot) ?? "pending"}`,
      data: {
        graphNode: node,
        runtime: snapshot,
        // Read-only data edges still need their handles for React Flow to locate them.
        // Supply nodes intentionally have neither source nor target data handles.
        sourcePorts: isSupply ? [] : mergeDeclaredPorts(node.outputs, sourcePortsByNode.get(node.id) ?? []),
        targetPorts: isSupply
          ? []
          : portsWithMain(
            node.inputs.map((input) => input.name),
            targetPortsByNode.get(node.id) ?? []
          ),
        canConnect: nodeCanConnect,
        onSelect,
        multiSelect,
        onToggle,
        onContextMenuKey,
        note: own(nodeNotes, node.name),
        metaNote: own(nodeMetaNotes, node.name),
        issues: nodeIssues?.(node),
        category: own(nodeCategories, node.type),
        typeLabel: own(nodeTypeLabels, node.type) ?? readableNodeType(node.type),
        policy: own(nodePolicies, node.id),
        nodeIcon,
        summary: Array.isArray(summaryRows) ? summaryRows : undefined,
        onNodeAction,
        onLocateParameter
      }
    };
  });
}

function sameStrings(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((value, index) => value === b[index]);
}

function sameSnapshot(a: RuntimeNodeSnapshot | undefined, b: RuntimeNodeSnapshot | undefined): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  return a.status === b.status && a.attempts === b.attempts && a.durationMs === b.durationMs && a.error === b.error;
}

/** Field-wise, because `toGraphModel` rebuilds every GraphNode on any workflow edit. */
function sameGraphNode(a: GraphNode, b: GraphNode): boolean {
  if (a === b) return true;
  return (
    a.id === b.id
    && a.name === b.name
    && a.type === b.type
    && a.kind === b.kind
    && a.label === b.label
    && a.disabled === b.disabled
    && a.position.x === b.position.x
    && a.position.y === b.position.y
    && sameStrings(a.outputs, b.outputs)
    && sameStrings(a.inputs.map((input) => input.name), b.inputs.map((input) => input.name))
  );
}

function sameNodeData(a: PreviewNodeData, b: PreviewNodeData): boolean {
  return (
    a.canConnect === b.canConnect
    && a.onSelect === b.onSelect
    && a.multiSelect === b.multiSelect
    && a.onToggle === b.onToggle
    && a.onContextMenuKey === b.onContextMenuKey
    && a.onNodeAction === b.onNodeAction
    && a.onLocateParameter === b.onLocateParameter
    && a.nodeIcon === b.nodeIcon
    && a.category === b.category
    && a.typeLabel === b.typeLabel
    && samePolicy(a.policy, b.policy)
    && sameSummaries(a.summary, b.summary)
    && sameNote(a.note, b.note)
    && a.metaNote === b.metaNote
    && sameIssues(a.issues, b.issues)
    && sameSnapshot(a.runtime, b.runtime)
    && sameStrings(a.sourcePorts, b.sourcePorts)
    && sameStrings(a.targetPorts, b.targetPorts)
    && sameGraphNode(a.graphNode, b.graphNode)
  );
}

/** Row-wise, because the owner rebuilds the summary arrays whenever the draft moves. */
function sameSummaries(a: readonly CardSummaryRow[] | undefined, b: readonly CardSummaryRow[] | undefined): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  return a.length === b.length
    && a.every((row, index) => {
      const other = b[index];
      if (row.label !== other.label || row.value !== other.value || row.kind !== other.kind) return false;
      if (!row.parts || !other.parts) return row.parts === other.parts;
      return row.parts.length === other.parts.length
        && row.parts.every((part, at) => part.text === other.parts![at].text && part.kind === other.parts![at].kind);
    });
}

function sameNote(a: XFlowPreviewNodeNote | undefined, b: XFlowPreviewNodeNote | undefined): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  return a.text === b.text && a.action === b.action;
}

function sameIssues(a: XFlowPreviewNodeIssues | undefined, b: XFlowPreviewNodeIssues | undefined): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  return a.errors === b.errors && a.warnings === b.warnings && a.message === b.message;
}

function samePolicy(a: PreviewNodePolicy | undefined, b: PreviewNodePolicy | undefined): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  return a.retryAttempts === b.retryAttempts && a.timeoutNs === b.timeoutNs && a.onError === b.onError;
}

function sameFlowNode(a: PreviewFlowNode, b: PreviewFlowNode): boolean {
  return (
    a.data === b.data
    && a.selected === b.selected
    && a.selectable === b.selectable
    && a.draggable === b.draggable
    && a.connectable === b.connectable
    && a.ariaLabel === b.ariaLabel
    && a.position.x === b.position.x
    && a.position.y === b.position.y
  );
}

/**
 * Hands back last render's node objects wherever nothing a card shows has
 * changed. xyflow keeps an internal node per user node and re-adopts it only when
 * the user node's reference changes; a card re-renders only when its `data` or
 * its own props change. So the unit of reuse is the object: a selection change
 * produces new objects for the two nodes whose `selected` flipped, a workflow edit
 * for the nodes it touched, and every other card keeps both references — which is
 * what lets `React.memo` skip it. When nothing at all changed the array itself is
 * returned, so xyflow sees no new `nodes` prop either.
 */
function reuseFlowNodes(previous: readonly PreviewFlowNode[], next: PreviewFlowNode[]): PreviewFlowNode[] {
  const previousById = new Map(previous.map((node) => [node.id, node]));
  const reused = next.map((node) => {
    const before = previousById.get(node.id);
    if (!before) return node;
    const data = sameNodeData(before.data, node.data) ? before.data : node.data;
    const candidate = data === node.data ? node : { ...node, data };
    return sameFlowNode(before, candidate) ? before : candidate;
  });
  return reused.length === previous.length && reused.every((node, index) => node === previous[index])
    ? (previous as PreviewFlowNode[])
    : reused;
}

/**
 * A callback with a permanent identity that always runs the latest closure. The
 * card's `onSelect` / `onContextMenuKey` live in `data`, so a changing identity
 * would invalidate every card on each parent render.
 */
/**
 * A switch the user flips locally, seeded by a prop. A new prop value overrides
 * the local flip; re-rendering with the same value leaves it alone. (State is
 * adjusted during render, React's documented pattern for deriving from a prop.)
 */
function useToggleSeededBy(seed: boolean): [boolean, (next: boolean) => void] {
  const [state, setState] = React.useState({ seed, value: seed });
  const current = state.seed === seed ? state : { seed, value: seed };
  if (current !== state) setState(current);
  const set = React.useCallback((value: boolean) => setState({ seed, value }), [seed]);
  return [current.value, set];
}

function useStableCallback<Args extends unknown[], Result>(
  callback: (...args: Args) => Result
): (...args: Args) => Result {
  const latest = React.useRef(callback);
  React.useLayoutEffect(() => {
    latest.current = callback;
  });
  return React.useCallback((...args: Args) => latest.current(...args), []);
}

function edgeConnection(edge: GraphEdge): PreviewConnection {
  return {
    source: edge.sourceName,
    sourcePort: edge.sourcePort,
    target: edge.targetName,
    targetPort: edge.targetPort
  };
}

function previewConnectionFromFlow(
  connection: Pick<ReactFlowConnection, "source" | "target"> & {
    sourceHandle?: string | null;
    targetHandle?: string | null;
  },
  graphNodesById: ReadonlyMap<string, GraphNode>
): PreviewConnection | undefined {
  const source = graphNodesById.get(connection.source);
  const target = graphNodesById.get(connection.target);
  if (!source || !target) return undefined;

  return {
    source: source.name,
    sourcePort: connection.sourceHandle ?? "main",
    target: target.name,
    targetPort: connection.targetHandle ?? "main"
  };
}

/**
 * How an edge is stroked, from its TARGET node's status (spec §4 入边 column):
 * the same edge on the same graph cannot be solid in one view and dashed in
 * another. A target with no run status is `default`, or the faint `disabled`
 * stroke when the node is disabled.
 */
function targetEdgeVariant(
  edge: GraphEdge,
  runtime: RuntimeSnapshot | undefined,
  graphNodesById: ReadonlyMap<string, GraphNode>
): EdgeVariant {
  const target = graphNodesById.get(edge.target);
  const snapshot = target ? nodeSnapshot(runtime, target) : undefined;
  const status = target ? displayStatus(target, snapshot) : undefined;
  return status ? edgeVariant(status) : "default";
}

function toFlowEdges(
  graphEdges: GraphEdge[],
  runtime: RuntimeSnapshot | undefined,
  graphNodesById: ReadonlyMap<string, GraphNode>,
  canDeleteConnections: boolean,
  selectedEdgeIds: ReadonlySet<string>,
  dangling: ReadonlySet<string>
): Edge[] {
  const edges: Edge[] = [];
  for (const edge of graphEdges) {
    const isDangling = dangling.has(danglingKey(edge.sourceName, edge.sourcePort));
    // Dangling and error are about the source *port*, so they outrank whatever
    // the target's status says about the line.
    const isError = !isDangling && edge.sourcePort === "error";
    const variant = targetEdgeVariant(edge, runtime, graphNodesById);
    // §4 「—」: nothing follows a canceled or expired node, so no line is drawn.
    if (!isDangling && !isError && variant === "none") continue;

    const showLabel = isDangling || edge.sourcePort !== "main";
    const connectionLabel = `${edge.sourceName} ${edge.sourcePort} 连接到 ${edge.targetName} ${edge.targetPort}${
      isDangling ? "（端口已不存在）" : ""
    }`;

    const flowEdge: BuiltInEdge = {
      id: edge.id,
      source: edge.source,
      target: edge.target,
      sourceHandle: edge.sourcePort,
      targetHandle: edge.targetPort,
      type: "smoothstep",
      // §6.2: orthogonal routing with rounded corners from the md radius token
      // (xyflow's default is 5).
      pathOptions: { borderRadius: Number.parseInt(foundationTokens.radiusMd, 10) },
      className: isDangling
        ? "xflow-preview-edge--dangling"
        : isError
          ? "xflow-preview-edge--error"
          : `xflow-preview-edge--${variant}`,
      label: showLabel ? (isDangling ? `${edge.sourcePort} · missing` : edge.sourcePort) : undefined,
      selectable: canDeleteConnections,
      // ED-08: edges stay click-selectable but out of the Tab order. Keyboard
      // access to a connection is the node Inspector's 连接 tab, so an edge no
      // longer claims a tab stop ahead of the nodes.
      focusable: false,
      deletable: canDeleteConnections,
      selected: canDeleteConnections && selectedEdgeIds.has(edge.id),
      ariaLabel: connectionLabel,
      interactionWidth: 16,
      // Every stroke — port-semantic ones included — lives in styles.css under
      // `.xflow-preview-edge--*`. Nothing inline: an inline stroke would outrank
      // both the hover rule and the selected rule (and the label colour moved to
      // CSS for the same reason).
      labelBgStyle: {
        fill: "var(--xflow-preview-edge-label-bg)"
      },
      // The baseline .edge-label pill: 6px corner, 6px horizontal inset, 18px tall
      // (React Flow's tuple is [horizontal, vertical]).
      labelBgPadding: [6, 1.75],
      labelBgBorderRadius: 6
    };
    edges.push(flowEdge);
  }
  return edges;
}

function EdgeList({ edges }: { edges: GraphEdge[] }): React.ReactElement {
  if (edges.length === 0) {
    return <p className="xflow-preview-empty">No connections</p>;
  }

  return (
    <ul className="xflow-preview-edges" aria-label="Workflow connections">
      {edges.map((edge) => (
        <li
          key={edge.id}
          aria-label={`${edge.sourceName} ${edge.sourcePort} 连接到 ${edge.targetName} ${edge.targetPort}`}
        >
          <span>{edge.sourceName}</span>
          <span>{edge.sourcePort}</span>
          <span aria-hidden="true">→</span>
          <span>{edge.targetName}</span>
          <span>{edge.targetPort}</span>
        </li>
      ))}
    </ul>
  );
}

function handleTop(index: number, count: number): string {
  return `${((index + 1) / (count + 1)) * 100}%`;
}

function activateHandle(
  event: React.KeyboardEvent<HTMLDivElement>,
  canConnect: boolean
): void {
  if (!canConnect || (event.key !== "Enter" && event.key !== " ")) return;
  event.preventDefault();
  event.currentTarget.click();
}

/**
 * §6.2: below the LOD threshold the chip loses its text and floats to the card's
 * right edge, the name scales by 1/zoom, and every row but the head leaves the
 * DOM (the ports row stays mounted but empty: xyflow still needs the handles to
 * place its edges). Between the `LOD_ZOOM` threshold and {@link READABLE_FIT_ZOOM} the
 * card keeps the head, the type name and the badge row but not the summary.
 */
/**
 * §6.2: the card's level of detail (two bits: 0 low, 1 compact, 2 full) plus
 * whether the multi-selection modifier is held. One selector keeps the card's
 * re-render count at one subscription per card, and a zoom move inside a level
 * does not re-render any card.
 *
 * The WeakMap is the selector's only state: `lodLevel` needs the level from the
 * previous frame to apply its one-percent hold above the compact band. Every
 * card on one canvas reads the same transform, and repeated selector calls for
 * one state return the level the first call stored, so the cache is idempotent.
 * Keyed by the canvas element, it cannot leak between two previews and is
 * collected with the DOM.
 */
const lodByCanvas = new WeakMap<Element, LodLevel>();

const cardChromeSelector = (state: ReactFlowState): number => {
  const canvas = state.domNode;
  const previous = canvas ? lodByCanvas.get(canvas) : undefined;
  const level = lodLevel(state.transform[2], previous);
  if (canvas) lodByCanvas.set(canvas, level);
  const code = level === "low" ? 0 : level === "compact" ? 1 : 2;
  return code | (state.multiSelectionActive ? 4 : 0);
};

/**
 * Writes the canvas scale variables straight onto the canvas element:
 * `--xflow-preview-lod-scale` (the capped low-detail factor) and
 * `--xflow-preview-inverse-zoom` (the 1/zoom factor every canvas hit target
 * multiplies its 24 screen-px size by, capped by {@link HIT_MAX_INVERSE}). A
 * store subscription, not a selector: the values move with every pinch step and
 * must never re-render a single card.
 */
function CanvasScaleVariables({ target }: { target: React.RefObject<HTMLDivElement | null> }): null {
  const store = useStoreApi();
  React.useEffect(() => {
    const element = target.current;
    if (!element) return;
    const write = (zoom: number) => {
      // Seed from the current viewport: a restored 14% viewport must get the
      // capped scale before the first zoom change, not the full-detail size.
      const lod = String(lodScale(zoom));
      const hit = String(hitInverseZoom(zoom));
      return { lod, hit };
    };
    let last = write(store.getState().transform[2]);
    element.style.setProperty("--xflow-preview-lod-scale", last.lod);
    element.style.setProperty("--xflow-preview-inverse-zoom", last.hit);
    return store.subscribe((state) => {
      const value = write(state.transform[2]);
      if (value.lod !== last.lod) element.style.setProperty("--xflow-preview-lod-scale", value.lod);
      if (value.hit !== last.hit) element.style.setProperty("--xflow-preview-inverse-zoom", value.hit);
      last = value;
    });
  }, [store, target]);
  return null;
}

/**
 * Hands the provider's store to the canvas wrapper, which wraps ReactFlow and
 * therefore cannot call useStoreApi itself. The wrapper's gesture-less
 * pane-click replay needs the store's own selection reset so an owner that
 * only listens for `onSelectionChange` sees the clear a real click performs.
 */
function FlowStoreHandle({ target }: { target: React.RefObject<ReturnType<typeof useStoreApi> | null> }): null {
  const store = useStoreApi();
  React.useLayoutEffect(() => {
    target.current = store;
    return () => {
      target.current = null;
    };
  }, [store, target]);
  return null;
}

/**
 * Ports drawn in the card-bottom port row: everything but a lone main outlet,
 * with the error port last (it is the one danger-toned port, and its handle
 * hangs off the row's right end per §6.2).
 */
function portsForRow(ports: string[]): string[] {
  if (ports.length === 1 && ports[0] === "main") return [];
  const rest = ports.filter((port) => port !== "error");
  return ports.includes("error") ? [...rest, "error"] : rest;
}

const PreviewNode = React.memo(function PreviewNode({
  data,
  selected
}: NodeProps<PreviewFlowNode>): React.ReactElement {
  const { graphNode: node, runtime, sourcePorts, targetPorts, canConnect, onSelect, multiSelect, onToggle, onContextMenuKey, note, metaNote, issues, category: declaredCategory, typeLabel, policy, summary, onNodeAction, onLocateParameter, nodeIcon } = data;
  const chrome = useStore(cardChromeSelector);
  const lod = chrome & 3;
  const lowDetail = lod === 0;
  // The line stack the full card sizes itself to (§6.1.1): at most three, with
  // the "+n 项" fold counting as the third. Compact and low detail draw no
  // summary, so the card keeps its header-and-strip height instead of reserving
  // three empty rows (the old 154px floor).
  const summaryRows = lod === 2 && summary ? Math.min(summary.length, SUMMARY_ROWS) : 0;
  const multiSelectionActive = (chrome & 4) !== 0;
  const category = categoryOf(node.type, declaredCategory);
  const categoryMeta = nodeCategoryMeta[category];
  const status = displayStatus(node, runtime);
  const statusMeta = status ? nodeStatusMeta[status] : undefined;
  const statusLabel = statusMeta?.label;
  const nameTitle = node.label === node.name ? node.label : `${node.label} (${node.name})`;
  const rowPorts = portsForRow(sourcePorts);
  const durationLabel = runtime?.durationMs !== undefined ? formatDuration(runtime.durationMs) : undefined;
  const attemptsLabel = runtime?.attempts !== undefined ? `第 ${runtime.attempts} 次` : undefined;
  // CP-01: one badge, errors first. A node with errors reports only the error
  // count; warnings surface only when the form found no error.
  const issueErrors = issues?.errors ?? 0;
  const issueWarnings = issues?.warnings ?? 0;
  const issueTone = issueErrors > 0 ? "danger" : issueWarnings > 0 ? "warning" : undefined;
  const issueCount = issueErrors > 0 ? issueErrors : issueWarnings;
  const issueLabel = issueTone === "danger"
    ? `${issueErrors} 个参数错误`
    : issueTone === "warning"
      ? `${issueWarnings} 个参数警告`
      : undefined;
  // The node's own failure route. `stop` is the engine default, so it earns no
  // badge; the three routes that change behavior each say what they do.
  const errorPolicyLabel = policy?.onError === "continue"
    ? "继续"
    : policy?.onError === "error_output"
      ? "错误出口"
      : policy?.onError === "main_output"
        ? "主输出"
        : undefined;
  const errorPolicyTitle = errorPolicyLabel ? `失败后${errorPolicyLabel === "继续" ? "继续" : `走 ${errorPolicyLabel}`}` : undefined;
  // Only the full card has room for the error line; compact keeps the badges.
  const error = lod === 2 ? runtime?.error : undefined;

  // With the owner's selection set in play, a modifier click toggles and a
  // plain click still replaces the selection. React Flow's own click handler
  // must not also run: it mutates store flags that lag one render behind the
  // owner's set, so the two paths would race.
  const activate = (event?: React.MouseEvent): void => {
    if (event) {
      // Handles start connections; a press there must not also pick the node.
      if ((event.target as Element).closest(".xflow-preview-node__handle")) {
        event.stopPropagation();
        return;
      }
      if (multiSelect) event.stopPropagation();
    }
    if (multiSelect && multiSelectionActive && onToggle) onToggle(node.id);
    else onSelect(node.id);
  };

  return (
    <article
      className="xflow-preview-node"
      data-node-id={node.id}
      data-selected={selected}
      data-status={status ?? "pending"}
      data-tone={statusMeta?.tone}
      data-disabled={node.disabled || undefined}
      data-lod={lod === 0 ? "low" : lod === 1 ? "compact" : "full"}
      data-summary-rows={summaryRows}
      // The category colour comes from core's table (§5); CSS derives the block's
      // background and icon colour from this one variable.
      style={{ "--xflow-preview-node-category": `var(--xflow-${categoryMeta.color})` } as React.CSSProperties}
      onClick={(event) => {
        // The card is the selection surface: handles stop here so their press
        // cannot also pick the node, and in multi-select mode the stop keeps
        // React Flow's own click handler out of the same gesture.
        activate(event);
      }}
    >
      {targetPorts.map((port, index) => (
        <Handle
          key={`target:${port}`}
          id={port}
          type="target"
          position={Position.Left}
          className="xflow-preview-node__handle xflow-preview-node__handle--target"
          style={targetPorts.length === 1 ? undefined : { top: handleTop(index, targetPorts.length) }}
          isConnectable={canConnect}
          isConnectableStart={false}
          isConnectableEnd={canConnect}
          role="button"
          tabIndex={canConnect ? 0 : -1}
          aria-disabled={!canConnect}
          aria-label={`${node.name} 输入端口 ${port}；按 Enter 或空格完成连线`}
          onKeyDown={(event) => activateHandle(event, canConnect)}
        />
      ))}
      {sourcePorts.length > 0 && rowPorts.length === 0 ? (
        <Handle
          id="main"
          type="source"
          position={Position.Right}
          className="xflow-preview-node__handle xflow-preview-node__handle--source"
          isConnectable={canConnect}
          isConnectableStart={canConnect}
          isConnectableEnd={false}
          role="button"
          tabIndex={canConnect ? 0 : -1}
          aria-disabled={!canConnect}
          aria-label={`${node.name} 输出端口 main；按 Enter 或空格开始连线`}
          onKeyDown={(event) => activateHandle(event, canConnect)}
        />
      ) : null}
      <div
        className="xflow-preview-node__select"
        role="button"
        tabIndex={0}
        aria-label={`${node.name} node ${status ?? "pending"}`}
        aria-keyshortcuts={onContextMenuKey ? "Shift+F10 ContextMenu" : undefined}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            activate();
            return;
          }
          if (onContextMenuKey && isContextMenuKey(event)) {
            event.preventDefault();
            // The canvas listener would otherwise answer the same press for
            // whichever node is selected, which need not be this focused one.
            event.stopPropagation();
            onContextMenuKey(node.id, event.currentTarget.closest(".xflow-preview-node") ?? event.currentTarget);
          }
        }}
      >
        <div className="xflow-preview-node__header">
          <span className="xflow-preview-node__icon" aria-label={`${node.type} icon`} data-cat={category}>
            {nodeIcon ? nodeIcon(node) : categoryGlyph[categoryMeta.icon]}
          </span>
          <div className="xflow-preview-node__title">
            <strong title={nameTitle}>{node.label}</strong>
            {/* The card speaks the descriptor's name; the raw id stays in the
                inspector and in this line's title. */}
            {lowDetail ? null : <span className="xflow-preview-node__type" title={node.type}>{typeLabel}</span>}
          </div>
          {status && statusMeta ? (
            <span
              className="xflow-preview-node__status"
              data-tone={statusMeta.tone}
              data-status={status}
              title={lowDetail ? statusLabel : undefined}
            >
              <span className="xflow-preview-node__status-icon" aria-hidden="true">
                {statusGlyph[statusMeta.icon]}
              </span>
              {lowDetail ? null : <span>{statusLabel}</span>}
            </span>
          ) : null}
        </div>
      </div>
      {!lowDetail && runtime?.status === "suspended" ? (
        <div className="xflow-preview-node__note">
          <span>{note?.text ?? "等待信号"}</span>
          {note?.action && onNodeAction ? (
            <button
              className="xflow-preview-node__note-action"
              type="button"
              onClick={(event) => {
                // The card's own click selects the node; the note's action is a
                // separate gesture and must not also change the selection.
                event.stopPropagation();
                onNodeAction(node.name, note.action as string);
              }}
            >
              {note.action}
            </button>
          ) : null}
        </div>
      ) : null}
      {/*
        §6.1.1 configuration summary: 标签 · 值 rows between the header and the
        card's other states. Empty values were already dropped by the owner, so
        every row drawn here has something behind it.
      */}
      {lod === 2 && summary && summary.length > 0 ? (
        <dl className="xflow-preview-node__summary">
          {(summary.length > SUMMARY_ROWS ? summary.slice(0, SUMMARY_ROWS - 1) : summary).map((row, index) => {
            // §6.1.1: a press on a row selects the node and reveals that field;
            // without an owner callback the row stays plain text.
            const locate = onLocateParameter && row.path
              ? () => onLocateParameter(node.id, row.path as string)
              : undefined;
            return (
              <React.Fragment key={`${index}:${row.label}`}>
                <dt>{row.label}</dt>
                <dd
                  data-summary-row={row.label}
                  data-kind={row.kind}
                  // The line ellipsizes at the card's fixed width; the title keeps
                  // the whole value readable without a click (E-A).
                  title={row.value}
                  role={locate ? "button" : undefined}
                  tabIndex={locate ? 0 : undefined}
                  onClick={locate ? (event) => { event.stopPropagation(); locate(); } : undefined}
                  onKeyDown={locate ? (event) => {
                    if (event.key !== "Enter" && event.key !== " ") return;
                    event.preventDefault();
                    event.stopPropagation();
                    locate();
                  } : undefined}
                >
                  {row.kind === "ref" ? (
                    <span aria-hidden="true" className="xflow-preview-node__summary-ref">
                      <KeyIcon />
                    </span>
                  ) : null}
                  {row.parts
                    ? row.parts.map((part, at) => (
                        <React.Fragment key={`${at}:${part.text}`}>
                          {at > 0 ? " " : null}
                          {part.kind === "mono" ? (
                            <span className="xflow-preview-node__summary-mono">{part.text}</span>
                          ) : part.kind === "expr" ? (
                            <span className="xflow-preview-node__summary-expr">{part.text}</span>
                          ) : (
                            part.text
                          )}
                        </React.Fragment>
                      ))
                    : row.value}
                </dd>
              </React.Fragment>
            );
          })}
          {summary.length > SUMMARY_ROWS ? (
            <>
              <dt>更多</dt>
              <dd className="xflow-preview-node__summary-more">+{summary.length - (SUMMARY_ROWS - 1)} 项</dd>
            </>
          ) : null}
        </dl>
      ) : null}
      {/*
        The error line sits directly under the summary; run meta and the issue
        badge moved into the bottom strip (E-A), which is also where the branch
        port labels live.
      */}
      {error ? <p className="xflow-preview-node__error" title={error}>{error}</p> : null}
      {/*
        One bottom strip (E-A): configuration/run badges on the left, branch
        port labels hugging the right edge. It is drawn at full and compact
        detail so every card in a level keeps the same height, and the port
        labels share the strip instead of adding a body row of their own.
      */}
      {lowDetail ? (
        rowPorts.length > 0 ? (
          <div className="xflow-preview-node__ports xflow-preview-node__ports--handles" aria-hidden="true">
            {rowPorts.map((port, index) => (
              <Handle
                key={`source:${port}`}
                id={port}
                type="source"
                position={Position.Bottom}
                className="xflow-preview-node__handle xflow-preview-node__handle--source xflow-preview-node__handle--spread"
                style={{ left: handleTop(index, rowPorts.length) }}
                isConnectable={canConnect}
                isConnectableStart={canConnect}
                isConnectableEnd={false}
                aria-hidden="true"
              />
            ))}
          </div>
        ) : null
      ) : (
        <div className="xflow-preview-node__meta">
          <div className="xflow-preview-node__badges">
            {status === "pinned" ? (
              <span className="xflow-preview-node__badge xflow-preview-node__pin" data-badge="pin" title="固定数据">
                <span aria-hidden="true" className="xflow-preview-node__badge-icon"><PinIcon /></span>
                固定数据
              </span>
            ) : null}
            {node.disabled ? (
              <span className="xflow-preview-node__badge" data-badge="disabled" title="已禁用">
                <span aria-hidden="true" className="xflow-preview-node__badge-icon"><BanIcon /></span>
                已禁用
              </span>
            ) : null}
            {issueLabel ? (
              <span
                aria-label={issueLabel}
                className="xflow-preview-node__issues"
                data-tone={issueTone}
                role="img"
                title={issues?.message}
              >
                <span aria-hidden="true">
                  {issueTone === "danger" ? <XMarkIcon /> : <WarnIcon />}
                </span>
                {issueCount}
              </span>
            ) : null}
            {policy?.retryAttempts !== undefined ? (
              <span className="xflow-preview-node__badge" data-badge="retry" title={`已配置重试 ${policy.retryAttempts} 次`}>
                <span aria-hidden="true" className="xflow-preview-node__badge-icon"><SpinIcon /></span>
                {`重试 ${policy.retryAttempts}`}
              </span>
            ) : null}
            {policy?.timeoutNs !== undefined ? (
              <span className="xflow-preview-node__badge" data-badge="timeout" title={`已配置超时 ${formatNodeTimeout(policy.timeoutNs)}`}>
                <span aria-hidden="true" className="xflow-preview-node__badge-icon"><ClockIcon /></span>
                {`超时 ${formatNodeTimeout(policy.timeoutNs)}`}
              </span>
            ) : null}
            {errorPolicyLabel ? (
              <span className="xflow-preview-node__badge" data-badge="error-policy" title={errorPolicyTitle}>
                <span aria-hidden="true" className="xflow-preview-node__badge-icon">
                  {policy?.onError === "continue" ? <SkipIcon /> : <BranchIcon />}
                </span>
                {errorPolicyLabel}
              </span>
            ) : null}
            {metaNote ? <span className="xflow-preview-node__badge" data-badge="note">{metaNote}</span> : null}
            {durationLabel ? <span className="xflow-preview-node__badge" data-badge="duration">{durationLabel}</span> : null}
            {attemptsLabel ? <span className="xflow-preview-node__badge" data-badge="attempts">{attemptsLabel}</span> : null}
          </div>
          {rowPorts.length > 0 ? (
            <div className="xflow-preview-node__ports">
              {rowPorts.map((port) => (
                <span
                  key={port}
                  data-port={port}
                  className={
                    port === "error"
                      ? "xflow-preview-node__port xflow-preview-node__port--error"
                      : "xflow-preview-node__port"
                  }
                >
                  <span className="xflow-preview-node__port-text">{port}</span>
                  {port !== "error" ? (
                    <Handle
                      id={port}
                      type="source"
                      position={Position.Bottom}
                      className="xflow-preview-node__handle xflow-preview-node__handle--source xflow-preview-node__handle--port"
                      isConnectable={canConnect}
                      isConnectableStart={canConnect}
                      isConnectableEnd={false}
                      role="button"
                      tabIndex={canConnect ? 0 : -1}
                      aria-disabled={!canConnect}
                      aria-label={`${node.name} 输出端口 ${port}；按 Enter 或空格开始连线`}
                      onKeyDown={(event) => activateHandle(event, canConnect)}
                    />
                  ) : null}
                </span>
              ))}
              {rowPorts.includes("error") ? (
                <Handle
                  id="error"
                  type="source"
                  position={Position.Right}
                  className="xflow-preview-node__handle xflow-preview-node__handle--source xflow-preview-node__handle--error"
                  isConnectable={canConnect}
                  isConnectableStart={canConnect}
                  isConnectableEnd={false}
                  role="button"
                  tabIndex={canConnect ? 0 : -1}
                  aria-disabled={!canConnect}
                  aria-label={`${node.name} 输出端口 error；按 Enter 或空格开始连线`}
                  onKeyDown={(event) => activateHandle(event, canConnect)}
                />
              ) : null}
            </div>
          ) : null}
        </div>
      )}
    </article>
  );
});

const nodeTypes = {
  xflowPreviewNode: PreviewNode
};

interface RuntimeSummary {
  total: number;
  counts: Partial<Record<NodeStatusKey, number>>;
}

/** Counts what each card shows, so a disabled node the run never reported reads as disabled here too. */
function summarizeRuntime(nodes: GraphNode[], runtime?: RuntimeSnapshot): RuntimeSummary {
  const counts: Partial<Record<NodeStatusKey, number>> = {};
  for (const node of nodes) {
    const status = displayStatus(node, nodeSnapshot(runtime, node)) ?? "pending";
    counts[status] = (counts[status] ?? 0) + 1;
  }
  return { total: nodes.length, counts };
}

function SummaryPill({ label }: { label: string }): React.ReactElement {
  return <span className="xflow-preview-summary__pill">{label}</span>;
}

/**
 * Run-summary pill order. Total over `NodeStatusKey` on purpose: a status missing
 * from this list is silently dropped from the summary (its nodes show no pill
 * at all), so `satisfies Record<NodeStatusKey, true>` turns an omission into a
 * compile error instead of a UI gap.
 */
const summaryStatusOrder = {
  running: true,
  committing: true,
  failed: true,
  waiting: true,
  suspended: true,
  success: true,
  pinned: true,
  continued: true,
  skipped: true,
  pending: true,
  expired: true,
  canceled: true,
  disabled: true
} satisfies Record<NodeStatusKey, true>;

const summaryStatuses = Object.keys(summaryStatusOrder) as NodeStatusKey[];

function RunSummary({ summary }: { summary: RuntimeSummary }): React.ReactElement {
  return (
    <div className="xflow-preview-summary" aria-label="Run summary">
      <span>Run summary</span>
      <div>
        <SummaryPill label={`${summary.total} nodes`} />
        {summaryStatuses.map((status) => {
          const count = summary.counts[status] ?? 0;
          return count > 0 ? <SummaryPill key={status} label={`${count} ${nodeStatusMeta[status].label}`} /> : null;
        })}
      </div>
    </div>
  );
}

function SelectedNodeDetails({
  node,
  runtime
}: {
  node?: GraphNode;
  runtime?: RuntimeNodeSnapshot;
}): React.ReactElement | null {
  if (!node || !runtime) return null;

  return (
    <section className="xflow-preview-details" aria-label="Selected node details">
      <span>Selected node</span>
      <strong>{node.name}</strong>
      <code>{node.type}</code>
      <p>{node.kind}</p>
      <p>{nodeStatusMeta[runtime.status].label}</p>
      {node.disabled ? <p>Disabled</p> : null}
      {node.notes ? <p>{node.notes}</p> : null}
      <p>Inputs {node.inputs.length}</p>
      <p>Attempts {runtime.attempts ?? 1}</p>
      {runtime.durationMs !== undefined ? <p>Duration {runtime.durationMs} ms</p> : null}
      {runtime.error ? <p className="xflow-preview-node__error">{runtime.error}</p> : null}
    </section>
  );
}

export function XFlowPreview({
  workflow,
  runtime,
  className,
  editable = false,
  interactionMode,
  minimap = false,
  ruler = true,
  zoomBar = true,
  onAutoLayout,
  emptyState,
  canvasFloat,
  canvasFloatEnd,
  canvasFloatEndCompact,
  selectedNodeId: selectedNodeIdProp,
  selectedNodeIds: selectedNodeIdsProp,
  onSelectNode,
  onSelectionChange,
  nodeNotes,
  nodeMetaNotes,
  nodeIssues,
  onNodeAction,
  nodeCategories,
  nodeTypeLabels,
  nodeIcon,
  nodeSummaries,
  onLocateParameter,
  onPaneClick,
  onNodePositionChange,
  onNodePositionsChange,
  onConnect,
  onDeleteConnection,
  onDeleteNode,
  onDeleteNodes,
  onDropNode,
  danglingPorts,
  defaultViewport,
  sections,
  onSectionTitleChange,
  onSectionDelete,
  onSectionGeometryChange,
  onViewportChange,
  onNodeContextMenu,
  onPaneContextMenu,
  onReady
}: XFlowPreviewProps): React.ReactElement {
  const graph = React.useMemo(() => toGraphModel(workflow), [workflow]);
  // Each node's own run policy for the card's config badges. Resume defaults
  // (settings.retry / settings.on_error) stay off the cards: a badge means this
  // node configures something itself.
  const nodePolicies = React.useMemo(() => {
    const byKey: Record<string, PreviewNodePolicy> = Object.create(null);
    for (const node of workflow.nodes ?? []) {
      const policy: PreviewNodePolicy = {};
      const retry = node.retry;
      if (retry && retry.enabled !== false && (retry.max_attempts ?? 0) > 0) policy.retryAttempts = retry.max_attempts;
      if (typeof node.timeout === "number" && node.timeout > 0) policy.timeoutNs = node.timeout;
      if (node.on_error) policy.onError = node.on_error;
      if (policy.retryAttempts !== undefined || policy.timeoutNs !== undefined || policy.onError !== undefined) {
        byKey[nodeKey(node)] = policy;
      }
    }
    return byKey;
  }, [workflow]);
  // §6.2: the visual grouping boxes, resolved once per definition/metadata
  // change. Drawing them is the whole feature; the preview never edits them.
  const sectionBoxes = React.useMemo(
    () => toSectionBoxes(sections, graph.nodes, nodeCategories),
    [graph.nodes, nodeCategories, sections]
  );
  const [internalSelectedNodeId, setInternalSelectedNodeId] = React.useState<string | undefined>();
  const [selectedEdgeIds, setSelectedEdgeIds] = React.useState<ReadonlySet<string>>(() => new Set());
  const [flowInstance, setFlowInstance] = React.useState<ReactFlowInstance<PreviewFlowNode, Edge> | null>(null);
  const selectionViewportRef = React.useRef<{
    flow: ReactFlowInstance<PreviewFlowNode, Edge> | null;
    selectedNodeId: string | undefined;
  }>({ flow: null, selectedNodeId: undefined });
  const canvasRef = React.useRef<HTMLDivElement | null>(null);
  /**
   * §6.1.1: cards must not be assumed to have one height, so the definition's
   * fallback layout is replaced once — after React Flow has measured the real
   * cards — for the nodes that pin no coordinate. A node the user (or a past
   * save) positioned is never moved.
   */
  const [measuredLayout, setMeasuredLayout] = React.useState<ReadonlyMap<string, Required<NodePosition>>>();
  const measuredLayoutDone = React.useRef(false);
  const loadedWorkflowRef = React.useRef(workflow.id ?? workflow.name);
  const unpositionedNodeIds = React.useMemo(
    () => new Set((workflow.nodes ?? []).filter((node) => !hasNodePosition(node)).map(nodeKey)),
    [workflow]
  );
  // The current definition's pinned nodes. Read by layoutNodes too: a drag or
  // 自动布局 writes a position, and that position must win over the one the
  // first measurement produced.
  const positionedNodeIds = React.useMemo(
    () => new Set((workflow.nodes ?? []).filter(hasNodePosition).map(nodeKey)),
    [workflow]
  );
  // A replaced definition starts a fresh measurement pass. Declared before the
  // measuring effect so the reset has already run when that effect looks.
  React.useEffect(() => {
    const identity = workflow.id ?? workflow.name;
    if (loadedWorkflowRef.current === identity) return;
    loadedWorkflowRef.current = identity;
    measuredLayoutDone.current = false;
    setMeasuredLayout(undefined);
  }, [workflow.id, workflow.name]);
  // Runs from the initial-fit retry, which is already waiting on animation
  // frames for the cards' measured bounds: by the time it succeeds every card
  // has a real box, so no extra frame loop is needed.
  const applyMeasuredLayout = React.useCallback((flow: ReactFlowInstance<PreviewFlowNode, Edge>) => {
    if (measuredLayoutDone.current || unpositionedNodeIds.size === 0) return;
    const boxes = new Map<string, AutoLayoutBox>();
    for (const flowNode of flow.getNodes()) {
      if (!unpositionedNodeIds.has(flowNode.id)) continue;
      const width = flowNode.measured?.width ?? flowNode.width ?? 0;
      const height = flowNode.measured?.height ?? flowNode.height ?? 0;
      if (width <= 0 || height <= 0) return;
      boxes.set(flowNode.id, { width, height });
    }
    if (boxes.size !== unpositionedNodeIds.size) return;
    measuredLayoutDone.current = true;
    setMeasuredLayout(autoLayoutPositions(workflow, {
      boxOf: (nodeId) => boxes.get(nodeId),
      onlyUnpositioned: true
    }));
  }, [unpositionedNodeIds, workflow]);
  const layoutNodes = React.useMemo(() => {
    if (!measuredLayout || measuredLayout.size === 0) return graph.nodes;
    return graph.nodes.map((node) => {
      const position = positionedNodeIds.has(node.id) ? undefined : measuredLayout.get(node.id);
      return position ? { ...node, position } : node;
    });
  }, [graph.nodes, measuredLayout, positionedNodeIds]);
  const selectedNodeId = selectedNodeIdProp ?? internalSelectedNodeId;
  const multiSelectEnabled = editable && Boolean(onSelectionChange);
  // One source of truth for the set: the owner's prop when supplied, otherwise
  // the single-selection fallback the standalone API has always supported.
  const selectedNodeIds = React.useMemo<readonly string[]>(() => {
    if (selectedNodeIdsProp !== undefined) return selectedNodeIdsProp;
    return selectedNodeId === undefined ? [] : [selectedNodeId];
  }, [selectedNodeId, selectedNodeIdsProp]);
  const selectedNodeIdSet = React.useMemo(() => new Set(selectedNodeIds), [selectedNodeIds]);
  const [rulerShown, setRulerShown] = useToggleSeededBy(ruler);
  const [minimapShown, setMinimapShown] = useToggleSeededBy(minimap);
  // Preserve the original standalone API when no mode is supplied. The editor
  // opts into a mode explicitly so selecting nodes never accidentally starts a
  // connection, and connecting never drags a node.
  const canDragNodes = editable && (interactionMode === undefined || interactionMode === "select");
  const canConnect = editable && (interactionMode === undefined || interactionMode === "connect") && Boolean(onConnect);
  const canDeleteConnections = editable && Boolean(onDeleteConnection);
  // A multi-select canvas must accept selection even when the host does not
  // wire connection deletion: the marquee needs `elementsSelectable`.
  const canSelect = editable && (multiSelectEnabled || canDeleteConnections);
  const canDeleteNodes = editable && Boolean(onDeleteNodes ?? onDeleteNode);
  const canDropNode = editable && Boolean(onDropNode);
  const graphNodesById = React.useMemo(
    () => new Map(graph.nodes.map((node) => [node.id, node])),
    [graph.nodes]
  );
  const canvasGraphEdges = React.useMemo(
    () => graph.edges.filter((edge) => (
      !isSupplyNode(graphNodesById.get(edge.source)) && !isSupplyNode(graphNodesById.get(edge.target))
    )),
    [graph.edges, graphNodesById]
  );
  const graphEdgesById = React.useMemo(
    () => new Map(canvasGraphEdges.map((edge) => [edge.id, edge])),
    [canvasGraphEdges]
  );
  // Stable on purpose: it travels in every card's `data`, so a new identity per
  // render (it used to follow `selectedNodeIdProp`) would invalidate all cards.
  const handleSelectNode = useStableCallback((nodeId: string) => {
    if (selectedNodeIdProp === undefined) {
      setInternalSelectedNodeId(nodeId);
    }
    onSelectNode?.(nodeId);
  });
  const handleFlowInit = React.useCallback((instance: ReactFlowInstance<PreviewFlowNode, Edge>) => {
    setFlowInstance(instance);
  }, []);
  // The owner echoes every change back through `selectedNodeIds`; the ref also
  // carries the change synchronously, so two deltas in one tick compose and a
  // marquee step that leaves the store unchanged cannot lose the last update.
  const selectionRef = React.useRef<readonly string[]>(selectedNodeIds);
  React.useEffect(() => {
    selectionRef.current = selectedNodeIds;
  }, [selectedNodeIds]);
  /** Selection in graph order, so the reported set does not reshuffle per delta. */
  const orderedSelection = React.useCallback((ids: ReadonlySet<string>): string[] => {
    const ordered = graph.nodes.filter((node) => ids.has(node.id)).map((node) => node.id);
    // An id the layout no longer knows must still leave the set rather than
    // linger invisibly.
    for (const id of ids) if (!ordered.includes(id)) ordered.push(id);
    return ordered;
  }, [graph.nodes]);
  const commitSelection = useStableCallback((ids: ReadonlySet<string>, primaryId?: string) => {
    const next = orderedSelection(ids);
    selectionRef.current = next;
    onSelectionChange?.(next, primaryId);
  });
  // Pane's pointerdown resets the store selection before the first marquee
  // delta, so a delta-only rule would always drop the owner's primary and pick
  // the first node in graph order. Remember the primary at gesture start and
  // hand it back as long as the marquee still covers it.
  const marqueePrimaryRef = React.useRef<string | undefined>(undefined);
  const handleSelectionStart = React.useCallback(() => {
    marqueePrimaryRef.current = selectedNodeId;
  }, [selectedNodeId]);
  const handleSelectionEnd = React.useCallback(() => {
    marqueePrimaryRef.current = undefined;
  }, []);
  // React Flow's marquee reports selection deltas. Applying them to the ref
  // (not to a render-time prop, which may still be a tick behind) is what makes
  // consecutive pointer-move deltas compose.
  const handleNodesChange = React.useCallback((changes: NodeChange<PreviewFlowNode>[]) => {
    const selected = new Set(selectionRef.current);
    const picked: string[] = [];
    let changed = false;
    for (const change of changes) {
      if (change.type !== "select") continue;
      changed = true;
      if (change.selected) {
        selected.add(change.id);
        picked.push(change.id);
      } else {
        selected.delete(change.id);
      }
    }
    if (!changed) return;
    // One picked node names the new primary (a click or a multi-key toggle); a
    // marquee reports its whole set, so the primary it started from survives
    // while that node remains selected.
    const marqueePrimary = marqueePrimaryRef.current;
    const primaryId = marqueePrimary !== undefined && selected.has(marqueePrimary)
      ? marqueePrimary
      : picked.length === 1 ? picked[0] : undefined;
    commitSelection(selected, primaryId);
  }, [commitSelection]);
  const handleToggleNode = useStableCallback((nodeId: string) => {
    const selected = new Set(selectionRef.current);
    let primaryId: string | undefined;
    if (selected.has(nodeId)) {
      selected.delete(nodeId);
    } else {
      selected.add(nodeId);
      primaryId = nodeId;
    }
    commitSelection(selected, primaryId);
  });
  const lastKeyboardMenuAt = React.useRef(-Infinity);
  const openNodeMenuFromKey = useStableCallback((nodeId: string, anchor: Element) => {
    if (!onNodeContextMenu) return;
    lastKeyboardMenuAt.current = Date.now();
    handleSelectNode(nodeId);
    onNodeContextMenu(nodeId, elementCenter(anchor));
  });
  // A keyboard press yields at most one native echo, so the window closes the
  // moment it is consumed: leaving it open would also swallow the next real
  // right click inside it.
  const isKeyboardMenuEcho = React.useCallback(() => {
    if (Date.now() - lastKeyboardMenuAt.current >= KEYBOARD_MENU_ECHO_MS) return false;
    lastKeyboardMenuAt.current = -Infinity;
    return true;
  }, []);
  const handleNodeContextMenu = React.useCallback((event: React.MouseEvent, node: PreviewFlowNode) => {
    if (!onNodeContextMenu) return;
    event.preventDefault();
    if (isKeyboardMenuEcho()) return;
    handleSelectNode(node.id);
    onNodeContextMenu(node.id, { clientX: event.clientX, clientY: event.clientY });
  }, [handleSelectNode, isKeyboardMenuEcho, onNodeContextMenu]);
  // Controlled owners keep their own selection; the uncontrolled one clears too.
  const handlePaneClick = React.useCallback(() => {
    if (selectedNodeIdProp === undefined) setInternalSelectedNodeId(undefined);
    onPaneClick?.();
  }, [onPaneClick, selectedNodeIdProp]);
  // The canvas wrapper sits outside ReactFlow, so it cannot call useStoreApi()
  // itself; FlowStoreHandle hooks the provider's store out for it. Null until
  // mount.
  const flowStoreRef = React.useRef<ReturnType<typeof useStoreApi> | null>(null);
  /**
   * With `selectionOnDrag` owning the left button, xyflow renders the pane with
   * `onClick` undefined (`hasActiveSelection`) and answers a real click from
   * `pointerup` instead. A click that no pointer gesture produced — a harness's
   * `fireEvent.click`, or an assistive-technology activation — then reaches a
   * pane that listens for nothing. Remember pointer gestures here so only that
   * gesture-less click is replayed, while a click trailing a real press (above
   * all a marquee's release) stays React Flow's to interpret.
   */
  const lastCanvasPointerAt = React.useRef(-Infinity);
  const noteCanvasPointer = React.useCallback(() => {
    lastCanvasPointerAt.current = Date.now();
  }, []);
  const handleCanvasClick = React.useCallback((event: React.MouseEvent<HTMLDivElement>) => {
    if (!multiSelectEnabled) return;
    const pane = event.currentTarget.querySelector<HTMLElement>(".react-flow__pane");
    // Only the pane itself is the empty canvas. Cards, connections, handles and
    // the floating chrome all have their own event targets; xyflow's Background
    // and viewport layers are pointer-events: none, so a blank click lands here.
    if (!pane || event.target !== pane) return;
    // While Space makes the pane draggable, xyflow restores its own click
    // handler; answering the same event here would call the owner twice.
    if (pane.classList.contains("draggable")) return;
    if (Date.now() - lastCanvasPointerAt.current < POINTER_CLICK_ECHO_MS) return;
    handlePaneClick();
    // What Pane.onClick does after the owner callback: drop the store's
    // selection (which reports a select:false change for every selected node
    // and edge) so an owner that only listens for onSelectionChange clears too.
    const store = flowStoreRef.current;
    if (store) {
      store.getState().resetSelectedElements();
      store.setState({ nodesSelectionActive: false });
    }
  }, [handlePaneClick, multiSelectEnabled]);
  const handlePaneContextMenu = React.useCallback((event: MouseEvent | React.MouseEvent) => {
    if (!onPaneContextMenu) return;
    event.preventDefault();
    if (isKeyboardMenuEcho()) return;
    // Placement is not a drag: keep the exact pointer position rather than
    // inheriting the canvas's drag-time grid (xyflow defaults this conversion
    // to the store's snapToGrid).
    const flowPosition = flowInstance?.screenToFlowPosition({ x: event.clientX, y: event.clientY }, { snapToGrid: false })
      ?? { x: event.clientX, y: event.clientY };
    onPaneContextMenu({
      clientX: event.clientX,
      clientY: event.clientY,
      flowX: flowPosition.x,
      flowY: flowPosition.y
    });
  }, [flowInstance, onPaneContextMenu]);
  // Nodes are not focusable through React Flow (nodesFocusable is off), so the
  // canvas answers the keyboard route for whichever node is selected; a focused
  // node card answers first and stops the event, see PreviewNode.
  const handleCanvasKeyDown = React.useCallback((event: React.KeyboardEvent<HTMLDivElement>) => {
    if (!onNodeContextMenu || !selectedNodeId || !isContextMenuKey(event)) return;
    if (isTextEntryTarget(event.target)) return;
    const anchor = [...event.currentTarget.querySelectorAll<HTMLElement>(".xflow-preview-node[data-node-id]")]
      .find((element) => element.dataset.nodeId === selectedNodeId);
    if (!anchor) return;
    event.preventDefault();
    openNodeMenuFromKey(selectedNodeId, anchor);
  }, [onNodeContextMenu, openNodeMenuFromKey, selectedNodeId]);
  const nodeFitCancelRef = React.useRef<(() => void) | undefined>(undefined);
  const viewFitCancelRef = React.useRef<(() => void) | undefined>(undefined);
  React.useEffect(() => () => {
    nodeFitCancelRef.current?.();
    viewFitCancelRef.current?.();
  }, []);
  // The view commands the owner's own chrome drives. Both reuse the same fit
  // paths as the in-canvas control and the selection follow, so an unmeasured
  // node is waited out identically.
  const previewHandle = React.useMemo<XFlowPreviewHandle>(() => ({
    fitView: (options) => {
      if (!flowInstance) return;
      viewFitCancelRef.current?.();
      viewFitCancelRef.current = fitMeasuredNodesSettled(flowInstance, options?.padding, canvasRef.current, options?.minZoom ?? MIN_ZOOM) || undefined;
    },
    fitNode: (nodeId, options) => {
      if (!flowInstance) return;
      const node = flowInstance.getNodes().find((candidate) => candidate.id === nodeId);
      if (!node) return;
      nodeFitCancelRef.current?.();
      nodeFitCancelRef.current = fitWhenMeasured(
        () => flowInstance.getNodesBounds([node]),
        (bounds) => {
          locateBoundsClear(flowInstance, bounds, canvasRef.current, options?.padding);
        }
      );
    }
  }), [flowInstance]);
  React.useEffect(() => {
    if (flowInstance) onReady?.(previewHandle);
  }, [flowInstance, onReady, previewHandle]);
  const handleFit = React.useCallback(() => previewHandle.fitView(), [previewHandle]);
  // §6.2: switching the minimap or the rulers changes what the content has to stay
  // clear of, so the graph is re-fitted. The first render is not a switch. This is
  // an automatic fit, not the user's "fit all": it keeps READABLE_FIT_ZOOM, so a
  // huge graph is never shrunk into the low-detail band by a mere toggle.
  const fittedFloats = React.useRef({ minimap: minimapShown, ruler: rulerShown });
  React.useEffect(() => {
    const fitted = fittedFloats.current;
    if (fitted.minimap === minimapShown && fitted.ruler === rulerShown) return;
    fittedFloats.current = { minimap: minimapShown, ruler: rulerShown };
    previewHandle.fitView({ minZoom: READABLE_FIT_ZOOM });
  }, [previewHandle, minimapShown, rulerShown]);
  // §6.2: a click on the minimap centres the canvas there, at the current zoom.
  const handleMiniMapClick = React.useCallback((_event: React.MouseEvent, position: { x: number; y: number }) => {
    flowInstance?.setCenter(position.x, position.y, { zoom: flowInstance.getZoom() });
  }, [flowInstance]);
  // ⇧R / ⇧M, advertised on the zoom bar, so only bound while it is shown. Keys aimed
  // at a text field, or at an element outside this preview, are left alone.
  const sectionRef = React.useRef<HTMLElement | null>(null);
  React.useEffect(() => {
    if (!zoomBar) return undefined;
    const handleToggleKey = (event: KeyboardEvent) => {
      if (!event.shiftKey || event.metaKey || event.ctrlKey || event.altKey) return;
      if (event.defaultPrevented || event.repeat || isTextEntryTarget(event.target)) return;
      const target = event.target;
      if (target instanceof Node && target !== document.body && !sectionRef.current?.contains(target)) return;
      const key = event.key.toLowerCase();
      if (key !== "r" && key !== "m") return;
      event.preventDefault();
      if (key === "r") setRulerShown(!rulerShown);
      else setMinimapShown(!minimapShown);
    };
    window.addEventListener("keydown", handleToggleKey);
    return () => window.removeEventListener("keydown", handleToggleKey);
  }, [minimapShown, rulerShown, setMinimapShown, setRulerShown, zoomBar]);
  const handleMoveEnd = React.useCallback((_event: unknown, viewport: ReactFlowViewport) => {
    onViewportChange?.({ x: viewport.x, y: viewport.y, zoom: viewport.zoom });
  }, [onViewportChange]);
  const handleDragOver = React.useCallback((event: React.DragEvent<HTMLDivElement>) => {
    if (!canDropNode) return;
    // Without preventDefault the browser refuses the drop outright, which would
    // make the node library's drag look broken instead of rejected.
    event.preventDefault();
    event.dataTransfer.dropEffect = "copy";
  }, [canDropNode]);
  const handleDrop = React.useCallback((event: React.DragEvent<HTMLDivElement>) => {
    if (!canDropNode) return;
    const type = event.dataTransfer.getData(XFLOW_NODE_DRAG_MIME);
    if (!type) return;
    event.preventDefault();
    // screenToFlowPosition is the only correct conversion: it folds in the
    // canvas pan and zoom, which a raw client-offset reading would ignore.
    // `snapToGrid: false` keeps a dropped node exactly under the pointer: the
    // canvas grid governs drag gestures, not placement.
    const position = flowInstance?.screenToFlowPosition({ x: event.clientX, y: event.clientY }, { snapToGrid: false });
    if (!position) return;
    onDropNode?.(type, position);
  }, [canDropNode, flowInstance, onDropNode]);
  React.useEffect(() => {
    const previous = selectionViewportRef.current;
    if (!flowInstance) {
      selectionViewportRef.current = { flow: null, selectedNodeId };
      return;
    }

    // Initial fitting is intentionally owned by InitialViewFitter. Recording
    // the initial selection here prevents an immediately selected node from
    // replacing that measured fit as soon as React Flow initializes.
    if (previous.flow !== flowInstance) {
      selectionViewportRef.current = { flow: flowInstance, selectedNodeId };
      return;
    }
    if (previous.selectedNodeId === selectedNodeId) return;

    selectionViewportRef.current = { flow: flowInstance, selectedNodeId };
    if (!selectedNodeId) return;

    const selectedNode = flowInstance.getNodes().find((node) => node.id === selectedNodeId);
    if (!selectedNode) return;
    // The fit exists to reveal a node that is off screen — one appended at a
    // default slot while the canvas is panned elsewhere, one picked from the
    // outline. A node that is already on screen is the one the user just placed,
    // and re-centering it would pull the graph out from under the pointer that
    // dropped it.
    const canvas = canvasRef.current?.getBoundingClientRect();
    if (!canvas || isNodeAnchorOnScreen(flowInstance, selectedNode, canvas)) return;

    // A just-appended node has no measured size yet, and fitting its 0-size
    // bounds would clamp the zoom to maxZoom; fitWhenMeasured retries until it
    // is measured, exactly like fitMeasuredNodes does for the initial view. The
    // locate keeps the readable zoom band and only pans when it can, so a
    // selection from ⌘K never jumps to an unreadable close-up.
    return fitWhenMeasured(
      () => flowInstance.getNodesBounds([selectedNode]),
      (bounds) => {
        locateBoundsClear(flowInstance, bounds, canvasRef.current);
      }
    );
  }, [flowInstance, selectedNodeId]);
  const isValidConnection = React.useCallback<IsValidConnection<Edge>>((connection) => {
    const source = graphNodesById.get(connection.source);
    const target = graphNodesById.get(connection.target);
    return Boolean(
      source
      && target
      && source.id !== target.id
      && !isSupplyNode(source)
      && !isSupplyNode(target)
    );
  }, [graphNodesById]);
  const handleConnect = React.useCallback((connection: ReactFlowConnection) => {
    if (!canConnect || !isValidConnection(connection)) return;
    const previewConnection = previewConnectionFromFlow(connection, graphNodesById);
    if (previewConnection) {
      onConnect?.(previewConnection);
    }
  }, [canConnect, graphNodesById, isValidConnection, onConnect]);
  const handleEdgesChange = React.useCallback((changes: EdgeChange[]) => {
    if (!canDeleteConnections) return;

    setSelectedEdgeIds((current) => {
      const next = new Set(current);
      for (const change of changes) {
        if (change.type !== "select" || !graphEdgesById.has(change.id)) continue;
        if (change.selected) {
          next.add(change.id);
        } else {
          next.delete(change.id);
        }
      }
      return next.size === current.size && [...next].every((id) => current.has(id)) ? current : next;
    });
  }, [canDeleteConnections, graphEdgesById]);
  const handleEdgesDelete = React.useCallback((deletedEdges: Edge[]) => {
    if (!canDeleteConnections || !onDeleteConnection) return;

    const deletedIds = new Set<string>();
    for (const deletedEdge of deletedEdges) {
      if (deletedIds.has(deletedEdge.id)) continue;
      const graphEdge = graphEdgesById.get(deletedEdge.id);
      if (!graphEdge) continue;
      deletedIds.add(graphEdge.id);
      onDeleteConnection(edgeConnection(graphEdge));
    }
    if (deletedIds.size > 0) {
      setSelectedEdgeIds((current) => {
        const next = new Set([...current].filter((id) => !deletedIds.has(id)));
        return next.size === current.size ? current : next;
      });
    }
  }, [canDeleteConnections, graphEdgesById, onDeleteConnection]);
  const handleBeforeDelete = React.useCallback<OnBeforeDelete<PreviewFlowNode>>(async ({ nodes, edges }) => ({
    nodes: [],
    // A multi-select canvas owns node deletion in the listener below. When it
    // has nodes selected, returning edges too would let the library remove
    // those connections in a second, separate write; the owner removes them
    // with the nodes in one undoable step instead. A legacy single-select host
    // keeps xyflow's edge behavior untouched.
    edges: canDeleteConnections && (!multiSelectEnabled || nodes.length === 0)
      ? edges.filter((edge) => graphEdgesById.has(edge.id))
      : []
  }), [canDeleteConnections, graphEdgesById, multiSelectEnabled]);
  React.useEffect(() => {
    setSelectedEdgeIds((current) => {
      if (!canDeleteConnections) {
        return current.size === 0 ? current : new Set();
      }
      const next = new Set([...current].filter((id) => graphEdgesById.has(id)));
      return next.size === current.size ? current : next;
    });
  }, [canDeleteConnections, graphEdgesById]);
  // Node deletion rides its own listener rather than React Flow's delete key:
  // onBeforeDelete returns `nodes: []`, so the library only ever removes edges.
  // Standing down while only an edge is selected keeps that press meaning
  // "remove the connection" — the library's handler runs for the same keystroke,
  // and letting both act would take the node behind the connection with it. A
  // node selection wins over that edge selection: a marquee selects the edges
  // attached to its nodes too, and the owner must remove both together.
  React.useEffect(() => {
    if (!canDeleteNodes) return undefined;
    const handleDeleteKey = (event: KeyboardEvent) => {
      if (event.key !== "Delete" && event.key !== "Backspace") return;
      if (isTextEntryTarget(event.target)) return;
      if (!multiSelectEnabled) {
        // Legacy single-selection contract: a selected edge is the subject of
        // the press, and the library's own handler removes it.
        if (selectedEdgeIds.size > 0) return;
        if (!selectedNodeId) return;
        event.preventDefault();
        onDeleteNode?.(selectedNodeId);
        return;
      }
      // A multi-select node selection wins over an edge selection: a marquee
      // selects the edges attached to its nodes too, and the owner must remove
      // both together in one change.
      if (selectedNodeIds.length > 0) {
        event.preventDefault();
        if (onDeleteNodes) onDeleteNodes(selectedNodeIds);
        else if (selectedNodeId) onDeleteNode?.(selectedNodeId);
        return;
      }
      if (selectedEdgeIds.size > 0) return;
      if (!selectedNodeId) return;
      event.preventDefault();
      onDeleteNode?.(selectedNodeId);
    };
    window.addEventListener("keydown", handleDeleteKey);
    return () => window.removeEventListener("keydown", handleDeleteKey);
  }, [canDeleteNodes, multiSelectEnabled, onDeleteNode, onDeleteNodes, selectedEdgeIds, selectedNodeId, selectedNodeIds]);
  // Last render's node objects, kept so unchanged cards hand React.memo the
  // same references (see reuseFlowNodes).
  const previousFlowNodes = React.useRef<PreviewFlowNode[]>([]);
  const hasNodeContextMenu = Boolean(onNodeContextMenu);
  // Permanent identity, like the other card callbacks: a new closure on every
  // owner render would rebuild every card's data object.
  const handleNodeAction = useStableCallback((nodeName: string, action: string) => {
    onNodeAction?.(nodeName, action);
  });
  const hasNodeAction = Boolean(onNodeAction);
  const handleLocateParameter = useStableCallback((nodeId: string, fieldPath: string) => {
    onLocateParameter?.(nodeId, fieldPath);
  });
  const hasLocateParameter = Boolean(onLocateParameter);
  const flowNodes = React.useMemo(() => {
    const next = reuseFlowNodes(
      previousFlowNodes.current,
      toFlowNodes(
        layoutNodes,
        canvasGraphEdges,
        runtime,
        selectedNodeIdSet,
        multiSelectEnabled,
        canDragNodes,
        canConnect,
        handleSelectNode,
        multiSelectEnabled ? handleToggleNode : undefined,
        hasNodeContextMenu ? openNodeMenuFromKey : undefined,
        nodeNotes,
        hasNodeAction ? handleNodeAction : undefined,
        nodeCategories,
        nodeTypeLabels,
        nodePolicies,
        nodeIcon,
        nodeSummaries,
        hasLocateParameter ? handleLocateParameter : undefined,
        nodeMetaNotes,
        nodeIssues
      )
    );
    previousFlowNodes.current = next;
    return next;
  }, [layoutNodes, canvasGraphEdges, runtime, selectedNodeIdSet, multiSelectEnabled, canDragNodes, canConnect, handleSelectNode, handleToggleNode, hasNodeContextMenu, openNodeMenuFromKey, nodeNotes, nodeMetaNotes, nodeIssues, hasNodeAction, handleNodeAction, nodeCategories, nodeTypeLabels, nodePolicies, nodeIcon, nodeSummaries, hasLocateParameter, handleLocateParameter]);
  const danglingSet = React.useMemo(
    () => new Set((danglingPorts ?? []).map((port) => danglingKey(port.source, port.sourcePort))),
    [danglingPorts]
  );
  const flowEdges = React.useMemo(
    () => toFlowEdges(canvasGraphEdges, runtime, graphNodesById, canDeleteConnections, selectedEdgeIds, danglingSet),
    [canvasGraphEdges, runtime, graphNodesById, canDeleteConnections, selectedEdgeIds, danglingSet]
  );
  const summary = React.useMemo(() => summarizeRuntime(graph.nodes, runtime), [graph.nodes, runtime]);
  const selectedNode = React.useMemo(
    () => graph.nodes.find((node) => node.id === selectedNodeId),
    [graph.nodes, selectedNodeId]
  );
  const selectedRuntime = selectedNode ? runtimeForNode(runtime, selectedNode) : undefined;
  // An empty canvas has nothing to measure, so §6.2 draws no rulers on it.
  const rulersDrawn = rulerShown && graph.nodes.length > 0;
  const title = workflow.name ?? "Untitled workflow";
  const workflowStatus = runtime?.status ?? "pending";

  return (
    <section
      ref={sectionRef}
      className={["xflow-preview", className].filter(Boolean).join(" ")}
      aria-label={`${title} preview`}
    >
      <header className="xflow-preview-toolbar">
        <div>
          <p>Preview</p>
          <h2>{title}</h2>
        </div>
        <span
          className="xflow-preview-status"
          data-status={workflowStatus}
          data-tone={workflowStatusMeta[workflowStatus].tone}
        >
          {workflowStatusMeta[workflowStatus].label}
        </span>
      </header>

      <div className="xflow-preview-shell">
        <aside className="xflow-preview-sidebar" aria-label="Workflow nodes">
          <span>Nodes</span>
          <strong>{graph.nodes.length}</strong>
        </aside>

        <div
          className="xflow-preview-canvas"
          ref={canvasRef}
          data-ruler={String(rulersDrawn)}
          onDragOver={canDropNode ? handleDragOver : undefined}
          onDrop={canDropNode ? handleDrop : undefined}
          onKeyDown={onNodeContextMenu ? handleCanvasKeyDown : undefined}
          // React Flow suppresses the pane's click while the marquee owns the
          // left button; these three answer only the gesture-less click it
          // cannot see. See handleCanvasClick.
          onClick={multiSelectEnabled ? handleCanvasClick : undefined}
          onPointerDown={multiSelectEnabled ? noteCanvasPointer : undefined}
          onPointerUp={multiSelectEnabled ? noteCanvasPointer : undefined}
        >
          {/* React Flow mounts even with zero nodes so an empty canvas can still
              accept a dropped node; the empty state sits above it, and only the
              owner's own content (its buttons) takes the pointer. */}
          {graph.nodes.length === 0 ? (
            emptyState === undefined ? (
              <p className="xflow-preview-empty">No nodes to preview</p>
            ) : (
              <div className="xflow-preview-empty xflow-preview-empty--custom">{emptyState}</div>
            )
          ) : null}
          <ReactFlow
            nodes={flowNodes}
            edges={flowEdges}
            nodeTypes={nodeTypes}
            onInit={handleFlowInit}
            minZoom={MIN_ZOOM}
            maxZoom={MAX_ZOOM}
            defaultViewport={defaultViewport as ReactFlowViewport | undefined}
            onMoveEnd={onViewportChange ? handleMoveEnd : undefined}
            onNodeContextMenu={onNodeContextMenu ? handleNodeContextMenu : undefined}
            onPaneContextMenu={onPaneContextMenu ? handlePaneContextMenu : undefined}
            onPaneClick={onPaneClick ? handlePaneClick : undefined}
            // §6.2 canvas gestures: a left drag draws the marquee, Space (xyflow's
            // panActivationKeyCode) or the middle button pans, and the right
            // button stays the pane's context menu. xyflow only honours
            // `selectionOnDrag` when `panOnDrag` is not `true`, hence the
            // explicit button list instead of the default pan-everything.
            selectionOnDrag={multiSelectEnabled}
            selectionMode={SelectionMode.Partial}
            multiSelectionKeyCode={multiSelectEnabled ? ["Meta", "Control"] : undefined}
            panOnDrag={multiSelectEnabled ? [1] : true}
            // §6.2: a drag lands on the same 16px lattice the dots paint. The
            // editor commits whatever positions xyflow reports, so a single
            // drag and a whole multi-selection drag arrive already quantized;
            // stored coordinates are never rewritten outside a gesture.
            snapToGrid
            snapGrid={CANVAS_SNAP_GRID}
            onNodesChange={multiSelectEnabled ? handleNodesChange : undefined}
            onSelectionStart={multiSelectEnabled ? handleSelectionStart : undefined}
            onSelectionEnd={multiSelectEnabled ? handleSelectionEnd : undefined}
            nodesDraggable={canDragNodes}
            nodesConnectable={canConnect}
            // ED-08: nodes hold the canvas tab order; edges are pointer-selectable
            // only (the Inspector's 连接 tab is the keyboard route).
            edgesFocusable={false}
            nodesFocusable={false}
            elementsSelectable={canSelect}
            connectOnClick={canConnect}
            deleteKeyCode={canDeleteConnections ? ["Backspace", "Delete"] : null}
            onConnect={canConnect ? handleConnect : undefined}
            onEdgesChange={canDeleteConnections ? handleEdgesChange : undefined}
            onEdgesDelete={canDeleteConnections ? handleEdgesDelete : undefined}
            onBeforeDelete={canDeleteConnections ? handleBeforeDelete : undefined}
            isValidConnection={canConnect ? isValidConnection : undefined}
            aria-label={`${title} canvas`}
            onNodeDragStop={canDragNodes ? (_, node, moved) => {
              // xyflow reports the grabbed node plus every other node the drag
              // moved (the whole selection); the owner commits them in one go.
              if (onNodePositionsChange) {
                const dragged = moved.length > 0 ? moved : [node];
                onNodePositionsChange(dragged.map((item) => ({ id: item.id, position: item.position })));
                return;
              }
              onNodePositionChange?.(node.id, node.position);
            } : undefined}
            // The design baseline carries no React Flow watermark. xyflow's
            // supported switch for this is proOptions.hideAttribution (the
            // attribution is a license request, not a functional dependency);
            // hiding it is recorded as a project decision rather than a CSS
            // hack, so an upgrade cannot silently bring the badge back.
            proOptions={{ hideAttribution: true }}
          >
            <InitialViewFitter flow={flowInstance} disabled={defaultViewport !== undefined} canvas={canvasRef} onMeasured={applyMeasuredLayout} />
            <CanvasScaleVariables target={canvasRef} />
            <FlowStoreHandle target={flowStoreRef} />
            {/* §6.2: a 16px dot grid in the canvas-dot colour, not the old
                minor/major line grid. Its pitch is CANVAS_GRID, which is also
                what `snapGrid` quantizes a drag to. */}
            <Background color="var(--xflow-canvas-dot)" gap={16} size={1} />
            {/* The visual grouping boxes: portal children inherit the flow's
                pan/zoom transform, and the layer's negative z-index keeps them
                behind every card. Read-only by construction (pointer-events:
                none in styles.css), so this layer needs no mode switch. */}
            {sectionBoxes.length > 0 ? (
              <ViewportPortal>
                <div
                  className="xflow-preview-section-layer"
                  aria-hidden={
                    editable && (onSectionTitleChange || onSectionDelete || onSectionGeometryChange)
                      ? undefined
                      : true
                  }
                >
                  {sectionBoxes.map((box) => (
                    <PreviewSectionFrame
                      box={box}
                      editable={editable}
                      key={box.key}
                      onDelete={onSectionDelete}
                      onGeometryChange={onSectionGeometryChange}
                      onTitleChange={onSectionTitleChange}
                    />
                  ))}
                </div>
              </ViewportPortal>
            ) : null}
            {rulersDrawn ? <CanvasRulers selectedNodeId={selectedNodeId} /> : null}
            {canvasFloat ? (
              <div className="xflow-preview-canvas-float" {...{ [FLOAT_ATTRIBUTE]: "" }}>
                {canvasFloat}
              </div>
            ) : null}
            {canvasFloatEnd ? (
              <div
                className={`xflow-preview-canvas-float xflow-preview-canvas-float--end${canvasFloatEndCompact ? " xflow-preview-canvas-float--compact" : ""}`}
                {...{ [FLOAT_ATTRIBUTE]: "" }}
              >
                {canvasFloatEnd}
              </div>
            ) : null}
            {zoomBar ? (
              <ZoomBar
                onFit={handleFit}
                onAutoLayout={onAutoLayout}
                ruler={rulerShown}
                onRulerChange={setRulerShown}
                minimap={minimapShown}
                onMinimapChange={setMinimapShown}
              />
            ) : null}
            {minimapShown ? (
              <MiniMap
                ariaLabel="缩略图"
                pannable
                zoomable
                style={MINIMAP_SIZE}
                maskColor="var(--xflow-preview-minimap-mask)"
                maskStrokeColor="var(--xflow-preview-minimap-view)"
                maskStrokeWidth={1.5}
                nodeBorderRadius={2}
                onClick={handleMiniMapClick}
                nodeColor={miniMapNodeColor}
                // The baseline swatch is a flat colour block (`.mm-node`, 2px
                // radius, no outline); xyflow's default draws a border.
                nodeStrokeWidth={0}
              />
            ) : null}
          </ReactFlow>
        </div>

        <aside className="xflow-preview-panel" aria-label="Workflow status">
          <RunSummary summary={summary} />
          <SelectedNodeDetails node={selectedNode} runtime={selectedRuntime} />
          <span>Connections</span>
          <EdgeList edges={canvasGraphEdges} />
        </aside>
      </div>
    </section>
  );
}
