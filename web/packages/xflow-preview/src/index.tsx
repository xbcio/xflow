import * as React from "react";
import { KeyOutlined, PushpinOutlined } from "@ant-design/icons";
import {
  Background,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  type Connection as ReactFlowConnection,
  type BuiltInEdge,
  type Edge,
  type EdgeChange,
  type IsValidConnection,
  type Node,
  type NodeProps,
  type OnBeforeDelete,
  type ReactFlowInstance,
  type ReactFlowState,
  type Viewport as ReactFlowViewport,
  useStore,
  useStoreApi
} from "@xyflow/react";
import {
  categoryOf,
  edgeVariant,
  isSupplyNode,
  isTextEntryTarget,
  nodeCategoryMeta,
  nodeStatusMeta,
  runtimeForNode,
  toGraphModel,
  workflowStatusMeta,
  type EdgeVariant,
  type GraphEdge,
  type GraphNode,
  type NodeStatusKey,
  type Position as NodePosition,
  type CardSummaryRow,
  type RuntimeNodeSnapshot,
  type RuntimeSnapshot,
  type Viewport,
  type WorkflowDef
} from "@xflow/core";
import { CanvasRulers, FLOAT_ATTRIBUTE, MAX_ZOOM, MINIMAP_SIZE, MIN_ZOOM, ZoomBar, measureSurface } from "./canvasChrome";
import { fitViewportClearOf } from "./fit";
import { categoryGlyph, statusGlyph } from "./icons";
import "./styles.css";

/**
 * Payload key the node library writes when a node tile starts a drag. It lives
 * here, not in the editor, because the preview owns the drop target and the
 * editor already depends on this package — the reverse import is not allowed.
 */
export const XFLOW_NODE_DRAG_MIME = "application/xflow-node-type";

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
   * Draws the §6.2 minimap in the canvas's bottom-right corner at 200x100. Off by
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
  selectedNodeId?: string;
  onSelectNode?: (nodeId: string) => void;
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

/** View commands the preview exposes to its owner; read-only, never mutating. */
export interface XFlowPreviewHandle {
  /** Fits the whole graph, exactly like the canvas's own fit control. */
  fitView: (options?: { padding?: number }) => void;
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
 * Zoom below which a card keeps only its category icon, name and status icon
 * (spec §6.2 细节层级). Cards subscribe to the boolean `zoom < LOD_ZOOM`, never to
 * the zoom value, so a pinch across 0.5 re-renders the graph once and every other
 * zoom step not at all.
 */
const LOD_ZOOM = 0.5;

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
  /** The type's declared descriptor category, when the host knows one. */
  category?: string;
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
  canvas: HTMLElement | null | undefined
): void {
  const surface = measureSurface(canvas ?? null);
  const viewport = surface
    ? fitViewportClearOf(bounds, surface.area, surface.layers, { minZoom: MIN_ZOOM, maxZoom: MAX_ZOOM, padding })
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
  canvas?: HTMLElement | null
): boolean {
  const nodes = flow.getNodes();
  if (nodes.length === 0) return false;

  const bounds = flow.getNodesBounds(nodes);
  if (bounds.width <= 0 || bounds.height <= 0) return false;

  fitBoundsClear(flow, bounds, padding, canvas);
  return true;
}

const sameBounds = (a: { x: number; y: number; width: number; height: number }, b: typeof a) =>
  Math.abs(a.x - b.x) < 0.5 && Math.abs(a.y - b.y) < 0.5
  && Math.abs(a.width - b.width) < 0.5 && Math.abs(a.height - b.height) < 0.5;

/** Frames without a bounds change after which a fit counts as settled. */
const fitSettleFrames = 4;

/**
 * fitMeasuredNodes, then follows the cards as they re-measure: a fit that crosses
 * LOD_ZOOM resizes them (low detail scales the header by 1/zoom), so the bounds it
 * used are stale a frame later and the content would run under the chrome. Refits
 * while the bounds keep changing, for at most fitRetryLimit frames. Returns a
 * cancel function for the follow-up still in flight; false when nothing was fitted.
 */
function fitMeasuredNodesSettled(
  flow: ReactFlowInstance<PreviewFlowNode, Edge>,
  padding: number | undefined,
  canvas: HTMLElement | null | undefined
): (() => void) | false {
  if (!fitMeasuredNodes(flow, padding, canvas)) return false;
  let fitted = flow.getNodesBounds(flow.getNodes());
  let frame: number | undefined;
  let quiet = 0;
  let frames = 0;
  const check = () => {
    frame = undefined;
    const bounds = flow.getNodesBounds(flow.getNodes());
    if (bounds.width > 0 && bounds.height > 0 && !sameBounds(bounds, fitted)) {
      fitBoundsClear(flow, bounds, padding ?? initialFitViewOptions.padding, canvas);
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
  canvas
}: {
  flow: ReactFlowInstance<PreviewFlowNode, Edge> | null;
  disabled?: boolean;
  canvas: React.RefObject<HTMLDivElement | null>;
}): null {
  const hasFittedInitialView = React.useRef(false);
  const fitViewFrame = React.useRef<number | undefined>(undefined);
  const settleCancel = React.useRef<(() => void) | undefined>(undefined);

  // `disabled` is a dependency so a viewport restored after mount cancels a
  // pending retry; hasFittedInitialView keeps a re-run from fitting twice.
  React.useEffect(() => {
    if (!flow || disabled || hasFittedInitialView.current) return;

    let attempts = 0;
    const tryFit = () => {
      fitViewFrame.current = undefined;
      if (hasFittedInitialView.current) return;

      const settling = fitMeasuredNodesSettled(flow, undefined, canvas.current);
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
  selectedNodeId: string | undefined,
  editable: boolean,
  canConnect: boolean,
  onSelect: (nodeId: string) => void,
  onContextMenuKey: ((nodeId: string, anchor: Element) => void) | undefined,
  nodeNotes: Readonly<Record<string, XFlowPreviewNodeNote>> | undefined,
  onNodeAction: ((nodeName: string, action: string) => void) | undefined,
  nodeCategories: Readonly<Record<string, string>> | undefined,
  nodeSummaries: Readonly<Record<string, readonly CardSummaryRow[]>> | undefined,
  onLocateParameter: ((nodeId: string, fieldPath: string) => void) | undefined,
  nodeMetaNotes: Readonly<Record<string, string>> | undefined
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
      // xyflow does not manage selection here (the owner does); the field is
      // still set so the card reads it as `NodeProps.selected` rather than `data`.
      selectable: false,
      selected: selectedNodeId === node.id,
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
        onContextMenuKey,
        note: own(nodeNotes, node.name),
        metaNote: own(nodeMetaNotes, node.name),
        category: own(nodeCategories, node.type),
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
    && a.onContextMenuKey === b.onContextMenuKey
    && a.onNodeAction === b.onNodeAction
    && a.onLocateParameter === b.onLocateParameter
    && a.category === b.category
    && sameSummaries(a.summary, b.summary)
    && sameNote(a.note, b.note)
    && a.metaNote === b.metaNote
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

function sameFlowNode(a: PreviewFlowNode, b: PreviewFlowNode): boolean {
  return (
    a.data === b.data
    && a.selected === b.selected
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
    const connectionLabel = `Connection from ${edge.sourceName} ${edge.sourcePort} to ${edge.targetName} ${edge.targetPort}${
      isDangling ? " (port no longer exists)" : ""
    }`;

    const flowEdge: BuiltInEdge = {
      id: edge.id,
      source: edge.source,
      target: edge.target,
      sourceHandle: edge.sourcePort,
      targetHandle: edge.targetPort,
      type: "smoothstep",
      // §6.2: orthogonal routing with 8px rounded corners (xyflow's default is 5).
      pathOptions: { borderRadius: 8 },
      className: isDangling
        ? "xflow-preview-edge--dangling"
        : isError
          ? "xflow-preview-edge--error"
          : `xflow-preview-edge--${variant}`,
      label: showLabel ? (isDangling ? `${edge.sourcePort} · missing` : edge.sourcePort) : undefined,
      selectable: canDeleteConnections,
      focusable: canDeleteConnections,
      deletable: canDeleteConnections,
      selected: canDeleteConnections && selectedEdgeIds.has(edge.id),
      ariaLabel: canDeleteConnections
        ? `${connectionLabel}; press Enter to select, then Delete or Backspace to remove`
        : connectionLabel,
      interactionWidth: 16,
      // Every stroke — port-semantic ones included — lives in styles.css under
      // `.xflow-preview-edge--*`. Nothing inline: an inline stroke would outrank
      // both the hover rule and the selected rule (and the label colour moved to
      // CSS for the same reason).
      labelBgStyle: {
        fill: "var(--xflow-preview-edge-label-bg)"
      }
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
          aria-label={`${edge.sourceName} ${edge.sourcePort} to ${edge.targetName} ${edge.targetPort}`}
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
 * place its edges).
 */
const lodSelector = (state: ReactFlowState): boolean => state.transform[2] < LOD_ZOOM;

/**
 * Writes the zoom scale the LOD rules divide by (`--xflow-preview-lod`) straight
 * onto the canvas element. A store subscription, not a selector: the value moves
 * with every pinch step and must never re-render a single card.
 */
function LodScaleVariables({ target }: { target: React.RefObject<HTMLDivElement | null> }): null {
  const store = useStoreApi();
  React.useEffect(() => {
    const element = target.current;
    if (!element) return;
    let last: string | undefined;
    return store.subscribe((state) => {
      const zoom = state.transform[2];
      // Above the threshold no rule reads the variable; pinning it to 1 stops
      // the writes once the user is zoomed in.
      const value = zoom < LOD_ZOOM ? String(zoom) : "1";
      if (value === last) return;
      last = value;
      element.style.setProperty("--xflow-preview-lod", value);
    });
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
  const { graphNode: node, runtime, sourcePorts, targetPorts, canConnect, onSelect, onContextMenuKey, note, metaNote, category: declaredCategory, summary, onNodeAction, onLocateParameter } = data;
  const lowDetail = useStore(lodSelector);
  const category = categoryOf(node.type, declaredCategory);
  const categoryMeta = nodeCategoryMeta[category];
  const status = displayStatus(node, runtime);
  const statusMeta = status ? nodeStatusMeta[status] : undefined;
  const statusLabel = statusMeta?.label;
  const nameTitle = node.label === node.name ? node.label : `${node.label} (${node.name})`;
  const rowPorts = portsForRow(sourcePorts);
  const runMetaLeft = metaNote ?? (runtime?.durationMs !== undefined ? formatDuration(runtime.durationMs) : undefined);
  const runMetaRight = runtime?.attempts !== undefined ? `第 ${runtime.attempts} 次` : undefined;
  const error = lowDetail ? undefined : runtime?.error;

  const activate = (): void => onSelect(node.id);

  return (
    <article
      className="xflow-preview-node"
      data-node-id={node.id}
      data-selected={selected}
      data-status={status ?? "pending"}
      data-tone={statusMeta?.tone}
      data-disabled={node.disabled || undefined}
      data-lod={lowDetail ? "low" : undefined}
      // The category colour comes from core's table (§5); CSS derives the block's
      // background and icon colour from this one variable.
      style={{ "--xflow-preview-node-category": `var(--xflow-${categoryMeta.color})` } as React.CSSProperties}
      onClick={(event) => {
        // Handles start connections; a press there must not also pick the node.
        if ((event.target as Element).closest(".xflow-preview-node__handle")) return;
        activate();
      }}
    >
      {status === "pinned" ? (
        <span className="xflow-preview-node__pin" aria-hidden="true">
          <PushpinOutlined />
        </span>
      ) : null}
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
          aria-label={`${node.name} input ${port}; press Enter or Space to complete a connection`}
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
          aria-label={`${node.name} output main; press Enter or Space to start a connection`}
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
            {categoryGlyph[categoryMeta.icon]}
          </span>
          <div className="xflow-preview-node__title">
            <strong title={nameTitle}>{node.label}</strong>
            {lowDetail ? null : <code className="xflow-preview-node__type" title={node.type}>{node.type}</code>}
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
      {!lowDetail && summary && summary.length > 0 ? (
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
                      <KeyOutlined />
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
        Run meta and the error line sit above the port row: the row is the card's
        last line, so the outlets leave as close to the bottom edge as the row's
        own padding allows (baseline /ui-preview §6.1).
      */}
      {!lowDetail && (runMetaLeft || runMetaRight) ? (
        <div className="xflow-preview-node__run-meta">
          {runMetaLeft ? <span>{runMetaLeft}</span> : null}
          {runMetaRight ? <span>{runMetaRight}</span> : null}
        </div>
      ) : null}
      {error ? <p className="xflow-preview-node__error" title={error}>{error}</p> : null}
      {rowPorts.length > 0 ? (
        lowDetail ? (
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
        ) : (
          <div className="xflow-preview-node__ports">
            {rowPorts.map((port) => (
              <span
                key={port}
                className={
                  port === "error"
                    ? "xflow-preview-node__port xflow-preview-node__port--error"
                    : "xflow-preview-node__port"
                }
              >
                {port}
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
                    aria-label={`${node.name} output ${port}; press Enter or Space to start a connection`}
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
                aria-label={`${node.name} output error; press Enter or Space to start a connection`}
                onKeyDown={(event) => activateHandle(event, canConnect)}
              />
            ) : null}
          </div>
        )
      ) : null}
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
  selectedNodeId: selectedNodeIdProp,
  onSelectNode,
  nodeNotes,
  nodeMetaNotes,
  onNodeAction,
  nodeCategories,
  nodeSummaries,
  onLocateParameter,
  onPaneClick,
  onNodePositionChange,
  onConnect,
  onDeleteConnection,
  onDeleteNode,
  onDropNode,
  danglingPorts,
  defaultViewport,
  onViewportChange,
  onNodeContextMenu,
  onPaneContextMenu,
  onReady
}: XFlowPreviewProps): React.ReactElement {
  const graph = React.useMemo(() => toGraphModel(workflow), [workflow]);
  const [internalSelectedNodeId, setInternalSelectedNodeId] = React.useState<string | undefined>();
  const [selectedEdgeIds, setSelectedEdgeIds] = React.useState<ReadonlySet<string>>(() => new Set());
  const [flowInstance, setFlowInstance] = React.useState<ReactFlowInstance<PreviewFlowNode, Edge> | null>(null);
  const selectionViewportRef = React.useRef<{
    flow: ReactFlowInstance<PreviewFlowNode, Edge> | null;
    selectedNodeId: string | undefined;
  }>({ flow: null, selectedNodeId: undefined });
  const canvasRef = React.useRef<HTMLDivElement | null>(null);
  const selectedNodeId = selectedNodeIdProp ?? internalSelectedNodeId;
  const [rulerShown, setRulerShown] = useToggleSeededBy(ruler);
  const [minimapShown, setMinimapShown] = useToggleSeededBy(minimap);
  // Preserve the original standalone API when no mode is supplied. The editor
  // opts into a mode explicitly so selecting nodes never accidentally starts a
  // connection, and connecting never drags a node.
  const canDragNodes = editable && (interactionMode === undefined || interactionMode === "select");
  const canConnect = editable && (interactionMode === undefined || interactionMode === "connect") && Boolean(onConnect);
  const canDeleteConnections = editable && Boolean(onDeleteConnection);
  const canDeleteNodes = editable && Boolean(onDeleteNode);
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
  const handlePaneContextMenu = React.useCallback((event: MouseEvent | React.MouseEvent) => {
    if (!onPaneContextMenu) return;
    event.preventDefault();
    if (isKeyboardMenuEcho()) return;
    const flowPosition = flowInstance?.screenToFlowPosition({ x: event.clientX, y: event.clientY })
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
      viewFitCancelRef.current = fitMeasuredNodesSettled(flowInstance, options?.padding, canvasRef.current) || undefined;
    },
    fitNode: (nodeId, options) => {
      if (!flowInstance) return;
      const node = flowInstance.getNodes().find((candidate) => candidate.id === nodeId);
      if (!node) return;
      nodeFitCancelRef.current?.();
      nodeFitCancelRef.current = fitWhenMeasured(
        () => flowInstance.getNodesBounds([node]),
        (bounds) => {
          fitBoundsClear(flowInstance, bounds, options?.padding ?? nodeFitPadding, canvasRef.current);
        }
      );
    }
  }), [flowInstance]);
  React.useEffect(() => {
    if (flowInstance) onReady?.(previewHandle);
  }, [flowInstance, onReady, previewHandle]);
  const handleFit = React.useCallback(() => previewHandle.fitView(), [previewHandle]);
  // §6.2: switching the minimap or the rulers changes what the content has to stay
  // clear of, so the graph is re-fitted. The first render is not a switch.
  const fittedFloats = React.useRef({ minimap: minimapShown, ruler: rulerShown });
  React.useEffect(() => {
    const fitted = fittedFloats.current;
    if (fitted.minimap === minimapShown && fitted.ruler === rulerShown) return;
    fittedFloats.current = { minimap: minimapShown, ruler: rulerShown };
    handleFit();
  }, [handleFit, minimapShown, rulerShown]);
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
    const position = flowInstance?.screenToFlowPosition({ x: event.clientX, y: event.clientY });
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
    // is measured, exactly like fitMeasuredNodes does for the initial view.
    return fitWhenMeasured(
      () => flowInstance.getNodesBounds([selectedNode]),
      (bounds) => {
        fitBoundsClear(flowInstance, bounds, nodeFitPadding, canvasRef.current);
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
  const handleBeforeDelete = React.useCallback<OnBeforeDelete<PreviewFlowNode>>(async ({ edges }) => ({
    nodes: [],
    edges: canDeleteConnections ? edges.filter((edge) => graphEdgesById.has(edge.id)) : []
  }), [canDeleteConnections, graphEdgesById]);
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
  // Standing down while an edge is selected keeps that press meaning "remove the
  // connection" — the library's handler runs for the same keystroke, and letting
  // both act would take the node behind the connection with it.
  React.useEffect(() => {
    if (!canDeleteNodes) return undefined;
    const handleDeleteKey = (event: KeyboardEvent) => {
      if (event.key !== "Delete" && event.key !== "Backspace") return;
      if (isTextEntryTarget(event.target)) return;
      if (selectedEdgeIds.size > 0) return;
      if (!selectedNodeId) return;
      event.preventDefault();
      onDeleteNode?.(selectedNodeId);
    };
    window.addEventListener("keydown", handleDeleteKey);
    return () => window.removeEventListener("keydown", handleDeleteKey);
  }, [canDeleteNodes, onDeleteNode, selectedEdgeIds, selectedNodeId]);
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
        graph.nodes,
        canvasGraphEdges,
        runtime,
        selectedNodeId,
        canDragNodes,
        canConnect,
        handleSelectNode,
        hasNodeContextMenu ? openNodeMenuFromKey : undefined,
        nodeNotes,
        hasNodeAction ? handleNodeAction : undefined,
        nodeCategories,
        nodeSummaries,
        hasLocateParameter ? handleLocateParameter : undefined,
        nodeMetaNotes
      )
    );
    previousFlowNodes.current = next;
    return next;
  }, [graph.nodes, canvasGraphEdges, runtime, selectedNodeId, canDragNodes, canConnect, handleSelectNode, hasNodeContextMenu, openNodeMenuFromKey, nodeNotes, nodeMetaNotes, hasNodeAction, handleNodeAction, nodeCategories, nodeSummaries, hasLocateParameter, handleLocateParameter]);
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
            nodesDraggable={canDragNodes}
            nodesConnectable={canConnect}
            edgesFocusable={canDeleteConnections}
            nodesFocusable={false}
            elementsSelectable={canDeleteConnections}
            connectOnClick={canConnect}
            deleteKeyCode={canDeleteConnections ? ["Backspace", "Delete"] : null}
            onConnect={canConnect ? handleConnect : undefined}
            onEdgesChange={canDeleteConnections ? handleEdgesChange : undefined}
            onEdgesDelete={canDeleteConnections ? handleEdgesDelete : undefined}
            onBeforeDelete={canDeleteConnections ? handleBeforeDelete : undefined}
            isValidConnection={canConnect ? isValidConnection : undefined}
            aria-label={`${title} canvas`}
            onNodeDragStop={canDragNodes ? (_, node) => {
              onNodePositionChange?.(node.id, node.position);
            } : undefined}
            // Bottom-left is the zoom bar's, bottom-right the minimap's.
            attributionPosition="bottom-center"
          >
            <InitialViewFitter flow={flowInstance} disabled={defaultViewport !== undefined} canvas={canvasRef} />
            <LodScaleVariables target={canvasRef} />
            {/* §6.2: a 16px dot grid in the canvas-dot colour, not the old
                minor/major line grid. */}
            <Background color="var(--xflow-canvas-dot)" gap={16} size={1} />
            {rulersDrawn ? <CanvasRulers selectedNodeId={selectedNodeId} /> : null}
            {canvasFloat ? (
              <div className="xflow-preview-canvas-float" {...{ [FLOAT_ATTRIBUTE]: "" }}>
                {canvasFloat}
              </div>
            ) : null}
            {canvasFloatEnd ? (
              <div className="xflow-preview-canvas-float xflow-preview-canvas-float--end" {...{ [FLOAT_ATTRIBUTE]: "" }}>
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
                pannable
                zoomable
                style={MINIMAP_SIZE}
                maskColor="var(--xflow-preview-minimap-mask)"
                maskStrokeColor="var(--xflow-preview-minimap-view)"
                maskStrokeWidth={1.5}
                onClick={handleMiniMapClick}
                nodeColor={miniMapNodeColor}
                nodeStrokeColor="var(--xflow-preview-node-border)"
                nodeStrokeWidth={2}
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
