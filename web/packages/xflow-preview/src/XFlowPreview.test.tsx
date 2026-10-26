import * as React from "react";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  edgeVariant,
  nodeStatusMeta,
  workflowStatusMeta,
  type ExecutionStatusKey,
  type NodeStatus,
  type RuntimeSnapshot,
  type WorkflowDef,
  type WorkflowEditorSection,
  type WorkflowStatus
} from "@xflow/core";
import { XFlowPreview, READABLE_FIT_ZOOM, readableNodeType } from "./index";
import { LOD_ZOOM } from "./lod";

const reactFlowMock = vi.hoisted(() => {
  // A stand-in for xyflow's store: just the transform the card reads, a way to
  // change zoom, and subscribers. `cardRenders` counts calls of the mocked
  // `useStore`, which only `PreviewNode` makes, so it is exactly the number of
  // card renders without a test switch in production code.
  type FlowState = {
    transform: [number, number, number];
    width?: number;
    height?: number;
    nodeLookup?: Map<string, unknown>;
    multiSelectionActive?: boolean;
    nodesSelectionActive?: boolean;
    /** The store action a pane click runs after the owner callback. */
    resetSelectedElements?: () => void;
  };
  const flowStore = {
    state: { transform: [0, 0, 1], resetSelectedElements: vi.fn() } as FlowState,
    listeners: new Set<() => void>(),
    cardRenders: 0,
    setZoom(zoom: number): void {
      flowStore.setState({ transform: [0, 0, zoom] });
    },
    /** Merges into the state, as xyflow's setState does, and notifies. */
    setState(partial: Partial<FlowState>): void {
      flowStore.state = { ...flowStore.state, ...partial };
      flowStore.listeners.forEach((listener) => listener());
    },
    reset(): void {
      flowStore.state = { transform: [0, 0, 1], resetSelectedElements: vi.fn() };
      flowStore.cardRenders = 0;
    }
  };
  return {
    latestProps: undefined as Record<string, unknown> | undefined,
    // What the mocked MiniMap last received; its `nodeColor` is the swatch rule.
    miniMapProps: undefined as { nodeColor?: (node: unknown) => string } | undefined,
    fitBounds: vi.fn<(bounds: unknown, options?: { padding?: number }) => Promise<boolean>>(),
    getNodesBounds: vi.fn(() => ({ x: 0, y: 0, width: 500, height: 200 })),
    zoomTo: vi.fn<(zoom: number) => Promise<boolean>>(),
    flowStore
  };
});

vi.mock("@xyflow/react", async () => {
  const React = await import("react");
  const { flowStore } = reactFlowMock;

  const subscribeToFlowStore = (listener: () => void): (() => void) => {
    flowStore.listeners.add(listener);
    return () => {
      flowStore.listeners.delete(listener);
    };
  };
  // Stable object, like xyflow's: effects keyed on it must not re-subscribe.
  const flowStoreApi = {
    getState: () => flowStore.state,
    setState: (partial: Partial<typeof flowStore.state>) => flowStore.setState(partial),
    subscribe: (listener: (state: typeof flowStore.state) => void): (() => void) => {
      const notify = (): void => listener(flowStore.state);
      flowStore.listeners.add(notify);
      return () => {
        flowStore.listeners.delete(notify);
      };
    }
  };

  const Handle = ({
    id,
    type,
    className,
    style,
    isConnectable,
    isConnectableStart: _isConnectableStart,
    isConnectableEnd: _isConnectableEnd,
    ...props
  }: Record<string, unknown>) => React.createElement("div", {
    ...props,
    className,
    style: style as React.CSSProperties | undefined,
    "data-connectable": String(isConnectable),
    "data-handle-id": id,
    "data-handle-type": type
  });

  const ReactFlow = ({
    nodes = [],
    edges = [],
    nodeTypes = {},
    children,
    ...props
  }: Record<string, unknown>) => {
    const flowNodes = nodes as Array<Record<string, unknown>>;
    const flowEdges = edges as Array<Record<string, unknown>>;
    const flowNodeTypes = nodeTypes as Record<string, React.ComponentType<Record<string, unknown>>>;
    const flowProps = props as MockFlowProps;
    reactFlowMock.latestProps = { nodes: flowNodes, edges: flowEdges, nodeTypes: flowNodeTypes, ...flowProps };

    return React.createElement("div", {
      className: "react-flow",
      "aria-label": flowProps["aria-label"]
    }, [
      // The real canvas renders cards and connections inside its pane; the
      // wrapper's empty-canvas click replay keys on that target. Panels and the
      // owner's floats stay outside it, as in xyflow.
      React.createElement("div", { className: "react-flow__pane", key: "pane" }, [
        ...flowNodes.map((node) => {
          const NodeComponent = flowNodeTypes[node.type as string];
          return NodeComponent ? React.createElement(NodeComponent, { key: node.id as string, ...node }) : null;
        }),
        ...flowEdges.map((edge) => React.createElement("button", {
          key: edge.id as string,
          type: "button",
          role: edge.focusable ? "button" : "img",
          "aria-label": edge.ariaLabel as string | undefined,
          "data-edge-id": edge.id as string,
          tabIndex: edge.focusable ? 0 : undefined,
          onClick: () => flowProps.onEdgesChange?.([{ id: edge.id as string, type: "select", selected: true }]),
          onKeyDown: (event: React.KeyboardEvent<HTMLButtonElement>) => {
            const deleteKeyCode = flowProps.deleteKeyCode;
            const deleteKeys = Array.isArray(deleteKeyCode) ? deleteKeyCode : [deleteKeyCode];
            if (edge.selected && deleteKeys.includes(event.key)) {
              flowProps.onEdgesDelete?.([edge]);
            }
          }
        }))
      ]),
      children as React.ReactNode
    ]);
  };

  return {
    // The grid props land on the DOM so a test can read what the canvas asked
    // for; in jsdom xyflow's own pattern svg is never measurable.
    Background: ({ color, gap, size }: Record<string, unknown>) => React.createElement("div", {
      className: "react-flow__background",
      "data-color": String(color),
      "data-gap": String(gap),
      "data-size": String(size)
    }),
    ControlButton: ({ children, ...props }: { children?: React.ReactNode } & Record<string, unknown>) => (
      React.createElement("button", props, children)
    ),
    Controls: ({ children }: { children?: React.ReactNode }) => React.createElement(React.Fragment, null, children),
    Handle,
    MiniMap: (props: Record<string, unknown>) => {
      reactFlowMock.miniMapProps = props as { nodeColor?: (node: unknown) => string };
      return React.createElement("div", { className: "react-flow__minimap" });
    },
    Position: { Left: "left", Right: "right" },
    SelectionMode: { Partial: "partial", Full: "full" },
    ReactFlow,
    ViewportPortal: ({ children }: { children?: React.ReactNode }) =>
      React.createElement("div", { className: "react-flow__viewport-portal" }, children),
    useReactFlow: () => ({
      fitBounds: reactFlowMock.fitBounds,
      getNodes: () => [{ id: "source-id" }],
      getNodesBounds: reactFlowMock.getNodesBounds,
      zoomTo: reactFlowMock.zoomTo
    }),
    // `PreviewNode` is the only caller, so each call is one card render. A
    // boolean selector snapshot keeps useSyncExternalStore from re-rendering the
    // card on zoom steps that leave the selected value unchanged, as xyflow does.
    useStore: <Selected,>(selector: (state: typeof flowStore.state) => Selected): Selected => {
      flowStore.cardRenders += 1;
      return React.useSyncExternalStore(subscribeToFlowStore, () => selector(flowStore.state));
    },
    useStoreApi: () => flowStoreApi
  };
});

interface MockFlowEdge extends Record<string, unknown> {
  id: string;
  selected?: boolean;
}

interface MockFlowNode {
  id: string;
  position?: { x: number; y: number };
  measured?: { width?: number; height?: number };
  width?: number;
  height?: number;
}

interface MockFlowInstance {
  fitBounds: (bounds: unknown, options?: { padding?: number }) => Promise<boolean>;
  getNodes: () => MockFlowNode[];
  getNodesBounds: (nodes: Array<{ id: string }>) => unknown;
  getViewport?: () => { x: number; y: number; zoom: number };
  screenToFlowPosition?: (clientPosition: { x: number; y: number }) => { x: number; y: number };
  getZoom?: () => number;
  setCenter?: (x: number, y: number, options?: { zoom?: number }) => void;
}

interface MockFlowProps extends Record<string, unknown> {
  edges: MockFlowEdge[];
  nodes: Array<Record<string, unknown>>;
  nodesDraggable?: boolean;
  nodesConnectable?: boolean;
  edgesFocusable?: boolean;
  elementsSelectable?: boolean;
  connectOnClick?: boolean;
  deleteKeyCode?: string[] | null;
  selectionOnDrag?: boolean;
  selectionMode?: string;
  multiSelectionKeyCode?: string[];
  panOnDrag?: boolean | number[];
  onNodesChange?: (changes: Array<{ id: string; type: "select"; selected: boolean }>) => void;
  onInit?: (instance: MockFlowInstance) => void;
  onConnect?: (connection: {
    source: string;
    sourceHandle: string | null;
    target: string;
    targetHandle: string | null;
  }) => void;
  onEdgesChange?: (changes: Array<{ id: string; type: "select"; selected: boolean }>) => void;
  onEdgesDelete?: (edges: MockFlowEdge[]) => void;
  onNodeContextMenu?: (event: MockMouseEvent, node: { id: string }) => void;
  onNodeDragStop?: (
    event: MockMouseEvent,
    node: { id: string; position: { x: number; y: number } },
    nodes: Array<{ id: string; position: { x: number; y: number } }>
  ) => void;
  onPaneContextMenu?: (event: MockMouseEvent) => void;
  onBeforeDelete?: (elements: {
    nodes: Array<Record<string, unknown>>;
    edges: MockFlowEdge[];
  }) => Promise<{ nodes: Array<Record<string, unknown>>; edges: MockFlowEdge[] }>;
  isValidConnection?: (connection: {
    source: string;
    sourceHandle?: string | null;
    target: string;
    targetHandle?: string | null;
  }) => boolean;
}

interface MockMouseEvent {
  clientX: number;
  clientY: number;
  preventDefault: () => void;
}

function mouseEventStub(clientX: number, clientY: number): MockMouseEvent {
  return { clientX, clientY, preventDefault: vi.fn() };
}

const contextMenuWorkflow = {
  name: "Menu flow",
  nodes: [
    { id: "start-id", name: "start", type: "xflow.start" },
    { id: "http-id", name: "http_1", type: "xflow.http" }
  ],
  connections: {
    start: {
      main: [{ node: "http_1" }]
    }
  }
} satisfies WorkflowDef;

function latestFlowProps(): MockFlowProps {
  expect(reactFlowMock.latestProps).toBeTruthy();
  return reactFlowMock.latestProps as MockFlowProps;
}

interface DragTransferStub {
  getData: ReturnType<typeof vi.fn>;
  setData: ReturnType<typeof vi.fn>;
  dropEffect: string;
  effectAllowed: string;
}

function dragTransferStub(type: string): DragTransferStub {
  return { getData: vi.fn(() => type), setData: vi.fn(), dropEffect: "", effectAllowed: "" };
}

// jsdom implements neither DragEvent nor DataTransfer, so a plain Event has to
// carry the drag payload and the pointer position by hand. `fireEvent.drop`
// cannot: it builds a `new Event(name, init)`, and Event ignores clientX/Y.
function fireCanvasDrop(
  canvas: Element,
  dataTransfer: DragTransferStub,
  clientX: number,
  clientY: number
): void {
  const event = new Event("drop", { bubbles: true, cancelable: true });
  Object.defineProperty(event, "dataTransfer", { value: dataTransfer });
  Object.defineProperty(event, "clientX", { value: clientX });
  Object.defineProperty(event, "clientY", { value: clientY });
  fireEvent(canvas, event);
}

const portWorkflow = {
  id: "wf-ports",
  name: "Port flow",
  nodes: [
    { id: "source-id", name: "source", type: "xflow.function" },
    {
      id: "target-id",
      name: "target",
      type: "xflow.http",
      inputs: [{ name: "payload", required: true }]
    }
  ],
  connections: {
    source: {
      success: [{ node: "target", input: "payload" }],
      dependency: {
        type: "dependency",
        targets: [{ node: "target", input: "main" }]
      }
    }
  }
} satisfies WorkflowDef;

afterEach(() => {
  cleanup();
  reactFlowMock.latestProps = undefined;
  reactFlowMock.miniMapProps = undefined;
  reactFlowMock.flowStore.reset();
  vi.clearAllMocks();
  vi.unstubAllGlobals();
});

describe("XFlowPreview", () => {
  it("passes defaultViewport through to ReactFlow and disables auto-fit", () => {
    const frameCallbacks: FrameRequestCallback[] = [];
    const requestFrame = vi.fn((callback: FrameRequestCallback) => {
      frameCallbacks.push(callback);
      return frameCallbacks.length;
    });
    vi.stubGlobal("requestAnimationFrame", requestFrame);
    vi.stubGlobal("cancelAnimationFrame", vi.fn());

    render(
      <XFlowPreview
        workflow={{
          id: "wf-viewport",
          name: "Viewport flow",
          nodes: [{ name: "start", type: "xflow.start" }],
          connections: {}
        }}
        defaultViewport={{ x: 10, y: 20, zoom: 1.5 }}
      />
    );

    const flowProps = latestFlowProps();
    expect(flowProps.defaultViewport).toEqual({ x: 10, y: 20, zoom: 1.5 });

    act(() => {
      flowProps.onInit?.({
        fitBounds: reactFlowMock.fitBounds,
        getNodes: () => [{ id: "source-id" }],
        getNodesBounds: reactFlowMock.getNodesBounds
      });
    });

    // A defaultViewport means InitialViewFitter's effect must not even
    // schedule a retry frame (ADR-D4 §2.3): the measured auto-fit must never
    // run, or it would clobber the restored pan/zoom.
    expect(requestFrame).not.toHaveBeenCalled();
    expect(reactFlowMock.fitBounds).not.toHaveBeenCalled();
  });

  it("auto-fits when no defaultViewport is supplied", () => {
    const frameCallbacks: FrameRequestCallback[] = [];
    const requestFrame = vi.fn((callback: FrameRequestCallback) => {
      frameCallbacks.push(callback);
      return frameCallbacks.length;
    });
    vi.stubGlobal("requestAnimationFrame", requestFrame);
    vi.stubGlobal("cancelAnimationFrame", vi.fn());
    reactFlowMock.fitBounds.mockResolvedValue(true);
    reactFlowMock.getNodesBounds.mockReturnValue({ x: 0, y: 0, width: 500, height: 200 });

    render(
      <XFlowPreview
        workflow={{
          id: "wf-no-viewport",
          name: "No viewport flow",
          nodes: [{ name: "start", type: "xflow.start" }],
          connections: {}
        }}
      />
    );

    const flowProps = latestFlowProps();
    expect(flowProps.defaultViewport).toBeUndefined();

    act(() => {
      flowProps.onInit?.({
        fitBounds: reactFlowMock.fitBounds,
        getNodes: () => [{ id: "source-id" }],
        getNodesBounds: reactFlowMock.getNodesBounds
      });
    });

    expect(requestFrame).toHaveBeenCalledTimes(1);
    act(() => {
      frameCallbacks.shift()?.(0);
    });

    // No defaultViewport: InitialViewFitter runs its measured auto-fit on
    // the first frame, proving the two branches are genuinely distinct.
    expect(reactFlowMock.fitBounds).toHaveBeenCalledTimes(1);
  });

  it("refits while the cards re-measure after the initial fit, then stops", () => {
    // A fit that crosses the LOD zoom resizes every card a frame later; the fit
    // follows the new bounds instead of leaving content under the chrome.
    const frameCallbacks: FrameRequestCallback[] = [];
    vi.stubGlobal("requestAnimationFrame", vi.fn((callback: FrameRequestCallback) => {
      frameCallbacks.push(callback);
      return frameCallbacks.length;
    }));
    vi.stubGlobal("cancelAnimationFrame", vi.fn());
    reactFlowMock.fitBounds.mockResolvedValue(true);
    const measured = { x: 0, y: 0, width: 500, height: 200 };
    reactFlowMock.getNodesBounds.mockImplementation(() => ({ ...measured }));

    render(
      <XFlowPreview
        workflow={{ id: "wf-settle", name: "Settle flow", nodes: [{ name: "start", type: "xflow.start" }], connections: {} }}
      />
    );
    act(() => {
      latestFlowProps().onInit?.({
        fitBounds: reactFlowMock.fitBounds,
        getNodes: () => [{ id: "source-id" }],
        getNodesBounds: reactFlowMock.getNodesBounds
      });
    });
    const runFrame = () => act(() => {
      frameCallbacks.shift()?.(0);
    });

    runFrame();
    expect(reactFlowMock.fitBounds).toHaveBeenCalledTimes(1);
    measured.height = 260;
    runFrame();
    expect(reactFlowMock.fitBounds).toHaveBeenCalledTimes(2);
    expect(reactFlowMock.fitBounds).toHaveBeenLastCalledWith(
      { x: 0, y: 0, width: 500, height: 260 },
      expect.objectContaining({ padding: expect.any(Number) })
    );
    // Stable bounds: a few quiet frames, then no more frames are asked for.
    for (let frame = 0; frame < 8 && frameCallbacks.length > 0; frame += 1) runFrame();
    expect(frameCallbacks).toHaveLength(0);
    expect(reactFlowMock.fitBounds).toHaveBeenCalledTimes(2);
  });

  it("reports the settled viewport through onMoveEnd, not before", () => {
    const onViewportChange = vi.fn();
    render(
      <XFlowPreview
        workflow={{
          id: "wf-viewport-report",
          name: "Viewport report flow",
          nodes: [{ name: "start", type: "xflow.start" }],
          connections: {}
        }}
        onViewportChange={onViewportChange}
      />
    );

    const flowProps = latestFlowProps();
    expect(flowProps.onMoveEnd).toBeTypeOf("function");
    expect(onViewportChange).not.toHaveBeenCalled();

    act(() => {
      (flowProps.onMoveEnd as (event: unknown, viewport: { x: number; y: number; zoom: number }) => void)(
        undefined,
        { x: 42, y: -7, zoom: 0.8 }
      );
    });

    expect(onViewportChange).toHaveBeenCalledTimes(1);
    expect(onViewportChange).toHaveBeenCalledWith({ x: 42, y: -7, zoom: 0.8 });
  });

  it("wires no onMoveEnd handler when onViewportChange is not supplied", () => {
    render(
      <XFlowPreview
        workflow={{
          id: "wf-no-callback",
          name: "No callback flow",
          nodes: [{ name: "start", type: "xflow.start" }],
          connections: {}
        }}
      />
    );

    const flowProps = latestFlowProps();
    expect(flowProps.onMoveEnd).toBeUndefined();
  });

  it("renders workflow nodes with runtime status", () => {
    const { container } = render(
      <XFlowPreview
        workflow={{
          id: "wf-1",
          name: "Payment flow",
          nodes: [
            { name: "start", type: "xflow.start" },
            {
              name: "charge",
              type: "xflow.http",
              notes: "Calls payment gateway",
              inputs: [{ name: "main", required: true }]
            },
            { name: "approval", type: "xflow.wait", disabled: true }
          ],
          connections: {
            start: {
              main: [{ node: "charge" }]
            },
            charge: {
              main: [{ node: "approval" }]
            }
          }
        }}
        runtime={{
          status: "running",
          nodes: {
            start: { status: "success", durationMs: 12 },
            charge: { status: "running", attempts: 2 },
            approval: { status: "skipped" }
          }
        }}
      />
    );

    expect(screen.getByRole("region", { name: "Payment flow preview" })).toBeTruthy();
    expect(container.querySelector(".react-flow")).toBeTruthy();
    expect(screen.getByText("Payment flow")).toBeTruthy();
    // Chip text and summary pills both come from core's status table. The bare
    // label also names the run in the page header, so scope the card chip by node.
    expect(container.querySelector('[data-node-id="charge"] .xflow-preview-node__status')?.textContent)
      .toBe(nodeStatusMeta.running.label);
    expect(screen.getAllByText(nodeStatusMeta.running.label)).toHaveLength(2);
    expect(screen.getByText("3 nodes")).toBeTruthy();
    expect(screen.getByText(`1 ${nodeStatusMeta.running.label}`)).toBeTruthy();
    expect(screen.getByText(`1 ${nodeStatusMeta.success.label}`)).toBeTruthy();
    expect(screen.getByText(`1 ${nodeStatusMeta.skipped.label}`)).toBeTruthy();
    expect(screen.getAllByText("start")).toHaveLength(2);
    expect(screen.getByText(nodeStatusMeta.success.label)).toBeTruthy();
    expect(screen.getAllByText("charge")).toHaveLength(3);
    expect(screen.getByLabelText("xflow.http icon")).toBeTruthy();
    expect(container.querySelector('[data-node-id="charge"] .xflow-preview-node__type')?.textContent).toBe("HTTP");
    expect(container.querySelector('[data-node-id="approval"]')?.getAttribute("data-disabled")).toBe("true");
    // A disabled node speaks through its status chip and its own badge.
    expect(container.querySelector('[data-node-id="approval"] [data-badge="disabled"]')?.textContent).toContain("已禁用");
    expect(container.querySelector('[data-node-id="approval"] .xflow-preview-node__status')?.textContent)
      .toBe(nodeStatusMeta.skipped.label);
    // The attempt count rides in the card's badge strip.
    expect(container.querySelector('[data-node-id="charge"] [data-badge="attempts"]')?.textContent)
      .toBe("第 2 次");
    // The canvas edge and the sidebar list row carry the same label now, so the
    // exact-name check is scoped to the edge role.
    expect(screen.getByRole("img", { name: "start main 连接到 charge main" })).toBeTruthy();

    const chargeNode = container.querySelector('[aria-label="charge node running"]');
    expect(chargeNode).toBeTruthy();
    fireEvent.click(chargeNode!);

    expect(screen.getByRole("region", { name: "Selected node details" })).toBeTruthy();
    expect(screen.getByText("Calls payment gateway")).toBeTruthy();
    expect(screen.getByText("Inputs 1")).toBeTruthy();
    expect(screen.getByText("Attempts 2")).toBeTruthy();
  });

  it("keeps a disabled node the run has not reported as disabled, not pending", () => {
    const { container } = render(
      <XFlowPreview
        minimap
        workflow={{
          id: "wf-disabled",
          name: "Disabled flow",
          nodes: [
            { name: "start", type: "xflow.start" },
            { name: "approval", type: "xflow.wait", disabled: true },
            { name: "notify", type: "xflow.http" }
          ],
          connections: { start: { main: [{ node: "approval" }, { node: "notify" }] } }
        }}
        runtime={{ status: "running", nodes: { start: { status: "success" } } }}
      />
    );

    // The engine never queues a disabled node, so the run's silence about it is not
    // "pending": the card keeps the edit-mode face, and the label follows the chip.
    const disabled = container.querySelector('[data-node-id="approval"]');
    expect(disabled?.getAttribute("data-status")).toBe("disabled");
    expect(disabled?.querySelector(".xflow-preview-node__status")?.textContent)
      .toBe(nodeStatusMeta.disabled.label);
    expect(container.querySelector('[aria-label="approval node disabled"]')).toBeTruthy();
    // An enabled node the run has not reached is still queued.
    expect(container.querySelector('[data-node-id="notify"]')?.getAttribute("data-status")).toBe("pending");
    // The minimap swatch speaks the disabled tone, not the pending one's.
    const flowNode = latestFlowProps().nodes!.find((node) => node.id === "approval");
    expect(reactFlowMock.miniMapProps?.nodeColor?.(flowNode)).toContain(`--xflow-${nodeStatusMeta.disabled.tone}`);
    // The inbound edge reads the same status: the disabled node's own stroke, while
    // the edge into the still-queued node keeps the resting line.
    const edges = latestFlowProps().edges;
    expect(edges.find((edge) => edge.target === "approval")?.className)
      .toBe(`xflow-preview-edge--${edgeVariant("disabled")}`);
    expect(edges.find((edge) => edge.target === "notify")?.className)
      .toBe(`xflow-preview-edge--${edgeVariant("pending")}`);
  });

  it("shows the status the run reports for a disabled node", () => {
    const { container } = render(
      <XFlowPreview
        workflow={{
          id: "wf-disabled-skipped",
          name: "Disabled skipped flow",
          nodes: [{ name: "approval", type: "xflow.wait", disabled: true }]
        }}
        runtime={{ status: "success", nodes: { approval: { status: "skipped" } } }}
      />
    );

    const card = container.querySelector('[data-node-id="approval"]');
    expect(card?.getAttribute("data-status")).toBe("skipped");
    expect(card?.querySelector(".xflow-preview-node__status")?.textContent).toBe(nodeStatusMeta.skipped.label);
    expect(container.querySelector('[aria-label="approval node skipped"]')).toBeTruthy();
  });

  it("draws the pin badge on a pinned node only", () => {
    const { container } = render(
      <XFlowPreview
        workflow={{
          id: "wf-pin",
          name: "Pin flow",
          nodes: [
            { name: "fixture", type: "xflow.http" },
            { name: "live", type: "xflow.http" }
          ]
        }}
        runtime={{
          status: "success",
          nodes: { fixture: { status: "pinned" }, live: { status: "success" } }
        }}
      />
    );

    const pinned = container.querySelector('[data-node-id="fixture"]');
    expect(pinned?.getAttribute("data-status")).toBe("pinned");
    expect(pinned?.querySelector(".xflow-preview-node__pin")).toBeTruthy();
    // A success card is the same tone but carries no badge.
    expect(container.querySelector('[data-node-id="live"] .xflow-preview-node__pin')).toBeNull();
  });

  it("labels the committing and expired node statuses", () => {
    const { container } = render(
      <XFlowPreview
        workflow={{
          id: "wf-commit",
          name: "Commit flow",
          nodes: [
            { name: "ingest", type: "xflow.http" },
            { name: "archive", type: "xflow.kafka" }
          ]
        }}
        runtime={{
          status: "running",
          nodes: {
            ingest: { status: "committing" },
            archive: { status: "expired" }
          }
        }}
      />
    );

    expect(container.querySelector('[aria-label="ingest node committing"]')).toBeTruthy();
    expect(container.querySelector('[aria-label="archive node expired"]')).toBeTruthy();
    expect(screen.getByText(nodeStatusMeta.committing.label)).toBeTruthy();
    expect(screen.getByText(nodeStatusMeta.expired.label)).toBeTruthy();
    // Both statuses are shown by the run summary too; a status missing from its
    // ordering list silently drops every node carrying it.
    expect(screen.getByText(`1 ${nodeStatusMeta.committing.label}`)).toBeTruthy();
    expect(screen.getByText(`1 ${nodeStatusMeta.expired.label}`)).toBeTruthy();
  });

  it("counts every node status in the run summary", () => {
    // The compile-time guard lives on summaryStatusOrder in index.tsx; this is
    // its behavioural half: every status must actually reach a pill.
    const nodeCountsByStatus: Record<NodeStatus, number> = {
      pending: 1,
      running: 1,
      committing: 1,
      success: 1,
      failed: 1,
      skipped: 1,
      pinned: 1,
      continued: 1,
      suspended: 1,
      waiting: 1,
      expired: 1,
      canceled: 1
    };
    const statuses = Object.keys(nodeCountsByStatus) as NodeStatus[];

    render(
      <XFlowPreview
        workflow={{
          id: "wf-every-status",
          name: "Every status",
          nodes: statuses.map((status) => ({ name: status, type: "xflow.http" }))
        }}
        runtime={{
          status: "failed",
          nodes: Object.fromEntries(statuses.map((status) => [status, { status }]))
        }}
      />
    );

    expect(screen.getByText(`${statuses.length} nodes`)).toBeTruthy();
    for (const status of statuses) {
      expect(screen.getByText(`1 ${nodeStatusMeta[status].label}`)).toBeTruthy();
    }
  });

  it("counts a disabled node the run has not reported as disabled, not pending", () => {
    render(
      <XFlowPreview
        workflow={{
          id: "wf-summary-disabled",
          name: "Summary disabled",
          nodes: [
            { name: "start", type: "xflow.start" },
            { name: "approval", type: "xflow.wait", disabled: true },
            { name: "notify", type: "xflow.http" }
          ]
        }}
        runtime={{ status: "running", nodes: { start: { status: "success" } } }}
      />
    );

    // Same face as the card: the silent disabled node is disabled, and only the
    // enabled node the run has not reached is still queued.
    expect(screen.getByText(`1 ${nodeStatusMeta.disabled.label}`)).toBeTruthy();
    expect(screen.getByText(`1 ${nodeStatusMeta.pending.label}`)).toBeTruthy();
    expect(screen.getByText(`1 ${nodeStatusMeta.success.label}`)).toBeTruthy();
    expect(screen.queryByText(`2 ${nodeStatusMeta.pending.label}`)).toBeNull();
  });

  it("retries the initial canvas fit until controlled nodes have measured dimensions", () => {
    const frameCallbacks: FrameRequestCallback[] = [];
    const requestFrame = vi.fn((callback: FrameRequestCallback) => {
      frameCallbacks.push(callback);
      return frameCallbacks.length;
    });
    vi.stubGlobal("requestAnimationFrame", requestFrame);
    vi.stubGlobal("cancelAnimationFrame", vi.fn());
    reactFlowMock.fitBounds.mockResolvedValue(true);
    reactFlowMock.getNodesBounds
      .mockReturnValueOnce({ x: 0, y: 0, width: 0, height: 0 })
      .mockReturnValue({ x: 0, y: 0, width: 500, height: 200 });
    const { rerender } = render(<XFlowPreview workflow={portWorkflow} />);
    const flow = latestFlowProps();

    act(() => {
      flow.onInit?.({
        fitBounds: reactFlowMock.fitBounds,
        getNodes: () => [{ id: "source-id" }],
        getNodesBounds: reactFlowMock.getNodesBounds
      });
    });

    expect(requestFrame).toHaveBeenCalledTimes(1);
    expect(reactFlowMock.fitBounds).not.toHaveBeenCalled();

    act(() => {
      frameCallbacks.shift()?.(0);
    });

    expect(reactFlowMock.getNodesBounds).toHaveBeenCalledWith([{ id: "source-id" }]);
    expect(reactFlowMock.fitBounds).not.toHaveBeenCalled();
    expect(requestFrame).toHaveBeenCalledTimes(2);

    act(() => {
      frameCallbacks.shift()?.(16);
    });

    expect(reactFlowMock.fitBounds).toHaveBeenCalledTimes(1);
    expect(reactFlowMock.fitBounds).toHaveBeenCalledWith(
      { x: 0, y: 0, width: 500, height: 200 },
      { padding: 0.18 }
    );

    rerender(
      <XFlowPreview
        workflow={{
          ...portWorkflow,
          nodes: portWorkflow.nodes.map((node) => (
            node.name === "source" ? { ...node, position: { x: 720, y: 180 } } : node
          ))
        }}
      />
    );

    expect(reactFlowMock.fitBounds).toHaveBeenCalledTimes(1);
  });

  it("fits the newly selected controlled node without replacing the initial view", () => {
    const frameCallbacks: FrameRequestCallback[] = [];
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => {
      frameCallbacks.push(callback);
      return frameCallbacks.length;
    });
    vi.stubGlobal("cancelAnimationFrame", vi.fn());

    const sourceNode = { id: "source-id" };
    const targetNode = { id: "target-id" };
    const sourceBounds = { x: 24, y: 48, width: 160, height: 64 };
    const targetBounds = { x: 680, y: 240, width: 240, height: 96 };
    const fitBounds = vi
      .fn<(bounds: unknown, options?: { padding?: number }) => Promise<boolean>>()
      .mockResolvedValue(true);
    const getNodesBounds = vi.fn((nodes: Array<{ id: string }>) => (
      nodes[0]?.id === targetNode.id ? targetBounds : sourceBounds
    ));
    const flowInstance: MockFlowInstance = {
      fitBounds,
      getNodes: vi.fn(() => [sourceNode, targetNode]),
      getNodesBounds,
      getViewport: () => ({ x: 0, y: 0, zoom: 1 })
    };
    const { rerender } = render(
      <XFlowPreview workflow={portWorkflow} selectedNodeId={sourceNode.id} />
    );

    act(() => {
      latestFlowProps().onInit?.(flowInstance);
    });

    // Initial fitting belongs to InitialViewFitter, so an initial selection must
    // not override it before measured bounds are available.
    expect(frameCallbacks).toHaveLength(1);
    expect(getNodesBounds).not.toHaveBeenCalled();
    expect(fitBounds).not.toHaveBeenCalled();

    rerender(<XFlowPreview workflow={portWorkflow} selectedNodeId={targetNode.id} />);

    expect(getNodesBounds).toHaveBeenLastCalledWith([targetNode]);
    expect(fitBounds).toHaveBeenLastCalledWith(targetBounds, { padding: 0.28 });

    rerender(<XFlowPreview workflow={portWorkflow} selectedNodeId="missing-node" />);

    expect(fitBounds).toHaveBeenCalledTimes(1);

    rerender(<XFlowPreview workflow={portWorkflow} selectedNodeId={sourceNode.id} />);

    expect(getNodesBounds).toHaveBeenLastCalledWith([sourceNode]);
    expect(fitBounds).toHaveBeenLastCalledWith(sourceBounds, { padding: 0.28 });
    expect(fitBounds).toHaveBeenCalledTimes(2);
  });

  it("waits for a newly selected node to be measured before fitting it", () => {
    const frameCallbacks: FrameRequestCallback[] = [];
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => {
      frameCallbacks.push(callback);
      return frameCallbacks.length;
    });
    vi.stubGlobal("cancelAnimationFrame", vi.fn());

    const targetNode = { id: "target-id" };
    const measuredBounds = { x: 680, y: 240, width: 240, height: 96 };
    const fitBounds = vi
      .fn<(bounds: unknown, options?: { padding?: number }) => Promise<boolean>>()
      .mockResolvedValue(true);
    // The first read is the unmeasured 0x0 box a just-appended node reports.
    const getNodesBounds = vi
      .fn((_nodes: Array<{ id: string }>) => measuredBounds)
      .mockReturnValueOnce({ x: 680, y: 240, width: 0, height: 0 });
    const flowInstance: MockFlowInstance = {
      fitBounds,
      getNodes: vi.fn(() => [{ id: "source-id" }, targetNode]),
      getNodesBounds,
      getViewport: () => ({ x: 0, y: 0, zoom: 1 })
    };
    const { rerender } = render(<XFlowPreview workflow={portWorkflow} defaultViewport={{ x: 0, y: 0, zoom: 1 }} />);

    act(() => {
      latestFlowProps().onInit?.(flowInstance);
    });
    rerender(
      <XFlowPreview workflow={portWorkflow} defaultViewport={{ x: 0, y: 0, zoom: 1 }} selectedNodeId={targetNode.id} />
    );

    // A 0-size box would clamp the zoom to maxZoom, so it must not be fitted.
    expect(fitBounds).not.toHaveBeenCalled();
    expect(frameCallbacks).toHaveLength(1);

    act(() => {
      frameCallbacks.shift()?.(0);
    });

    expect(fitBounds).toHaveBeenCalledTimes(1);
    expect(fitBounds).toHaveBeenCalledWith(measuredBounds, { padding: 0.28 });
  });

  it("cancels a pending initial fit when a viewport to restore arrives after mount", () => {
    const frameCallbacks = new Map<number, FrameRequestCallback>();
    let nextFrame = 0;
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => {
      nextFrame += 1;
      frameCallbacks.set(nextFrame, callback);
      return nextFrame;
    });
    vi.stubGlobal("cancelAnimationFrame", (frame: number) => {
      frameCallbacks.delete(frame);
    });
    reactFlowMock.fitBounds.mockResolvedValue(true);

    const workflow = {
      id: "wf-late-viewport",
      name: "Late viewport flow",
      nodes: [{ name: "start", type: "xflow.start" }],
      connections: {}
    } satisfies WorkflowDef;
    const { rerender } = render(<XFlowPreview workflow={workflow} />);

    act(() => {
      latestFlowProps().onInit?.({
        fitBounds: reactFlowMock.fitBounds,
        getNodes: () => [{ id: "source-id" }],
        getNodesBounds: reactFlowMock.getNodesBounds
      });
    });
    expect(frameCallbacks.size).toBe(1);

    rerender(<XFlowPreview workflow={workflow} defaultViewport={{ x: 10, y: 20, zoom: 1.5 }} />);

    act(() => {
      for (const callback of [...frameCallbacks.values()]) callback(0);
    });

    expect(reactFlowMock.fitBounds).not.toHaveBeenCalled();
  });

  it("leaves a selected node alone when it is already on screen", () => {
    vi.stubGlobal("requestAnimationFrame", vi.fn(() => 1));
    vi.stubGlobal("cancelAnimationFrame", vi.fn());
    // jsdom reports a zero-sized box for every element, which would make every
    // node look off screen, so the canvas needs a real box for this to mean anything.
    const rect = {
      x: 0, y: 0, left: 0, top: 0, right: 1000, bottom: 800, width: 1000, height: 800
    } as DOMRect;
    const rectSpy = vi.spyOn(Element.prototype, "getBoundingClientRect").mockReturnValue(rect);

    try {
      const onScreen = { id: "on-screen", position: { x: 120, y: 90 } };
      const offScreen = { id: "off-screen", position: { x: 2400, y: 1800 } };
      const fitBounds = vi
        .fn<(bounds: unknown, options?: { padding?: number }) => Promise<boolean>>()
        .mockResolvedValue(true);
      const flowInstance: MockFlowInstance = {
        fitBounds,
        getNodes: vi.fn(() => [onScreen, offScreen]),
        getNodesBounds: vi.fn(() => ({ x: 0, y: 0, width: 200, height: 100 })),
        getViewport: () => ({ x: 0, y: 0, zoom: 1 })
      };
      const { rerender } = render(<XFlowPreview workflow={portWorkflow} />);

      act(() => {
        latestFlowProps().onInit?.(flowInstance);
      });

      rerender(<XFlowPreview workflow={portWorkflow} selectedNodeId={onScreen.id} />);

      expect(fitBounds).not.toHaveBeenCalled();

      rerender(<XFlowPreview workflow={portWorkflow} selectedNodeId={offScreen.id} />);

      expect(fitBounds).toHaveBeenCalledTimes(1);
    } finally {
      rectSpy.mockRestore();
    }
  });

  it("gives a branch node its declared output handles before anything is wired", () => {
    const { container } = render(
      <XFlowPreview
        workflow={{ name: "Branch", nodes: [{ name: "gate", type: "xflow.if" }, { name: "done", type: "xflow.end" }] }}
        editable
        onConnect={vi.fn()}
      />
    );

    const handles = [...container.querySelectorAll("[data-handle-id]")].map((handle) => ({
      id: handle.getAttribute("data-handle-id"),
      type: handle.getAttribute("data-handle-type")
    }));

    // The branch node's named outputs and nothing else: the end node declares no
    // output at all, so it must not fall back to a main handle.
    expect(handles.filter((handle) => handle.type === "source").map((handle) => handle.id)).toEqual(["true", "false"]);
    // Both nodes take a main input, so two target handles are expected.
    expect(handles.filter((handle) => handle.type === "target").map((handle) => handle.id)).toEqual(["main", "main"]);
  });

  it("fits measured nodes when the zoom bar's fit control is used", () => {
    reactFlowMock.fitBounds.mockResolvedValue(true);
    render(<XFlowPreview workflow={portWorkflow} />);
    act(() => {
      latestFlowProps().onInit?.({
        fitBounds: reactFlowMock.fitBounds,
        getNodes: () => [{ id: "source-id" }],
        getNodesBounds: reactFlowMock.getNodesBounds
      });
    });

    fireEvent.click(screen.getByRole("button", { name: "适应画布" }));

    expect(reactFlowMock.getNodesBounds).toHaveBeenCalledWith([{ id: "source-id" }]);
    expect(reactFlowMock.fitBounds).toHaveBeenCalledWith(
      { x: 0, y: 0, width: 500, height: 200 },
      { padding: 0.18 }
    );
  });

  it("shows an empty state when the workflow has no nodes", () => {
    render(<XFlowPreview workflow={{ name: "Empty flow", nodes: [] }} />);

    expect(screen.getByText("No nodes to preview")).toBeTruthy();
  });

  it("reports a dropped node type at the canvas position for an empty workflow", () => {
    const onDropNode = vi.fn();
    const screenToFlowPosition = vi.fn((clientPosition: { x: number; y: number }) => ({
      x: clientPosition.x - 40,
      y: clientPosition.y - 60
    }));
    const { container } = render(
      <XFlowPreview
        workflow={{ name: "Empty flow", nodes: [] }}
        editable
        onDropNode={onDropNode}
      />
    );

    act(() => {
      latestFlowProps().onInit?.({
        fitBounds: reactFlowMock.fitBounds,
        getNodes: () => [],
        getNodesBounds: reactFlowMock.getNodesBounds,
        screenToFlowPosition
      });
    });

    const canvas = container.querySelector(".xflow-preview-canvas");
    expect(canvas).toBeTruthy();
    fireCanvasDrop(canvas!, dragTransferStub("xflow.function"), 300, 400);

    expect(screenToFlowPosition).toHaveBeenCalledWith({ x: 300, y: 400 }, { snapToGrid: false });
    expect(onDropNode).toHaveBeenCalledWith("xflow.function", { x: 260, y: 340 });
  });

  it("ignores a drop that carries no node type or no canvas instance", () => {
    const onDropNode = vi.fn();
    const { container } = render(
      <XFlowPreview
        workflow={{ name: "Empty flow", nodes: [] }}
        editable
        onDropNode={onDropNode}
      />
    );

    const canvas = container.querySelector(".xflow-preview-canvas");
    fireCanvasDrop(canvas!, dragTransferStub(""), 10, 20);
    expect(onDropNode).not.toHaveBeenCalled();

    fireCanvasDrop(canvas!, dragTransferStub("xflow.function"), 10, 20);
    expect(onDropNode).not.toHaveBeenCalled();
  });

  it("does not admit drops on a read-only canvas", () => {
    const onDropNode = vi.fn();
    const { container } = render(
      <XFlowPreview
        workflow={{ name: "Empty flow", nodes: [] }}
        onDropNode={onDropNode}
      />
    );

    const canvas = container.querySelector(".xflow-preview-canvas");
    fireCanvasDrop(canvas!, dragTransferStub("xflow.function"), 10, 20);

    expect(onDropNode).not.toHaveBeenCalled();
  });

  it("accepts a drag over the canvas only while a drop is admitted", () => {
    const { container } = render(
      <XFlowPreview
        workflow={{ name: "Empty flow", nodes: [] }}
        editable
        onDropNode={vi.fn()}
      />
    );

    const canvas = container.querySelector(".xflow-preview-canvas");
    const event = new Event("dragover", { bubbles: true, cancelable: true });
    const preventDefault = vi.spyOn(event, "preventDefault");
    Object.defineProperty(event, "dataTransfer", { value: dragTransferStub("") });
    fireEvent(canvas!, event);

    expect(preventDefault).toHaveBeenCalled();
  });

  it("notifies the owner when a canvas node is selected with mouse or keyboard", () => {
    const handleSelectNode = vi.fn();
    const { container } = render(
      <XFlowPreview
        workflow={{
          name: "Selectable flow",
          nodes: [
            { name: "start", type: "xflow.start" },
            { name: "http_1", type: "xflow.http" }
          ],
          connections: {
            start: {
              main: [{ node: "http_1" }]
            }
          }
        }}
        onSelectNode={handleSelectNode}
      />
    );

    const httpNode = container.querySelector('[aria-label="http_1 node pending"]');
    expect(httpNode).toBeTruthy();

    fireEvent.click(httpNode!);
    fireEvent.keyDown(httpNode!, { key: "Enter" });
    fireEvent.keyDown(httpNode!, { key: " " });

    expect(handleSelectNode).toHaveBeenCalledTimes(3);
    expect(handleSelectNode).toHaveBeenLastCalledWith("http_1");
  });

  it("maps React Flow ids and handles to a controlled, port-aware data connection", () => {
    const onConnect = vi.fn();
    render(<XFlowPreview workflow={portWorkflow} editable onConnect={onConnect} />);

    const flow = latestFlowProps();
    expect(flow.nodesConnectable).toBe(true);
    expect(flow.connectOnClick).toBe(true);
    expect(flow.onConnect).toEqual(expect.any(Function));
    expect(flow.nodes.map((node) => node.id)).toEqual(["source-id", "target-id"]);
    expect(flow.edges.map((edge) => edge.id)).toEqual(["source:success->target:payload"]);
    expect(screen.queryByLabelText("source dependency 连接到 target main")).toBeNull();

    const sourceHandle = screen.getByRole("button", {
      name: "source 输出端口 success；按 Enter 或空格开始连线"
    });
    const targetHandle = screen.getByRole("button", {
      name: "target 输入端口 payload；按 Enter 或空格完成连线"
    });
    expect(sourceHandle.getAttribute("aria-disabled")).toBe("false");
    expect(sourceHandle.getAttribute("tabindex")).toBe("0");
    expect(targetHandle.getAttribute("aria-disabled")).toBe("false");
    expect(targetHandle.getAttribute("tabindex")).toBe("0");
    expect(flow.edges[0]).toMatchObject({
      sourceHandle: "success",
      targetHandle: "payload",
      label: "success"
    });

    act(() => {
      flow.onConnect?.({
        source: "source-id",
        sourceHandle: "success",
        target: "target-id",
        targetHandle: "payload"
      });
      flow.onConnect?.({
        source: "unknown-id",
        sourceHandle: "success",
        target: "target-id",
        targetHandle: "payload"
      });
    });

    expect(onConnect).toHaveBeenCalledTimes(1);
    expect(onConnect).toHaveBeenCalledWith({
      source: "source",
      sourcePort: "success",
      target: "target",
      targetPort: "payload"
    });
  });

  it("draws edges of reported dangling ports dashed, labeled and never deleted", () => {
    const onDeleteConnection = vi.fn();
    render(
      <XFlowPreview
        workflow={portWorkflow}
        editable
        onDeleteConnection={onDeleteConnection}
        danglingPorts={[{ source: "source", sourcePort: "success" }, { source: "target", sourcePort: "main" }]}
      />
    );

    const [edge] = latestFlowProps().edges;
    expect(edge.className).toBe("xflow-preview-edge--dangling");
    expect(edge.label).toBe("success · missing");
    // The dashed stroke lives in styles.css (pinned by styleClasses.test.ts);
    // the edge object must carry no inline style, or it would outrank hover
    // and the selected rule.
    expect(edge.style).toBeUndefined();
    expect(edge.ariaLabel).toContain("（端口已不存在）");
    expect(onDeleteConnection).not.toHaveBeenCalled();

    // Without the prop the same edge is ordinary: the resting stroke, which core's
    // table gives a target with no run status.
    render(<XFlowPreview workflow={portWorkflow} editable onDeleteConnection={onDeleteConnection} />);
    const [plain] = latestFlowProps().edges;
    expect(plain.className).toBe("xflow-preview-edge--default");
    // §6.2: orthogonal routing with 8px corners, as the baseline's edge-path.ts.
    expect(plain).toMatchObject({ type: "smoothstep", pathOptions: { borderRadius: 8 } });
    expect(plain.label).toBe("success");
  });

  it("requests deletion for selected data edges and never passes node or dependency deletion through", async () => {
    const onDeleteConnection = vi.fn();
    render(<XFlowPreview workflow={portWorkflow} editable onDeleteConnection={onDeleteConnection} />);

    const flow = latestFlowProps();
    // ED-08: the canvas tab order belongs to nodes; edges stay click-selectable.
    expect(flow.edgesFocusable).toBe(false);
    expect(flow.elementsSelectable).toBe(true);
    expect(flow.deleteKeyCode).toEqual(["Backspace", "Delete"]);
    expect(flow.onEdgesDelete).toEqual(expect.any(Function));
    expect(flow.edges).toHaveLength(1);
    expect(flow.edges[0]).toMatchObject({ focusable: false, selectable: true, deletable: true });

    const edge = screen.getByRole("img", {
      name: "source success 连接到 target payload"
    });
    expect(edge.hasAttribute("tabindex")).toBe(false);
    fireEvent.click(edge);
    expect(latestFlowProps().edges[0].selected).toBe(true);
    // Re-query: the mock edge carries last render's `selected`, like xyflow's.
    fireEvent.keyDown(screen.getByRole("img", { name: "source success 连接到 target payload" }), { key: "Delete" });

    expect(onDeleteConnection).toHaveBeenCalledTimes(1);
    expect(onDeleteConnection).toHaveBeenCalledWith({
      source: "source",
      sourcePort: "success",
      target: "target",
      targetPort: "payload"
    });

    const beforeDelete = flow.onBeforeDelete;
    expect(beforeDelete).toBeTruthy();
    const permitted = await beforeDelete!({
      nodes: [{ id: "source-id" }],
      edges: [...flow.edges, { id: "dependency-edge" }]
    });
    expect(permitted.nodes).toEqual([]);
    expect(permitted.edges.map((item) => item.id)).toEqual(["source:success->target:payload"]);
  });

  it("requests node deletion on Delete and Backspace for a selected node", () => {
    const onDeleteNode = vi.fn();
    render(
      <XFlowPreview
        workflow={portWorkflow}
        editable
        selectedNodeId="source-id"
        onDeleteNode={onDeleteNode}
      />
    );

    fireEvent.keyDown(window, { key: "Delete" });
    expect(onDeleteNode).toHaveBeenCalledWith("source-id");

    fireEvent.keyDown(window, { key: "Backspace" });
    expect(onDeleteNode).toHaveBeenCalledTimes(2);
  });

  it("stands down for node deletion while a connection is selected", () => {
    const onDeleteNode = vi.fn();
    const onDeleteConnection = vi.fn();
    render(
      <XFlowPreview
        workflow={portWorkflow}
        editable
        selectedNodeId="source-id"
        onDeleteConnection={onDeleteConnection}
        onDeleteNode={onDeleteNode}
      />
    );

    const edge = () => screen.getByRole("img", { name: /source success 连接到/ });
    fireEvent.click(edge());
    fireEvent.keyDown(edge(), { key: "Delete" });
    // One press removes the connection the user selected, not the node behind it.
    expect(onDeleteConnection).toHaveBeenCalledTimes(1);
    expect(onDeleteNode).not.toHaveBeenCalled();
  });

  it("leaves Delete to a focused text field", () => {
    const onDeleteNode = vi.fn();
    const input = document.createElement("input");
    document.body.appendChild(input);
    try {
      render(
        <XFlowPreview
          workflow={portWorkflow}
          editable
          selectedNodeId="source-id"
          onDeleteNode={onDeleteNode}
        />
      );

      fireEvent.keyDown(input, { key: "Backspace" });

      expect(onDeleteNode).not.toHaveBeenCalled();
    } finally {
      input.remove();
    }
  });

  it("withholds node deletion without a handler, in read-only mode, or with nothing selected", () => {
    const onDeleteNode = vi.fn();
    const { rerender } = render(<XFlowPreview workflow={portWorkflow} selectedNodeId="source-id" />);
    fireEvent.keyDown(window, { key: "Delete" });

    rerender(<XFlowPreview workflow={portWorkflow} editable selectedNodeId="source-id" onDeleteNode={onDeleteNode} />);
    fireEvent.keyDown(window, { key: "Delete" });
    expect(onDeleteNode).toHaveBeenCalledTimes(1);

    // Read-only: the same handler and selection, but the canvas is not editable.
    rerender(<XFlowPreview workflow={portWorkflow} selectedNodeId="source-id" onDeleteNode={onDeleteNode} />);
    fireEvent.keyDown(window, { key: "Delete" });
    expect(onDeleteNode).toHaveBeenCalledTimes(1);

    // Editable again, but this time nothing is selected.
    rerender(<XFlowPreview workflow={portWorkflow} editable onDeleteNode={onDeleteNode} />);
    fireEvent.keyDown(window, { key: "Delete" });
    expect(onDeleteNode).toHaveBeenCalledTimes(1);
  });

  it("switches explicit editor canvas modes between selecting and connecting", () => {
    const onConnect = vi.fn();
    const { rerender } = render(
      <XFlowPreview
        editable
        interactionMode="select"
        onConnect={onConnect}
        workflow={portWorkflow}
      />
    );

    let flow = latestFlowProps();
    expect(flow.nodesDraggable).toBe(true);
    expect(flow.nodesConnectable).toBe(false);
    expect(flow.connectOnClick).toBe(false);
    expect(flow.onConnect).toBeUndefined();

    rerender(
      <XFlowPreview
        editable
        interactionMode="connect"
        onConnect={onConnect}
        workflow={portWorkflow}
      />
    );

    flow = latestFlowProps();
    expect(flow.nodesDraggable).toBe(false);
    expect(flow.nodesConnectable).toBe(true);
    expect(flow.connectOnClick).toBe(true);
    expect(flow.onConnect).toBeTruthy();
  });

  it("disables connection and deletion controls outside editable mode", () => {
    const onConnect = vi.fn();
    const onDeleteConnection = vi.fn();
    render(
      <XFlowPreview
        workflow={portWorkflow}
        editable={false}
        onConnect={onConnect}
        onDeleteConnection={onDeleteConnection}
      />
    );

    const flow = latestFlowProps();
    expect(flow.nodesConnectable).toBe(false);
    expect(flow.connectOnClick).toBe(false);
    expect(flow.onConnect).toBeUndefined();
    expect(flow.edgesFocusable).toBe(false);
    expect(flow.elementsSelectable).toBe(false);
    expect(flow.onEdgesDelete).toBeUndefined();
    expect(flow.deleteKeyCode).toBeNull();
    expect(flow.edges[0]).toMatchObject({ selectable: false, focusable: false, deletable: false });

    const sourceHandle = screen.getByRole("button", {
      name: "source 输出端口 success；按 Enter 或空格开始连线"
    });
    expect(sourceHandle.getAttribute("aria-disabled")).toBe("true");
    expect(sourceHandle.getAttribute("tabindex")).toBe("-1");
    fireEvent.keyDown(sourceHandle, { key: "Enter" });
    expect(onConnect).not.toHaveBeenCalled();
    expect(onDeleteConnection).not.toHaveBeenCalled();
  });

  it("excludes supply endpoints and self-loops from canvas data connections", () => {
    const onConnect = vi.fn();
    render(
      <XFlowPreview
        editable
        onConnect={onConnect}
        workflow={{
          name: "Supply boundaries",
          nodes: [
            { id: "regular-source", name: "regular_source", type: "xflow.function" },
            { id: "regular-target", name: "regular_target", type: "xflow.http" },
            { id: "kind-supply", name: "kind_supply", type: "xflow.custom", kind: "supply" },
            { id: "type-supply", name: "type_supply", type: "xflow.supply.external" }
          ],
          connections: {
            regular_source: {
              main: [
                { node: "regular_target" },
                { node: "kind_supply" },
                { node: "type_supply" }
              ]
            },
            kind_supply: {
              main: [{ node: "regular_target" }]
            },
            type_supply: {
              main: [{ node: "regular_target" }]
            }
          }
        }}
      />
    );

    const flow = latestFlowProps();
    expect(flow.edges.map((edge) => edge.id)).toEqual(["regular_source:main->regular_target:main"]);
    expect(screen.queryByRole("button", { name: /kind_supply (input|output)/ })).toBeNull();
    expect(screen.queryByRole("button", { name: /type_supply (input|output)/ })).toBeNull();
    expect(flow.isValidConnection?.({
      source: "regular-source",
      sourceHandle: "main",
      target: "regular-target",
      targetHandle: "main"
    })).toBe(true);
    expect(flow.isValidConnection?.({
      source: "regular-source",
      target: "regular-source"
    })).toBe(false);
    expect(flow.isValidConnection?.({
      source: "kind-supply",
      target: "regular-target"
    })).toBe(false);
    expect(flow.isValidConnection?.({
      source: "regular-source",
      target: "type-supply"
    })).toBe(false);

    act(() => {
      flow.onConnect?.({
        source: "regular-source",
        sourceHandle: "main",
        target: "regular-target",
        targetHandle: "main"
      });
      flow.onConnect?.({
        source: "kind-supply",
        sourceHandle: "main",
        target: "regular-target",
        targetHandle: "main"
      });
      flow.onConnect?.({
        source: "regular-source",
        sourceHandle: "main",
        target: "regular-source",
        targetHandle: "main"
      });
    });

    expect(onConnect).toHaveBeenCalledTimes(1);
    expect(onConnect).toHaveBeenCalledWith({
      source: "regular_source",
      sourcePort: "main",
      target: "regular_target",
      targetPort: "main"
    });
  });

  it("shows a status chip on every node once a runtime is passed, and on none without one", () => {
    const { container, rerender } = render(<XFlowPreview workflow={contextMenuWorkflow} />);

    // No `runtime` prop at all: edit mode, so no chip.
    expect(container.querySelector(".xflow-preview-node__status")).toBeNull();

    rerender(
      <XFlowPreview
        workflow={contextMenuWorkflow}
        runtime={{ status: "running", nodes: { http_1: { status: "failed", durationMs: 40 } } }}
      />
    );

    // With a runtime every node has a status, the one the run has not reached
    // included: `start` is absent from `nodes` and reads as pending.
    const chip = (nodeId: string) =>
      container.querySelector(`[data-node-id="${nodeId}"] .xflow-preview-node__status`);
    expect(container.querySelectorAll(".xflow-preview-node__status")).toHaveLength(2);
    expect(chip("http-id")?.textContent).toBe(nodeStatusMeta.failed.label);
    expect(chip("start-id")?.textContent).toBe(nodeStatusMeta.pending.label);
    expect(chip("start-id")?.getAttribute("data-tone")).toBe(nodeStatusMeta.pending.tone);
    expect(screen.getByText("40 ms")).toBeTruthy();

    // Dropping the runtime again takes every chip away.
    rerender(<XFlowPreview workflow={contextMenuWorkflow} />);
    expect(container.querySelector(".xflow-preview-node__status")).toBeNull();
  });

  it("keeps the full node name in a title so a truncated card stays readable", () => {
    const { container } = render(
      <XFlowPreview
        workflow={{
          name: "Long flow",
          nodes: [{ name: "fetch-pull-request-metadata", type: "xflow.http", ui: { label: "Fetch PR" } }]
        }}
      />
    );

    const title = container.querySelector(".xflow-preview-node__title strong");
    expect(title?.textContent).toBe("Fetch PR");
    expect(title?.getAttribute("title")).toBe("Fetch PR (fetch-pull-request-metadata)");
  });

  it("selects a right-clicked node and requests its context menu at the pointer", () => {
    const onSelectNode = vi.fn();
    const onNodeContextMenu = vi.fn();
    render(
      <XFlowPreview
        workflow={contextMenuWorkflow}
        onSelectNode={onSelectNode}
        onNodeContextMenu={onNodeContextMenu}
      />
    );

    const event = mouseEventStub(120, 240);
    act(() => {
      latestFlowProps().onNodeContextMenu?.(event, { id: "http-id" });
    });

    expect(event.preventDefault).toHaveBeenCalled();
    expect(onSelectNode).toHaveBeenCalledWith("http-id");
    expect(onNodeContextMenu).toHaveBeenCalledWith("http-id", { clientX: 120, clientY: 240 });
  });

  it("requests a pane context menu with both client and canvas coordinates", () => {
    const onPaneContextMenu = vi.fn();
    const screenToFlowPosition = vi.fn((point: { x: number; y: number }) => ({ x: point.x / 2, y: point.y / 2 }));
    render(<XFlowPreview workflow={contextMenuWorkflow} onPaneContextMenu={onPaneContextMenu} />);

    act(() => {
      latestFlowProps().onInit?.({
        fitBounds: reactFlowMock.fitBounds,
        getNodes: () => [],
        getNodesBounds: reactFlowMock.getNodesBounds,
        screenToFlowPosition
      });
    });
    const event = mouseEventStub(300, 500);
    act(() => {
      latestFlowProps().onPaneContextMenu?.(event);
    });

    expect(event.preventDefault).toHaveBeenCalled();
    expect(screenToFlowPosition).toHaveBeenCalledWith({ x: 300, y: 500 }, { snapToGrid: false });
    expect(onPaneContextMenu).toHaveBeenCalledWith({ clientX: 300, clientY: 500, flowX: 150, flowY: 250 });
  });

  it("leaves the native context menu alone when no menu callback is supplied", () => {
    const { container } = render(<XFlowPreview workflow={contextMenuWorkflow} selectedNodeId="http-id" />);

    const flow = latestFlowProps();
    expect(flow.onNodeContextMenu).toBeUndefined();
    expect(flow.onPaneContextMenu).toBeUndefined();

    const httpNode = container.querySelector('[aria-label="http_1 node pending"]')!;
    expect(httpNode.getAttribute("aria-keyshortcuts")).toBeNull();
    // fireEvent returns false only when a handler called preventDefault.
    expect(fireEvent.keyDown(httpNode, { key: "ContextMenu" })).toBe(true);
    expect(fireEvent.keyDown(httpNode, { key: "F10", shiftKey: true })).toBe(true);
  });

  it("opens a node menu from the keyboard at the focused card's center", () => {
    const onSelectNode = vi.fn();
    const onNodeContextMenu = vi.fn();
    const { container } = render(
      <XFlowPreview
        workflow={contextMenuWorkflow}
        onSelectNode={onSelectNode}
        onNodeContextMenu={onNodeContextMenu}
      />
    );

    const card = container.querySelector('[data-node-id="http-id"]') as HTMLElement;
    vi.spyOn(card, "getBoundingClientRect").mockReturnValue(
      { left: 100, top: 50, width: 232, height: 60, right: 332, bottom: 110, x: 100, y: 50, toJSON: () => ({}) }
    );
    const httpNode = container.querySelector('[aria-label="http_1 node pending"]')!;
    expect(httpNode.getAttribute("aria-keyshortcuts")).toBe("Shift+F10 ContextMenu");

    expect(fireEvent.keyDown(httpNode, { key: "F10", shiftKey: true })).toBe(false);
    expect(onSelectNode).toHaveBeenCalledWith("http-id");
    expect(onNodeContextMenu).toHaveBeenCalledTimes(1);
    expect(onNodeContextMenu).toHaveBeenCalledWith("http-id", { clientX: 216, clientY: 80 });

    // A plain F10 is not the context-menu chord.
    fireEvent.keyDown(httpNode, { key: "F10" });
    expect(onNodeContextMenu).toHaveBeenCalledTimes(1);

    // The native contextmenu echo of that key press must not open a second menu.
    act(() => {
      latestFlowProps().onNodeContextMenu?.(mouseEventStub(0, 0), { id: "http-id" });
    });
    expect(onNodeContextMenu).toHaveBeenCalledTimes(1);

    // ...but consuming that echo closes the window, so a real right click in the
    // same 500ms still opens a menu instead of being swallowed as another echo.
    const nextClick = mouseEventStub(40, 60);
    act(() => {
      latestFlowProps().onNodeContextMenu?.(nextClick, { id: "start-id" });
    });
    expect(nextClick.preventDefault).toHaveBeenCalled();
    expect(onNodeContextMenu).toHaveBeenCalledTimes(2);
    expect(onNodeContextMenu).toHaveBeenLastCalledWith("start-id", { clientX: 40, clientY: 60 });
  });

  it("opens the selected node's menu from the ContextMenu key anywhere on the canvas", () => {
    const onNodeContextMenu = vi.fn();
    const { container } = render(
      <XFlowPreview
        workflow={contextMenuWorkflow}
        selectedNodeId="start-id"
        onNodeContextMenu={onNodeContextMenu}
      />
    );

    const canvas = container.querySelector(".xflow-preview-canvas")!;
    expect(fireEvent.keyDown(canvas, { key: "ContextMenu" })).toBe(false);
    expect(onNodeContextMenu).toHaveBeenCalledWith("start-id", { clientX: 0, clientY: 0 });
  });

  it("does not open a keyboard menu from the canvas with nothing selected", () => {
    const onNodeContextMenu = vi.fn();
    const { container } = render(
      <XFlowPreview workflow={contextMenuWorkflow} onNodeContextMenu={onNodeContextMenu} />
    );

    const canvas = container.querySelector(".xflow-preview-canvas")!;
    expect(fireEvent.keyDown(canvas, { key: "ContextMenu" })).toBe(true);
    expect(onNodeContextMenu).not.toHaveBeenCalled();
  });

  it("exposes fit-view and center-node commands once the canvas is ready", () => {
    const frameCallbacks: FrameRequestCallback[] = [];
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => {
      frameCallbacks.push(callback);
      return frameCallbacks.length;
    });
    vi.stubGlobal("cancelAnimationFrame", vi.fn());

    const onReady = vi.fn();
    const sourceNode = { id: "source-id" };
    const targetNode = { id: "target-id" };
    const graphBounds = { x: 0, y: 0, width: 900, height: 400 };
    const targetBounds = { x: 680, y: 240, width: 240, height: 96 };
    const fitBounds = vi
      .fn<(bounds: unknown, options?: { padding?: number }) => Promise<boolean>>()
      .mockResolvedValue(true);
    const getNodesBounds = vi.fn((nodes: Array<{ id: string }>) => (
      nodes.length === 1 && nodes[0]?.id === targetNode.id ? targetBounds : graphBounds
    ));
    const flowInstance: MockFlowInstance = {
      fitBounds,
      getNodes: vi.fn(() => [sourceNode, targetNode]),
      getNodesBounds,
      getViewport: () => ({ x: 0, y: 0, zoom: 1 })
    };
    render(<XFlowPreview workflow={portWorkflow} onReady={onReady} />);

    expect(onReady).not.toHaveBeenCalled();

    act(() => {
      latestFlowProps().onInit?.(flowInstance);
    });

    expect(onReady).toHaveBeenCalledTimes(1);
    const handle = onReady.mock.calls[0]![0] as {
      fitView: (options?: { padding?: number }) => void;
      fitNode: (nodeId: string, options?: { padding?: number }) => void;
    };

    act(() => {
      handle.fitView();
    });

    expect(getNodesBounds).toHaveBeenLastCalledWith([sourceNode, targetNode]);
    expect(fitBounds).toHaveBeenLastCalledWith(graphBounds, { padding: 0.18 });

    act(() => {
      handle.fitView({ padding: 0.4 });
    });

    expect(fitBounds).toHaveBeenLastCalledWith(graphBounds, { padding: 0.4 });

    act(() => {
      handle.fitNode("target-id");
    });

    expect(getNodesBounds).toHaveBeenLastCalledWith([targetNode]);
    expect(fitBounds).toHaveBeenLastCalledWith(targetBounds, { padding: 0.28 });

    act(() => {
      handle.fitNode("missing-node");
    });

    expect(fitBounds).toHaveBeenCalledTimes(3);
  });

  it("waits for an unmeasured node before centering on it from the handle", () => {
    const frameCallbacks: FrameRequestCallback[] = [];
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => {
      frameCallbacks.push(callback);
      return frameCallbacks.length;
    });
    vi.stubGlobal("cancelAnimationFrame", vi.fn());

    const onReady = vi.fn();
    const targetNode = { id: "target-id" };
    const measuredBounds = { x: 680, y: 240, width: 240, height: 96 };
    const fitBounds = vi
      .fn<(bounds: unknown, options?: { padding?: number }) => Promise<boolean>>()
      .mockResolvedValue(true);
    // The first read is the unmeasured 0x0 box a just-appended node reports.
    const getNodesBounds = vi
      .fn((_nodes: Array<{ id: string }>) => measuredBounds)
      .mockReturnValueOnce({ x: 680, y: 240, width: 0, height: 0 });
    render(<XFlowPreview workflow={portWorkflow} onReady={onReady} />);

    act(() => {
      latestFlowProps().onInit?.({
        fitBounds,
        getNodes: () => [targetNode],
        getNodesBounds,
        getViewport: () => ({ x: 0, y: 0, zoom: 1 })
      });
    });

    const handle = onReady.mock.calls[0]![0] as { fitNode: (nodeId: string) => void };
    act(() => {
      handle.fitNode("target-id");
    });

    expect(getNodesBounds).toHaveBeenCalledTimes(1);
    expect(fitBounds).not.toHaveBeenCalled();

    act(() => {
      frameCallbacks.at(-1)?.(0);
    });

    expect(fitBounds).toHaveBeenCalledTimes(1);
    expect(fitBounds).toHaveBeenCalledWith(measuredBounds, { padding: 0.28 });
  });
});

// §6.2's canvas surface: the dot grid, the minimap switch and the zoom domain.
describe("XFlowPreview canvas surface", () => {
  const surfaceWorkflow = {
    id: "wf-surface",
    name: "Surface flow",
    nodes: [
      { name: "start", type: "xflow.start" },
      { name: "http_1", type: "xflow.http" }
    ],
    connections: { start: { main: [{ node: "http_1" }] } }
  } satisfies WorkflowDef;

  it("draws the §6.2 dot grid: 16px in core's canvas-dot colour", () => {
    const { container } = render(<XFlowPreview workflow={surfaceWorkflow} />);
    const background = container.querySelector(".react-flow__background");
    expect(background?.getAttribute("data-color")).toBe("var(--xflow-canvas-dot)");
    expect(background?.getAttribute("data-gap")).toBe("16");
    // The dot stays 1px; only the pitch and the colour moved.
    expect(background?.getAttribute("data-size")).toBe("1");

    // The same 16px pitch is what a node drag quantizes to, so a dropped card
    // never lands between two dots.
    const flow = latestFlowProps();
    expect(flow.snapToGrid).toBe(true);
    expect(flow.snapGrid).toEqual([16, 16]);
  });

  it("keeps the minimap off the canvas until the owner turns it on", () => {
    const { container, rerender } = render(<XFlowPreview workflow={surfaceWorkflow} />);
    expect(container.querySelector(".react-flow__minimap")).toBeNull();
    expect(reactFlowMock.miniMapProps).toBeUndefined();

    rerender(<XFlowPreview workflow={surfaceWorkflow} minimap />);
    expect(container.querySelector(".react-flow__minimap")).toBeTruthy();
    // Turning it on keeps the interactions and the swatch colour rule.
    expect(reactFlowMock.miniMapProps).toMatchObject({
      pannable: true,
      zoomable: true,
      nodeColor: expect.any(Function),
      // xyflow sizes the svg viewBox from style, so the 200x124 box is passed in,
      // not left to CSS (which would crop a 200x150 map).
      style: { width: 200, height: 124 },
      // §6.2's viewport frame: the baseline's 1.5px primary outline. The mask
      // fill only shades outside the viewport, which the baseline does not do.
      maskColor: "var(--xflow-preview-minimap-mask)",
      maskStrokeColor: "var(--xflow-preview-minimap-view)",
      maskStrokeWidth: 1.5,
      // The baseline swatch is a 2px-radius block with no outline.
      nodeBorderRadius: 2,
      nodeStrokeWidth: 0,
      // The accessible name of the baseline `role="img"` minimap.
      ariaLabel: "缩略图"
    });
  });

  it("centres the canvas where the minimap is clicked, keeping the zoom", () => {
    const setCenter = vi.fn();
    render(<XFlowPreview workflow={surfaceWorkflow} minimap />);
    act(() => {
      latestFlowProps().onInit?.({
        fitBounds: reactFlowMock.fitBounds,
        getNodes: () => [],
        getNodesBounds: reactFlowMock.getNodesBounds,
        getZoom: () => 0.6,
        setCenter
      });
    });
    const onClick = (reactFlowMock.miniMapProps as { onClick?: (event: unknown, position: { x: number; y: number }) => void })
      .onClick;
    expect(onClick).toEqual(expect.any(Function));
    act(() => onClick!({}, { x: 420, y: -80 }));
    expect(setCenter).toHaveBeenCalledWith(420, -80, { zoom: 0.6 });
  });

  it("spans the 10%-200% zoom domain §6.2's slider offers", () => {
    render(<XFlowPreview workflow={surfaceWorkflow} />);
    expect(latestFlowProps().minZoom).toBe(0.1);
    expect(latestFlowProps().maxZoom).toBe(2);
  });

  it("opens automatic fits at or above the LOD threshold, so the first screen is full detail", () => {
    // EL-01: a fit floor below LOD_ZOOM made every workflow open as icon+title
    // only. The floor and the threshold are separate knobs; only the ordered
    // pair is the contract.
    expect(READABLE_FIT_ZOOM).toBeGreaterThanOrEqual(LOD_ZOOM);
  });
});

describe("XFlowPreview status chip rule", () => {
  const chipWorkflow = {
    name: "Chip flow",
    nodes: [
      { id: "queued-id", name: "queued", type: "xflow.http" },
      { id: "absent-id", name: "absent", type: "xflow.http" }
    ]
  } satisfies WorkflowDef;
  const chipText = (container: HTMLElement, nodeId: string) =>
    container.querySelector(`[data-node-id="${nodeId}"] .xflow-preview-node__status`)?.textContent;

  it("gives every node a chip once a runtime is passed, a queued or unreported node included", () => {
    const { container } = render(
      <XFlowPreview
        workflow={chipWorkflow}
        runtime={{ status: "running", nodes: { queued: { status: "pending" } } }}
      />
    );

    // `queued` is reported as pending, `absent` is not reported at all: both are
    // part of a run, so both read as pending rather than as "no run".
    expect(chipText(container, "queued-id")).toBe(nodeStatusMeta.pending.label);
    expect(chipText(container, "absent-id")).toBe(nodeStatusMeta.pending.label);
  });

  it("draws no chip on any node when no runtime is passed", () => {
    const { container } = render(<XFlowPreview workflow={chipWorkflow} />);

    expect(container.querySelector(".xflow-preview-node__status")).toBeNull();
  });
});

describe("XFlowPreview card re-rendering", () => {
  // Built once: a new workflow object per render would rebuild every card's data.
  const bigWorkflow = {
    name: "Big flow",
    nodes: Array.from({ length: 200 }, (_, index) => ({
      id: `n${index}`,
      name: `n${index}`,
      type: "xflow.http"
    }))
  } satisfies WorkflowDef;
  const { flowStore } = reactFlowMock;

  it("re-renders at most the previous and the new selection when the selection moves", () => {
    const { rerender } = render(<XFlowPreview workflow={bigWorkflow} selectedNodeId="n0" />);
    // Guard: the counter must see the first paint, or a count of 0 proves nothing.
    expect(flowStore.cardRenders).toBeGreaterThanOrEqual(200);

    flowStore.cardRenders = 0;
    rerender(<XFlowPreview workflow={bigWorkflow} selectedNodeId="n1" />);

    expect(flowStore.cardRenders).toBeGreaterThan(0);
    expect(flowStore.cardRenders).toBeLessThanOrEqual(2);
  });
});

describe("XFlowPreview level of detail", () => {
  const lodWorkflow = {
    name: "Lod flow",
    nodes: [
      { id: "a-id", name: "a", type: "xflow.http" },
      { id: "b-id", name: "b", type: "xflow.http" }
    ]
  } satisfies WorkflowDef;
  const lodRuntime = { status: "failed", nodes: { a: { status: "failed", durationMs: 40, error: "boom" } } } as const;
  const { flowStore } = reactFlowMock;

  // What a card shows, as a set: flips with the level, and with nothing else.
  const shown = (container: HTMLElement) => ({
    lod: [...container.querySelectorAll(".xflow-preview-node")].map((card) => card.getAttribute("data-lod")),
    types: container.querySelectorAll(".xflow-preview-node__type").length,
    badges: container.querySelectorAll(".xflow-preview-node__meta").length,
    summaries: container.querySelectorAll(".xflow-preview-node__summary").length,
    errors: container.querySelectorAll(".xflow-preview-node__error").length,
    chipLabels: [...container.querySelectorAll(".xflow-preview-node__status")].map((chip) => chip.textContent)
  });

  it("renders full, compact and low detail at the two thresholds", () => {
    const { container } = render(<XFlowPreview workflow={lodWorkflow} runtime={lodRuntime} />);
    const full = shown(container);
    expect(full).toMatchObject({ lod: ["full", "full"], types: 2, badges: 2, summaries: 0, errors: 1 });
    expect(full.chipLabels.every((label) => label)).toBe(true);

    // 0.75–0.8: title, type and badges stay; summary, error line and chip text go.
    act(() => flowStore.setZoom(0.78));
    expect(shown(container)).toMatchObject({ lod: ["compact", "compact"], types: 2, badges: 2, summaries: 0, errors: 0 });
    expect(shown(container).chipLabels.every((label) => label)).toBe(true);

    // The workbench's default 62% view is already low detail (ED-20).
    act(() => flowStore.setZoom(0.62));
    expect(shown(container)).toEqual({
      lod: ["low", "low"],
      types: 0,
      badges: 0,
      summaries: 0,
      errors: 0,
      chipLabels: ["", ""]
    });

    act(() => flowStore.setZoom(0.75));
    expect(shown(container).lod).toEqual(["compact", "compact"]);
    act(() => flowStore.setZoom(0.8));
    expect(shown(container)).toEqual(full);
  });

  it("re-renders no card while the zoom moves inside one level", () => {
    render(<XFlowPreview workflow={lodWorkflow} runtime={lodRuntime} />);
    expect(flowStore.cardRenders).toBeGreaterThan(0);

    flowStore.cardRenders = 0;
    for (const zoom of [0.9, 1.4, 1.0, 2, 0.8]) {
      act(() => flowStore.setZoom(zoom));
    }
    expect(flowStore.cardRenders).toBe(0);

    // Crossing a threshold is the one thing that re-renders: once per card.
    act(() => flowStore.setZoom(0.79));
    expect(flowStore.cardRenders).toBe(2);
    flowStore.cardRenders = 0;
    act(() => flowStore.setZoom(0.76));
    expect(flowStore.cardRenders).toBe(0);
    act(() => flowStore.setZoom(0.74));
    expect(flowStore.cardRenders).toBe(2);
  });
});

describe("readableNodeType", () => {
  it("names a type by its last segment, with acronyms left upper-case", () => {
    expect(readableNodeType("xflow.http")).toBe("HTTP");
    expect(readableNodeType("xflow.trigger.timer")).toBe("Timer");
    expect(readableNodeType("xflow.trigger.webhook")).toBe("Webhook");
    expect(readableNodeType("xflow.ai")).toBe("AI");
  });

  it("splits a snake_case or kebab-case tail into words", () => {
    expect(readableNodeType("acme.custom_approval")).toBe("Custom Approval");
    expect(readableNodeType("acme.http-request")).toBe("HTTP Request");
  });

  it("stays total on an empty or trailing-separator id", () => {
    expect(readableNodeType("")).toBe("");
    expect(readableNodeType("xflow.")).toBe("Xflow");
  });
});

describe("XFlowPreview card type name (E-A)", () => {
  const typeWorkflow = {
    name: "Type flow",
    nodes: [
      { id: "a-id", name: "a", type: "xflow.http" },
      { id: "b-id", name: "b", type: "xflow.trigger.timer" }
    ]
  } satisfies WorkflowDef;
  const typeOf = (container: HTMLElement, id: string) =>
    container.querySelector(`[data-node-id="${id}"] .xflow-preview-node__type`);

  it("prints a readable name, never the raw id as visible text", () => {
    const { container } = render(<XFlowPreview workflow={typeWorkflow} />);
    expect(typeOf(container, "a-id")?.textContent).toBe("HTTP");
    expect(typeOf(container, "b-id")?.textContent).toBe("Timer");
    // The raw id stays available on hover; the line itself never reads it.
    expect(typeOf(container, "a-id")?.getAttribute("title")).toBe("xflow.http");
  });

  it("lets the owner's descriptor display name win over the fallback", () => {
    const { container } = render(
      <XFlowPreview nodeTypeLabels={{ "xflow.http": "HTTP 请求" }} workflow={typeWorkflow} />
    );
    expect(typeOf(container, "a-id")?.textContent).toBe("HTTP 请求");
    expect(typeOf(container, "b-id")?.textContent).toBe("Timer");
  });
});

describe("XFlowPreview card policy badges (E-A)", () => {
  const policyWorkflow = {
    name: "Policy flow",
    nodes: [
      {
        id: "both-id",
        name: "both",
        type: "xflow.http",
        retry: { max_attempts: 3 },
        timeout: 90_000_000_000
      },
      { id: "continue-id", name: "cont", type: "xflow.http", on_error: "continue" },
      { id: "error-out-id", name: "errout", type: "xflow.http", on_error: "error_output" },
      { id: "stop-id", name: "stop", type: "xflow.http", on_error: "stop" },
      {
        id: "off-id",
        name: "off",
        type: "xflow.http",
        retry: { enabled: false, max_attempts: 5 },
        timeout: 120_000_000_000
      }
    ]
  } satisfies WorkflowDef;
  const badges = (container: HTMLElement, id: string) =>
    [...container.querySelectorAll(`[data-node-id="${id}"] .xflow-preview-node__badges [data-badge]`)]
      .map((badge) => badge.getAttribute("data-badge"));
  const badge = (container: HTMLElement, id: string, kind: string) =>
    container.querySelector<HTMLElement>(`[data-node-id="${id}"] [data-badge="${kind}"]`);

  it("badges the node's own retry and timeout, never the workflow defaults", () => {
    const { container } = render(<XFlowPreview workflow={policyWorkflow} />);
    expect(badges(container, "both-id")).toEqual(["retry", "timeout"]);
    expect(badge(container, "both-id", "retry")?.textContent).toBe("重试 3");
    expect(badge(container, "both-id", "timeout")?.textContent).toBe("超时 90s");
    expect(badges(container, "stop-id")).toEqual([]);

    // A disabled retry is not configured retry; the timeout still speaks.
    expect(badges(container, "off-id")).toEqual(["timeout"]);
  });

  it("badges a non-default failure route only", () => {
    const { container } = render(<XFlowPreview workflow={policyWorkflow} />);
    expect(badges(container, "continue-id")).toEqual(["error-policy"]);
    expect(badge(container, "continue-id", "error-policy")?.textContent).toBe("继续");
    expect(badges(container, "error-out-id")).toEqual(["error-policy"]);
    expect(badge(container, "error-out-id", "error-policy")?.textContent).toBe("错误出口");
    // `stop` is the engine default and earns no badge.
    expect(badges(container, "stop-id")).toEqual([]);
  });
});

describe("XFlowPreview card layout", () => {
  // An `xflow.http` node under an `error_output` policy declares a main and an error
  // outlet; the run reports a duration, an attempt count and an error. One card
  // carries the badges, the error line and the port labels.
  const lastLineWorkflow = {
    name: "Last line flow",
    nodes: [{ id: "charge-id", name: "charge", type: "xflow.http", on_error: "error_output" }]
  } satisfies WorkflowDef;
  const lastLineRuntime = {
    status: "failed",
    nodes: { charge: { status: "failed", durationMs: 40, attempts: 2, error: "boom" } }
  } as const;

  it("draws one bottom strip last, with badges and the branch port labels together", () => {
    const { container } = render(<XFlowPreview workflow={lastLineWorkflow} runtime={lastLineRuntime} />);
    const card = container.querySelector('[data-node-id="charge-id"]');
    const kids = [...(card?.children ?? [])];
    const at = (name: string) => kids.findIndex((kid) => kid.classList.contains(name));

    const error = at("xflow-preview-node__error");
    const meta = at("xflow-preview-node__meta");
    // Guard: both rows must actually be drawn, or the order below proves nothing.
    expect([error, meta].every((index) => index >= 0), `error ${error}, meta ${meta}`).toBe(true);

    expect(meta).toBeGreaterThan(error);
    expect(meta).toBe(kids.length - 1);
    // Badges and ports share the strip; the port labels hug its right end.
    expect(kids[meta]?.querySelector(".xflow-preview-node__badges [data-badge=\"error-policy\"]")).not.toBeNull();
    expect(kids[meta]?.querySelector(".xflow-preview-node__ports .xflow-preview-node__port")?.textContent).toBe("main");
    // The error outlet's handle rides in the port group, not in the error line.
    expect(kids[meta]?.querySelector('.xflow-preview-node__ports .xflow-preview-node__handle[data-handle-id="error"]')).not.toBeNull();
    // The branch label is attached to its own handle: the handle lives inside
    // the label's box, on the label's bottom edge, so an edge leaving it cannot
    // cross the text (E-A).
    const mainPort = kids[meta]?.querySelector(".xflow-preview-node__ports .xflow-preview-node__port");
    expect(mainPort?.textContent).toBe("main");
    expect(mainPort?.querySelector('.xflow-preview-node__handle[data-handle-id="main"]')).not.toBeNull();
  });
});

describe("XFlowPreview toolbar status pill", () => {
  const pillWorkflow = {
    name: "Pill flow",
    nodes: [{ id: "only-id", name: "only", type: "xflow.http" }]
  } satisfies WorkflowDef;
  const executionStatuses = Object.keys(workflowStatusMeta) as ExecutionStatusKey[];
  // RuntimeSnapshot.status is the six-value WorkflowStatus. queued, paused and
  // canceling are ExecutionStatus values the prop type does not cover yet (P3 aligns
  // them), so those three are cast in; the pill reads all nine through the same table.
  const acceptedByProp = new Set<string>(["pending", "running", "success", "failed", "canceled", "timeout"]);
  const runtimeFor = (status: ExecutionStatusKey): RuntimeSnapshot =>
    acceptedByProp.has(status)
      ? { status: status as WorkflowStatus }
      : ({ status } as unknown as RuntimeSnapshot);

  it.each(executionStatuses)("colours the %s pill by the tone core's table assigns", (status) => {
    const { container } = render(<XFlowPreview workflow={pillWorkflow} runtime={runtimeFor(status)} />);

    const pill = container.querySelector(".xflow-preview-toolbar .xflow-preview-status");
    expect(pill?.getAttribute("data-tone")).toBe(workflowStatusMeta[status].tone);
    expect(pill?.textContent).toBe(workflowStatusMeta[status].label);
  });

  it("covers every execution status, and more than one tone", () => {
    expect(executionStatuses).toHaveLength(9);
    expect(new Set(executionStatuses.map((status) => workflowStatusMeta[status].tone)).size).toBeGreaterThan(1);
  });
});

// §6.2's canvas chrome: the rulers, the bottom-left zoom bar and its toggles.
describe("XFlowPreview canvas chrome", () => {
  const chromeWorkflow = {
    id: "wf-chrome",
    name: "Chrome flow",
    nodes: [
      { id: "start-id", name: "start", type: "xflow.start" },
      { id: "http-id", name: "http_1", type: "xflow.http" }
    ],
    connections: { start: { main: [{ node: "http_1" }] } }
  } satisfies WorkflowDef;
  const { flowStore } = reactFlowMock;
  const ticks = (container: HTMLElement, axis: "x" | "y") =>
    [...container.querySelectorAll<HTMLElement>(`.xflow-preview-ruler--${axis} .xflow-preview-ruler__tick`)]
      .map((tick) => [tick.textContent, tick.style.getPropertyValue(axis === "x" ? "left" : "top")]);
  const highlight = (container: HTMLElement, axis: "x" | "y") =>
    container.querySelector<HTMLElement>(`.xflow-preview-ruler--${axis} .xflow-preview-ruler__highlight`);
  const pressShift = (key: string, target: EventTarget = document.body) =>
    fireEvent.keyDown(target, { key, shiftKey: true });

  it("draws both rulers by default, and none on a canvas with no nodes", () => {
    const { container, rerender } = render(<XFlowPreview workflow={chromeWorkflow} />);
    expect(container.querySelector(".xflow-preview-ruler--x")).toBeTruthy();
    expect(container.querySelector(".xflow-preview-ruler--y")).toBeTruthy();
    expect(container.querySelector(".xflow-preview-ruler__corner")).toBeTruthy();
    expect(container.querySelector(".xflow-preview-canvas")?.getAttribute("data-ruler")).toBe("true");

    rerender(<XFlowPreview workflow={{ name: "Empty", nodes: [] }} />);
    expect(container.querySelector(".xflow-preview-ruler")).toBeNull();
    expect(container.querySelector(".xflow-preview-canvas")?.getAttribute("data-ruler")).toBe("false");
  });

  it("labels every 100 canvas units where pan and zoom put them, and highlights the selected node", () => {
    flowStore.setState({
      transform: [130, 60, 0.5],
      width: 420,
      height: 300,
      nodeLookup: new Map([["http-id", {
        internals: { positionAbsolute: { x: 100, y: 40 } },
        measured: { width: 264, height: 80 }
      }]])
    });
    const { container } = render(<XFlowPreview workflow={chromeWorkflow} selectedNodeId="http-id" />);

    // 100 units at 50% are 50px; the canvas origin sits at pan − 20px ruler.
    expect(ticks(container, "x")).toEqual([
      ["-200", "10px"], ["-100", "60px"], ["0", "110px"], ["100", "160px"],
      ["200", "210px"], ["300", "260px"], ["400", "310px"], ["500", "360px"]
    ]);
    expect(ticks(container, "y")[0]).toEqual(["0", "40px"]);
    const x = container.querySelector<HTMLElement>(".xflow-preview-ruler--x")!;
    expect(x.style.getPropertyValue("--xflow-preview-ruler-major")).toBe("50px");
    expect(x.style.getPropertyValue("--xflow-preview-ruler-minor")).toBe("5px");
    expect(x.style.getPropertyValue("--xflow-preview-ruler-origin")).toBe("110px");

    // The node's screen extent along each ruler: 130 + 100×0.5 − 20 = 160, 264×0.5 wide.
    expect(highlight(container, "x")?.style.left).toBe("160px");
    expect(highlight(container, "x")?.style.width).toBe("132px");
    expect(highlight(container, "y")?.style.top).toBe("60px");
    expect(highlight(container, "y")?.style.height).toBe("40px");

    // The rulers follow a pan without a re-render of the preview.
    act(() => flowStore.setState({ transform: [30, 60, 0.5] }));
    expect(ticks(container, "x")[0]).toEqual(["0", "10px"]);
    expect(highlight(container, "x")?.style.left).toBe("60px");
  });

  it("widens the labelled step once 100 units would crowd the labels", () => {
    flowStore.setState({ transform: [20, 20, 0.1], width: 600, height: 300 });
    const { container } = render(<XFlowPreview workflow={chromeWorkflow} />);
    // 100 units at 10% would be 10px apart; 500 units (50px) is the first step that fits.
    expect(ticks(container, "x").slice(0, 3)).toEqual([["0", "0px"], ["500", "50px"], ["1000", "100px"]]);
    expect(highlight(container, "x")).toBeNull();
    // 100 units keep their labels as long as they are 36px apart: 40px at 40% …
    act(() => flowStore.setState({ transform: [20, 20, 0.4] }));
    expect(ticks(container, "x").slice(0, 2)).toEqual([["0", "0px"], ["100", "40px"]]);
    // … but not 35px at 35%, where 200 units (70px) take over.
    act(() => flowStore.setState({ transform: [20, 20, 0.35] }));
    expect(ticks(container, "x").slice(0, 2)).toEqual([["0", "0px"], ["200", "70px"]]);
  });

  it("toggles the rulers from the zoom bar and with ⇧R, but not while typing", () => {
    const { container } = render(
      <>
        <input aria-label="outside field" />
        <button type="button">outside button</button>
        <XFlowPreview workflow={chromeWorkflow} />
      </>
    );
    const button = screen.getByRole("button", { name: "标尺 (⇧R)" });
    expect(button.getAttribute("aria-pressed")).toBe("true");

    fireEvent.click(button);
    expect(container.querySelector(".xflow-preview-ruler")).toBeNull();
    expect(button.getAttribute("aria-pressed")).toBe("false");

    pressShift("R");
    expect(container.querySelector(".xflow-preview-ruler")).toBeTruthy();

    // A key aimed at something outside this preview is someone else's.
    pressShift("R", screen.getByRole("textbox", { name: "outside field" }));
    expect(container.querySelector(".xflow-preview-ruler")).toBeTruthy();
    pressShift("R", screen.getByRole("button", { name: "outside button" }));
    expect(container.querySelector(".xflow-preview-ruler")).toBeTruthy();
    // ⌘⇧R is the browser's, not the canvas's.
    fireEvent.keyDown(document.body, { key: "R", shiftKey: true, metaKey: true });
    expect(container.querySelector(".xflow-preview-ruler")).toBeTruthy();
  });

  it("toggles the minimap with ⇧M and re-fits the graph on every flip, as the rulers do", () => {
    reactFlowMock.fitBounds.mockResolvedValue(true);
    const { container } = render(<XFlowPreview workflow={chromeWorkflow} />);
    act(() => {
      latestFlowProps().onInit?.({
        fitBounds: reactFlowMock.fitBounds,
        getNodes: () => [{ id: "start-id" }],
        getNodesBounds: reactFlowMock.getNodesBounds
      });
    });
    reactFlowMock.fitBounds.mockClear();
    const button = screen.getByRole("button", { name: "显示缩略图" });
    expect(button.getAttribute("aria-pressed")).toBe("false");

    pressShift("M");
    expect(container.querySelector(".react-flow__minimap")).toBeTruthy();
    expect(button.getAttribute("aria-pressed")).toBe("true");
    expect(reactFlowMock.fitBounds).toHaveBeenCalledTimes(1);

    fireEvent.click(button);
    expect(container.querySelector(".react-flow__minimap")).toBeNull();
    expect(reactFlowMock.fitBounds).toHaveBeenCalledTimes(2);

    // The rulers are floating layers too: switching them re-fits the same way.
    pressShift("R");
    expect(container.querySelector(".xflow-preview-ruler")).toBeNull();
    expect(reactFlowMock.fitBounds).toHaveBeenCalledTimes(3);
  });

  it("keeps the toggle-triggered re-fit readable, while the zoom bar's fit still shows everything", () => {
    vi.stubGlobal("requestAnimationFrame", vi.fn(() => 1));
    vi.stubGlobal("cancelAnimationFrame", vi.fn());
    // jsdom measures every box as 0×0, which makes the fit fall back to xyflow's own
    // fitBounds (no minZoom to observe). Give only the canvas pane a real box so the
    // fit takes the viewport path and the zoom clamp becomes visible.
    const pane = { x: 0, y: 0, left: 0, top: 0, right: 1000, bottom: 800, width: 1000, height: 800 } as DOMRect;
    const none = { x: 0, y: 0, left: 0, top: 0, right: 0, bottom: 0, width: 0, height: 0 } as DOMRect;
    const rectSpy = vi.spyOn(Element.prototype, "getBoundingClientRect").mockImplementation(function (this: Element) {
      return this.classList.contains("react-flow") ? pane : none;
    });
    try {
      const setViewport = vi.fn(() => Promise.resolve(true));
      // A graph so wide that the "everything fits" zoom is far below the readable floor.
      render(<XFlowPreview workflow={chromeWorkflow} />);
      act(() => {
        latestFlowProps().onInit?.({
          fitBounds: reactFlowMock.fitBounds,
          getNodes: () => [{ id: "start-id" }],
          getNodesBounds: () => ({ x: 0, y: 0, width: 40000, height: 8000 }),
          setViewport
        });
      });
      setViewport.mockClear();

      // Switching the minimap re-fits automatically: held at the readable floor.
      pressShift("M");
      expect(setViewport).toHaveBeenCalledTimes(1);
      expect((setViewport.mock.calls[0] as unknown as [{ zoom: number }])[0].zoom).toBe(READABLE_FIT_ZOOM);

      // The zoom bar's fit is the user's explicit "fit all": down to the 10% floor.
      setViewport.mockClear();
      fireEvent.click(screen.getByRole("button", { name: "适应画布" }));
      expect(setViewport).toHaveBeenCalledTimes(1);
      expect((setViewport.mock.calls[0] as unknown as [{ zoom: number }])[0].zoom).toBe(0.1);
    } finally {
      rectSpy.mockRestore();
    }
  });

  it("lets a new prop value override the local toggle", () => {
    const { container, rerender } = render(<XFlowPreview workflow={chromeWorkflow} minimap />);
    fireEvent.click(screen.getByRole("button", { name: "显示缩略图" }));
    expect(container.querySelector(".react-flow__minimap")).toBeNull();
    // The same value again is not a new instruction.
    rerender(<XFlowPreview workflow={chromeWorkflow} minimap />);
    expect(container.querySelector(".react-flow__minimap")).toBeNull();

    rerender(<XFlowPreview workflow={chromeWorkflow} minimap={false} ruler={false} />);
    rerender(<XFlowPreview workflow={chromeWorkflow} minimap ruler={false} />);
    expect(container.querySelector(".react-flow__minimap")).toBeTruthy();
    expect(container.querySelector(".xflow-preview-ruler")).toBeNull();
  });

  it("steps the zoom by 10% within 10%–200% and shows the current percent", () => {
    flowStore.setState({ transform: [0, 0, 0.5] });
    render(<XFlowPreview workflow={chromeWorkflow} />);
    const bar = screen.getByRole("toolbar", { name: "画布缩放" });
    expect(bar.textContent).toContain("50%");

    fireEvent.click(screen.getByRole("button", { name: "放大" }));
    expect(reactFlowMock.zoomTo).toHaveBeenLastCalledWith(0.6);
    fireEvent.click(screen.getByRole("button", { name: "缩小" }));
    expect(reactFlowMock.zoomTo).toHaveBeenLastCalledWith(0.4);

    act(() => flowStore.setState({ transform: [0, 0, 0.1] }));
    expect(bar.textContent).toContain("10%");
    fireEvent.click(screen.getByRole("button", { name: "缩小" }));
    expect(reactFlowMock.zoomTo).toHaveBeenLastCalledWith(0.1);

    act(() => flowStore.setState({ transform: [0, 0, 1.95] }));
    fireEvent.click(screen.getByRole("button", { name: "放大" }));
    expect(reactFlowMock.zoomTo).toHaveBeenLastCalledWith(2);

    // The baseline's native range carries the domain as min/max (a native
    // slider exposes those as aria-valuemin/aria-valuemax to AT).
    const slider = screen.getByRole("slider", { name: "缩放比例" });
    expect(slider.tagName).toBe("INPUT");
    expect(slider.getAttribute("min")).toBe("10");
    expect(slider.getAttribute("max")).toBe("200");
  });

  it("offers 自动布局 only when the owner can lay the graph out", () => {
    const onAutoLayout = vi.fn();
    const { rerender } = render(<XFlowPreview workflow={chromeWorkflow} />);
    expect(screen.queryByRole("button", { name: "自动布局" })).toBeNull();

    rerender(<XFlowPreview workflow={chromeWorkflow} onAutoLayout={onAutoLayout} />);
    fireEvent.click(screen.getByRole("button", { name: "自动布局" }));
    expect(onAutoLayout).toHaveBeenCalledTimes(1);
  });

  it("drops the zoom bar and its shortcuts when the owner turns it off", () => {
    const { container } = render(<XFlowPreview workflow={chromeWorkflow} zoomBar={false} />);
    expect(screen.queryByRole("toolbar", { name: "画布缩放" })).toBeNull();
    pressShift("M");
    expect(container.querySelector(".react-flow__minimap")).toBeNull();
    // The rulers do not depend on the bar.
    expect(container.querySelector(".xflow-preview-ruler")).toBeTruthy();
  });

  it("leaves ⇧M alone while a field inside the preview has the keyboard", () => {
    const { container } = render(
      <XFlowPreview workflow={{ name: "Empty", nodes: [] }} emptyState={<input aria-label="trigger search" />} />
    );
    pressShift("M", screen.getByRole("textbox", { name: "trigger search" }));
    expect(container.querySelector(".react-flow__minimap")).toBeNull();
    pressShift("M", container.querySelector(".xflow-preview-canvas")!);
    expect(container.querySelector(".react-flow__minimap")).toBeTruthy();
  });

  it("shows the owner's empty state in place of the default note", () => {
    render(
      <XFlowPreview
        workflow={{ name: "Empty", nodes: [] }}
        emptyState={<button type="button">选择触发器</button>}
      />
    );
    expect(screen.queryByText("No nodes to preview")).toBeNull();
    expect(screen.getByRole("button", { name: "选择触发器" })).toBeTruthy();
  });

  it("draws the owner's top-left float as a floating layer, only when given", () => {
    const flow = { name: "Float", nodes: [{ name: "start", type: "xflow.start" }] };
    const { container, rerender } = render(<XFlowPreview workflow={flow} />);
    expect(container.querySelector(".xflow-preview-canvas-float")).toBeNull();
    rerender(<XFlowPreview workflow={flow} canvasFloat={<button type="button">添加节点</button>} />);
    const float = container.querySelector(".xflow-preview-canvas-float")!;
    // Marked like the zoom bar, so every fit keeps the content clear of it.
    expect(float.hasAttribute("data-xflow-float")).toBe(true);
    expect(within(float as HTMLElement).getByRole("button", { name: "添加节点" })).toBeTruthy();
  });

  it("draws the owner's top-right float as its own marked layer, only when given", () => {
    const flow = { name: "FloatEnd", nodes: [{ name: "start", type: "xflow.start" }] };
    const { container, rerender } = render(<XFlowPreview workflow={flow} />);
    expect(container.querySelector(".xflow-preview-canvas-float--end")).toBeNull();

    rerender(
      <XFlowPreview
        workflow={flow}
        canvasFloatEnd={<button type="button">属性</button>}
      />
    );
    const end = container.querySelector(".xflow-preview-canvas-float--end")!;
    expect(end.classList.contains("xflow-preview-canvas-float")).toBe(true);
    expect(end.hasAttribute("data-xflow-float")).toBe(true);
    expect(within(end as HTMLElement).getByRole("button", { name: "属性" })).toBeTruthy();
    // The end variant is its own slot: the top-left float stays empty.
    expect(container.querySelector(".xflow-preview-canvas-float:not(.xflow-preview-canvas-float--end)")).toBeNull();
  });
});

describe("XFlowPreview wait notes (§6.12)", () => {
  const approvalFlow: WorkflowDef = {
    name: "Approval",
    nodes: [{ name: "manager_approval", type: "xflow.approval" }],
    connections: {}
  };
  const suspended: RuntimeSnapshot = {
    status: "running",
    nodes: { manager_approval: { status: "suspended" } }
  };
  const note = { text: "依次审批 2/3 · 等待 finance-mgr", action: "处理" };
  const cardOf = (container: HTMLElement): HTMLElement => {
    const card = container.querySelector<HTMLElement>('[data-node-id="manager_approval"]');
    if (!card) throw new Error("approval card not found");
    return card;
  };

  it("keeps the generic line without an override and shows the owner's note with one", () => {
    const { container, rerender } = render(<XFlowPreview runtime={suspended} workflow={approvalFlow} />);
    expect(cardOf(container).querySelector(".xflow-preview-node__note")?.textContent).toBe("等待信号");

    rerender(
      <XFlowPreview nodeNotes={{ manager_approval: note }} runtime={suspended} workflow={approvalFlow} />
    );
    const line = cardOf(container).querySelector(".xflow-preview-node__note");
    expect(line?.textContent).toContain(note.text);
    // An action without a listener is not drawn: no button that does nothing.
    expect(within(line as HTMLElement).queryByRole("button", { name: "处理" })).toBeNull();
  });

  it("reports the action with the node name and does not also select the node", () => {
    const onNodeAction = vi.fn();
    const onSelectNode = vi.fn();
    const { container } = render(
      <XFlowPreview
        nodeNotes={{ manager_approval: note }}
        onNodeAction={onNodeAction}
        onSelectNode={onSelectNode}
        runtime={suspended}
        workflow={approvalFlow}
      />
    );

    fireEvent.click(within(cardOf(container)).getByRole("button", { name: "处理" }));
    expect(onNodeAction).toHaveBeenCalledWith("manager_approval", "处理");
    expect(onSelectNode).not.toHaveBeenCalled();
  });
});

describe("XFlowPreview node categories and card summaries (§6.13 / §6.1.1)", () => {
  const categoryFlow = {
    id: "wf-category",
    name: "Category flow",
    nodes: [{ id: "review-id", name: "review", type: "acme.review" }],
    connections: {}
  } satisfies WorkflowDef;

  const icon = (container: HTMLElement): HTMLElement => {
    const block = container.querySelector<HTMLElement>(".xflow-preview-node__icon");
    if (!block) throw new Error("category icon block not found");
    return block;
  };

  it("draws the category the descriptor declares, and keeps the prefix map for everything else", () => {
    const { container, rerender } = render(<XFlowPreview workflow={categoryFlow} />);
    // A custom type with no declaration is unknown (the grid icon), never "?".
    expect(icon(container).dataset.cat).toBe("unknown");

    rerender(<XFlowPreview nodeCategories={{ "acme.review": "human" }} workflow={categoryFlow} />);
    expect(icon(container).dataset.cat).toBe("human");

    // A declaration the table does not know does not beat the type's own prefix.
    const builtin = { ...categoryFlow, nodes: [{ id: "http-id", name: "http", type: "xflow.http" }] };
    rerender(<XFlowPreview nodeCategories={{ "xflow.http": "bogus" }} workflow={builtin} />);
    expect(icon(container).dataset.cat).toBe("action");
  });

  it("uses the declared category for the minimap swatch too", () => {
    render(<XFlowPreview minimap nodeCategories={{ "acme.review": "human" }} workflow={categoryFlow} />);
    const nodeColor = reactFlowMock.miniMapProps?.nodeColor;
    expect(nodeColor).toBeTruthy();
    expect(nodeColor?.({ data: latestFlowProps().nodes[0].data })).toBe("var(--xflow-warning)");
  });

  it("keeps the category glyph by default and lets the owner swap it per node", () => {
    const { container, rerender } = render(<XFlowPreview workflow={categoryFlow} />);
    expect(icon(container).querySelector('[data-xflow-icon="grid"]')).toBeTruthy();

    rerender(
      <XFlowPreview
        nodeIcon={(node) => <span data-own-icon={node.type} />}
        workflow={categoryFlow}
      />
    );
    expect(icon(container).querySelector('[data-own-icon="acme.review"]')).toBeTruthy();
    expect(icon(container).querySelector('[data-xflow-icon="grid"]')).toBeNull();
  });

  it("draws the summary rows it was handed, with expression and credential kinds", () => {
    const rows = [
      { label: "审批人", value: "ops-lead → cfo", kind: "plain" as const },
      { label: "条件", value: "$json.tier == \"vip\"", kind: "expr" as const },
      { label: "认证", value: "crm_token", kind: "ref" as const }
    ];
    const { container } = render(<XFlowPreview nodeSummaries={{ "review-id": rows }} workflow={categoryFlow} />);
    const summary = container.querySelector<HTMLElement>(".xflow-preview-node__summary");
    expect(summary).toBeTruthy();
    expect([...summary!.querySelectorAll("dt")].map((cell) => cell.textContent)).toEqual(["审批人", "条件", "认证"]);
    const values = [...summary!.querySelectorAll("dd")];
    expect(values.map((cell) => cell.textContent)).toEqual(["ops-lead → cfo", "$json.tier == \"vip\"", "crm_token"]);
    expect(values.map((cell) => cell.dataset.kind)).toEqual(["plain", "expr", "ref"]);
    // A line clips at the card's fixed width; its title keeps the whole value (E-A).
    expect(values.map((cell) => cell.getAttribute("title"))).toEqual([
      "ops-lead → cfo",
      "$json.tier == \"vip\"",
      "crm_token"
    ]);
    // The credential row carries the key glyph; its value is the reference name.
    expect(values[2].querySelector(".xflow-preview-node__summary-ref")).toBeTruthy();
  });

  it("caps the card at three lines and folds the rest into +n 项", () => {
    const rows = ["a", "b", "c", "d", "e"].map((label) => ({ label, value: `${label}-value`, kind: "plain" as const }));
    const { container } = render(<XFlowPreview nodeSummaries={{ "review-id": rows }} workflow={categoryFlow} />);
    const summary = container.querySelector<HTMLElement>(".xflow-preview-node__summary");
    expect([...summary!.querySelectorAll("dt")].map((cell) => cell.textContent)).toEqual(["a", "b", "更多"]);
    expect([...summary!.querySelectorAll("dd")].map((cell) => cell.textContent)).toEqual(["a-value", "b-value", "+3 项"]);
    // The "+n" fold is the third row, so the card sizes itself to three lines
    // instead of keeping the old fixed reserve.
    expect(summary!.closest(".xflow-preview-node")?.getAttribute("data-summary-rows")).toBe("3");
  });

  it("draws no summary without rows, and drops it below the level-of-detail threshold", () => {
    const { container, rerender } = render(<XFlowPreview workflow={categoryFlow} />);
    expect(container.querySelector(".xflow-preview-node__summary")).toBeNull();
    expect(container.querySelector(".xflow-preview-node")?.getAttribute("data-summary-rows")).toBe("0");

    rerender(
      <XFlowPreview
        nodeSummaries={{ "review-id": [{ label: "审批人", value: "alice", kind: "plain" }] }}
        workflow={categoryFlow}
      />
    );
    expect(container.querySelector(".xflow-preview-node__summary")).toBeTruthy();
    expect(container.querySelector(".xflow-preview-node")?.getAttribute("data-summary-rows")).toBe("1");

    act(() => reactFlowMock.flowStore.setZoom(0.4));
    expect(container.querySelector(".xflow-preview-node__summary")).toBeNull();
    act(() => reactFlowMock.flowStore.setZoom(1));
    expect(container.querySelector(".xflow-preview-node__summary")).toBeTruthy();
  });

  it("draws a required missing value as a danger row", () => {
    const rows = [{ label: "URL", value: "URL 未填写", kind: "missing" as const, path: "/parameters/url" }];
    const { container } = render(<XFlowPreview nodeSummaries={{ "review-id": rows }} workflow={categoryFlow} />);
    const value = container.querySelector<HTMLElement>(".xflow-preview-node__summary dd");
    expect(value?.dataset.kind).toBe("missing");
    expect(value?.textContent).toBe("URL 未填写");
  });

  it("renders each half of a joined row with its own kind", () => {
    const rows = [{
      label: "Method + URL",
      value: "GET /hooks/{{id}}",
      kind: "plain" as const,
      path: "/parameters/method",
      parts: [
        { text: "GET", kind: "plain" as const },
        { text: "/hooks/{{id}}", kind: "mono" as const }
      ]
    }];
    const { container } = render(<XFlowPreview nodeSummaries={{ "review-id": rows }} workflow={categoryFlow} />);
    expect(container.querySelector(".xflow-preview-node__summary-mono")?.textContent).toBe("/hooks/{{id}}");
  });

  it("does not read inherited members for a node keyed like Object.prototype", () => {
    const flow = {
      ...categoryFlow,
      nodes: [{ id: "constructor", name: "constructor", type: "xflow.start" }]
    };
    const { container } = render(<XFlowPreview nodeSummaries={{}} workflow={flow} />);
    expect(container.querySelector(".xflow-preview-node")).toBeTruthy();
    expect(container.querySelector(".xflow-preview-node__summary")).toBeNull();
  });

  it("selects and locates the field when a summary row is pressed", () => {
    const onLocateParameter = vi.fn();
    const rows = [{ label: "URL", value: "https://a", kind: "plain" as const, path: "/parameters/url" }];
    const { container } = render(
      <XFlowPreview nodeSummaries={{ "review-id": rows }} onLocateParameter={onLocateParameter} workflow={categoryFlow} />
    );
    const row = container.querySelector<HTMLElement>(".xflow-preview-node__summary dd");
    expect(row?.getAttribute("role")).toBe("button");
    fireEvent.click(row as HTMLElement);
    expect(onLocateParameter).toHaveBeenCalledWith("review-id", "/parameters/url");
    fireEvent.keyDown(row as HTMLElement, { key: "Enter" });
    expect(onLocateParameter).toHaveBeenCalledTimes(2);
  });
});

describe("XFlowPreview node issue badges (CP-01)", () => {
  const issueFlow = {
    id: "wf-issues",
    name: "Issue flow",
    nodes: [{ id: "api-id", name: "api", type: "xflow.http" }],
    connections: {}
  } satisfies WorkflowDef;
  const card = (container: HTMLElement): HTMLElement => {
    const element = container.querySelector<HTMLElement>('[data-node-id="api-id"]');
    if (!element) throw new Error("issue card not found");
    return element;
  };
  const badge = (container: HTMLElement) => card(container).querySelector<HTMLElement>(".xflow-preview-node__issues");

  it("draws the error count in the danger tone, named and titled for assistive tech", () => {
    const { container } = render(
      <XFlowPreview
        nodeIssues={() => ({ errors: 2, warnings: 1, message: "必填" })}
        workflow={issueFlow}
      />
    );

    const found = badge(container);
    expect(found?.textContent).toBe("2");
    expect(found?.dataset.tone).toBe("danger");
    expect(found?.getAttribute("aria-label")).toBe("2 个参数错误");
    expect(found?.getAttribute("title")).toBe("必填");
    // It rides first in the card's badge strip.
    expect(found?.closest(".xflow-preview-node__badges")).toBeTruthy();
    expect(card(container).querySelectorAll(".xflow-preview-node__badges")).toHaveLength(1);
  });

  it("shows the warning count in the warning tone when there is no error", () => {
    const { container } = render(
      <XFlowPreview nodeIssues={() => ({ errors: 0, warnings: 3 })} workflow={issueFlow} />
    );

    const found = badge(container);
    expect(found?.textContent).toBe("3");
    expect(found?.dataset.tone).toBe("warning");
    expect(found?.getAttribute("aria-label")).toBe("3 个参数警告");
    // No message to show: the title attribute is simply absent.
    expect(found?.hasAttribute("title")).toBe(false);
  });

  it("draws no badge when the owner reports nothing, or supplies no callback", () => {
    const { container, rerender } = render(<XFlowPreview workflow={issueFlow} />);
    expect(badge(container)).toBeNull();
    // E-A: the bottom strip is part of the fixed-height card even when empty;
    // what it must not carry is a badge.
    expect(card(container).querySelector(".xflow-preview-node__meta [data-badge]")).toBeNull();

    rerender(<XFlowPreview nodeIssues={() => undefined} workflow={issueFlow} />);
    expect(badge(container)).toBeNull();
    expect(card(container).querySelector(".xflow-preview-node__meta [data-badge]")).toBeNull();

    // A summary that exists but counts nothing is not a badge either.
    rerender(<XFlowPreview nodeIssues={() => ({ errors: 0, warnings: 0 })} workflow={issueFlow} />);
    expect(badge(container)).toBeNull();
  });

  it("hands the callback the canvas node, so the owner can key findings by name or id", () => {
    const seen: string[] = [];
    render(
      <XFlowPreview
        nodeIssues={(node) => {
          seen.push(`${node.id}/${node.name}`);
          return undefined;
        }}
        workflow={issueFlow}
      />
    );
    expect(seen).toEqual(["api-id/api"]);
  });
});

// §6.2 / ADR-D4 §2.3: the visual grouping boxes from editor_metadata.sections.
// They are read-only decoration: nothing here selects, drags or opens a menu.
describe("XFlowPreview canvas sections (§6.2)", () => {
  const sectionWorkflow = {
    id: "wf-sections",
    name: "Section flow",
    nodes: [
      { id: "start-id", name: "start", type: "xflow.start" },
      { id: "fetch-id", name: "fetch", type: "xflow.http" },
      { id: "approve-id", name: "approve", type: "xflow.approval" },
      { id: "end-id", name: "done", type: "xflow.end" }
    ],
    connections: {}
  } satisfies WorkflowDef;

  const boxes = (): HTMLElement[] => [...document.querySelectorAll<HTMLElement>(".xflow-preview-section")];

  it("draws one box per section, with its title, id and canvas geometry", () => {
    const sections: WorkflowEditorSection[] = [
      { id: "sec-vip", title: "VIP 审批", node_keys: ["approve-id"], x: 640, y: 40, width: 520, height: 240 },
      { id: "sec-normal", title: "普通客户", node_keys: ["fetch-id"], x: 640, y: 330, width: 520, height: 200 }
    ];
    render(<XFlowPreview sections={sections} workflow={sectionWorkflow} />);

    expect(boxes()).toHaveLength(2);
    expect(boxes().map((box) => box.dataset.sectionId)).toEqual(["sec-vip", "sec-normal"]);
    expect(boxes().map((box) => box.querySelector(".xflow-preview-section__title")?.textContent)).toEqual([
      "VIP 审批",
      "普通客户"
    ]);
    expect(boxes()[0]?.style.left).toBe("640px");
    expect(boxes()[0]?.style.top).toBe("40px");
    expect(boxes()[0]?.style.width).toBe("520px");
    expect(boxes()[0]?.style.height).toBe("240px");
    // Flow coordinates require the viewport portal: panning/zooming the canvas
    // moves the boxes with the cards, not with the screen.
    expect(boxes()[0]?.closest(".react-flow__viewport-portal")).toBeTruthy();
  });

  it("derives the accent from the dominant member category, never from a stored colour", () => {
    const sections: WorkflowEditorSection[] = [
      { id: "sec-action", title: "动作", node_keys: ["fetch-id", "fetch-id", "approve-id"], x: 0, y: 0, width: 300, height: 200 },
      { id: "sec-human", title: "审批", node_keys: ["approve-id"], x: 0, y: 300, width: 300, height: 200 }
    ];
    render(<XFlowPreview sections={sections} workflow={sectionWorkflow} />);

    // Two api cards outvote one approval -> action (primary); one approval alone -> human (warning).
    expect(boxes()[0]?.style.getPropertyValue("--xflow-preview-section-category")).toBe("var(--xflow-primary)");
    expect(boxes()[1]?.style.getPropertyValue("--xflow-preview-section-category")).toBe("var(--xflow-warning)");
  });

  it("skips a section that covers no node, has no keys, or has no positive box", () => {
    const sections: WorkflowEditorSection[] = [
      { id: "sec-empty-keys", title: "空", node_keys: [], x: 0, y: 0, width: 300, height: 200 },
      { id: "sec-unknown", title: "已删除", node_keys: ["gone", "also-gone"], x: 0, y: 0, width: 300, height: 200 },
      { id: "sec-no-size", title: "没有尺寸", node_keys: ["fetch-id"], x: 10, y: 10, width: 0, height: 200 },
      { id: "sec-no-keys-field", title: "缺字段", x: 0, y: 0, width: 300, height: 200 },
      { id: "sec-ok", title: "有效", node_keys: ["fetch-id"], x: 10, y: 10, width: 300, height: 200 }
    ];
    render(<XFlowPreview sections={sections} workflow={sectionWorkflow} />);

    expect(boxes()).toHaveLength(1);
    expect(boxes()[0]?.dataset.sectionId).toBe("sec-ok");
  });

  it("renders nothing extra when sections are absent or empty", () => {
    const { rerender } = render(<XFlowPreview workflow={sectionWorkflow} />);
    expect(document.querySelector(".xflow-preview-section-layer")).toBeNull();
    expect(boxes()).toHaveLength(0);

    rerender(<XFlowPreview sections={[]} workflow={sectionWorkflow} />);
    expect(document.querySelector(".xflow-preview-section-layer")).toBeNull();
    expect(boxes()).toHaveLength(0);
  });

  it("keeps a section with no title drawn as a plain frame", () => {
    render(
      <XFlowPreview
        sections={[{ id: "sec-untitled", node_keys: ["fetch-id"], x: 0, y: 0, width: 200, height: 120 }]}
        workflow={sectionWorkflow}
      />
    );

    expect(boxes()).toHaveLength(1);
    expect(boxes()[0]?.querySelector(".xflow-preview-section__title")).toBeNull();
  });

  // WB-01: the editor supplies these optional callbacks to turn the title chip
  // into its writing surface. Standalone hosts get the read-only decoration.
  describe("editable section handles", () => {
    const sections: WorkflowEditorSection[] = [
      { id: "sec-vip", title: "VIP 审批", node_keys: ["approve-id"], x: 0, y: 0, width: 300, height: 200 }
    ];
    const titleChip = (): HTMLElement => {
      const chip = boxes()[0]?.querySelector<HTMLElement>(".xflow-preview-section__title-action");
      if (!chip) throw new Error("section title is not editable");
      return chip;
    };

    it("draws no handles without the owner callbacks", () => {
      render(<XFlowPreview sections={sections} workflow={sectionWorkflow} />);

      expect(boxes()[0]?.querySelector(".xflow-preview-section__title-action")).toBeNull();
      expect(boxes()[0]?.querySelector(".xflow-preview-section__delete")).toBeNull();
      expect(document.querySelector(".xflow-preview-section-layer")?.getAttribute("aria-hidden")).toBe("true");
    });

    it("reports a renamed title on Enter and closes the editor", () => {
      const onSectionTitleChange = vi.fn();
      render(
        <XFlowPreview
          editable
          sections={sections}
          workflow={sectionWorkflow}
          onSectionTitleChange={onSectionTitleChange}
        />
      );

      fireEvent.doubleClick(titleChip());
      const input = within(boxes()[0] as HTMLElement).getByLabelText(/分区标题/);
      fireEvent.change(input, { target: { value: "  重点客户  " } });
      fireEvent.keyDown(input, { key: "Enter" });

      expect(onSectionTitleChange).toHaveBeenCalledTimes(1);
      expect(onSectionTitleChange).toHaveBeenCalledWith("sec-vip", "重点客户");
      expect(boxes()[0]?.querySelector(".xflow-preview-section__title-action")).toBeTruthy();
      // The preview only reports the gesture; the owner's next `sections` prop
      // is what actually renames the box.
      expect(boxes()[0]?.querySelector(".xflow-preview-section__title")?.textContent).toBe("VIP 审批");
    });

    it("reports a rename on blur and drops it on Escape", () => {
      const onSectionTitleChange = vi.fn();
      render(
        <XFlowPreview editable sections={sections} workflow={sectionWorkflow} onSectionTitleChange={onSectionTitleChange} />
      );

      fireEvent.doubleClick(titleChip());
      let input = within(boxes()[0] as HTMLElement).getByLabelText(/分区标题/);
      fireEvent.change(input, { target: { value: "失焦提交" } });
      fireEvent.blur(input);
      expect(onSectionTitleChange).toHaveBeenLastCalledWith("sec-vip", "失焦提交");

      fireEvent.doubleClick(titleChip());
      input = within(boxes()[0] as HTMLElement).getByLabelText(/分区标题/);
      fireEvent.change(input, { target: { value: "不要提交" } });
      fireEvent.keyDown(input, { key: "Escape" });
      expect(onSectionTitleChange).toHaveBeenCalledTimes(1);
      expect(boxes()[0]?.querySelector(".xflow-preview-section__title-action")).toBeTruthy();
    });

    it("reports a delete request with the section id and removes nothing itself", () => {
      const onSectionDelete = vi.fn();
      render(
        <XFlowPreview editable sections={sections} workflow={sectionWorkflow} onSectionDelete={onSectionDelete} />
      );

      fireEvent.click(within(boxes()[0] as HTMLElement).getByRole("button", { name: "删除分区 VIP 审批" }));

      expect(onSectionDelete).toHaveBeenCalledWith("sec-vip");
      expect(boxes()).toHaveLength(1);
    });

    it("leaves an id-less section without handles", () => {
      render(
        <XFlowPreview
          editable
          sections={[{ title: "无 id", node_keys: ["approve-id"], x: 0, y: 0, width: 300, height: 200 }]}
          workflow={sectionWorkflow}
          onSectionDelete={vi.fn()}
          onSectionTitleChange={vi.fn()}
        />
      );

      expect(boxes()).toHaveLength(1);
      expect(boxes()[0]?.querySelector(".xflow-preview-section__title-action")).toBeNull();
      expect(boxes()[0]?.querySelector(".xflow-preview-section__delete")).toBeNull();
    });
  });

  // WB-01 step 2: moving and resizing. jsdom ships no PointerEvent, so each
  // gesture is mouse-shaped and dispatched under the pointer* names React
  // listens for; the handlers only read clientX/clientY, button and target.
  describe("section move and resize (WB-01 step 2)", () => {
    const firePointer = (
      target: Element,
      type: "pointerdown" | "pointermove" | "pointerup" | "pointercancel",
      clientX: number,
      clientY: number
    ): void => {
      fireEvent(target, new window.MouseEvent(type, { bubbles: true, cancelable: true, button: 0, clientX, clientY }));
    };
    const sections: WorkflowEditorSection[] = [
      { id: "sec-move", title: "VIP 审批", node_keys: ["approve-id"], x: 100, y: 50, width: 300, height: 200 }
    ];
    const positions = (): Record<string, { x: number; y: number }> =>
      Object.fromEntries(
        (reactFlowMock.latestProps?.nodes ?? []).map((node) => {
          const flowNode = node as { id: string; position: { x: number; y: number } };
          return [flowNode.id, { ...flowNode.position }];
        })
      );

    it("moves by the pointer delta, follows locally and commits once on release", () => {
      const onSectionGeometryChange = vi.fn();
      render(
        <XFlowPreview
          editable
          onSectionGeometryChange={onSectionGeometryChange}
          sections={sections}
          workflow={sectionWorkflow}
        />
      );
      const box = boxes()[0] as HTMLElement;
      const chip = box.querySelector<HTMLElement>(".xflow-preview-section__title") as HTMLElement;

      firePointer(chip, "pointerdown", 120, 70);
      firePointer(box, "pointermove", 150, 100);
      // The box follows the pointer on the canvas's 16px lattice: 130/80
      // quantizes to 128/80.
      expect(box.style.left).toBe("128px");
      expect(box.style.top).toBe("80px");
      firePointer(box, "pointermove", 180, 130);
      // 160/110 quantizes to 160/112, still without notifying the owner.
      expect(box.style.left).toBe("160px");
      expect(box.style.top).toBe("112px");
      expect(box.style.width).toBe("300px");
      expect(onSectionGeometryChange).not.toHaveBeenCalled();

      firePointer(box, "pointerup", 180, 130);

      expect(onSectionGeometryChange).toHaveBeenCalledTimes(1);
      expect(onSectionGeometryChange).toHaveBeenCalledWith("sec-move", { x: 160, y: 112, width: 300, height: 200 });
      // The preview only reports; the owner's echoed `sections` prop moves it.
      expect(box.style.left).toBe("100px");
      expect(box.style.top).toBe("50px");
    });

    it("captures the pointer only after the drag crosses the threshold", () => {
      // Real browsers retarget the click/dblclick that follows a captured
      // pointer to the capture element. Capturing on pointerdown therefore
      // stole the title chip's rename double-click; jsdom has no pointer
      // capture at all, so the call is stubbed to prove when it happens.
      const proto = HTMLElement.prototype as unknown as {
        setPointerCapture?: (pointerId: number) => void;
        releasePointerCapture?: (pointerId: number) => void;
      };
      const originalCapture = proto.setPointerCapture;
      const originalRelease = proto.releasePointerCapture;
      const setPointerCapture = vi.fn();
      const releasePointerCapture = vi.fn();
      proto.setPointerCapture = setPointerCapture;
      proto.releasePointerCapture = releasePointerCapture;
      try {
        // jsdom's MouseEvent has no pointerId; the handlers only read the
        // coordinates, but releasePointerCapture is skipped without one.
        const firePointerEvent = (
          target: Element,
          type: "pointerdown" | "pointermove" | "pointerup",
          clientX: number,
          clientY: number
        ): void => {
          const event = new window.MouseEvent(type, {
            bubbles: true,
            cancelable: true,
            button: 0,
            clientX,
            clientY
          });
          Object.defineProperty(event, "pointerId", { value: 7 });
          fireEvent(target, event);
        };
        const onSectionGeometryChange = vi.fn();
        render(
          <XFlowPreview
            editable
            onSectionGeometryChange={onSectionGeometryChange}
            onSectionTitleChange={vi.fn()}
            sections={sections}
            workflow={sectionWorkflow}
          />
        );
        const box = boxes()[0] as HTMLElement;
        const chip = within(box).getByRole("button", { name: "重命名分区 VIP 审批" });

        // A press/release that never travels is a plain click: the pointer is
        // never captured, so the browser targets the chip and its double-click
        // opens the title editor.
        firePointerEvent(chip, "pointerdown", 120, 70);
        expect(setPointerCapture).not.toHaveBeenCalled();
        firePointerEvent(chip, "pointerup", 120, 70);
        fireEvent.click(chip);
        fireEvent.doubleClick(chip);
        expect(setPointerCapture).not.toHaveBeenCalled();
        const input = within(box).getByLabelText(/分区标题/);
        expect(input).toBeTruthy();

        // Cancel the rename, then press the chip again and travel less than
        // 4px: the pending press stays uncaptured.
        fireEvent.keyDown(input, { key: "Escape" });
        const chipAgain = within(box).getByRole("button", { name: "重命名分区 VIP 审批" });
        firePointerEvent(chipAgain, "pointerdown", 120, 70);
        firePointerEvent(box, "pointermove", 123, 72);
        expect(setPointerCapture).not.toHaveBeenCalled();
        expect(releasePointerCapture).not.toHaveBeenCalled();
        expect(onSectionGeometryChange).not.toHaveBeenCalled();

        // Past the threshold the gesture becomes a drag and takes capture.
        firePointerEvent(box, "pointermove", 160, 100);
        expect(setPointerCapture).toHaveBeenCalledWith(7);
        firePointerEvent(box, "pointerup", 160, 100);
        expect(releasePointerCapture).toHaveBeenCalledTimes(1);
        expect(onSectionGeometryChange).toHaveBeenCalledWith("sec-move", {
          x: 144,
          y: 80,
          width: 300,
          height: 200
        });
      } finally {
        if (originalCapture === undefined) delete proto.setPointerCapture;
        else proto.setPointerCapture = originalCapture;
        if (originalRelease === undefined) delete proto.releasePointerCapture;
        else proto.releasePointerCapture = originalRelease;
      }
    });

    it("leaves every member card exactly where it was", () => {
      const onSectionGeometryChange = vi.fn();
      render(
        <XFlowPreview
          editable
          onSectionGeometryChange={onSectionGeometryChange}
          sections={sections}
          workflow={sectionWorkflow}
        />
      );
      const before = positions();
      const box = boxes()[0] as HTMLElement;

      firePointer(box.querySelector<HTMLElement>(".xflow-preview-section__title") as HTMLElement, "pointerdown", 120, 70);
      firePointer(box, "pointermove", 220, 170);
      firePointer(box, "pointerup", 220, 170);

      expect(onSectionGeometryChange).toHaveBeenCalledTimes(1);
      expect(positions()).toEqual(before);
    });

    it("converts screen pixels to flow pixels at the current zoom", () => {
      const onSectionGeometryChange = vi.fn();
      render(
        <XFlowPreview
          editable
          onSectionGeometryChange={onSectionGeometryChange}
          sections={sections}
          workflow={sectionWorkflow}
        />
      );
      act(() => reactFlowMock.flowStore.setZoom(2));
      const box = boxes()[0] as HTMLElement;

      firePointer(box.querySelector<HTMLElement>(".xflow-preview-section__title") as HTMLElement, "pointerdown", 100, 100);
      firePointer(box, "pointermove", 160, 140);
      firePointer(box, "pointerup", 160, 140);

      // 60x40 screen pixels are 30x20 flow pixels at 200%; the canvas grid then
      // quantizes 130/70 to 128/64.
      expect(onSectionGeometryChange).toHaveBeenCalledWith("sec-move", { x: 128, y: 64, width: 300, height: 200 });
    });

    it("resizes from the bottom-right handle and never below the minimum", () => {
      const onSectionGeometryChange = vi.fn();
      render(
        <XFlowPreview
          editable
          onSectionGeometryChange={onSectionGeometryChange}
          sections={sections}
          workflow={sectionWorkflow}
        />
      );
      const box = boxes()[0] as HTMLElement;
      const grip = box.querySelector<HTMLElement>(".xflow-preview-section__resize") as HTMLElement;
      expect(grip).toBeTruthy();

      firePointer(grip, "pointerdown", 400, 250);
      firePointer(box, "pointermove", 430, 270);
      firePointer(box, "pointermove", 450, 280);
      firePointer(box, "pointerup", 450, 280);

      expect(onSectionGeometryChange).toHaveBeenCalledTimes(1);
      // 300x200 grows by the 50x30 delta, then snaps to 352x224.
      expect(onSectionGeometryChange).toHaveBeenCalledWith("sec-move", { x: 100, y: 50, width: 352, height: 224 });

      // A second drag far past the top-left corner clamps instead of vanishing.
      firePointer(grip, "pointerdown", 400, 250);
      firePointer(box, "pointermove", -200, -200);
      firePointer(box, "pointerup", -200, -200);

      expect(onSectionGeometryChange).toHaveBeenCalledTimes(2);
      expect(onSectionGeometryChange).toHaveBeenLastCalledWith("sec-move", {
        x: 100,
        y: 50,
        width: 120,
        height: 80
      });
    });

    it("leaves a press on a card inside the box to the card", () => {
      const onSectionGeometryChange = vi.fn();
      render(
        <XFlowPreview
          editable
          onSectionGeometryChange={onSectionGeometryChange}
          sections={sections}
          workflow={sectionWorkflow}
        />
      );
      const box = boxes()[0] as HTMLElement;
      const card = screen.getByRole("button", { name: /^approve node / });
      // The box is drawn behind the cards, not around them: its handler can
      // never see a press that starts on a card.
      expect(box.contains(card)).toBe(false);

      firePointer(card, "pointerdown", 120, 70);
      firePointer(card, "pointermove", 220, 170);
      firePointer(card, "pointerup", 220, 170);

      expect(onSectionGeometryChange).not.toHaveBeenCalled();
      expect(box.style.left).toBe("100px");
    });

    it("draws no move or resize surface without the geometry callback", () => {
      render(
        <XFlowPreview
          editable
          onSectionDelete={vi.fn()}
          onSectionTitleChange={vi.fn()}
          sections={sections}
          workflow={sectionWorkflow}
        />
      );

      expect(boxes()[0]?.querySelector(".xflow-preview-section__resize")).toBeNull();
      expect(boxes()[0]?.classList.contains("xflow-preview-section--interactive")).toBe(false);
    });

    it("keeps a press on the delete button out of the move gesture", () => {
      const onSectionDelete = vi.fn();
      const onSectionGeometryChange = vi.fn();
      render(
        <XFlowPreview
          editable
          onSectionDelete={onSectionDelete}
          onSectionGeometryChange={onSectionGeometryChange}
          sections={sections}
          workflow={sectionWorkflow}
        />
      );
      const box = boxes()[0] as HTMLElement;
      const remove = within(box).getByRole("button", { name: "删除分区 VIP 审批" });

      firePointer(remove, "pointerdown", 120, 70);
      firePointer(box, "pointermove", 220, 170);
      firePointer(box, "pointerup", 220, 170);

      expect(onSectionGeometryChange).not.toHaveBeenCalled();
      fireEvent.click(remove);
      expect(onSectionDelete).toHaveBeenCalledWith("sec-move");
    });
  });
});

// §6.2 canvas gestures: the editor's multi-select surface. The ReactFlow mock
// never runs xyflow's internal selection, so the marquee is exercised through
// the `onNodesChange` deltas xyflow would emit; the prop assertions pin the
// gesture configuration that produces them.
describe("XFlowPreview multi-selection (§6.2)", () => {
  const multiWorkflow = {
    id: "wf-multi",
    name: "Multi flow",
    nodes: [
      { id: "a-id", name: "a", type: "xflow.start" },
      { id: "b-id", name: "b", type: "xflow.function" },
      { id: "c-id", name: "c", type: "xflow.http" }
    ],
    connections: {
      a: { main: [{ node: "b" }] },
      b: { main: [{ node: "c" }] }
    }
  } satisfies WorkflowDef;

  const card = (name: string): HTMLElement =>
    screen.getByRole("button", { name: new RegExp(`^${name} node `) });

  it("configures a marquee on the left button while leaving pan and the context menu reachable", () => {
    render(
      <XFlowPreview
        workflow={multiWorkflow}
        editable
        selectedNodeIds={["a-id", "c-id"]}
        onSelectionChange={vi.fn()}
      />
    );

    const flow = latestFlowProps();
    expect(flow.selectionOnDrag).toBe(true);
    // Partial: a marquee that touches a card selects it.
    expect(flow.selectionMode).toBe("partial");
    // selectionOnDrag only takes effect when panOnDrag !== true; the middle
    // button pans, and xyflow's default Space activation key still works (it is
    // not overridden here), while the right button stays the context menu.
    expect(flow.panOnDrag).toEqual([1]);
    expect(flow.panActivationKeyCode).toBeUndefined();
    expect(flow.multiSelectionKeyCode).toEqual(["Meta", "Control"]);
    expect(flow.elementsSelectable).toBe(true);
    expect(flow.onNodesChange).toEqual(expect.any(Function));

    // Selection is drawn from the owner's set: both cards carry their flag.
    expect(flow.nodes.find((node) => node.id === "a-id")).toMatchObject({ selectable: true, selected: true });
    expect(flow.nodes.find((node) => node.id === "b-id")).toMatchObject({ selectable: true, selected: false });
    expect(flow.nodes.find((node) => node.id === "c-id")).toMatchObject({ selectable: true, selected: true });
  });

  it("keeps the legacy single-selection contract when no owner set is supplied", () => {
    render(<XFlowPreview workflow={multiWorkflow} editable selectedNodeId="a-id" onSelectNode={vi.fn()} />);

    const flow = latestFlowProps();
    expect(flow.selectionOnDrag).toBe(false);
    expect(flow.panOnDrag).toBe(true);
    expect(flow.multiSelectionKeyCode).toBeUndefined();
    expect(flow.nodes.find((node) => node.id === "a-id")).toMatchObject({ selectable: false, selected: true });
  });

  it("reports every node a finished drag moved in one batch, or the one node on the legacy path", () => {
    const dragged = [
      { id: "a-id", position: { x: 10, y: 20 } },
      { id: "c-id", position: { x: 210, y: 20 } }
    ];
    const onPositions = vi.fn();
    const onPosition = vi.fn();
    const first = render(
      <XFlowPreview
        workflow={multiWorkflow}
        editable
        selectedNodeIds={["a-id", "c-id"]}
        onSelectionChange={vi.fn()}
        onNodePositionsChange={onPositions}
        onNodePositionChange={onPosition}
      />
    );

    // xyflow hands the grabbed node plus every node the gesture moved; the
    // batch callback gets all of them at once.
    act(() => { latestFlowProps().onNodeDragStop?.(mouseEventStub(0, 0), dragged[0], dragged); });
    expect(onPositions).toHaveBeenCalledWith(dragged);
    expect(onPosition).not.toHaveBeenCalled();
    first.unmount();

    // Without the batch callback the package keeps its original contract.
    const legacy = vi.fn();
    render(
      <XFlowPreview
        workflow={multiWorkflow}
        editable
        selectedNodeIds={["a-id"]}
        onSelectionChange={vi.fn()}
        onNodePositionChange={legacy}
      />
    );
    act(() => { latestFlowProps().onNodeDragStop?.(mouseEventStub(0, 0), dragged[0], dragged); });
    expect(legacy).toHaveBeenCalledWith("a-id", { x: 10, y: 20 });
  });

  it("composes marquee deltas into graph order and names the primary only when one node was picked", () => {
    const onSelectionChange = vi.fn();
    render(
      <XFlowPreview
        workflow={multiWorkflow}
        editable
        selectedNodeIds={[]}
        onSelectionChange={onSelectionChange}
      />
    );
    const flow = latestFlowProps();

    act(() => {
      flow.onNodesChange?.([
        { id: "c-id", type: "select", selected: true },
        { id: "a-id", type: "select", selected: true }
      ]);
    });
    // Two nodes picked at once: a marquee, so the owner keeps its primary.
    expect(onSelectionChange).toHaveBeenLastCalledWith(["a-id", "c-id"], undefined);

    act(() => {
      flow.onNodesChange?.([{ id: "b-id", type: "select", selected: true }]);
    });
    expect(onSelectionChange).toHaveBeenLastCalledWith(["a-id", "b-id", "c-id"], "b-id");

    act(() => {
      flow.onNodesChange?.([{ id: "a-id", type: "select", selected: false }]);
    });
    expect(onSelectionChange).toHaveBeenLastCalledWith(["b-id", "c-id"], undefined);
  });

  it("toggles a card in and out of the owner's set while the multi-selection key is held", () => {
    const onSelectNode = vi.fn();
    const onSelectionChange = vi.fn();
    render(
      <XFlowPreview
        workflow={multiWorkflow}
        editable
        selectedNodeIds={["a-id"]}
        selectedNodeId="a-id"
        onSelectNode={onSelectNode}
        onSelectionChange={onSelectionChange}
      />
    );
    act(() => reactFlowMock.flowStore.setState({ multiSelectionActive: true }));

    fireEvent.click(card("b"));
    expect(onSelectionChange).toHaveBeenLastCalledWith(["a-id", "b-id"], "b-id");
    // The card's own selection path handled it; the plain-click owner callback
    // must not also fire for the same press.
    expect(onSelectNode).not.toHaveBeenCalled();

    fireEvent.click(card("b"));
    expect(onSelectionChange).toHaveBeenLastCalledWith(["a-id"], undefined);
  });

  it("replaces the whole selection on a plain click", () => {
    const onSelectNode = vi.fn();
    render(
      <XFlowPreview
        workflow={multiWorkflow}
        editable
        selectedNodeIds={["a-id", "b-id"]}
        selectedNodeId="b-id"
        onSelectNode={onSelectNode}
        onSelectionChange={vi.fn()}
      />
    );

    fireEvent.click(card("c"));
    expect(onSelectNode).toHaveBeenCalledWith("c-id");
  });

  it("asks the owner to delete the whole selection in one call", async () => {
    const onDeleteNodes = vi.fn();
    render(
      <XFlowPreview
        workflow={multiWorkflow}
        editable
        selectedNodeIds={["a-id", "b-id", "c-id"]}
        onSelectionChange={vi.fn()}
        onDeleteConnection={vi.fn()}
        onDeleteNodes={onDeleteNodes}
      />
    );

    fireEvent.keyDown(window, { key: "Delete" });
    expect(onDeleteNodes).toHaveBeenCalledTimes(1);
    expect(onDeleteNodes).toHaveBeenCalledWith(["a-id", "b-id", "c-id"]);

    // The library's delete handler must not also remove the marquee-selected
    // edges: the owner removes them together with the nodes in one write.
    const permitted = await latestFlowProps().onBeforeDelete?.({
      nodes: [{ id: "a-id" }],
      edges: latestFlowProps().edges
    });
    expect(permitted?.nodes).toEqual([]);
    expect(permitted?.edges).toEqual([]);

    // With no node selected the press still means "remove the selected edge",
    // which stays xyflow's job.
    const edgeOnly = latestFlowProps().onBeforeDelete;
    const edgePermitted = await edgeOnly?.({ nodes: [], edges: latestFlowProps().edges });
    expect(edgePermitted?.edges.map((edge) => edge.id)).toEqual(["a:main->b:main", "b:main->c:main"]);
  });
});

// React Flow suppresses the pane's own onClick while `selectionOnDrag` owns the
// left button and answers a real gesture from pointerup instead. The canvas
// wrapper replays only a click that no pointer produced, so a plain click still
// clears, while the click trailing a marquee cannot undo what it selected.
describe("XFlowPreview empty-canvas click under the marquee gesture", () => {
  const paneOf = (container: HTMLElement): HTMLElement => {
    const pane = container.querySelector<HTMLElement>(".react-flow__pane");
    if (!pane) throw new Error("pane missing");
    return pane;
  };

  const renderEditable = (
    onPaneClick = vi.fn(),
    extra: Partial<React.ComponentProps<typeof XFlowPreview>> = {}
  ) => render(
    <XFlowPreview
      workflow={contextMenuWorkflow}
      editable
      selectedNodeIds={["http-id"]}
      onSelectionChange={vi.fn()}
      onPaneClick={onPaneClick}
      {...extra}
    />
  );

  it("replays a pane click that no pointer gesture produced", () => {
    const onPaneClick = vi.fn();
    const { container } = renderEditable(onPaneClick);

    fireEvent.click(paneOf(container));

    expect(onPaneClick).toHaveBeenCalledTimes(1);
    // The store reset a real pane click performs, so an owner listening only
    // for onSelectionChange clears too.
    expect(reactFlowMock.flowStore.state.resetSelectedElements).toHaveBeenCalledTimes(1);
    expect(reactFlowMock.flowStore.state.nodesSelectionActive).toBe(false);
  });

  it("leaves the click that follows a real press to React Flow", () => {
    const onPaneClick = vi.fn();
    const { container } = renderEditable(onPaneClick);
    const pane = paneOf(container);

    fireEvent.pointerDown(pane, { button: 0, pointerId: 1 });
    fireEvent.pointerUp(pane, { pointerId: 1 });
    fireEvent.click(pane, { detail: 1 });

    // xyflow answers the no-move press from pointerup itself; replaying the
    // trailing click would call the owner twice.
    expect(onPaneClick).not.toHaveBeenCalled();
  });

  it("keeps the marquee's selection when the release's click follows it", () => {
    const onPaneClick = vi.fn();
    const { container } = renderEditable(onPaneClick);
    const pane = paneOf(container);

    fireEvent.pointerDown(pane, { button: 0, clientX: 10, clientY: 10, pointerId: 1 });
    fireEvent.pointerMove(pane, { clientX: 220, clientY: 180, pointerId: 1 });
    fireEvent.pointerUp(pane, { clientX: 220, clientY: 180, pointerId: 1 });
    fireEvent.click(pane, { detail: 1 });

    expect(onPaneClick).not.toHaveBeenCalled();
    expect(reactFlowMock.flowStore.state.resetSelectedElements).not.toHaveBeenCalled();
  });

  it("ignores clicks on a card and on a connection", () => {
    const onPaneClick = vi.fn();
    const { container } = renderEditable(onPaneClick);

    fireEvent.click(screen.getByRole("button", { name: /^http_1 node / }));
    fireEvent.click(container.querySelector<HTMLElement>("[data-edge-id]")!);

    expect(onPaneClick).not.toHaveBeenCalled();
  });

  it("keeps the pane context menu path untouched", () => {
    const onPaneClick = vi.fn();
    const onPaneContextMenu = vi.fn();
    renderEditable(onPaneClick, { onPaneContextMenu });

    // The mock pane does not wire xyflow's contextmenu handler; invoke the
    // prop xyflow would call, as the context-menu tests do.
    act(() => {
      latestFlowProps().onPaneContextMenu?.(mouseEventStub(30, 40));
    });

    expect(onPaneContextMenu).toHaveBeenCalledTimes(1);
    expect(onPaneClick).not.toHaveBeenCalled();
    expect(reactFlowMock.flowStore.state.resetSelectedElements).not.toHaveBeenCalled();
  });
});
