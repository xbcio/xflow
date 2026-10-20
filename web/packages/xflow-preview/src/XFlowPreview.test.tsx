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
  type WorkflowStatus
} from "@xflow/core";
import { XFlowPreview } from "./index";

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
  };
  const flowStore = {
    state: { transform: [0, 0, 1] } as FlowState,
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
      flowStore.state = { transform: [0, 0, 1] };
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
      ...flowNodes.map((node) => {
        const NodeComponent = flowNodeTypes[node.type as string];
        return NodeComponent ? React.createElement(NodeComponent, { key: node.id as string, ...node }) : null;
      }),
      ...flowEdges.map((edge) => React.createElement("button", {
        key: edge.id as string,
        type: "button",
        "aria-label": edge.ariaLabel as string | undefined,
        "data-edge-id": edge.id as string,
        tabIndex: edge.focusable ? 0 : -1,
        onClick: () => flowProps.onEdgesChange?.([{ id: edge.id as string, type: "select", selected: true }]),
        onKeyDown: (event: React.KeyboardEvent<HTMLButtonElement>) => {
          const deleteKeyCode = flowProps.deleteKeyCode;
          const deleteKeys = Array.isArray(deleteKeyCode) ? deleteKeyCode : [deleteKeyCode];
          if (edge.selected && deleteKeys.includes(event.key)) {
            flowProps.onEdgesDelete?.([edge]);
          }
        }
      })),
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
    ReactFlow,
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
    expect(container.querySelector('[data-node-id="charge"] .xflow-preview-node__type')?.textContent).toBe("xflow.http");
    expect(container.querySelector('[data-node-id="approval"]')?.getAttribute("data-disabled")).toBe("true");
    // A disabled node has no badge of its own: it speaks through its status chip
    // (here the run's "skipped"), and the stripes/55% are CSS on data-disabled.
    expect(container.querySelector('[data-node-id="approval"] .xflow-preview-node__badge')).toBeNull();
    expect(container.querySelector('[data-node-id="approval"] .xflow-preview-node__status')?.textContent)
      .toBe(nodeStatusMeta.skipped.label);
    // The attempt count rides in the card's run-meta row, after the duration.
    expect(container.querySelector('[data-node-id="charge"] .xflow-preview-node__run-meta')?.textContent)
      .toContain("2");
    expect(screen.getByLabelText("start main to charge main")).toBeTruthy();

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

    expect(screenToFlowPosition).toHaveBeenCalledWith({ x: 300, y: 400 });
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
    expect(screen.queryByLabelText("source dependency to target main")).toBeNull();

    const sourceHandle = screen.getByRole("button", {
      name: "source output success; press Enter or Space to start a connection"
    });
    const targetHandle = screen.getByRole("button", {
      name: "target input payload; press Enter or Space to complete a connection"
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
    expect(edge.ariaLabel).toContain("(port no longer exists)");
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
    expect(flow.edgesFocusable).toBe(true);
    expect(flow.elementsSelectable).toBe(true);
    expect(flow.deleteKeyCode).toEqual(["Backspace", "Delete"]);
    expect(flow.onEdgesDelete).toEqual(expect.any(Function));
    expect(flow.edges).toHaveLength(1);

    const edge = screen.getByRole("button", {
      name: "Connection from source success to target payload; press Enter to select, then Delete or Backspace to remove"
    });
    expect(edge.getAttribute("tabindex")).toBe("0");
    fireEvent.click(edge);
    expect(latestFlowProps().edges[0].selected).toBe(true);
    fireEvent.keyDown(screen.getByRole("button", { name: /Connection from source success/ }), { key: "Delete" });

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

    fireEvent.click(screen.getByRole("button", { name: /Connection from source success/ }));
    fireEvent.keyDown(screen.getByRole("button", { name: /Connection from source success/ }), { key: "Delete" });

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
      name: "source output success; press Enter or Space to start a connection"
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
    expect(screenToFlowPosition).toHaveBeenCalledWith({ x: 300, y: 500 });
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
      // xyflow sizes the svg viewBox from style, so the 200x100 box is passed in,
      // not left to CSS (which would crop a 200x150 map).
      style: { width: 200, height: 100 },
      // §6.2's viewport frame: the baseline's 1.5px primary outline.
      maskStrokeColor: "var(--xflow-preview-minimap-view)",
      maskStrokeWidth: 1.5
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
    runMeta: container.querySelectorAll(".xflow-preview-node__run-meta").length,
    errors: container.querySelectorAll(".xflow-preview-node__error").length,
    chipLabels: [...container.querySelectorAll(".xflow-preview-node__status")].map((chip) => chip.textContent)
  });

  it("drops type, run meta, error and chip text below 50% zoom and restores them above", () => {
    const { container } = render(<XFlowPreview workflow={lodWorkflow} runtime={lodRuntime} />);
    const full = shown(container);
    expect(full).toMatchObject({ lod: [null, null], types: 2, runMeta: 1, errors: 1 });
    expect(full.chipLabels.every((label) => label)).toBe(true);

    act(() => flowStore.setZoom(0.4));
    expect(shown(container)).toEqual({
      lod: ["low", "low"],
      types: 0,
      runMeta: 0,
      errors: 0,
      chipLabels: ["", ""]
    });

    act(() => flowStore.setZoom(0.5));
    expect(shown(container)).toEqual(full);
  });

  it("re-renders no card while the zoom moves above 50%", () => {
    render(<XFlowPreview workflow={lodWorkflow} runtime={lodRuntime} />);
    expect(flowStore.cardRenders).toBeGreaterThan(0);

    flowStore.cardRenders = 0;
    for (const zoom of [0.9, 1.4, 0.7, 2, 0.5]) {
      act(() => flowStore.setZoom(zoom));
    }
    expect(flowStore.cardRenders).toBe(0);

    // Crossing the threshold is the one thing that re-renders: once per card.
    act(() => flowStore.setZoom(0.3));
    expect(flowStore.cardRenders).toBe(2);
  });
});

describe("XFlowPreview card layout", () => {
  // An `xflow.http` node under an `error_output` policy declares a main and an error
  // outlet, so its card draws the port row; the run reports a duration, an attempt count and an error, so the
  // run-meta row and the error line are drawn too. One card carries all three.
  const lastLineWorkflow = {
    name: "Last line flow",
    nodes: [{ id: "charge-id", name: "charge", type: "xflow.http", on_error: "error_output" }]
  } satisfies WorkflowDef;
  const lastLineRuntime = {
    status: "failed",
    nodes: { charge: { status: "failed", durationMs: 40, attempts: 2, error: "boom" } }
  } as const;

  it("draws the port row as the card's last line, under run meta and the error", () => {
    const { container } = render(<XFlowPreview workflow={lastLineWorkflow} runtime={lastLineRuntime} />);
    const card = container.querySelector('[data-node-id="charge-id"]');
    const kids = [...(card?.children ?? [])];
    const at = (name: string) => kids.findIndex((kid) => kid.classList.contains(name));

    const meta = at("xflow-preview-node__run-meta");
    const error = at("xflow-preview-node__error");
    const ports = at("xflow-preview-node__ports");
    // Guard: each row must actually be drawn, or the order below proves nothing.
    expect([meta, error, ports].every((index) => index >= 0), `run-meta ${meta}, error ${error}, ports ${ports}`).toBe(true);

    expect(error).toBeGreaterThan(meta);
    expect(ports).toBeGreaterThan(error);
    expect(ports).toBe(kids.length - 1);
    // The error outlet's handle rides in the port row, not in the error line.
    expect(kids[ports]?.querySelector('.xflow-preview-node__handle[data-handle-id="error"]')).not.toBeNull();
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
    const button = screen.getByRole("button", { name: "缩略图 (⇧M)" });
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

  it("lets a new prop value override the local toggle", () => {
    const { container, rerender } = render(<XFlowPreview workflow={chromeWorkflow} minimap />);
    fireEvent.click(screen.getByRole("button", { name: "缩略图 (⇧M)" }));
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

    const slider = screen.getByRole("slider", { name: "缩放比例" });
    expect(slider.getAttribute("aria-valuemin")).toBe("10");
    expect(slider.getAttribute("aria-valuemax")).toBe("200");
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
    // The credential row carries the key glyph; its value is the reference name.
    expect(values[2].querySelector(".xflow-preview-node__summary-ref")).toBeTruthy();
  });

  it("caps the card at three lines and folds the rest into +n 项", () => {
    const rows = ["a", "b", "c", "d", "e"].map((label) => ({ label, value: `${label}-value`, kind: "plain" as const }));
    const { container } = render(<XFlowPreview nodeSummaries={{ "review-id": rows }} workflow={categoryFlow} />);
    const summary = container.querySelector<HTMLElement>(".xflow-preview-node__summary");
    expect([...summary!.querySelectorAll("dt")].map((cell) => cell.textContent)).toEqual(["a", "b", "更多"]);
    expect([...summary!.querySelectorAll("dd")].map((cell) => cell.textContent)).toEqual(["a-value", "b-value", "+3 项"]);
  });

  it("draws no summary without rows, and drops it below the level-of-detail threshold", () => {
    const { container, rerender } = render(<XFlowPreview workflow={categoryFlow} />);
    expect(container.querySelector(".xflow-preview-node__summary")).toBeNull();

    rerender(
      <XFlowPreview
        nodeSummaries={{ "review-id": [{ label: "审批人", value: "alice", kind: "plain" }] }}
        workflow={categoryFlow}
      />
    );
    expect(container.querySelector(".xflow-preview-node__summary")).toBeTruthy();

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
