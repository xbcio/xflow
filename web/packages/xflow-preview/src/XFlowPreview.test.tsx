import * as React from "react";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { WorkflowDef } from "@xflow/core";
import { XFlowPreview } from "./index";

const reactFlowMock = vi.hoisted(() => ({
  latestProps: undefined as Record<string, unknown> | undefined,
  fitBounds: vi.fn<(bounds: unknown, options?: { padding?: number }) => Promise<boolean>>(),
  getNodesBounds: vi.fn(() => ({ x: 0, y: 0, width: 500, height: 200 }))
}));

vi.mock("@xyflow/react", async () => {
  const React = await import("react");

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
    Background: () => null,
    ControlButton: ({ children, ...props }: { children?: React.ReactNode } & Record<string, unknown>) => (
      React.createElement("button", props, children)
    ),
    Controls: ({ children }: { children?: React.ReactNode }) => React.createElement(React.Fragment, null, children),
    Handle,
    MiniMap: () => null,
    Position: { Left: "left", Right: "right" },
    ReactFlow,
    useReactFlow: () => ({
      fitBounds: reactFlowMock.fitBounds,
      getNodes: () => [{ id: "source-id" }],
      getNodesBounds: reactFlowMock.getNodesBounds
    })
  };
});

interface MockFlowEdge extends Record<string, unknown> {
  id: string;
  selected?: boolean;
}

interface MockFlowInstance {
  fitBounds: (bounds: unknown, options?: { padding?: number }) => Promise<boolean>;
  getNodes: () => Array<{ id: string }>;
  getNodesBounds: (nodes: Array<{ id: string }>) => unknown;
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

function latestFlowProps(): MockFlowProps {
  expect(reactFlowMock.latestProps).toBeTruthy();
  return reactFlowMock.latestProps as MockFlowProps;
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
  vi.clearAllMocks();
  vi.unstubAllGlobals();
});

describe("XFlowPreview", () => {
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
    expect(screen.getAllByText("running")).toHaveLength(2);
    expect(screen.getByText("3 nodes")).toBeTruthy();
    expect(screen.getByText("1 running")).toBeTruthy();
    expect(screen.getByText("1 success")).toBeTruthy();
    expect(screen.getByText("1 skipped")).toBeTruthy();
    expect(screen.getAllByText("start")).toHaveLength(2);
    expect(screen.getByText("success")).toBeTruthy();
    expect(screen.getAllByText("charge")).toHaveLength(3);
    expect(screen.getByLabelText("xflow.http icon")).toBeTruthy();
    expect(screen.getAllByText("action").length).toBeGreaterThan(0);
    expect(screen.getByText("input: main")).toBeTruthy();
    expect(screen.getByText("2 attempts")).toBeTruthy();
    expect(screen.getByLabelText("start main to charge main")).toBeTruthy();

    const chargeNode = container.querySelector('[aria-label="charge node running"]');
    expect(chargeNode).toBeTruthy();
    fireEvent.click(chargeNode!);

    expect(screen.getByRole("region", { name: "Selected node details" })).toBeTruthy();
    expect(screen.getByText("Calls payment gateway")).toBeTruthy();
    expect(screen.getByText("Inputs 1")).toBeTruthy();
    expect(screen.getByText("Attempts 2")).toBeTruthy();
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
      getNodesBounds
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

  it("fits measured nodes when the fit-view control is used", () => {
    reactFlowMock.fitBounds.mockResolvedValue(true);
    render(<XFlowPreview workflow={portWorkflow} />);

    fireEvent.click(screen.getByRole("button", { name: "fit view" }));

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
});
