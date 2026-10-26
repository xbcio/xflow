import { act, fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";

/**
 * The canvas card's own select button. Clicking it goes through the card's
 * selection surface, which reads the platform multi-selection key from React
 * Flow's store (Meta on every platform here: both Meta and Control are bound).
 */
const canvasNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} 节点 "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  return node;
};

const inspector = (): HTMLElement => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });

/** Every card the canvas currently paints as selected, by node key. */
const selectedCards = (): string[] =>
  Array.from(document.querySelectorAll<HTMLElement>('.xflow-preview-node[data-selected="true"]'))
    .map((card) => card.getAttribute("data-node-id") ?? "")
    .sort();

const lastWorkflow = (spy: ReturnType<typeof vi.fn>): WorkflowDef => spy.mock.calls.at(-1)?.[0] as WorkflowDef;

const multiKeyDown = () => fireEvent.keyDown(window, { key: "Meta" });
const multiKeyUp = () => fireEvent.keyUp(window, { key: "Meta" });
const clickCard = (name: string) => fireEvent.click(canvasNode(name));
const undo = () =>
  fireEvent.click(within(screen.getByRole("group", { name: "常用画布与历史工具" })).getByRole("button", { name: "撤销" }));

const workflow: WorkflowDef = {
  id: "wf-multi-select",
  name: "Multi select",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "worker", type: "xflow.function" },
    { name: "tail", type: "xflow.http" }
  ],
  connections: {
    start: { main: [{ node: "worker" }] },
    worker: { main: [{ node: "tail" }] }
  }
};

/** The same graph with pinned coordinates, so a drag has a known origin. */
const placedWorkflow: WorkflowDef = {
  id: "wf-multi-drag",
  name: "Multi drag",
  nodes: [
    // Off-grid on purpose: a gesture elsewhere must leave this stored
    // coordinate exactly as authored.
    { name: "start", type: "xflow.start", kind: "trigger", position: { x: 7, y: 13 } },
    { name: "worker", type: "xflow.function", position: { x: 100, y: 40 } },
    { name: "tail", type: "xflow.http", position: { x: 200, y: 80 } }
  ],
  connections: {
    start: { main: [{ node: "worker" }] },
    worker: { main: [{ node: "tail" }] }
  }
};

/** The card root for one node (the node wrapper React Flow attaches drag to). */
const card = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node[data-node-id="${name}"]`);
  if (!node) throw new Error(`card ${name} not found`);
  return node;
};

const positionsOf = (def: WorkflowDef): Record<string, { x: number; y: number }> =>
  Object.fromEntries((def.nodes ?? []).map((node) => [
    node.name as string,
    { x: node.position?.x ?? 0, y: node.position?.y ?? 0 }
  ]));

/**
 * Drives one real node drag. jsdom has no PointerEvent and no layout, so the
 * gesture is mouse-shaped and the move is a plain pixel delta the canvas
 * applies on top of the pinned position.
 *
 * d3-drag listens on `event.view`; vitest's jsdom global is not the branded
 * Window its MouseEvent constructor accepts, so `view` is attached after
 * construction instead of going through the init dictionary. React Flow's
 * `nodeDragThreshold` (1px) starts the drag on the first move and pins the
 * origin to that pointer, so the first move only crosses the threshold and the
 * second one carries the intended delta past it.
 */
const fireMouse = (type: string, target: EventTarget, x: number, y: number): void => {
  const event = new window.MouseEvent(type, {
    bubbles: true,
    cancelable: true,
    composed: true,
    button: 0,
    clientX: x,
    clientY: y
  });
  Object.defineProperty(event, "view", { value: window });
  if (target === window) window.dispatchEvent(event);
  else fireEvent(target, event);
};

const dragCard = (name: string, dx: number, dy: number): void => {
  const target = card(name);
  fireMouse("mousedown", target, 300, 200);
  fireMouse("mousemove", window, 302, 200);
  fireMouse("mousemove", window, 302 + dx, 200 + dy);
  fireMouse("mouseup", window, 302 + dx, 200 + dy);
};

/**
 * Runs one drag and lets React flush it. d3-drag suppresses the click that
 * follows a drag by installing a capture listener for one tick, so the tick has
 * to pass before the next click (undo, a card pick) can land.
 */
const runDrag = async (name: string, dx: number, dy: number): Promise<void> => {
  act(() => dragCard(name, dx, dy));
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
};

const selectPair = (first: string, second: string): void => {
  clickCard(first);
  multiKeyDown();
  clickCard(second);
  multiKeyUp();
};

describe("XFlowEditor canvas multi-selection", () => {
  it("selects several cards with the multi-selection key and keeps the primary in the Inspector", () => {
    render(<XFlowEditor value={workflow} onChange={vi.fn()} />);
    // The editor opens with the first node selected, as the single-selection
    // rule always did.
    expect(selectedCards()).toEqual(["start"]);

    multiKeyDown();
    clickCard("worker");
    clickCard("tail");
    multiKeyUp();

    // All three cards paint the one selection outline; the Inspector stays on
    // the last node picked.
    expect(selectedCards()).toEqual(["start", "tail", "worker"]);
    expect((within(inspector()).getByLabelText("节点名称") as HTMLInputElement).value).toBe("tail");
  });

  it("collapses to one node on a plain click and drops the others", () => {
    render(<XFlowEditor value={workflow} onChange={vi.fn()} />);
    multiKeyDown();
    clickCard("worker");
    clickCard("tail");
    multiKeyUp();
    expect(selectedCards()).toEqual(["start", "tail", "worker"]);

    clickCard("worker");
    expect(selectedCards()).toEqual(["worker"]);
    expect((within(inspector()).getByLabelText("节点名称") as HTMLInputElement).value).toBe("worker");
  });

  it("deletes every selected node and its edges in one undoable snapshot", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    multiKeyDown();
    clickCard("worker");
    clickCard("tail");
    multiKeyUp();
    expect(selectedCards()).toEqual(["start", "tail", "worker"]);
    // Selecting is not an edit: no snapshot, no host write.
    expect(handleChange).not.toHaveBeenCalled();

    fireEvent.keyDown(window, { key: "Delete" });

    // One commit for the whole batch …
    expect(handleChange).toHaveBeenCalledTimes(1);
    const deleted = lastWorkflow(handleChange);
    expect(deleted.nodes).toEqual([]);
    expect(deleted.connections).toEqual({});
    // … and the Inspector returns to the empty (workflow settings) state.
    expect(screen.getByRole("region", { name: "工作流设置" })).toBeTruthy();

    // Exactly one undo entry: a single undo restores the nodes and both edges.
    undo();
    expect(handleChange).toHaveBeenCalledTimes(2);
    const restored = lastWorkflow(handleChange);
    expect(restored.nodes).toHaveLength(3);
    expect(restored.connections?.start?.main).toEqual([{ node: "worker" }]);
    expect(restored.connections?.worker?.main).toEqual([{ node: "tail" }]);
  });

  it("deletes a single node from the keyboard with the same one-snapshot path", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.keyDown(window, { key: "Delete" });
    expect(handleChange).toHaveBeenCalledTimes(1);
    const deleted = lastWorkflow(handleChange);
    expect((deleted.nodes ?? []).map((node) => node.name)).toEqual(["worker", "tail"]);
    expect(deleted.connections?.start).toBeUndefined();

    undo();
    expect(handleChange).toHaveBeenCalledTimes(2);
    expect((lastWorkflow(handleChange).nodes ?? []).map((node) => node.name)).toEqual(["start", "worker", "tail"]);
  });
});

// §6.2: the marquee is React Flow's `selectionOnDrag`. jsdom ships no
// PointerEvent and no layout, so this drives the real gesture path with a
// mouse-shaped PointerEvent and asserts the model result, not geometry: every
// unmeasured card counts as inside the rectangle.
describe("XFlowEditor marquee selection", () => {
  const dragPane = (from = { x: 10, y: 10 }, to = { x: 900, y: 700 }): void => {
    const pane = document.querySelector(".react-flow__pane");
    if (!pane) throw new Error("pane missing");
    const OriginalPointerEvent = window.PointerEvent;
    // jsdom ships no PointerEvent; xyflow only reads the mouse-shaped fields.
    window.PointerEvent = window.MouseEvent as unknown as typeof PointerEvent;
    try {
      fireEvent.pointerDown(pane, { button: 0, clientX: from.x, clientY: from.y, pointerId: 1 });
      fireEvent.pointerMove(pane, { clientX: to.x, clientY: to.y, pointerId: 1 });
      fireEvent.pointerUp(pane, { clientX: to.x, clientY: to.y, pointerId: 1 });
    } finally {
      window.PointerEvent = OriginalPointerEvent;
    }
  };

  it("selects every card the marquee covers without writing a workflow change", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    dragPane();

    expect(selectedCards()).toEqual(["start", "tail", "worker"]);
    // A selection is not an edit: no commit, no undo snapshot.
    expect(handleChange).not.toHaveBeenCalled();
  });

  it("keeps the current primary while it stays inside the marquee", () => {
    render(<XFlowEditor value={workflow} onChange={vi.fn()} />);
    clickCard("tail");
    expect((within(inspector()).getByLabelText("节点名称") as HTMLInputElement).value).toBe("tail");

    dragPane();

    expect(selectedCards()).toEqual(["start", "tail", "worker"]);
    // The rectangle picked several nodes at once, so it names no primary and
    // the Inspector keeps following the node it already showed.
    expect((within(inspector()).getByLabelText("节点名称") as HTMLInputElement).value).toBe("tail");
  });

  it("leaves the marquee to Space-pan instead of selecting", () => {
    render(<XFlowEditor value={workflow} onChange={vi.fn()} />);
    const pane = document.querySelector(".react-flow__pane");
    if (!pane) throw new Error("pane missing");

    fireEvent.keyDown(window, { key: "Space" });
    // Space turns xyflow's pan mode on: the pane becomes draggable and the
    // marquee (`selectionOnDrag`) steps aside for the duration of the key.
    expect(pane.classList.contains("draggable")).toBe(true);
    dragPane();
    fireEvent.keyUp(window, { key: "Space" });

    expect(pane.classList.contains("draggable")).toBe(false);
    expect(selectedCards()).toEqual(["start"]);
  });

  // React Flow sets the pane's own onClick to undefined while the marquee owns
  // the left button and answers a real click from pointerup instead. The canvas
  // wrapper replays only a click that no pointer gesture produced, so a plain
  // click still clears and a marquee's trailing click cannot undo the marquee.
  it("clears the selection when the empty canvas is clicked without a pointer gesture", () => {
    render(<XFlowEditor value={workflow} onChange={vi.fn()} />);
    clickCard("tail");
    expect(selectedCards()).toEqual(["tail"]);
    const pane = document.querySelector(".react-flow__pane");
    if (!pane) throw new Error("pane missing");

    // What a harness or an assistive activation dispatches: a click with no
    // pointerdown/pointerup behind it.
    fireEvent.click(pane);

    expect(selectedCards()).toEqual([]);
  });

  it("keeps the marquee's selection when the release's click follows it", () => {
    render(<XFlowEditor value={workflow} onChange={vi.fn()} />);
    dragPane();
    expect(selectedCards()).toEqual(["start", "tail", "worker"]);
    const pane = document.querySelector(".react-flow__pane");
    if (!pane) throw new Error("pane missing");

    // A real release dispatches one click (detail 1) right after pointerup;
    // replaying it as a blank-canvas click would wipe the marquee at once.
    fireEvent.click(pane, { detail: 1 });

    expect(selectedCards()).toEqual(["start", "tail", "worker"]);
  });
});

// §6.2 multi-selection: one gesture moves the whole set, and the definition is
// written once on drop — never per mousemove, never once per node. A drag also
// lands on the canvas's 16px grid (canvas snap), so the committed positions are
// the quantized ones xyflow reports, not the raw pointer delta.
describe("XFlowEditor batch drag", () => {
  it("quantizes every selected node to the 16px grid and lands as one undo snapshot", async () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={placedWorkflow} onChange={handleChange} />);

    selectPair("worker", "tail");
    expect(selectedCards()).toEqual(["tail", "worker"]);

    // The native d3 gesture runs outside React's synthetic events: `runDrag`
    // lands its state updates before the toolbar or the next snapshot is read.
    await runDrag("worker", 40, 30);
    // One commit for the whole gesture …
    expect(handleChange).toHaveBeenCalledTimes(1);
    const moved = positionsOf(lastWorkflow(handleChange));
    // The ideal delta (40,30) would leave (140,70) and (240,110); each moved
    // node is snapped independently, so both land on the lattice.
    expect(moved.worker).toEqual({ x: 144, y: 64 });
    expect(moved.tail).toEqual({ x: 240, y: 112 });
    // A node outside the gesture keeps its authored, off-grid coordinate:
    // snapping is a drag-time rule, never a load/save reflow.
    expect(moved.start).toEqual({ x: 7, y: 13 });
    for (const position of [moved.worker, moved.tail]) {
      expect(position.x % 16).toBe(0);
      expect(position.y % 16).toBe(0);
    }

    // … and exactly one undo entry: a single undo returns both nodes at once.
    undo();
    expect(handleChange).toHaveBeenCalledTimes(2);
    expect(positionsOf(lastWorkflow(handleChange))).toEqual(positionsOf(placedWorkflow));
  });

  it("quantizes a single-node drag to the grid with one commit and one undo", async () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={placedWorkflow} onChange={handleChange} />);
    expect(selectedCards()).toEqual(["start"]);

    await runDrag("start", 25, 15);

    expect(handleChange).toHaveBeenCalledTimes(1);
    // (7,13) plus (25,15) is (32,28), which snaps to (32,32).
    const moved = positionsOf(lastWorkflow(handleChange)).start;
    expect(moved).toEqual({ x: 32, y: 32 });
    expect(moved.x % 16).toBe(0);
    expect(moved.y % 16).toBe(0);

    undo();
    expect(handleChange).toHaveBeenCalledTimes(2);
    expect(positionsOf(lastWorkflow(handleChange)).start).toEqual({ x: 7, y: 13 });
  });
});

// §6.2 copy: ⌘/Ctrl+D is the existing shortcut table's 复制选中节点 binding. It
// now covers the whole selection and its internal edges, in one snapshot.
describe("XFlowEditor batch copy", () => {
  it("copies the selection with its internal edge and undoes it in one step", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={placedWorkflow} onChange={handleChange} />);

    selectPair("worker", "tail");
    fireEvent.keyDown(window, { key: "d", metaKey: true });

    expect(handleChange).toHaveBeenCalledTimes(1);
    const copied = lastWorkflow(handleChange);
    expect((copied.nodes ?? []).map((node) => node.name)).toEqual([
      "start", "worker", "tail", "worker_1", "tail_1"
    ]);
    // New keys differ from the sources, and every clone is offset once. A
    // clone carries no `id`: like the single duplicate, its name is its key.
    expect(positionsOf(copied).worker_1).toEqual({ x: 136, y: 76 });
    expect(positionsOf(copied).tail_1).toEqual({ x: 236, y: 116 });
    expect(copied.nodes.find((node) => node.name === "worker_1")?.id).toBeUndefined();
    // Only the worker → tail edge is internal to the copied pair; the source
    // edge from start, which is not copied, stays with the originals only.
    expect(copied.connections?.worker_1?.main).toEqual([{ node: "tail_1" }]);
    expect(copied.connections?.worker?.main).toEqual([{ node: "tail" }]);
    expect(copied.connections?.start?.main).toEqual([{ node: "worker" }]);

    // The selection moves to the copies; the primary follows its own clone.
    expect(selectedCards()).toEqual(["tail_1", "worker_1"]);
    expect((within(inspector()).getByLabelText("节点名称") as HTMLInputElement).value).toBe("tail_1");

    // One undo removes every clone and its copied edge.
    undo();
    expect(handleChange).toHaveBeenCalledTimes(2);
    const restored = lastWorkflow(handleChange);
    expect((restored.nodes ?? []).map((node) => node.name)).toEqual(["start", "worker", "tail"]);
    expect(restored.connections?.worker_1).toBeUndefined();
  });

  it("copies no edge whose other end is outside the selection", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={placedWorkflow} onChange={handleChange} />);

    // start → worker is internal here; worker → tail leaves the copied pair.
    selectPair("start", "worker");
    fireEvent.keyDown(window, { key: "d", metaKey: true });

    const copied = lastWorkflow(handleChange);
    expect((copied.nodes ?? []).map((node) => node.name)).toEqual([
      "start", "worker", "tail", "start_1", "worker_1"
    ]);
    expect(copied.connections?.start_1?.main).toEqual([{ node: "worker_1" }]);
    expect(copied.connections?.worker_1).toBeUndefined();
  });

  it("walks each consecutive copy one offset further from the source", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={placedWorkflow} onChange={handleChange} />);

    clickCard("worker");
    fireEvent.keyDown(window, { key: "d", metaKey: true });
    const first = positionsOf(lastWorkflow(handleChange));
    expect(first.worker_1).toEqual({ x: 136, y: 76 });

    // The selection is the copy now, so the next press copies the copy rather
    // than stacking a second clone exactly on the first one.
    fireEvent.keyDown(window, { key: "d", metaKey: true });
    const second = positionsOf(lastWorkflow(handleChange));
    expect(second.worker_1).toEqual({ x: 136, y: 76 });
    expect(second.worker_2).toEqual({ x: 172, y: 112 });
  });
});
