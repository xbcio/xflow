import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { WorkflowDef, WorkflowEditorSection } from "@xflow/core";
import { XFlowEditor, type XFlowEditorSaveContext } from "./index";
import { defaultNodeBox } from "./panels/autoLayout";

// WB-01: the writing half of §6.2's visual sections. A section is drawn by
// the preview and grouped by the outline, both fed from one editor-owned
// state; this suite drives the create / rename / delete gestures through the
// real canvas DOM and then reads the state back through onSave's context.

const canvasNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} 节点 "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  return node;
};

const selectedCards = (): string[] =>
  Array.from(document.querySelectorAll<HTMLElement>('.xflow-preview-node[data-selected="true"]'))
    .map((card) => card.getAttribute("data-node-id") ?? "")
    .sort();

const multiKeyDown = () => fireEvent.keyDown(window, { key: "Meta" });
const multiKeyUp = () => fireEvent.keyUp(window, { key: "Meta" });
const clickCard = (name: string) => fireEvent.click(canvasNode(name));

/** Select two cards with the platform multi-selection key, as the canvas reports it. */
const selectPair = (first: string, second: string): void => {
  clickCard(first);
  multiKeyDown();
  clickCard(second);
  multiKeyUp();
};

const openPaneMenu = (): HTMLElement => {
  const pane = document.querySelector(".react-flow__pane");
  if (!pane) throw new Error("canvas pane not found");
  fireEvent.contextMenu(pane, { clientX: 420, clientY: 260 });
  return screen.getByRole("menu");
};

const createSectionFromMenu = (): void => {
  fireEvent.click(within(openPaneMenu()).getByRole("menuitem", { name: /创建分区/ }));
};

const sectionBoxes = (): HTMLElement[] => [
  ...document.querySelectorAll<HTMLElement>(".xflow-preview-section")
];

const sectionGeometry = (box: HTMLElement) => ({
  x: Number.parseFloat(box.style.left),
  y: Number.parseFloat(box.style.top),
  width: Number.parseFloat(box.style.width),
  height: Number.parseFloat(box.style.height)
});

/** The canvas grid §6.2 paints and every drag quantizes to. */
const snap16 = (value: number): number => Math.round(value / 16) * 16;

const outlineRegion = () => screen.getByRole("region", { name: "大纲" });
const outlineTree = () => within(outlineRegion()).getByRole("tree", { name: "工作流大纲" });

const workflow: WorkflowDef = {
  id: "wf-sections",
  name: "Sections",
  nodes: [
    { name: "alpha", type: "xflow.http", position: { x: 100, y: 100 } },
    { name: "beta", type: "xflow.function", position: { x: 400, y: 200 } }
  ],
  connections: {}
};

/** The same graph as a draft, so 保存 is enabled before anything is persisted. */
const draft: WorkflowDef = { ...workflow, id: undefined };

describe("visual section creation (§6.2)", () => {
  it("draws one section around the selected cards and groups both nodes under it", () => {
    render(<XFlowEditor value={workflow} />);
    selectPair("alpha", "beta");
    expect(selectedCards()).toEqual(["alpha", "beta"]);

    createSectionFromMenu();

    const boxes = sectionBoxes();
    expect(boxes).toHaveLength(1);
    expect(boxes[0]?.dataset.sectionId).toBeTruthy();
    expect(boxes[0]?.querySelector(".xflow-preview-section__title")?.textContent).toBe("分区 1");

    // The box covers both measured cards plus padding: the cards' rendered
    // size is read, not assumed (jsdom reports 0, so the layout default is the
    // same measured fallback 自动布局 uses).
    const box = sectionGeometry(boxes[0] as HTMLElement);
    expect(box.x).toBeLessThanOrEqual(100);
    expect(box.y).toBeLessThanOrEqual(100);
    expect(box.x + box.width).toBeGreaterThanOrEqual(400 + defaultNodeBox.width);
    expect(box.y + box.height).toBeGreaterThanOrEqual(200 + defaultNodeBox.height);

    // §6.0.1: the outline creates the matching section heading with its count.
    const group = within(outlineTree()).getByRole("group", { name: /分区 1/ });
    const rows = within(group).getAllByRole("treeitem").map((row) => row.dataset.nodeKey).sort();
    expect(rows).toEqual(["alpha", "beta"]);
    // A section is pure decoration: it does not steal the node selection.
    expect(selectedCards()).toEqual(["alpha", "beta"]);
  });

  it("names the next section with the first free number", () => {
    render(<XFlowEditor value={workflow} />);

    selectPair("alpha", "beta");
    createSectionFromMenu();
    selectPair("alpha", "beta");
    createSectionFromMenu();

    expect(sectionBoxes().map((box) => box.querySelector(".xflow-preview-section__title")?.textContent))
      .toEqual(["分区 1", "分区 2"]);
  });
});

describe("visual section rename (§6.2)", () => {
  const rename = (box: HTMLElement, title: string): void => {
    const chip = box.querySelector<HTMLElement>(".xflow-preview-section__title-action");
    if (!chip) throw new Error("section title is not editable");
    fireEvent.doubleClick(chip);
    const input = within(box).getByLabelText(/分区标题/);
    fireEvent.change(input, { target: { value: title } });
    fireEvent.keyDown(input, { key: "Enter" });
  };

  it("commits a new title to the canvas and the outline", () => {
    render(<XFlowEditor value={workflow} />);
    selectPair("alpha", "beta");
    createSectionFromMenu();

    rename(sectionBoxes()[0] as HTMLElement, "VIP 审批");

    expect(sectionBoxes()[0]?.querySelector(".xflow-preview-section__title")?.textContent).toBe("VIP 审批");
    expect(within(outlineTree()).getByRole("group", { name: /VIP 审批/ })).toBeTruthy();
    expect(within(outlineTree()).queryByRole("group", { name: /分区 1/ })).toBeNull();
  });

  it("cancels on Escape without touching the title", () => {
    render(<XFlowEditor value={workflow} />);
    selectPair("alpha", "beta");
    createSectionFromMenu();

    const chip = sectionBoxes()[0]?.querySelector<HTMLElement>(".xflow-preview-section__title-action");
    fireEvent.doubleClick(chip as HTMLElement);
    const input = screen.getByLabelText(/分区标题/);
    fireEvent.change(input, { target: { value: "临时标题" } });
    fireEvent.keyDown(input, { key: "Escape" });

    expect(sectionBoxes()[0]?.querySelector(".xflow-preview-section__title")?.textContent).toBe("分区 1");
  });

  it("falls back to the default name when the title is emptied", () => {
    render(<XFlowEditor value={workflow} />);
    selectPair("alpha", "beta");
    createSectionFromMenu();
    rename(sectionBoxes()[0] as HTMLElement, "VIP 审批");

    rename(sectionBoxes()[0] as HTMLElement, "   ");

    expect(sectionBoxes()[0]?.querySelector(".xflow-preview-section__title")?.textContent).toBe("分区 1");
  });
});

describe("visual section delete (§6.2)", () => {
  const confirmDelete = async (box: HTMLElement): Promise<void> => {
    fireEvent.click(within(box).getByRole("button", { name: /删除分区/ }));
    const dialog = await screen.findByRole("dialog");
    fireEvent.click(within(dialog).getByRole("button", { name: "删除" }));
    await waitFor(() => expect(sectionBoxes()).toHaveLength(0));
  };

  it("removes only the box after confirmation and leaves every node in place", async () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    selectPair("alpha", "beta");
    createSectionFromMenu();
    expect(sectionBoxes()).toHaveLength(1);

    await confirmDelete(sectionBoxes()[0] as HTMLElement);

    expect(sectionBoxes()).toHaveLength(0);
    // The nodes stay on the canvas and in the outline.
    expect(canvasNode("alpha")).toBeTruthy();
    expect(canvasNode("beta")).toBeTruthy();
    const rows = within(outlineTree()).getAllByRole("treeitem").map((row) => row.dataset.nodeKey);
    expect(rows).toEqual(["alpha", "beta"]);
    // Removing a visual box is metadata only, never a workflow edit.
    expect(handleChange).not.toHaveBeenCalled();
  });
});

describe("visual section state (§6.2 / WB-01)", () => {
  it("carries a section created in the editor into onSave's context", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef, _context?: XFlowEditorSaveContext) => next);
    render(<XFlowEditor onSave={handleSave} value={draft} />);
    selectPair("alpha", "beta");
    createSectionFromMenu();

    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
    const context = handleSave.mock.calls[0]?.[1] as XFlowEditorSaveContext;
    expect(context.sections).toHaveLength(1);
    const section = context.sections[0] as WorkflowEditorSection;
    expect(section.title).toBe("分区 1");
    // The same stable identity the canvas and positions use.
    expect(section.node_keys).toEqual(["alpha", "beta"]);
    expect(section.id).toBeTruthy();
    expect(section.x).toBeLessThanOrEqual(100);
    expect(section.y).toBeLessThanOrEqual(100);
    expect((section.x ?? 0) + (section.width ?? 0)).toBeGreaterThanOrEqual(400 + defaultNodeBox.width);
    expect((section.y ?? 0) + (section.height ?? 0)).toBeGreaterThanOrEqual(200 + defaultNodeBox.height);
  });

  it("keeps an edited section across an undo of a workflow change", () => {
    // Ruling (a): sections are editor metadata outside the WorkflowDef history.
    render(<XFlowEditor value={workflow} />);

    // One undoable workflow change first, so undo has something to revert.
    fireEvent.contextMenu(canvasNode("alpha"), { clientX: 420, clientY: 260 });
    fireEvent.click(screen.getByRole("menuitem", { name: /禁用节点/ }));

    selectPair("alpha", "beta");
    createSectionFromMenu();
    expect(sectionBoxes()).toHaveLength(1);

    fireEvent.click(
      within(screen.getByRole("group", { name: "常用画布与历史工具" })).getByRole("button", { name: "撤销" })
    );

    // The node change is undone; the section is not part of that snapshot.
    expect(sectionBoxes()).toHaveLength(1);
  });

  it("keeps the one-argument save call for a workflow that still has no sections", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef, _context?: XFlowEditorSaveContext) => next);
    render(<XFlowEditor onSave={handleSave} value={draft} />);

    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
    expect(handleSave.mock.calls[0]).toHaveLength(1);
  });

  it("clears persisted sections when the last box is deleted before a save", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef, _context?: XFlowEditorSaveContext) => next);
    const existing: WorkflowEditorSection[] = [
      { id: "sec-existing", title: "旧分区", node_keys: ["alpha"], x: 0, y: 0, width: 300, height: 200 }
    ];
    render(<XFlowEditor onSave={handleSave} sections={existing} value={draft} />);

    const box = sectionBoxes()[0] as HTMLElement;
    fireEvent.click(within(box).getByRole("button", { name: /删除分区/ }));
    fireEvent.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "删除" }));
    await waitFor(() => expect(sectionBoxes()).toHaveLength(0));

    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
    // The host must receive the emptying, not silence that keeps stale metadata.
    expect(handleSave.mock.calls[0]?.[1]).toEqual({ sections: [] });
  });
});

// WB-01 step 2: moving and resizing a visual section. jsdom ships no
// PointerEvent, so each gesture is mouse-shaped and dispatched under the
// pointer* names React listens for (the section handlers read clientX/clientY);
// card drags keep their own native mouse route through xyflow.
describe("visual section geometry (§6.2 / WB-01 step 2)", () => {
  const firePointer = (
    target: Element,
    type: "pointerdown" | "pointermove" | "pointerup",
    clientX: number,
    clientY: number
  ): void => {
    fireEvent(target, new window.MouseEvent(type, { bubbles: true, cancelable: true, button: 0, clientX, clientY }));
  };

  const titleChip = (box: HTMLElement): HTMLElement => {
    const chip = box.querySelector<HTMLElement>(".xflow-preview-section__title-action");
    if (!chip) throw new Error("section title is not draggable");
    return chip;
  };

  const resizeGrip = (box: HTMLElement): HTMLElement => {
    const grip = box.querySelector<HTMLElement>(".xflow-preview-section__resize");
    if (!grip) throw new Error("section resize handle missing");
    return grip;
  };

  const lastWorkflow = (spy: ReturnType<typeof vi.fn>): WorkflowDef => spy.mock.calls.at(-1)?.[0] as WorkflowDef;

  // React Flow's drag is d3-driven: mouse events on the card, moves on window,
  // `view` attached after construction (vitest's jsdom global is not the
  // Window d3's MouseEvent accepts). Same route as the multi-select suite.
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

  const runDrag = async (name: string, dx: number, dy: number): Promise<void> => {
    const target = canvasNode(name).closest(".xflow-preview-node") as HTMLElement;
    act(() => {
      fireMouse("mousedown", target, 300, 200);
      fireMouse("mousemove", window, 302, 200);
      fireMouse("mousemove", window, 302 + dx, 200 + dy);
      fireMouse("mouseup", window, 302 + dx, 200 + dy);
    });
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
  };

  it("moves only the box and saves the new rectangle with its members unchanged", async () => {
    const handleChange = vi.fn();
    const handleSave = vi.fn(async (next: WorkflowDef, _context?: XFlowEditorSaveContext) => next);
    render(<XFlowEditor onChange={handleChange} onSave={handleSave} value={draft} />);
    selectPair("alpha", "beta");
    createSectionFromMenu();

    const before = sectionGeometry(sectionBoxes()[0] as HTMLElement);
    firePointer(titleChip(sectionBoxes()[0] as HTMLElement), "pointerdown", 200, 150);
    firePointer(sectionBoxes()[0] as HTMLElement, "pointermove", 260, 190);
    firePointer(sectionBoxes()[0] as HTMLElement, "pointerup", 260, 190);

    const moved = sectionGeometry(sectionBoxes()[0] as HTMLElement);
    // A section moves on the same 16px lattice as a card drag.
    expect(moved).toEqual({
      ...before,
      x: snap16(before.x + 60),
      y: snap16(before.y + 40)
    });
    expect(moved.x % 16).toBe(0);
    expect(moved.y % 16).toBe(0);
    // Moving a frame is metadata only: no card moves, so no workflow change.
    expect(handleChange).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
    const context = handleSave.mock.calls[0]?.[1] as XFlowEditorSaveContext;
    expect(context.sections).toHaveLength(1);
    expect(context.sections[0]).toMatchObject({ ...moved, node_keys: ["alpha", "beta"] });
  });

  it("resizes from the bottom-right handle without moving its origin", () => {
    render(<XFlowEditor value={workflow} />);
    selectPair("alpha", "beta");
    createSectionFromMenu();
    const box = sectionBoxes()[0] as HTMLElement;
    const before = sectionGeometry(box);

    firePointer(resizeGrip(box), "pointerdown", 400, 300);
    firePointer(box, "pointermove", 460, 340);
    firePointer(box, "pointerup", 460, 340);

    const resized = sectionGeometry(sectionBoxes()[0] as HTMLElement);
    expect(resized).toEqual({
      x: before.x,
      y: before.y,
      width: snap16(before.width + 60),
      height: snap16(before.height + 40)
    });
    expect(resized.width % 16).toBe(0);
    expect(resized.height % 16).toBe(0);
  });

  it("still drags a card that sits inside a section", async () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    selectPair("alpha", "beta");
    createSectionFromMenu();
    const frame = sectionGeometry(sectionBoxes()[0] as HTMLElement);

    await runDrag("alpha", 30, 10);

    // The card gesture is xyflow's, not the box's: both selected cards move,
    // quantized to the 16px canvas grid, and the frame that covers them stays
    // exactly where it was.
    const moved = Object.fromEntries(
      (lastWorkflow(handleChange).nodes ?? []).map((node) => [node.name, node.position])
    );
    expect(moved.alpha).toEqual({ x: 128, y: 112 });
    expect(moved.beta).toEqual({ x: 432, y: 208 });
    expect(sectionGeometry(sectionBoxes()[0] as HTMLElement)).toEqual(frame);
  });
});
