import * as React from "react";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { nodeStatusMeta } from "@xflow/core";
import type { RuntimeSnapshot, WorkflowDef, WorkflowEditorSection } from "@xflow/core";
import type { XFlowPreview as RealXFlowPreview } from "@xflow/preview";
import { XFlowEditor, type XFlowEditorSaveContext } from "./index";

// E5b (spec §6.0.1): the outline tree in the left column's lower zone. The
// builder's ordering rules are covered in panels/outline.test.ts; here the tree
// is read through the DOM: its rows, the selection both ways, the keyboard, the
// status dots and the empty state.

// React Flow never initialises in jsdom, so the preview's view handle would
// never arrive. This pass-through wrapper hands the editor a spy handle the way
// the real preview does once it is live.
const preview = vi.hoisted(() => ({
  handle: { fitView: vi.fn(), fitNode: vi.fn() }
}));
vi.mock("@xflow/preview", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@xflow/preview")>();
  const Wrapped: typeof RealXFlowPreview = (props) => {
    React.useEffect(() => {
      props.onReady?.(preview.handle);
    }, [props.onReady]);
    return React.createElement(actual.XFlowPreview, props);
  };
  return { ...actual, XFlowPreview: Wrapped };
});

beforeEach(() => {
  preview.handle.fitView.mockClear();
  preview.handle.fitNode.mockClear();
});

const workflow: WorkflowDef = {
  id: "wf-outline",
  name: "Outline",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "check", type: "xflow.if" },
    { name: "vip", type: "xflow.http", ui: { label: "VIP 审批" } },
    { name: "normal", type: "xflow.http" },
    { name: "finish", type: "xflow.end" }
  ],
  connections: {
    start: { main: [{ node: "check" }] },
    check: { true: [{ node: "vip" }], false: [{ node: "normal" }] },
    vip: { main: [{ node: "finish" }] },
    normal: { main: [{ node: "finish" }] }
  }
};

const canvasNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} 节点 "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  return node;
};
const outlineRegion = () => screen.getByRole("region", { name: "大纲" });
const tree = () => within(outlineRegion()).getByRole("tree", { name: "工作流大纲" });
const items = () => within(tree()).getAllByRole("treeitem");
/** The node name of a row: its mono second segment, else its only text. */
const rowName = (row: HTMLElement) => row.dataset.nodeKey ?? "";
const rowOf = (name: string): HTMLElement => {
  const row = items().find((candidate) => rowName(candidate) === name);
  if (!row) throw new Error(`outline row ${name} not found`);
  return row;
};
const current = () => items().filter((row) => row.getAttribute("aria-current") === "true").map(rowName);

describe("outline tree (§6.0.1)", () => {
  it("lists the nodes in execution order under 入口 / 流程 / 结束 headings with counts", () => {
    render(<XFlowEditor value={workflow} />);

    const groups = within(tree()).getAllByRole("group");
    expect(groups.map((group) => group.querySelector(".xflow-editor-outline__heading")?.textContent)).toEqual([
      "入口1",
      "流程3",
      "结束1"
    ]);
    expect(items().map(rowName)).toEqual(["start", "check", "vip", "normal", "finish"]);
    // The section heading names its group for assistive tech.
    expect(within(tree()).getByRole("group", { name: /入口/ })).toBeTruthy();
  });

  it("shows the display name with the node name as its mono second segment", () => {
    render(<XFlowEditor value={workflow} />);

    const vip = rowOf("vip");
    expect(vip.querySelector(".xflow-editor-outline__name")?.firstChild?.textContent).toBe("VIP 审批");
    expect(vip.querySelector(".xflow-editor-outline__sub")?.textContent).toBe("vip");
    // No label of its own: the name is not printed twice.
    expect(rowOf("normal").querySelector(".xflow-editor-outline__sub")).toBeNull();
  });

  it("indents a branch one step and names each output port above its nodes", () => {
    render(<XFlowEditor value={workflow} />);

    expect(rowOf("check").hasAttribute("data-indent")).toBe(false);
    expect(rowOf("vip").getAttribute("data-indent")).toBe("1");
    expect(rowOf("normal").getAttribute("data-indent")).toBe("1");
    // The merge steps back out.
    expect(rowOf("finish").hasAttribute("data-indent")).toBe(false);
    const ports = [...tree().querySelectorAll(".xflow-editor-outline__port")].map((port) => port.textContent);
    expect(ports).toEqual(["true", "false"]);
  });

  it("counts the same nodes as the title bar", () => {
    render(<XFlowEditor value={workflow} />);

    const titleCount = outlineRegion().querySelector(".xflow-editor-panel-title__count")?.textContent;
    expect(titleCount).toBe("5");
    expect(items()).toHaveLength(Number(titleCount));
    const sectionCounts = [...tree().querySelectorAll(".xflow-editor-outline__count")].map((count) => Number(count.textContent));
    expect(sectionCounts.reduce((sum, count) => sum + count, 0)).toBe(Number(titleCount));
  });

  it("draws a 20px category icon per row from the node's type", () => {
    render(<XFlowEditor value={workflow} />);

    const icon = rowOf("start").querySelector(".xflow-editor-category-icon");
    expect(icon?.classList.contains("xflow-editor-category-icon--sm")).toBe(true);
    expect(icon?.getAttribute("data-cat")).toBe("control");
    expect(rowOf("vip").querySelector(".xflow-editor-category-icon")?.getAttribute("data-cat")).toBe("action");
  });
});

describe("outline selection (§6.0.1)", () => {
  it("selects the node and centres it on a click, without opening the inspector", () => {
    render(<XFlowEditor value={workflow} />);
    const before = screen.queryByRole("region", { name: /^(属性|工作流设置)$/ });

    fireEvent.click(rowOf("normal"));

    expect(preview.handle.fitNode).toHaveBeenCalledTimes(1);
    expect(preview.handle.fitNode).toHaveBeenCalledWith("normal");
    expect(canvasNode("normal").closest(".xflow-preview-node")?.getAttribute("data-selected")).toBe("true");
    expect(current()).toEqual(["normal"]);
    // The inspector is left as it was: a click here is a locate, not an edit.
    expect(Boolean(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ }))).toBe(Boolean(before));
  });

  it("follows a selection made on the canvas, and scrolls the row into view", () => {
    const scrollIntoView = vi.fn();
    const original = HTMLElement.prototype.scrollIntoView;
    HTMLElement.prototype.scrollIntoView = scrollIntoView;
    try {
      render(<XFlowEditor value={workflow} />);
      scrollIntoView.mockClear();

      fireEvent.click(canvasNode("vip"));

      expect(current()).toEqual(["vip"]);
      expect(rowOf("vip").getAttribute("aria-current")).toBe("true");
      expect(scrollIntoView).toHaveBeenCalledWith({ block: "nearest" });
      // A canvas pick is not an outline click: nothing is re-centred.
      expect(preview.handle.fitNode).not.toHaveBeenCalled();
    } finally {
      HTMLElement.prototype.scrollIntoView = original;
    }
  });

  it("marks exactly one row current at a time", () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.click(canvasNode("check"));
    expect(current()).toEqual(["check"]);
    fireEvent.click(canvasNode("finish"));
    expect(current()).toEqual(["finish"]);
  });
});

describe("outline keyboard (§6.0.1)", () => {
  it("keeps one row in the tab order, the selected one", () => {
    render(<XFlowEditor value={workflow} />);

    const tabbable = items().filter((row) => row.tabIndex === 0).map(rowName);
    expect(tabbable).toEqual(["start"]);

    fireEvent.click(canvasNode("vip"));
    expect(items().filter((row) => row.tabIndex === 0).map(rowName)).toEqual(["vip"]);
  });

  it("hands the tab stop to a node selected elsewhere, even after a row was focused", () => {
    render(<XFlowEditor value={workflow} />);

    act(() => rowOf("start").focus());
    expect(items().filter((row) => row.tabIndex === 0).map(rowName)).toEqual(["start"]);
    fireEvent.click(canvasNode("vip"));
    expect(items().filter((row) => row.tabIndex === 0).map(rowName)).toEqual(["vip"]);
  });

  it("does not let Delete on a row other than the selected one remove the selected node", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("check"));
    const check = rowOf("check");
    act(() => check.focus());
    fireEvent.keyDown(check, { key: "ArrowDown" });
    const vip = document.activeElement as HTMLElement;
    expect(rowName(vip)).toBe("vip");
    // The key is for the row in front of the user, not the selection behind it.
    fireEvent.keyDown(vip, { key: "Delete" });
    fireEvent.keyDown(vip, { key: "Backspace" });
    expect(handleChange).not.toHaveBeenCalled();
    expect(items().map(rowName)).toEqual(["start", "check", "vip", "normal", "finish"]);
  });

  it("keeps focus in the tree when Delete removes the focused, selected row", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.click(canvasNode("check"));
    const check = rowOf("check");
    act(() => check.focus());
    fireEvent.keyDown(check, { key: "Delete" });

    expect(handleChange).toHaveBeenCalled();
    expect(items().map(rowName)).not.toContain("check");
    // Focus lands on the row that took its place rather than dropping to <body>.
    expect(document.activeElement).not.toBe(document.body);
    expect(document.activeElement?.getAttribute("role")).toBe("treeitem");
  });

  it("hands focus to the library search key when Delete removes the last row", () => {
    const single: WorkflowDef = { id: "wf-one", name: "One", nodes: [{ name: "only", type: "xflow.http" }], connections: {} };
    const handleChange = vi.fn();
    render(<XFlowEditor value={single} onChange={handleChange} />);

    fireEvent.click(canvasNode("only"));
    const row = rowOf("only");
    act(() => row.focus());
    fireEvent.keyDown(row, { key: "Delete" });

    expect(handleChange).toHaveBeenCalled();
    expect(within(outlineRegion()).queryByRole("tree")).toBeNull();
    // No row is left to take focus and the empty note cannot hold it: it must
    // not drop to <body>, it goes to the library's search key.
    expect(document.activeElement).not.toBe(document.body);
    expect(document.activeElement).toBe(
      within(screen.getByRole("region", { name: "节点" })).getByRole("button", { name: "搜索节点" })
    );
  });

  it("hands focus to the rail's 节点与大纲 key when the last row goes away and no library is on screen", () => {
    const single: WorkflowDef = { id: "wf-one", name: "One", nodes: [{ name: "only", type: "xflow.http" }], connections: {} };
    const emptied: WorkflowDef = { ...single, nodes: [] };
    const { rerender } = render(<XFlowEditor value={single} readOnlyReason="没有写权限" />);

    // A read-only viewer cannot delete; the node leaves the way the host can
    // there: it swaps the value while the row has focus.
    const row = rowOf("only");
    fireEvent.click(row);
    act(() => row.focus());
    fireEvent.keyDown(row, { key: "Delete" });
    rerender(<XFlowEditor value={emptied} readOnlyReason="没有写权限" />);

    expect(within(outlineRegion()).queryByRole("tree")).toBeNull();
    expect(screen.queryByPlaceholderText(/搜索 Webhook/)).toBeNull();
    expect(document.activeElement).not.toBe(document.body);
    expect(document.activeElement).toBe(screen.getByRole("button", { name: "节点与大纲" }));
  });

  it("moves focus with the arrows, Home and End, skipping the headings, and selects nothing", () => {
    render(<XFlowEditor value={workflow} />);
    const first = rowOf("start");
    act(() => first.focus());

    fireEvent.keyDown(first, { key: "ArrowDown" });
    expect(document.activeElement).toBe(rowOf("check"));
    fireEvent.keyDown(document.activeElement as HTMLElement, { key: "ArrowDown" });
    expect(document.activeElement).toBe(rowOf("vip"));
    fireEvent.keyDown(document.activeElement as HTMLElement, { key: "ArrowUp" });
    expect(document.activeElement).toBe(rowOf("check"));
    fireEvent.keyDown(document.activeElement as HTMLElement, { key: "End" });
    expect(document.activeElement).toBe(rowOf("finish"));
    // Past the ends it stays put.
    fireEvent.keyDown(document.activeElement as HTMLElement, { key: "ArrowDown" });
    expect(document.activeElement).toBe(rowOf("finish"));
    fireEvent.keyDown(document.activeElement as HTMLElement, { key: "Home" });
    expect(document.activeElement).toBe(rowOf("start"));

    // Focus moved, the selection did not, and nothing was centred.
    expect(current()).toEqual(["start"]);
    expect(preview.handle.fitNode).not.toHaveBeenCalled();
    // The focused row now holds the single tab stop.
    expect(items().filter((row) => row.tabIndex === 0).map(rowName)).toEqual(["start"]);
    fireEvent.keyDown(rowOf("start"), { key: "End" });
    expect(items().filter((row) => row.tabIndex === 0).map(rowName)).toEqual(["finish"]);
  });

  it("selects and centres on Enter and on Space", () => {
    render(<XFlowEditor value={workflow} />);

    const check = rowOf("check");
    act(() => check.focus());
    fireEvent.keyDown(check, { key: "Enter" });
    expect(current()).toEqual(["check"]);
    expect(preview.handle.fitNode).toHaveBeenLastCalledWith("check");

    const normal = rowOf("normal");
    act(() => normal.focus());
    fireEvent.keyDown(normal, { key: " " });
    expect(current()).toEqual(["normal"]);
    expect(preview.handle.fitNode).toHaveBeenLastCalledWith("normal");
    expect(preview.handle.fitNode).toHaveBeenCalledTimes(2);
  });
});

describe("outline status dots (§4, §6.0.1)", () => {
  const runtime: RuntimeSnapshot = {
    status: "running",
    nodes: {
      start: { status: "success" },
      check: { status: "failed" },
      vip: { status: "running" },
      normal: { status: "skipped" }
    }
  };

  it("takes each dot's tone from the status table, the same snapshot the canvas reads", () => {
    render(<XFlowEditor runtime={runtime} value={workflow} />);

    for (const [name, status] of [["start", "success"], ["check", "failed"], ["vip", "running"], ["normal", "skipped"]] as const) {
      const row = rowOf(name);
      expect(row.getAttribute("data-status")).toBe(status);
      expect(row.getAttribute("data-tone")).toBe(nodeStatusMeta[status].tone);
      expect(row.querySelector(".xflow-editor-outline__dot")?.getAttribute("title")).toBe(nodeStatusMeta[status].label);
      // The canvas card for the same node says the same thing.
      expect(canvasNode(name).closest(".xflow-preview-node")?.getAttribute("data-status")).toBe(status);
    }
    expect(rowOf("start").getAttribute("data-tone")).toBe("success");
    expect(rowOf("check").getAttribute("data-tone")).toBe("danger");
  });

  it("marks a node the run has not reached pending (the status the dashed ring is drawn for), and a skipped one neutral", () => {
    render(<XFlowEditor runtime={runtime} value={workflow} />);

    expect(rowOf("finish").getAttribute("data-status")).toBe("pending");
    expect(rowOf("finish").getAttribute("data-tone")).toBe("neutral");
    expect(rowOf("normal").getAttribute("data-tone")).toBe("neutral");
  });

  it("is all pending when there is no run", () => {
    render(<XFlowEditor value={workflow} />);

    expect(items().map((row) => row.getAttribute("data-status"))).toEqual(["pending", "pending", "pending", "pending", "pending"]);
  });

  it("reads a disabled node as disabled until the run reports it", () => {
    render(<XFlowEditor value={{ ...workflow, nodes: workflow.nodes?.map((node) => (node.name === "normal" ? { ...node, disabled: true } : node)) }} />);

    expect(rowOf("normal").getAttribute("data-status")).toBe("disabled");
  });
});

describe("outline empty state", () => {
  it("says what fills it when the workflow has no node, and has no tree", () => {
    render(<XFlowEditor value={{ ...workflow, nodes: [], connections: {} }} />);

    expect(within(outlineRegion()).queryByRole("tree")).toBeNull();
    expect(within(outlineRegion()).getByText("添加节点后，这里按执行顺序列出。")).toBeTruthy();
    expect(outlineRegion().querySelector(".xflow-editor-panel-title__count")?.textContent).toBe("0");
  });
});

// §6.2 / WB-02: editor_metadata.sections is read-only input here. The editor
// forwards it to the canvas and the outline, and echoes it back through the
// save context so a host rebuild of editor_metadata cannot drop it.
describe("visual sections (§6.2 / WB-02)", () => {
  const sections: WorkflowEditorSection[] = [
    { id: "sec-vip", title: "VIP 审批", node_keys: ["vip"], x: 900, y: 40, width: 400, height: 200 }
  ];
  const sectionBox = (): HTMLElement | null =>
    document.querySelector<HTMLElement>('.xflow-preview-section[data-section-id="sec-vip"]');

  it("draws the section box on the canvas and groups the outline under its title", () => {
    render(<XFlowEditor sections={sections} value={workflow} />);

    const box = sectionBox();
    expect(box).toBeTruthy();
    expect(box?.querySelector(".xflow-preview-section__title")?.textContent).toBe("VIP 审批");
    expect(box?.style.left).toBe("900px");
    expect(box?.style.width).toBe("400px");

    const groups = within(tree()).getAllByRole("group");
    expect(groups.map((group) => group.querySelector(".xflow-editor-outline__heading")?.textContent)).toEqual([
      "入口1",
      "流程2",
      "VIP 审批1",
      "结束1"
    ]);
    // The covered node moved out of 流程; no row was duplicated or lost.
    expect(items().map(rowName)).toEqual(["start", "check", "normal", "vip", "finish"]);
    expect(within(tree()).getByRole("group", { name: /VIP 审批/ }).getAttribute("data-section-id")).toBe("sec-vip");
  });

  it("keeps the canvas and outline exactly as before when no sections are supplied", () => {
    render(<XFlowEditor value={workflow} />);

    expect(document.querySelector(".xflow-preview-section")).toBeNull();
    const groups = within(tree()).getAllByRole("group");
    expect(groups.map((group) => group.querySelector(".xflow-editor-outline__heading")?.textContent)).toEqual([
      "入口1",
      "流程3",
      "结束1"
    ]);
    expect(items().map(rowName)).toEqual(["start", "check", "vip", "normal", "finish"]);
  });

  it("hands the sections to onSave so a save cannot silently drop them", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef, _context?: XFlowEditorSaveContext) => next);
    // A draft (no persisted id) opens with 保存 enabled; the point here is the
    // arguments, not the clean/dirty gate.
    const draft: WorkflowDef = { ...workflow, id: undefined };
    render(<XFlowEditor onSave={handleSave} sections={sections} value={draft} />);

    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
    expect(handleSave.mock.calls[0]?.[0]).toMatchObject({ name: workflow.name });
    expect(handleSave.mock.calls[0]?.[1]).toEqual({ sections });
    // The editor kept drawing them: a save does not consume its editor metadata.
    expect(sectionBox()).toBeTruthy();
  });

  it("keeps the old one-argument save call when the workflow has no sections", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef, _context?: XFlowEditorSaveContext) => next);
    const draft: WorkflowDef = { ...workflow, id: undefined };
    render(<XFlowEditor onSave={handleSave} value={draft} />);

    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
    expect(handleSave.mock.calls[0]).toHaveLength(1);
  });
});
