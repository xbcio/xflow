import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { RuntimeSnapshot, WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";

/** The canvas node's own select button: the canvas cards stay a selectable list beside the outline tree. */
const canvasNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} node "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  return node;
};

const lastWorkflow = (spy: ReturnType<typeof vi.fn>) => spy.mock.calls.at(-1)?.[0] as WorkflowDef;
const historyButton = (name: "撤销" | "重做") =>
  within(screen.getByRole("group", { name: "常用画布与历史工具" })).getByRole("button", { name });
const nodeNames = (workflow: WorkflowDef) => (workflow.nodes ?? []).map((node) => node.name);

/** Right-click on a node card: the canvas forwards it as a node context menu. */
const openNodeMenu = (name: string): HTMLElement => {
  fireEvent.contextMenu(canvasNode(name), { clientX: 420, clientY: 260 });
  return screen.getByRole("menu");
};
const menuItem = (name: string | RegExp) => screen.getByRole("menuitem", { name });
/** Hovering a submenu title opens its popup (antd's default trigger). */
const openSubmenu = async (name: string | RegExp) => {
  const before = screen.getAllByRole("menu").length;
  fireEvent.mouseEnter(menuItem(name));
  await waitFor(() => expect(screen.getAllByRole("menu").length).toBeGreaterThan(before));
};

const workflow: WorkflowDef = {
  id: "wf-context-menu",
  name: "Context menu",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "api", type: "xflow.http", position: { x: 240, y: 120 } },
    { name: "tail", type: "xflow.end", position: { x: 480, y: 120 } }
  ],
  connections: { start: { main: [{ node: "api" }] }, api: { main: [{ node: "tail" }] } }
};

describe("canvas context menu", () => {
  it("opens the node menu on right-click and closes it on Escape", () => {
    render(<XFlowEditor value={workflow} />);

    const menu = openNodeMenu("api");
    expect(within(menu).getByRole("menuitem", { name: /打开属性/ })).toBeTruthy();
    expect(within(menu).getByRole("menuitem", { name: /复制节点/ })).toBeTruthy();
    expect(within(menu).getByRole("menuitem", { name: /删除节点/ })).toBeTruthy();

    fireEvent.keyDown(window, { key: "Escape" });
    expect(screen.queryByRole("menu")).toBeNull();
  });

  it("rebases the menu onto an offset editor root instead of the viewport", () => {
    const { container } = render(<XFlowEditor value={workflow} />);
    const root = container.querySelector<HTMLElement>(".xflow-editor");
    expect(root).toBeTruthy();
    // An embedded host: the editor root starts 240px right / 80px down.
    const bounds = {
      bottom: 980,
      height: 900,
      left: 240,
      right: 1440,
      top: 80,
      width: 1200,
      x: 240,
      y: 80
    } as DOMRect;
    if (root) root.getBoundingClientRect = () => bounds;

    const pane = container.querySelector(".react-flow__pane");
    const focus = vi.spyOn(HTMLElement.prototype, "focus");
    try {
      fireEvent.contextMenu(pane as Element, { clientX: 640, clientY: 380 });

      const anchor = screen.getByRole("menu").closest<HTMLElement>(".xflow-editor-context-menu__anchor");
      expect(anchor).toBeTruthy();
      expect(anchor?.style.left).toBe("400px");
      expect(anchor?.style.top).toBe("300px");
      // Focusing the first row must not scroll the editor root under the menu.
      expect(focus).toHaveBeenCalledWith({ preventScroll: true });
    } finally {
      focus.mockRestore();
    }
  });

  it("duplicates the right-clicked node", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    openNodeMenu("api");
    fireEvent.click(menuItem(/复制节点/));

    const next = lastWorkflow(handleChange);
    expect(next.nodes).toHaveLength(4);
    expect(nodeNames(next).filter((name) => name?.startsWith("api"))).toHaveLength(2);
    expect(screen.queryByRole("menu")).toBeNull();
  });

  it("disables the node through the form's patch path, and undo restores it", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    openNodeMenu("api");
    fireEvent.click(menuItem(/禁用节点/));
    expect(lastWorkflow(handleChange).nodes?.[1]?.disabled).toBe(true);

    // Undo is one step back: the node is enabled again (the field is gone).
    fireEvent.click(historyButton("撤销"));
    expect(lastWorkflow(handleChange).nodes?.[1]?.disabled).toBeUndefined();

    // And the menu offers 禁用节点 once more, not 启用节点.
    openNodeMenu("api");
    expect(menuItem(/禁用节点/)).toBeTruthy();
  });

  it("removes every connection of the node in one undoable step", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    openNodeMenu("api");
    fireEvent.click(menuItem(/断开所有连接/));

    const remaining = Object.values(lastWorkflow(handleChange).connections ?? {})
      .flatMap((ports) => Object.values(ports));
    expect(remaining.every((targets) => targets.length === 0)).toBe(true);

    // One updater, so one undo restores every edge the node had.
    fireEvent.click(historyButton("撤销"));
    expect(lastWorkflow(handleChange).connections).toEqual(workflow.connections);
  });

  it("adds a connection to a node that is not connected yet", async () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    openNodeMenu("start");
    // start already feeds api, so only tail is offered.
    await openSubmenu(/连接到/);
    expect(menuItem("api").getAttribute("aria-disabled")).toBe("true");
    fireEvent.click(menuItem("tail"));

    expect(lastWorkflow(handleChange).connections?.start?.main).toEqual([{ node: "api" }, { node: "tail" }]);
  });

  it("keeps the partial-run items disabled while the host provides no onRunNode", () => {
    render(<XFlowEditor value={workflow} />);

    openNodeMenu("api");
    expect(menuItem(/从此节点运行/).getAttribute("aria-disabled")).toBe("true");
    expect(menuItem(/运行到此节点/).getAttribute("aria-disabled")).toBe("true");
    expect(screen.getAllByText("宿主未提供 onRunNode").length).toBe(2);
  });

  it("runs from a node through the host's onRunNode", async () => {
    const snapshot: RuntimeSnapshot = { status: "success", nodes: { api: { status: "success" } } };
    const onRunNode = vi.fn(async () => snapshot);
    render(<XFlowEditor value={workflow} onRunNode={onRunNode} />);

    openNodeMenu("api");
    fireEvent.click(menuItem(/从此节点运行/));

    await waitFor(() => expect(onRunNode).toHaveBeenCalledTimes(1));
    expect(onRunNode.mock.calls[0]?.[1]).toEqual({ node: "api", mode: "from" });
    // The snapshot lands where a whole-workflow run puts it: the Inspector's 运行 tab.
    fireEvent.click(screen.getByRole("tab", { name: "运行" }));
    // The header pill and the 运行 tab both read nodeStatusMeta (§6.4).
    await waitFor(() => {
      const panel = within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByRole("tabpanel");
      expect(panel.querySelector(".xflow-editor-chip")?.textContent).toBe("成功");
    });
  });

  it("opens the pane menu with the node library and adds the chosen node", async () => {
    const handleChange = vi.fn();
    const { container } = render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const pane = container.querySelector(".react-flow__pane");
    expect(pane).toBeTruthy();
    fireEvent.contextMenu(pane as Element, { clientX: 640, clientY: 380 });

    const menu = screen.getByRole("menu");
    expect(within(menu).getByRole("menuitem", { name: /添加节点/ })).toBeTruthy();

    await openSubmenu(/添加节点/);
    // The tile's icon is part of the accessible name (antd icons label themselves).
    fireEvent.click(menuItem(/HTTP/));

    const added = lastWorkflow(handleChange).nodes?.find((node) => node.type === "xflow.http" && node.name !== "api");
    expect(added).toBeTruthy();
    expect(Number.isFinite(added?.position?.x)).toBe(true);
    expect(Number.isFinite(added?.position?.y)).toBe(true);
  });

  it("leaves the native menu alone in a read-only viewer", () => {
    render(<XFlowEditor value={workflow} readOnlyReason="没有写权限" />);

    fireEvent.contextMenu(canvasNode("api"), { clientX: 420, clientY: 260 });
    expect(screen.queryByRole("menu")).toBeNull();
  });

  it("takes the keyboard focus when it opens through Shift+F10", () => {
    const { container } = render(<XFlowEditor value={workflow} />);
    fireEvent.click(canvasNode("api"));

    const canvas = container.querySelector(".xflow-preview-canvas");
    expect(canvas).toBeTruthy();
    fireEvent.keyDown(canvas as Element, { key: "F10", shiftKey: true });

    const menu = screen.getByRole("menu");
    // Arrow keys and Enter only work if the menu owns the focus.
    expect(menu.contains(document.activeElement)).toBe(true);
    expect(document.activeElement?.getAttribute("role")).toBe("menuitem");
  });

  it("keeps the canvas shortcuts out while the menu is open", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const menu = openNodeMenu("api");
    expect(menu).toBeTruthy();

    // Delete would remove the node the menu points at (React Flow's own delete
    // key), ⌘D would duplicate it. The menu swallows both.
    fireEvent.keyDown(document.body, { key: "Delete" });
    fireEvent.keyDown(document.body, { key: "d", metaKey: true });

    expect(handleChange).not.toHaveBeenCalled();
    expect(screen.getByRole("menu")).toBeTruthy();
    expect(screen.getByRole("menuitem", { name: /删除节点/ })).toBeTruthy();
  });

  it("keeps Tab inside the menu instead of letting it fall out", () => {
    render(<XFlowEditor value={workflow} />);
    const menu = openNodeMenu("api");
    const rows = screen.getAllByRole("menuitem");
    expect(menu.contains(document.activeElement)).toBe(true);

    // Tab must not drop the focus onto the panel behind the menu (the menu
    // would stay open, pointing at nothing). It walks the menu's own rows.
    expect(fireEvent.keyDown(document.body, { key: "Tab" })).toBe(false);
    expect(menu.contains(document.activeElement)).toBe(true);
    expect(document.activeElement).not.toBe(rows[0]);

    fireEvent.keyDown(document.body, { key: "Tab", shiftKey: true });
    expect(menu.contains(document.activeElement)).toBe(true);
    fireEvent.keyDown(document.body, { key: "Escape" });
    expect(screen.queryByRole("menu")).toBeNull();
  });

  it("closes when the node it points at disappears", () => {    const { rerender } = render(<XFlowEditor value={workflow} />);
    openNodeMenu("tail");

    // Any other writer (an undo, a host update) can take the node away.
    rerender(
      <XFlowEditor
        value={{ ...workflow, nodes: (workflow.nodes ?? []).filter((node) => node.name !== "tail") }}
      />
    );
    expect(screen.queryByRole("menu")).toBeNull();
  });

  it("disables 连接到 for a node with no output port instead of doing nothing", () => {
    render(<XFlowEditor value={workflow} />);

    openNodeMenu("tail");
    // tail is xflow.end: it consumes, it never produces.
    expect(menuItem(/连接到/).getAttribute("aria-disabled")).toBe("true");
    expect(screen.getByText("该节点没有输出端口")).toBeTruthy();
  });

  it("enters the header inline rename from 重命名 without leaving the JSON view", async () => {
    render(<XFlowEditor value={workflow} />);

    fireEvent.click(canvasNode("api"));
    fireEvent.click(screen.getByRole("button", { name: "以 JSON 编辑参数" }));
    expect(screen.getByRole("textbox", { name: "参数 JSON" })).toBeTruthy();

    openNodeMenu("api");
    fireEvent.click(menuItem(/重命名/));

    await waitFor(() => {
      const field = screen.getByLabelText("节点显示名") as HTMLInputElement;
      expect(document.activeElement).toBe(field);
      expect(field.value).toBe("api");
    });
    // The rename lives in the header now: the parameters tab is untouched.
    expect(screen.getByRole("textbox", { name: "参数 JSON" })).toBeTruthy();
  });

  it("enters the header rename when 重命名 itself reopens a collapsed Inspector", async () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.click(screen.getByRole("button", { name: "收起属性面板" }));
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();

    openNodeMenu("api");
    fireEvent.click(menuItem(/重命名/));

    await waitFor(() => {
      const field = screen.getByLabelText("节点显示名") as HTMLInputElement;
      expect(document.activeElement).toBe(field);
    });
  });

  it("enters the header rename when 重命名 opens the compact drawer", async () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.click(within(screen.getByRole("toolbar", { name: "编辑器工具栏" })).getByRole("button", { name: "更多操作" }));
    fireEvent.click(await screen.findByRole("menuitem", { name: "进入沉浸布局" }));
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();

    openNodeMenu("api");
    fireEvent.click(menuItem(/重命名/));

    await waitFor(() => {
      const field = screen.getByLabelText("节点显示名") as HTMLInputElement;
      expect(document.activeElement).toBe(field);
    });
  });

  it("offers 适应视图 and 自动布局 on the pane menu, laying the graph out in one step", async () => {
    const handleChange = vi.fn();
    const { container } = render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const pane = container.querySelector(".react-flow__pane");
    fireEvent.contextMenu(pane as Element, { clientX: 640, clientY: 380 });
    const menu = screen.getByRole("menu");
    expect(within(menu).getByRole("menuitem", { name: /适应视图/ })).toBeTruthy();
    fireEvent.click(menuItem(/自动布局/));

    const next = lastWorkflow(handleChange);
    const positions = Object.fromEntries((next.nodes ?? []).map((node) => [node.name ?? "", node.position]));
    expect(positions.start?.x).toBeLessThan(positions.api?.x ?? 0);
    expect(positions.api?.x).toBeLessThan(positions.tail?.x ?? 0);

    // One updater, so one undo restores every position.
    fireEvent.click(historyButton("撤销"));
    expect((lastWorkflow(handleChange).nodes ?? []).map((node) => node.position)).toEqual(
      (workflow.nodes ?? []).map((node) => node.position)
    );
  });
});
