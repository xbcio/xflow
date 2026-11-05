import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import * as React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { RuntimeSnapshot, WorkflowDef } from "@xflow/core";

import { XFlowEditor } from "./index";

const workflow: WorkflowDef = {
  id: "wf-toolbar",
  name: "Toolbar workflow",
  spec: "1.0",
  version: "1.0.0",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "api", type: "xflow.http" }
  ],
  connections: { start: { main: [{ node: "api" }] } }
};

/**
 * A hand-driven ResizeObserver. §6.3 folds the top bar on the header's own
 * width, so both the editor root and the header must be measurable from one
 * `setWidth` call (they span the same box).
 */
const observed: { callback: ResizeObserverCallback; targets: Set<Element> }[] = [];
class MockResizeObserver {
  readonly targets = new Set<Element>();
  private readonly callback: ResizeObserverCallback;
  constructor(callback: ResizeObserverCallback) {
    this.callback = callback;
    observed.push({ callback, targets: this.targets });
  }
  observe(target: Element) {
    this.targets.add(target);
  }
  unobserve(target: Element) {
    this.targets.delete(target);
  }
  disconnect() {
    this.targets.clear();
  }
}

/** Fold tiers (§6.3): 1327 / 1100 / 1083 / 924, measured on the real bar. */
function setWidth(width: number): void {
  act(() => {
    for (const observer of [...observed]) {
      for (const target of [...observer.targets]) {
        if (!(target instanceof HTMLElement)) continue;
        if (!(target.classList.contains("xflow-editor") || target.classList.contains("xflow-editor-toolbar"))) continue;
        observer.callback(
          [{ target, contentRect: { width, height: 800 } } as unknown as ResizeObserverEntry],
          observer as unknown as ResizeObserver
        );
      }
    }
  });
}

const originalResizeObserver = globalThis.ResizeObserver;
beforeEach(() => {
  observed.length = 0;
  globalThis.ResizeObserver = MockResizeObserver as unknown as typeof ResizeObserver;
});
afterEach(() => {
  globalThis.ResizeObserver = originalResizeObserver;
});

const canvasTools = () => screen.getByRole("toolbar", { name: "画布工具" });
const toolNames = () => within(canvasTools()).getAllByRole("button").map((button) => button.getAttribute("aria-label"));

describe("top bar canvas tools (§6.3, 2026-11-04)", () => {
  it("draws the eight tools in order and makes 选择 / 连线 mutually exclusive", () => {
    const onRunNode = vi.fn(async () => ({ status: "success" }) as RuntimeSnapshot);
    render(<XFlowEditor value={workflow} onRunNode={onRunNode} />);

    expect(toolNames()).toEqual([
      "撤销",
      "重做",
      "选择",
      "连线",
      "添加节点",
      "适应视图",
      "测试此节点",
      "运行工作流"
    ]);

    const editor = screen.getByRole("region", { name: "Toolbar workflow 编辑器" });
    const select = within(canvasTools()).getByRole("button", { name: "选择" });
    const connect = within(canvasTools()).getByRole("button", { name: "连线" });
    expect(select.getAttribute("aria-pressed")).toBe("true");
    expect(connect.getAttribute("aria-pressed")).toBe("false");

    fireEvent.click(connect);
    expect(editor.getAttribute("data-canvas-tool")).toBe("connect");
    expect(connect.getAttribute("aria-pressed")).toBe("true");
    expect(select.getAttribute("aria-pressed")).toBe("false");

    // 测试此节点 is the only tool keyed on onRunNode. The editor starts with a
    // node selected, so it is live; clearing the selection disables it.
    const runToNode = within(canvasTools()).getByRole("button", { name: "测试此节点" }) as HTMLButtonElement;
    expect(runToNode.disabled).toBe(false);
    // Esc leaves 连线 first, then clears the canvas selection.
    fireEvent.keyDown(window, { key: "Escape" });
    fireEvent.keyDown(window, { key: "Escape" });
    expect((within(canvasTools()).getByRole("button", { name: "测试此节点" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("drops 测试此节点 when the host supplies no onRunNode", () => {
    render(<XFlowEditor value={workflow} />);

    expect(toolNames()).toEqual(["撤销", "重做", "选择", "连线", "添加节点", "适应视图", "运行工作流"]);
  });

  it("keeps 运行 as the bar's only primary button and drops the 运行选项 split", () => {
    const onRun = vi.fn(async () => ({ status: "success" }) as RuntimeSnapshot);
    render(<XFlowEditor value={workflow} onRun={onRun} />);

    const primaries = Array.from(
      screen.getByRole("toolbar", { name: "编辑器工具栏" }).querySelectorAll<HTMLElement>(".ant-btn-color-primary")
    );
    expect(primaries).toHaveLength(1);
    expect(primaries[0].getAttribute("aria-label")).toBe("运行工作流");
    // §6.3 overrides the 2026-10-17 placeholder ruling.
    expect(screen.queryByRole("button", { name: "运行选项" })).toBeNull();
  });

  it("hooks both segmented glyphs so preflight cannot drop them off the line", async () => {
    render(<XFlowEditor value={workflow} />);
    expect(document.querySelector(".xflow-editor-mode-segmented__icon svg")).not.toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "主题设置" }));
    await screen.findByText("颜色模式");
    expect(document.querySelector(".xflow-editor-theme-segmented__icon svg")).not.toBeNull();
  });

  it("keeps the separator before 运行 whether or not 测试此节点 is present", () => {
    const view = render(
      <XFlowEditor value={workflow} onRunNode={vi.fn(async () => ({ status: "success" }) as RuntimeSnapshot)} />
    );
    expect(within(canvasTools()).getAllByRole("separator")).toHaveLength(3);

    view.unmount();
    render(<XFlowEditor value={workflow} />);
    // 1–2 │ 3–6 │ 8: the run key still reads as its own segment.
    expect(within(canvasTools()).getAllByRole("separator")).toHaveLength(2);
  });

  it("gives 发布 the upload glyph", () => {
    render(<XFlowEditor value={workflow} onPublish={async (next) => next} />);

    const publish = screen.getByRole("button", { name: "发布" });
    expect(publish.querySelector('svg[data-xflow-icon="upload"]')).not.toBeNull();
  });

  it("puts an icon in each 编辑 | 运行记录 option and no tools while replaying", () => {
    render(<XFlowEditor value={workflow} mode="runs" />);

    const segmented = document.querySelector(".xflow-editor-mode-segmented");
    expect(segmented).not.toBeNull();
    expect(segmented?.querySelectorAll("svg[data-xflow-icon]")).toHaveLength(2);
    expect(within(segmented as HTMLElement).getByText("编辑")).toBeTruthy();
    expect(within(segmented as HTMLElement).getByText("运行记录")).toBeTruthy();

    // §6.3: the replay canvas is read-only, so the middle group is not drawn.
    expect(screen.queryByRole("toolbar", { name: "画布工具" })).toBeNull();
    expect(screen.queryByRole("button", { name: "撤销" })).toBeNull();
  });
});

describe("top bar folding (§6.3)", () => {
  const openMenu = async () => {
    fireEvent.click(within(screen.getByRole("toolbar", { name: "编辑器工具栏" })).getByRole("button", { name: "更多操作" }));
    return within(await screen.findByRole("menu"));
  };
  const closeMenu = async () => {
    fireEvent.click(within(screen.getByRole("toolbar", { name: "编辑器工具栏" })).getByRole("button", { name: "更多操作" }));
    await waitFor(() => expect(screen.queryByRole("menu")).toBeNull());
  };
  const menuLabels = (menu: ReturnType<typeof within>) =>
    menu.getAllByRole("menuitem").map((item) => item.textContent);

  it("moves 校验 / 主题, then the subtitle, then the canvas extras as the header narrows", async () => {
    render(<XFlowEditor value={workflow} />);
    const toolbar = screen.getByRole("toolbar", { name: "编辑器工具栏" });

    // Tier 0: the menu carries only the view-level entries.
    setWidth(1600);
    expect(toolbar.getAttribute("data-fold")).toBe("0");
    expect(within(toolbar).getByRole("button", { name: "校验" })).toBeTruthy();
    expect(within(toolbar).getByRole("button", { name: "主题设置" })).toBeTruthy();
    expect(toolbar.querySelector(".xflow-editor-subtitle")?.textContent).toBe("wf-toolbar · 2 个节点");
    expect(toolbar.querySelector(".xflow-editor-spec-chip")?.textContent).toBe("spec 1.0");
    let menu = await openMenu();
    expect(menuLabels(menu)).toEqual(["进入沉浸布局", "新建工作流"]);
    expect(menu.queryByRole("menuitem", { name: "撤销" })).toBeNull();
    await closeMenu();

    // Tier 1 (fits 1327): 校验 and the theme radio group join ⋯. 1280 is the
    // §6.3 hard case, so the tier is exercised at its real width.
    setWidth(1280);
    expect(toolbar.getAttribute("data-fold")).toBe("1");
    expect(within(toolbar).queryByRole("button", { name: "校验" })).toBeNull();
    expect(within(toolbar).queryByRole("button", { name: "主题设置" })).toBeNull();
    menu = await openMenu();
    expect(menuLabels(menu)).toEqual([
      "校验",
      "浅色",
      "深色",
      "跟随系统（当前）",
      "进入沉浸布局",
      "新建工作流"
    ]);
    await closeMenu();

    // Tier 2 (fits 1083): the subtitle and the spec chip give way. The menu is
    // the same; nothing else folds yet.
    setWidth(1090);
    expect(toolbar.getAttribute("data-fold")).toBe("2");
    expect(toolbar.querySelector(".xflow-editor-subtitle")).toBeNull();
    expect(toolbar.querySelector(".xflow-editor-spec-chip")).toBeNull();
    expect(toolNames()).toEqual(["撤销", "重做", "选择", "连线", "添加节点", "适应视图", "运行工作流"]);

    // Tier 3 (fits 924): the canvas extras join ⋯; 撤销 / 重做 never do.
    setWidth(1000);
    expect(toolbar.getAttribute("data-fold")).toBe("3");
    expect(toolNames()).toEqual(["撤销", "重做", "运行工作流"]);
    // 1 · 2 · 8: no groups left to separate, so no rule is drawn.
    expect(within(canvasTools()).queryAllByRole("separator")).toHaveLength(0);
    menu = await openMenu();
    expect(menuLabels(menu)).toEqual([
      "选择",
      "连线",
      "添加节点",
      "适应视图",
      "校验",
      "浅色",
      "深色",
      "跟随系统（当前）",
      "进入沉浸布局",
      "新建工作流"
    ]);
    await closeMenu();
  });

  it("folds 测试此节点 into ⋯ only when the host wires it", async () => {
    const onRunNode = vi.fn(async () => ({ status: "success" }) as RuntimeSnapshot);
    render(<XFlowEditor value={workflow} onRunNode={onRunNode} />);

    setWidth(1000);
    const menu = await openMenu();
    const labels = menuLabels(menu);
    expect(labels.slice(0, 5)).toEqual(["选择", "连线", "添加节点", "适应视图", "测试此节点"]);
    expect(labels).not.toContain("撤销");
    expect(labels).not.toContain("重做");
  });

  it("keeps all eight tools on the bar at a 1280 header", () => {
    const onRunNode = vi.fn(async () => ({ status: "success" }) as RuntimeSnapshot);
    render(<XFlowEditor value={workflow} onRunNode={onRunNode} />);
    const onBar = screen.getByRole("toolbar", { name: "编辑器工具栏" });

    // The §6.3 hard requirement: at 1280 the middle group is complete. The bar
    // is in tier 1 there (校验 / 主题 folded), not tier 0.
    setWidth(1280);
    expect(onBar.getAttribute("data-fold")).toBe("1");
    expect(toolNames()).toHaveLength(8);
    // Tier 2 folds only the subtitle / spec chip, so the tools stay whole.
    setWidth(1090);
    expect(onBar.getAttribute("data-fold")).toBe("2");
    expect(toolNames()).toHaveLength(8);
    expect(within(canvasTools()).getByRole("button", { name: "适应视图" })).toBeTruthy();
  });

  it("folds the view switch to icons and drops the ⌘S keycap at fold 4", () => {
    render(<XFlowEditor value={workflow} />);
    const toolbar = screen.getByRole("toolbar", { name: "编辑器工具栏" });

    setWidth(900);
    expect(toolbar.getAttribute("data-fold")).toBe("4");

    // The mode switch is icon-only, but its accessible name survives, so name
    // queries (and screen readers) still find both options.
    const segmented = document.querySelector(".xflow-editor-mode-segmented") as HTMLElement;
    expect(segmented).not.toBeNull();
    expect(segmented.querySelectorAll("svg[data-xflow-icon]")).toHaveLength(2);
    expect(within(segmented).getByRole("radio", { name: "编辑" })).toBeTruthy();
    expect(within(segmented).getByRole("radio", { name: "运行记录" })).toBeTruthy();
    // The visible label is gone; only the visually hidden one carries the name.
    expect(within(segmented).getByText("编辑").classList.contains("xflow-editor-sr-only")).toBe(true);
    expect(within(segmented).getByText("运行记录").classList.contains("xflow-editor-sr-only")).toBe(true);

    // The keycap is hidden, the save button and its shortcut tooltip are not.
    expect(screen.queryByText("⌘S")).toBeNull();
    expect(within(toolbar).getByRole("button", { name: "保存" })).toBeTruthy();
  });

  /** Text of every tooltip currently visible (closed ones stay mounted hidden). */
  const openTooltipText = (): (string | null)[] =>
    [...document.querySelectorAll(".ant-tooltip")]
      .filter((element) => !element.classList.contains("ant-tooltip-hidden"))
      .map((element) => element.textContent);

  it("names the save shortcut once in the fold-4 tooltip", async () => {
    const onSave = vi.fn(async (next: WorkflowDef) => next);
    render(<XFlowEditor value={workflow} onSave={onSave} />);
    const toolbar = screen.getByRole("toolbar", { name: "编辑器工具栏" });

    // Dirty the draft while the settings panel is reachable; the fold only
    // changes how the tooltip names the shortcut, not the save state.
    // The start node opens selected; Esc returns the Inspector to the
    // workflow settings, whose name field is the simplest real draft edit.
    fireEvent.keyDown(window, { key: "Escape" });
    fireEvent.change(
      within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByLabelText("工作流名称"),
      { target: { value: "Toolbar workflow edited" } }
    );
    setWidth(900);
    expect(toolbar.getAttribute("data-fold")).toBe("4");

    const save = within(toolbar).getByRole("button", { name: "保存" });
    fireEvent.mouseEnter(save.parentElement as HTMLElement);
    await waitFor(() => expect(openTooltipText()).toContain("保存工作流（⌘ / Ctrl + S）"));
    // The default copy already names the shortcut, so the fold-4 suffix must
    // not append a second one.
    const tip = openTooltipText().find((text) => text?.includes("保存工作流"));
    expect((tip?.match(/⌘/g) ?? []).length).toBe(1);
    fireEvent.mouseLeave(save.parentElement as HTMLElement);
  });

  it("adds the short shortcut to the clean-state tooltip at fold 4", async () => {
    const onSave = vi.fn(async (next: WorkflowDef) => next);
    render(<XFlowEditor value={workflow} onSave={onSave} />);
    const toolbar = screen.getByRole("toolbar", { name: "编辑器工具栏" });

    setWidth(900);
    expect(toolbar.getAttribute("data-fold")).toBe("4");
    const save = within(toolbar).getByRole("button", { name: "保存" }) as HTMLButtonElement;
    expect(save.disabled).toBe(true);

    fireEvent.mouseEnter(save.parentElement as HTMLElement);
    await waitFor(() => expect(openTooltipText()).toContain("没有未保存的修改（⌘S）"));
  });
});
