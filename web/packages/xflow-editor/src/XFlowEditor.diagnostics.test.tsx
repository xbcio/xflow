import * as React from "react";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { RuntimeSnapshot, WorkflowDef } from "@xflow/core";
import type { XFlowPreview as RealXFlowPreview } from "@xflow/preview";
import { XFlowEditor } from "./index";

// E4a: the 28px status strip and the 256px diagnostics drawer (spec §6.5).
// E4b: the three dock states, ⌘J, focus hand-over and 定位.

// React Flow never initialises in jsdom, so the preview's view handle would
// never arrive. This pass-through wrapper hands the editor a spy handle the way
// the real preview does once it is live, and keeps the last props for assertions.
const preview = vi.hoisted(() => ({
  props: undefined as undefined | Record<string, unknown>,
  mounts: 0,
  handle: { fitView: vi.fn(), fitNode: vi.fn() }
}));
vi.mock("@xflow/preview", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@xflow/preview")>();
  const Wrapped: typeof RealXFlowPreview = (props) => {
    preview.props = props as unknown as Record<string, unknown>;
    React.useEffect(() => {
      preview.mounts += 1;
    }, []);
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
  // Panel collapse is persisted per workflow id (§6.0); a case that ends with
  // the inspector collapsed must not hand that state to the next case.
  window.localStorage?.clear();
});

const workflow: WorkflowDef = {
  id: "wf-diagnostics",
  name: "Diagnostics",
  version: "1.2.0",
  credentials: { crm_token: { type: "bearer" } },
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "worker", type: "xflow.function" },
    { name: "tail", type: "xflow.http" }
  ],
  connections: { start: { main: [{ node: "worker" }] }, worker: { main: [{ node: "tail" }] } }
};

/** worker → ghost: one dangling target, so exactly one error attributed to `worker`. */
const broken: WorkflowDef = {
  ...workflow,
  connections: { start: { main: [{ node: "worker" }] }, worker: { main: [{ node: "ghost" }] } }
};

/** The same graph with no credential declared: one extra, workflow-level warning. */
const withoutCredentials = (def: WorkflowDef): WorkflowDef => ({ ...def, credentials: undefined });

const strip = (name: "展开诊断台" | "诊断台摘要" = "展开诊断台") => screen.getByRole("button", { name });
const openDrawer = () => fireEvent.click(strip("展开诊断台"));
const drawer = () => screen.getByRole("region", { name: "诊断台" });
const summary = () => screen.getByRole("complementary", { name: "摘要" });
const rowOf = (text: string | RegExp): HTMLElement => {
  const row = screen.getByText(text).closest("li");
  if (!row) throw new Error("issue row not found");
  return row;
};

/** The 未接入 notes currently showing: AntD keeps a closed tooltip mounted but hidden. */
const openNotes = () => screen.queryAllByText("未接入").filter((note) => !note.closest(".ant-tooltip-hidden"));

describe("status strip (§6.5)", () => {
  it("is one button named 展开诊断台 while closed and 诊断台摘要 while open", () => {
    render(<XFlowEditor value={workflow} />);

    expect(strip().getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByRole("button", { name: "诊断台摘要" })).toBeNull();
    expect(screen.queryByRole("button", { name: "收起诊断台" })).toBeNull();
    // The shortcut lives on the title and aria-keyshortcuts, never in the name.
    expect(strip().getAttribute("title")).toContain("⌘J");
    expect(strip().getAttribute("aria-keyshortcuts")).toContain("Meta+J");

    openDrawer();
    expect(strip("诊断台摘要").getAttribute("aria-expanded")).toBe("true");
    expect(screen.queryByRole("button", { name: "展开诊断台" })).toBeNull();
    // Exactly one control closes the drawer by name.
    expect(screen.getAllByRole("button", { name: "收起诊断台" })).toHaveLength(1);
  });

  it("reads 校验通过 with no warning chip when there is nothing to report", () => {
    render(<XFlowEditor value={{ ...workflow, credentials: { crm_token: { type: "bearer" } } }} />);

    const text = strip().textContent ?? "";
    expect(text).toContain("校验通过");
    expect(text).not.toContain("警告");
    expect(text).not.toContain("错误");
    expect(text).not.toContain("最近运行");
  });

  it("counts errors and warnings from the findings, and drops the pass mark on an error", () => {
    render(<XFlowEditor value={withoutCredentials(broken)} />);

    const text = strip().textContent ?? "";
    expect(text).toContain("1 错误");
    expect(text).not.toContain("校验通过");
    // No credentials declared: that is the one warning.
    expect(text).toContain("1 警告");
  });

  it("reports node and edge counts read from the workflow arrays, and follows an edit", () => {
    const { rerender } = render(<XFlowEditor value={workflow} />);
    const stats = () => strip().querySelector("[data-graph-stats]");

    expect(stats()?.textContent).toBe("3 节点 · 2 连线");
    expect(stats()?.getAttribute("data-graph-stats")).toBe("3/2");

    rerender(
      <XFlowEditor
        value={{ ...workflow, nodes: workflow.nodes?.slice(0, 2), connections: { start: { main: [{ node: "worker" }] } } }}
      />
    );
    expect(stats()?.textContent).toBe("2 节点 · 1 连线");
  });

  it("names the last run from the shared status table, and omits the segment before any run", () => {
    const runtime: RuntimeSnapshot = { status: "failed", nodes: { start: { status: "success" } } };
    const { rerender } = render(<XFlowEditor value={workflow} />);
    expect(strip().textContent).not.toContain("最近运行");

    rerender(<XFlowEditor value={workflow} runtime={runtime} />);
    expect(strip().textContent).toContain("最近运行 失败");
    expect(strip().textContent).not.toMatch(/failed|success/);
  });
});

describe("issue list (§6.5)", () => {
  it("gives every row an icon, a target, a Chinese sentence and a locate action, with no internal ids", () => {
    render(<XFlowEditor value={broken} />);
    openDrawer();

    const row = rowOf("worker.main 连接目标不存在: ghost");
    expect(row.getAttribute("data-tone")).toBe("danger");
    expect(within(row).getByRole("img", { name: "错误" })).toBeTruthy();
    expect(row.querySelector(".xflow-editor-issue__target")?.textContent).toBe("worker");
    expect(within(row).getByRole("button", { name: "定位节点 worker" }).textContent).toBe("定位节点");

    const visible = drawer().textContent ?? "";
    expect(visible).not.toMatch(/\b(pass|warn|error|waiting|errors|warnings)\b/);
    expect(visible).not.toMatch(/\b(edges|nodes|DSL valid|DSL invalid|credentials|runtime)\b/);
    // The internal area key rides on a data attribute, not on text.
    expect(row.getAttribute("data-area")).toBe("edges");
  });

  it("targets 工作流 for a finding about the workflow itself, with no node to locate", () => {
    render(<XFlowEditor value={withoutCredentials(workflow)} />);
    openDrawer();

    const row = rowOf("未声明凭证引用；涉及外部资源的节点运行前需要配置");
    expect(row.getAttribute("data-tone")).toBe("warning");
    expect(row.querySelector(".xflow-editor-issue__target")?.textContent).toBe("工作流");
    expect(within(row).queryByRole("button")).toBeNull();
  });

  it("selects the node on 定位节点 and does not select anything on its own when a finding appears", () => {
    render(<XFlowEditor value={broken} />);
    // Nothing is selected for the user just because the drawer lists a failure.
    openDrawer();
    expect(screen.queryByText("worker · xflow.function", { selector: ".xflow-editor-inspector-head__type" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "定位节点 worker" }));
    expect(
      within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByText("worker · xflow.function", {
        selector: ".xflow-editor-inspector-head__type"
      })
    ).toBeTruthy();
  });

  it("folds the passed checks into one line that expands to each item and folds back", () => {
    render(<XFlowEditor value={{ ...workflow, credentials: { crm_token: { type: "bearer" } } }} />);
    openDrawer();

    const line = screen.getByText(/^\d+ 项检查通过：/);
    expect(line.textContent).toContain("DSL 结构");
    const toggle = within(line.closest("li") as HTMLElement).getByRole("button", { name: "展开通过项" });
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByText("2 条数据连接和 0 条依赖连接已解析")).toBeNull();

    fireEvent.click(toggle);
    expect(toggle.getAttribute("aria-expanded")).toBe("true");
    expect(screen.getByText("2 条数据连接和 0 条依赖连接已解析")).toBeTruthy();
    expect(screen.getAllByRole("img", { name: "通过" }).length).toBeGreaterThan(1);

    fireEvent.click(toggle);
    expect(screen.queryByText("2 条数据连接和 0 条依赖连接已解析")).toBeNull();
  });

  it("leaves only the pass line when nothing is wrong", () => {
    render(<XFlowEditor value={{ ...workflow, credentials: { crm_token: { type: "bearer" } } }} />);
    openDrawer();

    expect(drawer().querySelectorAll(".xflow-editor-issue")).toHaveLength(0);
    expect(drawer().querySelectorAll(".xflow-editor-issues__pass")).toHaveLength(1);
    // The focused tab carries rc-tabs' "Tab 1 of 4" live region in its name.
    expect(screen.getByRole("tab", { name: /问题 0$/ })).toBeTruthy();
  });
});

describe("drawer head and tabs (§6.0 / §6.5)", () => {
  it("is a region named by its 诊断台 heading, with the issue count tab and three tabs not wired yet", () => {
    render(<XFlowEditor value={withoutCredentials(broken)} />);
    openDrawer();

    const heading = within(drawer()).getByRole("heading", { name: "诊断台" });
    expect(drawer().getAttribute("aria-labelledby")).toBe(heading.id);
    expect(drawer().getAttribute("aria-label")).toBeNull();

    const tabs = within(drawer()).getAllByRole("tab");
    expect(tabs.map((tab) => tab.textContent?.replace(/^Tab \d+ of \d+/, ""))).toEqual(["问题 2", "运行日志", "输入", "输出"]);
    expect(tabs[0].getAttribute("aria-selected")).toBe("true");
    // The unwired three get no panel to switch to.
    expect(within(drawer()).queryAllByRole("tabpanel").filter((panel) => panel.textContent)).toHaveLength(0);
  });

  it("explains each unwired tab as 未接入 on hover, one tab at a time", async () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    for (const name of ["运行日志", "输入", "输出"]) {
      const label = within(drawer()).getByText(name, { selector: "span" });
      fireEvent.mouseEnter(label);
      expect(await screen.findAllByText("未接入")).toHaveLength(1);
      fireEvent.mouseLeave(label);
      // The previous tooltip is gone before the next tab is tried.
      await waitFor(() => expect(openNotes()).toHaveLength(0));
    }
  });

  it("reaches the 未接入 note from the keyboard and the accessibility tree, and points at no panel", async () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    for (const name of ["运行日志", "输入", "输出"]) {
      const tab = within(drawer()).getByRole("tab", { name: new RegExp(`${name}$`) });
      expect(tab.getAttribute("aria-description")).toBe("未接入");
      expect(tab.hasAttribute("aria-controls")).toBe(false);
      // Focus alone opens the note (no pointer), and leaving closes it again.
      fireEvent.focus(tab);
      await waitFor(() => expect(openNotes()).toHaveLength(1));
      fireEvent.blur(tab);
      await waitFor(() => expect(openNotes()).toHaveLength(0));
    }
    // The wired tab keeps the plain tab contract.
    expect(within(drawer()).getByRole("tab", { name: /问题 \d+$/ }).hasAttribute("aria-description")).toBe(false);
  });

  it("closes from the drawer's own button and hands focus back to the strip", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));
    expect(screen.queryByRole("tab", { name: "运行日志" })).toBeNull();
    expect(strip().getAttribute("aria-expanded")).toBe("false");
    expect(document.activeElement).toBe(strip());
  });

  it("offers 扩展到画布高度 as a labelled 24px-class action beside 收起诊断台", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    const expand = within(drawer()).getByRole("button", { name: "扩展到画布高度" });
    expect(expand.className).toContain("xflow-editor-panel-collapse");
    expect(within(drawer()).getByRole("button", { name: "收起诊断台" })).toBeTruthy();
  });

  it("names 扩展到画布高度 in its tooltip, and 还原诊断台高度 once expanded", async () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    fireEvent.mouseEnter(within(drawer()).getByRole("button", { name: "扩展到画布高度" }));
    expect(await screen.findByRole("tooltip", { name: "扩展到画布高度" })).toBeTruthy();
    fireEvent.mouseLeave(within(drawer()).getByRole("button", { name: "扩展到画布高度" }));

    fireEvent.click(within(drawer()).getByRole("button", { name: "扩展到画布高度" }));
    fireEvent.mouseEnter(within(drawer()).getByRole("button", { name: "还原诊断台高度" }));
    expect(await screen.findByRole("tooltip", { name: "还原诊断台高度" })).toBeTruthy();
  });

  it("groups the heading icon and title 8px apart inside the head", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    const group = within(drawer()).getByRole("heading", { name: "诊断台" }).parentElement;
    expect(group?.className).toBe("xflow-editor-drawer__title");
    expect(group?.querySelector(".xflow-editor-panel-title__icon")).toBeTruthy();
  });
});

describe("summary column (§6.5)", () => {
  it("lists 定义摘要 from the draft: node mix, edge count, credentials and the top bar's version wording", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    const side = within(summary());
    expect(side.getByRole("heading", { name: "定义摘要" })).toBeTruthy();
    expect(side.getByRole("heading", { name: "最近运行" })).toBeTruthy();

    const dd = (label: string) => side.getByText(label, { selector: "dt" }).nextElementSibling as HTMLElement;
    expect(dd("节点").textContent).toBe("3（流程控制 1 · 动作 1 · 代码 1）");
    // Each category is its own nowrap span, so a wrap can only fall on a `·`.
    expect(dd("节点").querySelectorAll(".xflow-editor-kvlist__mix")).toHaveLength(3);
    expect(dd("连线").textContent).toBe("2");
    expect(dd("凭证").textContent).toBe("crm_token");
    expect(dd("版本").textContent).toBe(document.querySelector(".xflow-editor-chip")?.textContent);
    expect(dd("版本").textContent).toBe("线上 v1.2.0");
  });

  it("draws a dash where there is no data source, and the running node when there is one", () => {
    const empty = render(<XFlowEditor value={{ ...workflow, credentials: undefined }} />);
    openDrawer();
    const dd = (label: string) => within(summary()).getByText(label, { selector: "dt" }).nextElementSibling as HTMLElement;
    expect(dd("凭证").textContent).toBe("—");
    expect(dd("执行").textContent).toBe("—");
    expect(dd("当前节点").textContent).toBe("—");
    expect(dd("命中分支").textContent).toBe("—");
    empty.unmount();

    const runtime: RuntimeSnapshot = { status: "running", nodes: { worker: { status: "running" } } };
    render(<XFlowEditor value={workflow} runtime={runtime} />);
    openDrawer();
    expect(dd("执行").textContent).toBe("运行中");
    expect(dd("当前节点").textContent).toBe("worker");
  });

  it("carries none of the retired English dump", () => {
    render(<XFlowEditor value={workflow} runtime={{ status: "success" }} />);
    openDrawer();

    expect(screen.queryByLabelText("DSL 预览")).toBeNull();
    expect(drawer().querySelector("pre, meter, progress, .xflow-editor-diagnostics-meter")).toBeNull();
    expect(drawer().textContent).not.toMatch(/last run|valid|execution-local|errors|warnings/);
  });
});

describe("empty canvas (§6.5.1)", () => {
  it("waits for nodes instead of reporting a pass, and disables 导入 DSL with 未接入", async () => {
    render(<XFlowEditor value={{ name: "Empty", spec: "1.0", nodes: [], connections: {} }} />);

    expect(strip().textContent).toContain("等待节点");
    expect(strip().textContent).not.toContain("校验通过");
    expect(strip().querySelector("[data-graph-stats]")?.textContent).toBe("0 节点 · 0 连线");

    const importDsl = within(screen.getByRole("region", { name: "画布" })).getByRole("button", { name: "导入 DSL" });
    expect((importDsl as HTMLButtonElement).disabled).toBe(true);
    fireEvent.mouseEnter(importDsl.parentElement as HTMLElement);
    expect((await screen.findAllByText("未接入")).length).toBeGreaterThan(0);
    expect(screen.queryByText("DSL 导入尚未接入。")).toBeNull();
  });
});

// ---- E4b: dock states, ⌘J, focus, 定位 ---------------------------------------

const dockState = () => screen.getByRole("region", { name: "诊断台" }).getAttribute("data-dock");
const pressJ = (init: KeyboardEventInit = {}, target: EventTarget = window) =>
  fireEvent.keyDown(target, { key: "j", code: "KeyJ", metaKey: true, ...init });
const inspected = () => document.querySelector(".xflow-editor-inspector-head__type")?.textContent;
const expandButton = () => within(drawer()).getByRole("button", { name: /^(扩展到画布高度|还原诊断台高度)$/ });
const expandDrawer = () => fireEvent.click(within(drawer()).getByRole("button", { name: "扩展到画布高度" }));
const activeTab = () => within(drawer()).getAllByRole("tab").find((tab) => tab.getAttribute("aria-selected") === "true");

/** `worker` answers to the id `node-worker`, so a name-for-id mix-up cannot pass by accident. */
const withIds: WorkflowDef = {
  ...broken,
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { id: "node-worker", name: "worker", type: "xflow.function" },
    { name: "tail", type: "xflow.http" }
  ]
};

describe("dock states (E4b)", () => {
  it("starts collapsed and walks collapsed -> docked -> expanded -> docked -> collapsed", () => {
    render(<XFlowEditor value={workflow} />);
    expect(dockState()).toBe("collapsed");

    openDrawer();
    expect(dockState()).toBe("docked");

    expandDrawer();
    expect(dockState()).toBe("expanded");
    expect(drawer().querySelector(".xflow-editor-drawer")?.getAttribute("data-dock")).toBe("expanded");

    fireEvent.click(within(drawer()).getByRole("button", { name: "还原诊断台高度" }));
    expect(dockState()).toBe("docked");
    expect(drawer().querySelector(".xflow-editor-drawer")?.getAttribute("data-dock")).toBe("docked");

    fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));
    expect(dockState()).toBe("collapsed");
  });

  it("collapses straight from expanded with the drawer's own button", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();
    expandDrawer();

    fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));
    expect(dockState()).toBe("collapsed");
    expect(screen.queryByRole("tab", { name: "运行日志" })).toBeNull();
  });

  it("turns one dock button between two labels and icons, pressed only while expanded", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    const docked = expandButton();
    expect(docked.getAttribute("aria-label")).toBe("扩展到画布高度");
    expect(docked.getAttribute("aria-pressed")).toBe("false");
    expect(docked.querySelector('[aria-label="vertical-align-top"]')).toBeTruthy();

    fireEvent.click(docked);
    const restore = expandButton();
    expect(restore.getAttribute("aria-label")).toBe("还原诊断台高度");
    expect(restore.getAttribute("aria-pressed")).toBe("true");
    expect(restore.querySelector('[aria-label="vertical-align-middle"]')).toBeTruthy();
    // The collapse button never changes, and the name is not duplicated.
    expect(within(drawer()).getAllByRole("button", { name: "收起诊断台" })).toHaveLength(1);
    expect(
      within(drawer()).getByRole("button", { name: "收起诊断台" }).querySelector('[aria-label="vertical-align-bottom"]')
    ).toBeTruthy();
  });

  it("keeps the drawer inside the editor root and the strip in the tab order while expanded", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();
    expandDrawer();

    const root = document.querySelector(".xflow-editor") as HTMLElement;
    expect(root.contains(drawer())).toBe(true);
    expect(strip("诊断台摘要").hasAttribute("inert")).toBe(true);
    fireEvent.click(within(drawer()).getByRole("button", { name: "还原诊断台高度" }));
    expect(strip("诊断台摘要").hasAttribute("inert")).toBe(false);
  });

  it("does not refit or re-mount the canvas when the drawer expands and restores", () => {
    render(<XFlowEditor value={workflow} />);
    // The preview's own section and a node card inside it: a remount of the
    // preview or of React Flow would replace both elements.
    const section = document.querySelector(".xflow-editor-preview-frame > *");
    const card = document.querySelector(".xflow-preview-node__select");
    expect(section).not.toBeNull();
    expect(card).not.toBeNull();
    const mounts = preview.mounts;
    preview.handle.fitView.mockClear();

    openDrawer();
    expandDrawer();
    fireEvent.click(within(drawer()).getByRole("button", { name: "还原诊断台高度" }));
    fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));

    expect(preview.handle.fitView).not.toHaveBeenCalled();
    expect(preview.handle.fitNode).not.toHaveBeenCalled();
    expect(preview.mounts).toBe(mounts);
    expect(document.querySelector(".xflow-editor-preview-frame > *")).toBe(section);
    expect(document.querySelector(".xflow-preview-node__select")).toBe(card);
  });

  it("takes the covered workbench out of the tab order while expanded, and only then", () => {
    render(<XFlowEditor value={workflow} />);
    const main = document.querySelector(".xflow-editor-main") as HTMLElement;
    expect(main.hasAttribute("inert")).toBe(false);

    openDrawer();
    expect(main.hasAttribute("inert")).toBe(false);
    expandDrawer();
    expect(main.hasAttribute("inert")).toBe(true);
    // The toolbar stays live: the drawer stops short of it.
    expect((document.querySelector(".xflow-editor-toolbar") as HTMLElement).closest("[inert]")).toBeNull();
    expect(drawer().closest("[inert]")).toBeNull();

    fireEvent.click(within(drawer()).getByRole("button", { name: "还原诊断台高度" }));
    expect(main.hasAttribute("inert")).toBe(false);
    expandDrawer();
    pressJ();
    expect(main.hasAttribute("inert")).toBe(false);
  });
});

describe("auto-open points (E4b)", () => {
  it("opens a collapsed dock to docked on 校验 and never selects a node for the user", () => {
    render(<XFlowEditor value={broken} />);
    const before = inspected();
    expect(before).toContain("start");

    fireEvent.click(screen.getByRole("button", { name: "校验" }));
    expect(dockState()).toBe("docked");
    expect(inspected()).toBe(before);
  });

  it("does not press an expanded drawer back down when 校验 runs again", () => {
    render(<XFlowEditor value={broken} />);
    openDrawer();
    expandDrawer();

    fireEvent.click(screen.getByRole("button", { name: "校验" }));
    expect(dockState()).toBe("expanded");
  });

  it("opens on a failed save and keeps the selection and an expanded drawer as they were", async () => {
    const onSave = vi.fn().mockRejectedValue(new Error("boom"));
    render(<XFlowEditor value={broken} onSave={onSave} />);
    const before = inspected();

    // A clean draft keeps ⌘S a no-op (baseline clean state): dirty the selected
    // node without changing which node/type the Inspector shows.
    const inspectorRoot = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(inspectorRoot).getByRole("button", { name: "更多操作" }));
    fireEvent.click(await screen.findByRole("menuitem", { name: "禁用" }));

    fireEvent.keyDown(window, { key: "s", metaKey: true });
    expect(await screen.findAllByText("boom")).not.toHaveLength(0);
    expect(dockState()).toBe("docked");
    expect(inspected()).toBe(before);

    expandDrawer();
    fireEvent.keyDown(window, { key: "s", metaKey: true });
    await vi.waitFor(() => expect(onSave).toHaveBeenCalledTimes(2));
    await screen.findAllByText("boom");
    expect(dockState()).toBe("expanded");
  });

  it("opens on a failed run without selecting the failing node, and stays expanded once expanded", async () => {
    const onRun = vi.fn().mockRejectedValue(new Error("run exploded"));
    render(<XFlowEditor value={workflow} onRun={onRun} />);
    const before = inspected();

    fireEvent.keyDown(window, { key: "Enter", metaKey: true });
    await vi.waitFor(() => expect(dockState()).toBe("docked"));
    expect(inspected()).toBe(before);

    expandDrawer();
    fireEvent.keyDown(window, { key: "Enter", metaKey: true });
    await vi.waitFor(() => expect(onRun).toHaveBeenCalledTimes(2));
    expect(dockState()).toBe("expanded");
    expect(inspected()).toBe(before);
  });
});

describe("⌘J / Ctrl+J (E4b)", () => {
  it("toggles collapsed <-> docked with either modifier and prevents the browser default", () => {
    render(<XFlowEditor value={workflow} />);

    expect(pressJ()).toBe(false);
    expect(dockState()).toBe("docked");
    expect(pressJ({ metaKey: false, ctrlKey: true })).toBe(false);
    expect(dockState()).toBe("collapsed");
  });

  it("closes an expanded drawer outright", () => {
    render(<XFlowEditor value={workflow} />);
    pressJ();
    expandDrawer();

    pressJ();
    expect(dockState()).toBe("collapsed");
  });

  it("goes by the physical key, ignores Shift / Alt chords and a bare J", () => {
    render(<XFlowEditor value={workflow} />);

    // A layout that produces another letter for the J key still toggles.
    pressJ({ key: "о", metaKey: false, ctrlKey: true });
    expect(dockState()).toBe("docked");
    // Each chord on its own, so two stray toggles cannot cancel each other out.
    pressJ({ shiftKey: true });
    expect(dockState()).toBe("docked");
    pressJ({ altKey: true });
    expect(dockState()).toBe("docked");
    fireEvent.keyDown(window, { key: "j", code: "KeyJ" });
    expect(dockState()).toBe("docked");
    fireEvent.keyDown(window, { key: "j", code: "KeyK", metaKey: true });
    expect(dockState()).toBe("docked");
  });

  it("fires from inside a text field, like the other global shortcuts", () => {
    render(<XFlowEditor value={workflow} />);
    const field = document.body.appendChild(document.createElement("input"));
    field.focus();

    pressJ({}, field);
    expect(dockState()).toBe("docked");
    field.remove();
  });

  it("is listed in the shortcut reference", () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.keyDown(window, { key: "?" });

    const dialog = screen.getByRole("dialog");
    expect(within(dialog).getByText("收起 / 展开诊断台")).toBeTruthy();
    expect(within(dialog).getByText("⌘ / Ctrl + J")).toBeTruthy();
  });
});

describe("focus hand-over (E4b)", () => {
  it("moves focus to the active tab when the strip opens the drawer", () => {
    render(<XFlowEditor value={workflow} />);
    strip().focus();

    openDrawer();
    expect(document.activeElement).toBe(activeTab());
    expect(activeTab()?.textContent).toMatch(/问题 \d+$/);
  });

  it("moves focus to the active tab when ⌘J opens it", () => {
    render(<XFlowEditor value={workflow} />);

    pressJ();
    expect(document.activeElement).toBe(activeTab());
  });

  it("does not take focus when a failure opens the drawer on its own", () => {
    render(<XFlowEditor value={broken} />);
    const validate = screen.getByRole("button", { name: "校验" });
    validate.focus();

    fireEvent.click(validate);
    expect(dockState()).toBe("docked");
    expect(document.activeElement).toBe(validate);
  });

  it("returns focus to the strip when 收起诊断台 closes a docked drawer", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();

    fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));
    expect(document.activeElement).toBe(strip());
  });

  it("returns focus to the strip when ⌘J closes the drawer from inside it, docked or expanded", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();
    expect(drawer().contains(document.activeElement)).toBe(true);

    pressJ();
    expect(document.activeElement).toBe(strip());

    pressJ();
    expandDrawer();
    within(drawer()).getByRole("button", { name: "还原诊断台高度" }).focus();
    pressJ();
    expect(document.activeElement).toBe(strip());
  });

  it("leaves focus where it is when ⌘J closes a drawer that does not hold it", () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();
    const validate = screen.getByRole("button", { name: "校验" });
    validate.focus();

    pressJ();
    expect(dockState()).toBe("collapsed");
    expect(document.activeElement).toBe(validate);
  });

  it("never lets focus fall to the body when the drawer unmounts", () => {
    render(<XFlowEditor value={workflow} />);
    for (const close of [
      () => fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" })),
      () => pressJ(),
      () => {
        expandDrawer();
        fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));
      }
    ]) {
      openDrawer();
      close();
      expect(document.activeElement).not.toBe(document.body);
      expect(document.activeElement).toBe(strip());
    }
  });
});

describe("定位节点 (E4b)", () => {
  it("selects the node, pans to it by its id and lands focus on its card in the canvas", () => {
    render(<XFlowEditor value={withIds} />);
    openDrawer();
    expect(inspected()).toContain("start");

    fireEvent.click(screen.getByRole("button", { name: "定位节点 worker" }));
    expect(inspected()).toBe("worker · xflow.function");
    expect(preview.handle.fitNode).toHaveBeenCalledTimes(1);
    // The preview names nodes by id (falling back to name), not by the finding's name.
    expect(preview.handle.fitNode).toHaveBeenCalledWith("node-worker");
    expect(preview.handle.fitView).not.toHaveBeenCalled();
    const card = document.querySelector('.react-flow__node[data-id="node-worker"] .xflow-preview-node__select');
    expect(card).not.toBeNull();
    expect(document.activeElement).toBe(card);
    expect(document.querySelector(".xflow-editor-preview-frame")?.contains(document.activeElement)).toBe(true);
  });

  it("falls back to the node name when the node has no id", () => {
    render(<XFlowEditor value={broken} />);
    openDrawer();

    fireEvent.click(screen.getByRole("button", { name: "定位节点 worker" }));
    expect(preview.handle.fitNode).toHaveBeenCalledWith("worker");
  });

  it("hands an expanded drawer back to docked so the canvas it located on is visible", () => {
    render(<XFlowEditor value={broken} />);
    openDrawer();
    expandDrawer();

    fireEvent.click(screen.getByRole("button", { name: "定位节点 worker" }));
    expect(dockState()).toBe("docked");
    expect(inspected()).toBe("worker · xflow.function");
  });

  it("does not reopen a collapsed inspector", () => {
    render(<XFlowEditor value={broken} />);
    fireEvent.keyDown(window, { key: "b", code: "KeyB", metaKey: true, altKey: true });
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
    openDrawer();

    fireEvent.click(screen.getByRole("button", { name: "定位节点 worker" }));
    expect(screen.queryByRole("region", { name: /^(属性|工作流设置)$/ })).toBeNull();
    expect(preview.handle.fitNode).toHaveBeenCalledWith("worker");
  });

  it("still lands focus on the card when the locate comes from an expanded drawer", () => {
    render(<XFlowEditor value={withIds} />);
    openDrawer();
    expandDrawer();

    // jsdom does not refuse focus on inert elements, so record whether the card
    // sat inside an inert subtree at the moment it was handed focus.
    const inertAtFocus: boolean[] = [];
    const realFocus = HTMLElement.prototype.focus;
    const focusSpy = vi.spyOn(HTMLElement.prototype, "focus").mockImplementation(function (this: HTMLElement, ...args) {
      if (this.classList.contains("xflow-preview-node__select")) inertAtFocus.push(this.closest("[inert]") !== null);
      return realFocus.apply(this, args);
    });
    try {
      fireEvent.click(screen.getByRole("button", { name: "定位节点 worker" }));
    } finally {
      focusSpy.mockRestore();
    }
    expect(dockState()).toBe("docked");
    const card = document.querySelector('.react-flow__node[data-id="node-worker"] .xflow-preview-node__select');
    expect(card).not.toBeNull();
    expect(document.activeElement).toBe(card);
    expect(inertAtFocus).toEqual([false]);
  });
});

describe("acting on a finding from an expanded drawer (E4b)", () => {
  const withParams: WorkflowDef = {
    ...workflow,
    nodes: [
      { name: "start", type: "xflow.start", kind: "trigger" },
      { name: "worker", type: "xflow.function", parameters: { a: 1 } },
      { name: "tail", type: "xflow.http" }
    ]
  };
  const workerCard = () =>
    document.querySelector('.xflow-preview-node__select[aria-label^="worker node "]') as HTMLElement;

  it("hands the drawer back to docked for 修正 ... 的参数 JSON", () => {
    render(<XFlowEditor value={withParams} onSave={vi.fn()} />);
    fireEvent.click(workerCard());
    const root = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(root).getByRole("button", { name: "以 JSON 编辑参数" }));
    fireEvent.change(within(root).getByLabelText("参数 JSON"), { target: { value: "{ broken" } });
    fireEvent.click(screen.getByRole("button", { name: /保存/ }));
    expect(dockState()).toBe("docked");
    expandDrawer();

    fireEvent.click(screen.getByRole("button", { name: "修正 worker 的参数 JSON" }));
    expect(dockState()).toBe("docked");
  });

  it("hands the drawer back to docked for 定位字段", async () => {
    const issue = { node: "worker", path: "/parameters/a", code: "type.number", message: "bad a", severity: "error" as const };
    const onSave = vi.fn(async (next: WorkflowDef) => ({ workflow: next, paramIssues: [issue] }));
    render(<XFlowEditor value={withParams} onSave={onSave} />);
    // A clean draft keeps ⌘S a no-op (baseline clean state): dirty the selected
    // node first, without touching the worker issue path this test follows.
    const inspectorRoot = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(inspectorRoot).getByRole("button", { name: "更多操作" }));
    fireEvent.click(await screen.findByRole("menuitem", { name: "禁用" }));
    fireEvent.keyDown(window, { key: "s", metaKey: true });
    await vi.waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    openDrawer();
    const locate = await screen.findByRole("button", { name: "定位字段 worker /parameters/a" });
    expandDrawer();

    fireEvent.click(locate);
    expect(dockState()).toBe("docked");
  });
});

describe("the other auto-open points keep the selection (E4b)", () => {
  it("opens on a save blocked by an unparseable parameters draft, selecting nothing", () => {
    const onSave = vi.fn();
    const withParams: WorkflowDef = {
      ...workflow,
      nodes: [
        { name: "start", type: "xflow.start", kind: "trigger" },
        { name: "worker", type: "xflow.function", parameters: { a: 1 } },
        { name: "tail", type: "xflow.http" }
      ]
    };
    render(<XFlowEditor value={withParams} onSave={onSave} />);
    fireEvent.click(document.querySelector('.xflow-preview-node__select[aria-label^="worker node "]') as HTMLElement);
    const root = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.click(within(root).getByRole("button", { name: "以 JSON 编辑参数" }));
    fireEvent.change(within(root).getByLabelText("参数 JSON"), { target: { value: "{ broken" } });
    const before = inspected();

    expect(dockState()).toBe("collapsed");
    fireEvent.click(screen.getByRole("button", { name: /保存/ }));
    expect(onSave).not.toHaveBeenCalled();
    expect(dockState()).toBe("docked");
    expect(inspected()).toBe(before);
  });

  it("opens after a successful run, selecting nothing, and leaves an expanded drawer expanded", async () => {
    const onRun = vi.fn().mockResolvedValue({ status: "success", nodes: { worker: { status: "success" } } });
    render(<XFlowEditor value={workflow} onRun={onRun} />);
    const before = inspected();

    fireEvent.keyDown(window, { key: "Enter", metaKey: true });
    await vi.waitFor(() => expect(dockState()).toBe("docked"));
    expect(inspected()).toBe(before);

    expandDrawer();
    fireEvent.keyDown(window, { key: "Enter", metaKey: true });
    await vi.waitFor(() => expect(onRun).toHaveBeenCalledTimes(2));
    expect(dockState()).toBe("expanded");
    expect(inspected()).toBe(before);
  });

  it("opens after a partial run, successful or failed, selecting nothing", async () => {
    const onRunNode = vi
      .fn()
      .mockResolvedValueOnce({ status: "success", nodes: { worker: { status: "success" } } })
      .mockRejectedValueOnce(new Error("partial exploded"));
    render(<XFlowEditor value={workflow} onRunNode={onRunNode} />);
    const before = inspected();
    const node = (name: string) =>
      document.querySelector(`.xflow-preview-node__select[aria-label^="${name} node "]`) as HTMLElement;

    fireEvent.contextMenu(node("tail"), { clientX: 420, clientY: 260 });
    fireEvent.click(screen.getByRole("menuitem", { name: /从此节点运行/ }));
    await vi.waitFor(() => expect(dockState()).toBe("docked"));
    // Right-click selects nothing by itself in this editor; the ruling is that the dock does not either.
    const afterSuccess = inspected();
    expect(afterSuccess === before || afterSuccess?.startsWith("tail")).toBe(true);

    fireEvent.click(within(drawer()).getByRole("button", { name: "收起诊断台" }));
    fireEvent.contextMenu(node("tail"), { clientX: 420, clientY: 260 });
    fireEvent.click(screen.getByRole("menuitem", { name: /从此节点运行/ }));
    await vi.waitFor(() => expect(onRunNode).toHaveBeenCalledTimes(2));
    await vi.waitFor(() => expect(dockState()).toBe("docked"));
    expect(inspected()).toBe(afterSuccess);
  });
});

describe("⌘J guards and ⌘K locate (E4b)", () => {
  it("ignores a held chord but still swallows the browser default", () => {
    render(<XFlowEditor value={workflow} />);

    expect(pressJ({ repeat: true })).toBe(false);
    expect(dockState()).toBe("collapsed");
    expect(pressJ()).toBe(false);
    expect(dockState()).toBe("docked");
    expect(pressJ({ repeat: true })).toBe(false);
    expect(dockState()).toBe("docked");
  });

  it("does not toggle the dock behind an open shortcut reference", () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.keyDown(window, { key: "?" });
    expect(screen.getByRole("dialog")).toBeTruthy();

    pressJ();
    expect(dockState()).toBe("collapsed");
  });

  it("steps an expanded drawer down to docked when ⌘K locates a node", async () => {
    render(<XFlowEditor value={workflow} />);
    openDrawer();
    expandDrawer();
    expect(dockState()).toBe("expanded");

    fireEvent.keyDown(window, { key: "k", code: "KeyK", metaKey: true });
    const search = await screen.findByRole("combobox", { name: "定位节点" });
    fireEvent.change(search, { target: { value: "tail" } });
    fireEvent.keyDown(search, { key: "Enter", code: "Enter" });

    await vi.waitFor(() => expect(dockState()).toBe("docked"));
    expect(inspected()).toContain("tail");
  });
});
