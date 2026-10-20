import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { nodeCategoryMeta, nodeStatusMeta, type NodeCategory, type RuntimeSnapshot, type WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";
import { NodeFormCompiler, createNodeFormRegistry } from "./node-form";
import { Inspector } from "./panels/inspector";
import { countNodeConnections } from "./panels/workflowOps";

const workflow: WorkflowDef = {
  id: "wf-inspector",
  name: "Inspector",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "api", type: "xflow.http", position: { x: 240, y: 120 } },
    { name: "tail", type: "xflow.end", position: { x: 480, y: 120 } }
  ],
  connections: { start: { main: [{ node: "api" }] }, api: { main: [{ node: "tail" }] } }
};

const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
const canvasNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} node "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  return node;
};
const selectNode = (name: string): HTMLElement => {
  fireEvent.click(canvasNode(name));
  return inspector();
};
const headerName = (panel: HTMLElement): HTMLElement =>
  within(panel).getByText("api", { selector: ".xflow-editor-inspector-head__name" });
const lastWorkflow = (spy: ReturnType<typeof vi.fn>) => spy.mock.calls.at(-1)?.[0] as WorkflowDef;
const undo = () =>
  within(screen.getByRole("group", { name: "常用画布与历史工具" })).getByRole("button", { name: "撤销" });

describe("inspector header (§6.4)", () => {
  it("draws the status chip from nodeStatusMeta, and the type line as plain text", () => {
    const runtime: RuntimeSnapshot = { nodes: { api: { status: "failed" } } };
    render(<XFlowEditor value={workflow} runtime={runtime} />);
    const panel = selectNode("api");

    const chip = panel.querySelector(".xflow-editor-inspector-head__status");
    expect(chip?.textContent).toBe(nodeStatusMeta.failed.label);
    expect(chip?.getAttribute("data-tone")).toBe(nodeStatusMeta.failed.tone);
    // Pinned literally: the table is the source, but the spec fixes the Chinese copy.
    expect(chip?.textContent).toBe("失败");
    expect(chip?.getAttribute("data-tone")).toBe("danger");

    const typeLine = panel.querySelector(".xflow-editor-inspector-head__type");
    expect(typeLine?.textContent).toBe("api · xflow.http");
    expect(typeLine?.querySelector("code, em")).toBeNull();
  });

  it("draws no 工作流设置 button in the title bar: only 收起属性面板 sits there", () => {
    render(<XFlowEditor value={workflow} />);
    const panel = selectNode("api");
    expect(within(panel).queryByRole("button", { name: "工作流设置" })).toBeNull();
    const actions = panel.querySelector(".xflow-editor-inspector-header-actions");
    expect(Array.from(actions?.querySelectorAll("button") ?? []).map((button) => button.getAttribute("aria-label"))).toEqual([
      "收起属性面板"
    ]);
  });

  it("draws the 排队 chip while the node is pending, and none without a run", () => {
    const runtime: RuntimeSnapshot = { nodes: { api: { status: "pending" } } };
    const { unmount } = render(<XFlowEditor value={workflow} runtime={runtime} />);
    const chip = selectNode("api").querySelector(".xflow-editor-inspector-head__status");
    expect(chip?.textContent).toBe(nodeStatusMeta.pending.label);
    expect(chip?.getAttribute("data-tone")).toBe(nodeStatusMeta.pending.tone);
    expect(chip?.getAttribute("data-status")).toBe("pending");
    unmount();

    render(<XFlowEditor value={workflow} />);
    expect(selectNode("api").querySelector(".xflow-editor-inspector-head__status")).toBeNull();
  });

  it("commits an inline rename on Enter and one undo restores the name", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const panel = selectNode("api");

    fireEvent.doubleClick(headerName(panel));
    const field = within(inspector()).getByLabelText("节点显示名") as HTMLInputElement;
    expect(document.activeElement).toBe(field);

    fireEvent.change(field, { target: { value: "fetch" } });
    fireEvent.keyDown(field, { key: "Enter" });

    // The header renames the *display* name (ui.label); the node's identity stays.
    expect(lastWorkflow(handleChange).nodes?.[1]?.name).toBe("api");
    expect(lastWorkflow(handleChange).nodes?.[1]?.ui?.label).toBe("fetch");
    expect(within(inspector()).getByText("fetch", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();

    fireEvent.click(undo());
    expect(lastWorkflow(handleChange).nodes?.[1]?.ui?.label).toBeUndefined();
    expect(within(inspector()).getByText("api", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();
  });

  it("cancels inline rename on Escape without committing", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const panel = selectNode("api");

    fireEvent.doubleClick(headerName(panel));
    const field = within(inspector()).getByLabelText("节点显示名") as HTMLInputElement;
    fireEvent.change(field, { target: { value: "fetch" } });
    fireEvent.keyDown(field, { key: "Escape" });

    expect(within(inspector()).queryByLabelText("节点显示名")).toBeNull();
    expect(handleChange).not.toHaveBeenCalled();
    expect(within(inspector()).getByText("api", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();
  });

  it("does not commit a blank or unchanged name", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const panel = selectNode("api");

    fireEvent.doubleClick(headerName(panel));
    fireEvent.change(within(inspector()).getByLabelText("节点显示名"), { target: { value: "   " } });
    fireEvent.blur(within(inspector()).getByLabelText("节点显示名"));
    expect(handleChange).not.toHaveBeenCalled();

    fireEvent.doubleClick(within(inspector()).getByText("api", { selector: ".xflow-editor-inspector-head__name" }));
    const field = within(inspector()).getByLabelText("节点显示名") as HTMLInputElement;
    fireEvent.change(field, { target: { value: "api" } });
    fireEvent.blur(field);
    expect(handleChange).not.toHaveBeenCalled();
  });

  it("does not start a rename on a read-only panel", () => {
    const registry = createNodeFormRegistry();
    const formHost = {
      registry,
      compiler: new NodeFormCompiler(registry),
      renameRef: { current: undefined }
    };
    render(
      <Inspector
        editable={false}
        formHost={formHost}
        parameterTab="form"
        selectedIndex={1}
        selectedNode={workflow.nodes?.[1]}
        workflow={workflow}
      />
    );
    fireEvent.doubleClick(document.querySelector(".xflow-editor-inspector-head__name") as HTMLElement);
    expect(screen.queryByLabelText("节点显示名")).toBeNull();
  });

  it("copies, toggles and deletes through the header actions, not the title bar", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const panel = selectNode("api");

    expect(within(panel).queryByRole("button", { name: "删除节点" })).toBeNull();

    fireEvent.click(within(panel).getByRole("button", { name: "复制节点" }));
    expect(lastWorkflow(handleChange).nodes).toHaveLength(4);

    selectNode("api");
    fireEvent.click(within(inspector()).getByRole("button", { name: "更多操作" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "禁用" }));
    expect(lastWorkflow(handleChange).nodes?.[1]?.disabled).toBe(true);

    fireEvent.click(within(inspector()).getByRole("button", { name: "更多操作" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "启用" }));
    expect(lastWorkflow(handleChange).nodes?.[1]?.disabled).toBeFalsy();

    fireEvent.click(within(inspector()).getByRole("button", { name: "更多操作" }));
    fireEvent.click(screen.getByRole("menuitem", { name: /删除/ }));
    expect(lastWorkflow(handleChange).nodes).toHaveLength(3);
  });
  it("duplicates from the ⋯ menu's 复制 item too", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    selectNode("api");
    fireEvent.click(within(inspector()).getByRole("button", { name: "更多操作" }));
    fireEvent.click(screen.getByRole("menuitem", { name: /复制/ }));
    expect(lastWorkflow(handleChange).nodes).toHaveLength(4);
  });

  it("titles the panel 工作流设置 with the 未选中节点 count, and draws no node head or tabs", () => {
    render(<XFlowEditor value={workflow} />);
    selectNode("api");
    fireEvent.keyDown(window, { key: "Escape" });
    const panel = inspector();
    expect(within(panel).getByRole("heading", { name: "工作流设置" })).toBeTruthy();
    expect(panel.querySelector(".xflow-editor-panel-title__count")?.textContent).toBe("未选中节点");
    expect(panel.querySelector(".xflow-editor-inspector-head")).toBeNull();
    expect(within(panel).queryByRole("tab")).toBeNull();
    expect(within(panel).queryByRole("button", { name: "复制节点" })).toBeNull();
    expect(within(panel).queryByRole("button", { name: "更多操作" })).toBeNull();
    expect(within(panel).getByLabelText("工作流名称")).toBeTruthy();
  });
});

describe("top bar copy", () => {
  it("writes two-character button labels without the AntD inserted space", () => {
    render(<XFlowEditor value={workflow} />);
    expect(screen.getByRole("button", { name: "发布" }).textContent).toBe("发布");
  });
});

describe("inspector head tile follows the node's category (E6b)", () => {
  // The tile is the canvas card's block: category and colour from core's table,
  // never the node-library presentation map (which coloured the same node
  // differently in the Inspector and on the canvas).
  const headTile = (panel: HTMLElement) => panel.querySelector(".xflow-editor-inspector-head .xflow-editor-category-icon");
  const cases: Array<[string, string]> = [
    ["start", "control"],
    ["api", "action"],
    ["tail", "control"]
  ];

  it.each(cases)("draws %s as the %s block, the same one its canvas card derives", (name, category) => {
    render(<XFlowEditor value={workflow} />);
    const panel = selectNode(name);
    const tile = headTile(panel);
    expect(tile?.getAttribute("data-cat")).toBe(category);
    expect(tile?.getAttribute("style")).toContain(`var(--xflow-${nodeCategoryMeta[category as NodeCategory].color})`);
    expect(panel.querySelector(".xflow-editor-inspector-head__icon")).toBeNull();
  });

  it("re-derives the block when another node is picked", () => {
    const mixed: WorkflowDef = {
      ...workflow,
      nodes: [...(workflow.nodes ?? []), { name: "review", type: "xflow.approval" }, { name: "calc", type: "xflow.function" }]
    };
    render(<XFlowEditor value={mixed} />);
    expect(headTile(selectNode("review"))?.getAttribute("data-cat")).toBe("human");
    expect(headTile(selectNode("calc"))?.getAttribute("data-cat")).toBe("code");
  });
});

describe("inspector head actions (E6b)", () => {
  it("names the copy and ⋯ keys as the baseline does and gives both a tooltip", async () => {
    render(<XFlowEditor value={workflow} />);
    const panel = selectNode("api");
    for (const name of ["复制节点", "更多操作"]) {
      const button = within(panel).getByRole("button", { name });
      fireEvent.mouseEnter(button);
      expect((await screen.findAllByText(name, { selector: ".ant-tooltip-container, .ant-tooltip-inner, .ant-tooltip *" })).length).toBeGreaterThan(0);
      fireEvent.mouseLeave(button);
    }
    expect(within(panel).queryByRole("button", { name: "节点操作" })).toBeNull();
  });
});

describe("inspector footer (baseline 测试此节点)", () => {
  it("runs the workflow up to the selected node through onRunNode", async () => {
    const onRunNode = vi.fn(async () => ({ status: "success" }) as RuntimeSnapshot);
    render(<XFlowEditor value={workflow} onRunNode={onRunNode} />);
    const panel = selectNode("api");
    // The baseline draws the key with a leading ▶ (caret-right).
    expect(within(panel).getByRole("button", { name: "测试此节点" }).querySelector(".anticon-caret-right")).not.toBeNull();
    fireEvent.click(within(panel).getByRole("button", { name: "测试此节点" }));
    await vi.waitFor(() => expect(onRunNode).toHaveBeenCalledTimes(1));
    expect(onRunNode.mock.calls[0]?.[1]).toEqual({ node: "api", mode: "to" });
  });

  it("draws the key disabled with the reason when the host has no onRunNode, and none without a node", () => {
    render(<XFlowEditor value={workflow} />);
    const panel = selectNode("api");
    const button = within(panel).getByRole("button", { name: "测试此节点" }) as HTMLButtonElement;
    expect(button.disabled).toBe(true);
    expect(button.closest(".xflow-editor-inspector-foot")?.getAttribute("title")).toBe("宿主未接 onRunNode");
    fireEvent.keyDown(window, { key: "Escape" });
    expect(within(inspector()).queryByRole("button", { name: "测试此节点" })).toBeNull();
  });
});

describe("deselecting a node (baseline: Esc and the empty canvas)", () => {
  const settingsShown = () =>
    inspector().querySelector(".xflow-editor-panel-title__count")?.textContent === "未选中节点";

  it("clears the selection when the empty canvas is clicked", () => {
    const { container } = render(<XFlowEditor value={workflow} />);
    selectNode("api");
    expect(settingsShown()).toBe(false);
    const pane = container.querySelector<HTMLElement>(".react-flow__pane");
    if (!pane) throw new Error("canvas pane not found");
    fireEvent.click(pane);
    expect(settingsShown()).toBe(true);
  });

  it("clears the selection on Escape once no transient mode is open", () => {
    render(<XFlowEditor value={workflow} />);
    selectNode("api");
    fireEvent.keyDown(window, { key: "Escape" });
    expect(settingsShown()).toBe(true);
  });

  it("leaves the selection alone when Escape is typed into a field or a popup", () => {
    render(<XFlowEditor value={workflow} />);
    const panel = selectNode("api");
    const field = panel.querySelector<HTMLElement>("input, textarea");
    if (!field) throw new Error("no inspector field");
    fireEvent.keyDown(field, { key: "Escape" });
    expect(settingsShown()).toBe(false);
  });

  it("spends the first Escape on the connect tool, and the second on the selection", () => {
    render(<XFlowEditor value={workflow} />);
    selectNode("api");
    fireEvent.keyDown(window, { key: "l" });
    fireEvent.keyDown(window, { key: "Escape" });
    expect(settingsShown()).toBe(false);
    fireEvent.keyDown(window, { key: "Escape" });
    expect(settingsShown()).toBe(true);
  });
});

describe("inspector header rename keeps display name and identity apart", () => {
  const labelled: WorkflowDef = {
    ...workflow,
    nodes: [
      workflow.nodes![0],
      { name: "fetch_profile", type: "xflow.http", template: "tpl_a", ui: { label: "拉取客户档案" } },
      workflow.nodes![2]
    ],
    connections: { start: { main: [{ node: "fetch_profile" }] }, fetch_profile: { main: [{ node: "tail" }] } }
  };
  const label = () =>
    within(inspector()).getByText("拉取客户档案", { selector: ".xflow-editor-inspector-head__name" });

  it("shows the display name and a type line with the technical name and template", () => {
    render(<XFlowEditor value={labelled} />);
    const panel = selectNode("fetch_profile");
    expect(panel.querySelector(".xflow-editor-inspector-head__type")?.textContent).toBe(
      "fetch_profile · xflow.http · 模板 tpl_a"
    );
  });

  it("does not commit an untouched Enter or blur, and never renames the node identity", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={labelled} onChange={handleChange} />);
    selectNode("fetch_profile");

    fireEvent.doubleClick(label());
    fireEvent.keyDown(within(inspector()).getByLabelText("节点显示名"), { key: "Enter" });
    fireEvent.doubleClick(label());
    fireEvent.blur(within(inspector()).getByLabelText("节点显示名"));
    expect(handleChange).not.toHaveBeenCalled();
    // No history entry either: an untouched rename is not an edit.
    expect(undo().hasAttribute("disabled")).toBe(true);
  });

  it("writes ui.label for a real edit, leaves name and connections alone, and undoes in one step", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={labelled} onChange={handleChange} />);
    selectNode("fetch_profile");

    fireEvent.doubleClick(label());
    const field = within(inspector()).getByLabelText("节点显示名");
    fireEvent.change(field, { target: { value: "拉取档案 v2" } });
    fireEvent.keyDown(field, { key: "Enter" });

    const next = lastWorkflow(handleChange);
    expect(next.nodes?.[1]?.name).toBe("fetch_profile");
    expect(next.nodes?.[1]?.ui?.label).toBe("拉取档案 v2");
    expect(next.connections?.start?.main?.[0]?.node).toBe("fetch_profile");
    expect(within(inspector()).getByText("拉取档案 v2", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();

    fireEvent.click(undo());
    expect(lastWorkflow(handleChange).nodes?.[1]?.ui?.label).toBe("拉取客户档案");
  });

  it("does not start while the parameters draft is unparseable", () => {
    render(<XFlowEditor value={labelled} />);
    const panel = selectNode("fetch_profile");
    fireEvent.click(within(panel).getByRole("button", { name: "以 JSON 编辑参数" }));
    fireEvent.change(within(panel).getByLabelText("参数 JSON"), { target: { value: "{ broken" } });
    fireEvent.doubleClick(label());
    expect(within(inspector()).queryByLabelText("节点显示名")).toBeNull();
  });
});

const tab = (name: string): HTMLElement => within(inspector()).getByRole("tab", { name });
const tabPanels = (): HTMLElement[] => within(inspector()).getAllByRole("tabpanel", { hidden: true });

describe("inspector tabs (§6.4, AntD Tabs)", () => {
  it("names the four tabs exactly, in order, with parameters selected", () => {
    render(<XFlowEditor value={workflow} />);
    selectNode("api");

    const tabs = within(inspector()).getAllByRole("tab");
    expect(tabs.map((node) => node.textContent?.replace(/^Tab \d of \d/, ""))).toEqual(["参数", "设置", "连接 2", "运行"]);
    expect(tab("参数").getAttribute("aria-selected")).toBe("true");
    expect(tab("设置").getAttribute("aria-selected")).toBe("false");
  });

  it("mounts only the active panel", () => {
    render(<XFlowEditor value={workflow} />);
    selectNode("api");
    expect(tabPanels()).toHaveLength(1);
    expect(inspector().querySelector('[aria-label="节点参数"]')).not.toBeNull();
    expect(inspector().querySelector('[aria-label="节点设置"]')).toBeNull();

    fireEvent.click(tab("设置"));
    expect(tabPanels()).toHaveLength(1);
    expect(inspector().querySelector('[aria-label="节点设置"]')).not.toBeNull();
    expect(inspector().querySelector('[aria-label="节点参数"]')).toBeNull();

    fireEvent.click(tab("运行"));
    expect(tabPanels()).toHaveLength(1);
    expect(inspector().querySelector('[aria-label="节点设置"]')).toBeNull();
  });

  it("moves between tabs with the arrow, Home and End keys, and selection follows focus", () => {
    render(<XFlowEditor value={workflow} />);
    selectNode("api");

    // A focused AntD tab carries a hidden "Tab n of N" live region, so follow
    // the focused element instead of re-querying it by name.
    const focused = (): HTMLElement => document.activeElement as HTMLElement;
    const press = (key: string) => fireEvent.keyDown(focused(), { key, code: key });
    const parameters = tab("参数");
    parameters.focus();

    press("ArrowRight");
    expect(focused().textContent).toContain("设置");
    expect(focused().getAttribute("aria-selected")).toBe("true");
    expect(parameters.getAttribute("aria-selected")).toBe("false");
    press("End");
    expect(focused().textContent).toContain("运行");
    expect(focused().getAttribute("aria-selected")).toBe("true");
    press("Home");
    expect(focused()).toBe(parameters);
    expect(parameters.getAttribute("aria-selected")).toBe("true");
    press("ArrowLeft");
    expect(focused().textContent).toContain("运行");
    expect(focused().getAttribute("aria-selected")).toBe("true");
  });
});

describe("inspector tab count and run dot", () => {
  const countOf = (name: string): string | undefined =>
    tab(name).querySelector(".xflow-editor-inspector-tab-count")?.textContent?.trim() ?? undefined;

  it("counts wired edges in and out, hides zero, and follows add / remove", () => {
    render(<XFlowEditor value={workflow} />);
    selectNode("api");
    expect(countOf("连接")).toBe("2");
    // Baseline: inline text, the count a faint run with its own leading space
    // (`连接<span> 2</span>`), not a flex item 4px away from the label.
    expect(tab("连接").querySelector(".xflow-editor-inspector-tab-label")).toBeNull();
    expect(tab("连接").querySelector(".xflow-editor-inspector-tab-count")?.textContent).toBe(" 2");

    selectNode("tail");
    expect(countOf("连接")).toBe("1");

    selectNode("api");
    fireEvent.click(tab("连接"));
    fireEvent.click(within(inspector()).getByRole("button", { name: /^删除连接 api main 到 tail/ }));
    expect(countOf("连接")).toBe("1");

    fireEvent.click(within(inspector()).getByRole("button", { name: /^删除连接 start main 到 api/ }));
    expect(tab("连接").querySelector(".xflow-editor-inspector-tab-count")).toBeNull();

    fireEvent.click(within(inspector()).getByRole("button", { name: "连接到 tail" }));
    expect(countOf("连接")).toBe("1");
  });

  it("counts the pure helper the same way, including legacy dependency edges", () => {
    const wf: WorkflowDef = { ...workflow, dependency_edges: [{ supply: "start", node: "tail" }] };
    expect(countNodeConnections(wf, wf.nodes?.[2])).toBe(2);
    expect(countNodeConnections(workflow, undefined)).toBe(0);
    expect(countNodeConnections({ ...workflow, connections: {} }, workflow.nodes?.[1])).toBe(0);
  });

  it("shows a named danger dot on 运行 only while the node's latest run failed", () => {
    const failed: RuntimeSnapshot = { nodes: { api: { status: "failed" } } };
    const { rerender } = render(<XFlowEditor value={workflow} runtime={failed} />);
    selectNode("api");
    expect(within(tab("运行")).getByRole("img", { name: "最近一次运行失败" })).toBeTruthy();

    rerender(<XFlowEditor value={workflow} runtime={{ nodes: { api: { status: "success" } } }} />);
    expect(within(tab("运行")).queryByRole("img", { name: "最近一次运行失败" })).toBeNull();

    // `continued` is a warning tone, not danger: no dot.
    rerender(<XFlowEditor value={workflow} runtime={{ nodes: { api: { status: "continued" } } }} />);
    expect(within(tab("运行")).queryByRole("img", { name: "最近一次运行失败" })).toBeNull();

    rerender(<XFlowEditor value={workflow} runtime={failed} />);
    expect(within(tab("运行")).getByRole("img", { name: "最近一次运行失败" })).toBeTruthy();
    // Another node's failure does not mark this tab.
    selectNode("tail");
    expect(within(tab("运行")).queryByRole("img", { name: "最近一次运行失败" })).toBeNull();
  });
});
