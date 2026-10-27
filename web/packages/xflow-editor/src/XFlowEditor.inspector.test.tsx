import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { nodeCategoryMeta, nodeStatusMeta, type NodeCategory, type RuntimeSnapshot, type WorkflowDef, type WorkflowNode } from "@xflow/core";
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
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} 节点 "]`);
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
    // C03: no label, so the name is not repeated — the type line carries the type only.
    expect(typeLine?.textContent).toBe("xflow.http");
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
    expect(within(panel).getByRole("button", { name: "测试此节点" }).querySelector('[data-xflow-icon="play"]')).not.toBeNull();
    fireEvent.click(within(panel).getByRole("button", { name: "测试此节点" }));
    await vi.waitFor(() => expect(onRunNode).toHaveBeenCalledTimes(1));
    expect(onRunNode.mock.calls[0]?.[1]).toEqual({ node: "api", mode: "to" });
  });

  it("draws the key disabled with a user-language reason when the host has no onRunNode, and none without a node", () => {
    render(<XFlowEditor value={workflow} />);
    const panel = selectNode("api");
    const button = within(panel).getByRole("button", { name: "测试此节点" }) as HTMLButtonElement;
    expect(button.disabled).toBe(true);
    // The reason is both the tooltip and the accessible description (ED-12).
    const descriptionId = button.getAttribute("aria-describedby");
    expect(descriptionId).toBeTruthy();
    expect(document.getElementById(descriptionId as string)?.textContent).toBe("当前环境不支持单节点运行");
    expect(panel.textContent).not.toContain("宿主");
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

describe("node display name and header identity (IN-01 / C03)", () => {
  const labelled: WorkflowDef = {
    ...workflow,
    nodes: [
      workflow.nodes![0],
      { name: "fetch_profile", type: "xflow.http", ui: { label: "拉取客户档案" } },
      workflow.nodes![2]
    ],
    connections: { start: { main: [{ node: "fetch_profile" }] }, fetch_profile: { main: [{ node: "tail" }] } }
  };
  const displayNameField = (panel: HTMLElement): HTMLInputElement =>
    within(panel).getByLabelText("显示名") as HTMLInputElement;
  const head = (panel: HTMLElement): HTMLElement => panel.querySelector(".xflow-editor-inspector-head") as HTMLElement;
  const formHost = () => {
    const registry = createNodeFormRegistry();
    return { registry, compiler: new NodeFormCompiler(registry), renameRef: { current: undefined } };
  };

  it("writes editor_metadata ui.label from 基础信息 and falls back to the node name when cleared", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const panel = selectNode("api");
    const field = displayNameField(panel);
    expect(field.value).toBe("");
    // The placeholder is the identity, never a stored value.
    expect(field.placeholder).toBe("api");

    fireEvent.change(field, { target: { value: "拉取客户档案" } });
    fireEvent.blur(field);

    const next = lastWorkflow(handleChange);
    expect(next.nodes?.[1]?.name).toBe("api");
    expect(next.nodes?.[1]?.ui?.label).toBe("拉取客户档案");
    expect(within(inspector()).getByText("拉取客户档案", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();
    // A distinct label takes the primary line; the name moves to the type line.
    expect(inspector().querySelector(".xflow-editor-inspector-head__type")?.textContent).toBe("api · xflow.http");

    fireEvent.change(displayNameField(inspector()), { target: { value: "" } });
    fireEvent.blur(displayNameField(inspector()));

    expect(lastWorkflow(handleChange).nodes?.[1]?.ui?.label).toBeUndefined();
    expect(within(inspector()).getByText("api", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();
  });

  it("never stores the node name as a display name and writes nothing for a blank edit", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const field = displayNameField(selectNode("api"));

    fireEvent.change(field, { target: { value: " api " } });
    fireEvent.blur(field);
    expect(handleChange).not.toHaveBeenCalled();

    fireEvent.change(field, { target: { value: "   " } });
    fireEvent.blur(field);
    expect(handleChange).not.toHaveBeenCalled();
    expect(field.value).toBe("");
  });

  it("commits on Enter as well as blur", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const field = displayNameField(selectNode("api"));
    fireEvent.change(field, { target: { value: "抓取档案" } });
    fireEvent.keyDown(field, { key: "Enter" });
    expect(lastWorkflow(handleChange).nodes?.[1]?.ui?.label).toBe("抓取档案");
  });

  it("seeds the field from ui.label and disables it on a read-only panel", () => {
    const { unmount } = render(
      <Inspector
        editable={false}
        formHost={formHost()}
        parameterTab="form"
        selectedIndex={1}
        selectedNode={labelled.nodes?.[1]}
        workflow={labelled}
      />
    );
    const field = displayNameField(inspector());
    expect(field.value).toBe("拉取客户档案");
    expect(field.disabled).toBe(true);
    unmount();

    render(
      <Inspector
        formHost={formHost()}
        parameterTab="form"
        selectedIndex={1}
        selectedNode={workflow.nodes?.[1]}
        workflow={workflow}
        onChangeWith={vi.fn()}
      />
    );
    expect(displayNameField(inspector()).disabled).toBe(false);
  });

  it("shows the node name once when the label is empty or identical", () => {
    const echoed: WorkflowDef = {
      ...workflow,
      nodes: [
        workflow.nodes![0],
        { name: "api", type: "xflow.http", position: { x: 240, y: 120 }, ui: { label: "api" } },
        workflow.nodes![2]
      ]
    };
    const { unmount } = render(<XFlowEditor value={echoed} />);
    let panel = selectNode("api");
    expect(within(head(panel)).getAllByText("api")).toHaveLength(1);
    expect(panel.querySelector(".xflow-editor-inspector-head__type")?.textContent).toBe("xflow.http");
    unmount();

    // An empty label falls back to the name, still exactly once.
    render(
      <XFlowEditor
        value={{
          ...echoed,
          nodes: echoed.nodes?.map((node) => (node.name === "api" ? { ...node, ui: { label: "" } } : node))
        }}
      />
    );
    panel = selectNode("api");
    expect(within(head(panel)).getAllByText("api")).toHaveLength(1);
    expect(panel.querySelector(".xflow-editor-inspector-head__type")?.textContent).toBe("xflow.http");
  });

  it("shows a distinct label as the primary line and the name as the secondary one", () => {
    render(<XFlowEditor value={labelled} />);
    const panel = selectNode("fetch_profile");
    expect(within(panel).getByText("拉取客户档案", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();
    expect(panel.querySelector(".xflow-editor-inspector-head__type")?.textContent).toBe(
      "fetch_profile · xflow.http"
    );
  });
});

describe("node template field (IN-02 / §6.4)", () => {
  const templates = {
    tpl_a: { type: "xflow.http", parameters: { url: "https://example.com" } },
    tpl_b: { type: "xflow.function", parameters: { code: "return 1;" } }
  };
  const templated: WorkflowDef = {
    ...workflow,
    node_templates: templates,
    nodes: [
      workflow.nodes![0],
      { name: "api", type: "xflow.http", position: { x: 240, y: 120 } },
      workflow.nodes![2]
    ]
  };
  const usingTemplate: WorkflowDef = {
    ...templated,
    nodes: [
      templated.nodes![0],
      { name: "api", template: "tpl_a", position: { x: 240, y: 120 } },
      templated.nodes![2]
    ]
  };
  const formHost = () => {
    const registry = createNodeFormRegistry();
    return { registry, compiler: new NodeFormCompiler(registry), renameRef: { current: undefined } };
  };
  const templateField = (panel: HTMLElement): HTMLInputElement =>
    within(panel).getByRole("combobox", { name: "模板" }) as HTMLInputElement;
  const templateContent = (panel: HTMLElement): HTMLElement | null | undefined =>
    templateField(panel).closest(".ant-select")?.querySelector<HTMLElement>(".ant-select-content");
  const selectedLabel = (panel: HTMLElement): string | null =>
    templateContent(panel)?.textContent ?? null;
  const openOptions = (panel: HTMLElement): HTMLElement[] => {
    fireEvent.mouseDown(templateField(panel));
    const popup = [...document.querySelectorAll(".xflow-editor-select__popup")].at(-1);
    return [...(popup?.querySelectorAll<HTMLElement>(".ant-select-item-option") ?? [])];
  };
  const pickTemplate = (panel: HTMLElement, label: string): void => {
    const option = openOptions(panel).find((candidate) => candidate.textContent === label);
    if (!option) throw new Error(`no template option ${label}`);
    fireEvent.click(option);
  };

  it("lists 无模板 plus every node_templates key, and picking one writes template while dropping type", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={templated} onChange={handleChange} />);
    const panel = selectNode("api");

    const options = openOptions(panel);
    expect(options.map((option) => option.textContent)).toEqual(["无模板", "tpl_a", "tpl_b"]);
    expect(selectedLabel(panel)).toBe("无模板");

    fireEvent.click(options[1]!);

    const next = lastWorkflow(handleChange);
    expect(next.nodes?.[1]?.template).toBe("tpl_a");
    expect(next.nodes?.[1]?.type).toBeUndefined();
    expect("type" in (next.nodes?.[1] ?? {})).toBe(false);
    const updatedPanel = inspector();
    expect(selectedLabel(updatedPanel)).toBe("tpl_a");
    expect(updatedPanel.querySelector(".xflow-editor-inspector-head__type")?.textContent).toContain("模板 tpl_a");
    expect(within(updatedPanel).getByText("参数由模板提供，切换模板会改变表单。")).toBeTruthy();

    // One undo entry puts both fields back.
    fireEvent.click(undo());
    const undone = lastWorkflow(handleChange);
    expect(undone.nodes?.[1]?.type).toBe("xflow.http");
    expect(undone.nodes?.[1]?.template).toBeUndefined();
    expect(selectedLabel(inspector())).toBe("无模板");
  });

  it("clears template on 无模板 and restores the type the template shadowed", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={usingTemplate} onChange={handleChange} />);
    const panel = selectNode("api");
    expect(selectedLabel(panel)).toBe("tpl_a");

    pickTemplate(panel, "无模板");

    const next = lastWorkflow(handleChange);
    expect(next.nodes?.[1]?.template).toBeUndefined();
    expect("template" in (next.nodes?.[1] ?? {})).toBe(false);
    expect(next.nodes?.[1]?.type).toBe("xflow.http");
    expect(selectedLabel(inspector())).toBe("无模板");
    expect(within(inspector()).queryByText("参数由模板提供，切换模板会改变表单。")).toBeNull();
  });

  it("writes nothing when 无模板 is picked on a node that has no template", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={templated} onChange={handleChange} />);
    const panel = selectNode("api");
    expect(selectedLabel(panel)).toBe("无模板");

    pickTemplate(panel, "无模板");

    expect(handleChange).not.toHaveBeenCalled();
    expect(lastWorkflow(handleChange)).toBeUndefined();
  });

  it("draws antd's own empty-state copy in Chinese without an admin locale around the editor", () => {
    render(<XFlowEditor value={templated} onChange={vi.fn()} />);
    const panel = selectNode("api");

    fireEvent.change(templateField(panel), { target: { value: "no-such-template" } });

    const popup = [...document.querySelectorAll(".xflow-editor-select__popup")].at(-1);
    expect(popup?.textContent).toContain("暂无数据");
    expect(popup?.textContent).not.toContain("No data");
  });

  it("draws no 模板 row when node_templates is missing or empty", () => {
    const { unmount } = render(<XFlowEditor value={workflow} onChange={vi.fn()} />);
    let panel = selectNode("api");
    expect(within(panel).queryByRole("combobox", { name: "模板" })).toBeNull();
    expect(within(panel).queryByText("参数由模板提供，切换模板会改变表单。")).toBeNull();
    unmount();

    render(<XFlowEditor value={{ ...workflow, node_templates: {} }} onChange={vi.fn()} />);
    panel = selectNode("api");
    expect(within(panel).queryByRole("combobox", { name: "模板" })).toBeNull();
  });

  it("disables the 模板 select on a read-only panel", () => {
    const { unmount } = render(
      <Inspector
        editable={false}
        formHost={formHost()}
        parameterTab="form"
        selectedIndex={1}
        selectedNode={usingTemplate.nodes?.[1]}
        workflow={usingTemplate}
      />
    );
    expect(templateField(inspector()).disabled).toBe(true);
    unmount();

    render(
      <Inspector
        formHost={formHost()}
        parameterTab="form"
        selectedIndex={1}
        selectedNode={templated.nodes?.[1]}
        workflow={templated}
        onChangeWith={vi.fn()}
      />
    );
    expect(templateField(inspector()).disabled).toBe(false);
  });

  it("moves the chosen value with the selected node", () => {
    const mixed: WorkflowDef = {
      ...templated,
      nodes: [
        templated.nodes![0],
        templated.nodes![1],
        { name: "tail", template: "tpl_b", position: { x: 480, y: 120 } }
      ]
    };
    render(<XFlowEditor value={mixed} />);

    expect(selectedLabel(selectNode("api"))).toBe("无模板");
    const tailPanel = selectNode("tail");
    expect(selectedLabel(tailPanel)).toBe("tpl_b");
    expect(templateContent(tailPanel)?.getAttribute("title")).toBe("tpl_b");
  });
});

const tab = (name: string): HTMLElement => within(inspector()).getByRole("tab", { name });
const tabPanels = (): HTMLElement[] => within(inspector()).getAllByRole("tabpanel", { hidden: true });

describe("inspector tabs (§6.4, AntD Tabs)", () => {
  it("names the three tabs exactly, in order, with 配置 selected", () => {
    render(<XFlowEditor value={workflow} />);
    selectNode("api");

    const tabs = within(inspector()).getAllByRole("tab");
    expect(tabs.map((node) => node.textContent?.replace(/^Tab \d of \d/, ""))).toEqual(["配置", "连接 2", "运行"]);
    expect(tab("配置").getAttribute("aria-selected")).toBe("true");
    expect(tab("连接").getAttribute("aria-selected")).toBe("false");
  });

  it("mounts only the active panel, with the execution settings inside 配置", () => {
    render(<XFlowEditor value={workflow} />);
    selectNode("api");
    expect(tabPanels()).toHaveLength(1);
    expect(inspector().querySelector('[aria-label="节点参数"]')).not.toBeNull();
    expect(within(inspector()).getByRole("button", { name: "执行策略" })).toBeTruthy();

    fireEvent.click(tab("连接"));
    expect(tabPanels()).toHaveLength(1);
    expect(inspector().querySelector('[aria-label="节点参数"]')).toBeNull();
    expect(within(inspector()).queryByRole("button", { name: "执行策略" })).toBeNull();

    fireEvent.click(tab("运行"));
    expect(tabPanels()).toHaveLength(1);
    expect(inspector().querySelector('[aria-label="节点参数"]')).toBeNull();
  });

  it("keeps every old 设置 function in the merged execution section", () => {
    render(<XFlowEditor value={workflow} />);
    const panel = selectNode("api");

    // Collapsed by default, with the design's summary on the header.
    const execution = within(panel).getByRole("button", { name: "执行策略" });
    expect(execution.getAttribute("aria-expanded")).toBe("false");
    expect(panel.textContent).toContain("继承工作流");
    fireEvent.click(execution);
    expect(execution.getAttribute("aria-expanded")).toBe("true");

    // Error policy and timeout.
    expect(within(panel).getByLabelText("错误处理")).toBeTruthy();
    expect(within(panel).getByLabelText("超时")).toBeTruthy();

    // Retry: every knob of the old 设置 tab (the object group renders inline).
    for (const label of ["启用", "最大尝试次数", "策略", "初始间隔", "最大间隔", "倍率"]) {
      expect(within(panel).getByLabelText(label), `${label} is still editable`).toBeTruthy();
    }

    // Node-level Runner selector.
    const labels = within(panel).getByRole("group", { name: "匹配标签" });
    expect(labels).toBeTruthy();
    expect(within(labels).getByRole("button", { name: /添加/ })).toBeTruthy();
  });

  it("summarises the node's execution settings on the merged section header", () => {
    const configured: WorkflowDef = {
      ...workflow,
      settings: { retry: { max_attempts: 5 } },
      nodes: [
        { name: "start", type: "xflow.start", kind: "trigger" },
        {
          name: "api",
          type: "xflow.http",
          position: { x: 240, y: 120 },
          on_error: "continue",
          retry: { max_attempts: 2 },
          timeout: 30_000_000_000
        },
        { name: "tail", type: "xflow.end", position: { x: 480, y: 120 } }
      ]
    };
    render(<XFlowEditor value={configured} />);
    const panel = selectNode("api");

    // A node-level error policy, retry count and timeout win over the
    // workflow-level retry default (5), exactly as the engine resolves them.
    expect(panel.querySelector(".xflow-composer-section-summary")?.textContent).toBe(
      "错误处理 继续执行 · 重试 2 次 · 超时 30s"
    );
  });

  it("omits the retry summary when retries are explicitly disabled", () => {
    const configured: WorkflowDef = {
      ...workflow,
      settings: { retry: { max_attempts: 5 } },
      nodes: [
        { name: "start", type: "xflow.start", kind: "trigger" },
        {
          name: "api",
          type: "xflow.http",
          position: { x: 240, y: 120 },
          retry: { enabled: false, max_attempts: 2 },
          timeout: 30_000_000_000
        },
        { name: "tail", type: "xflow.end", position: { x: 480, y: 120 } }
      ]
    };
    const { unmount } = render(<XFlowEditor value={configured} />);
    let panel = selectNode("api");
    // engine/graph resolveRetry: enabled=false on the node suppresses both the
    // node count and the workflow fallback.
    expect(panel.querySelector(".xflow-composer-section-summary")?.textContent).toBe("继承工作流 · 超时 30s");
    unmount();

    render(
      <XFlowEditor
        value={{
          ...configured,
          settings: { retry: { enabled: false, max_attempts: 5 } },
          nodes: configured.nodes?.map((node) =>
            node.name === "api" ? { ...node, retry: undefined, on_error: undefined } : node
          )
        }}
      />
    );
    panel = selectNode("api");
    expect(panel.querySelector(".xflow-composer-section-summary")?.textContent).toBe("继承工作流 · 超时 30s");
  });

  it("moves between tabs with the arrow, Home and End keys, and selection follows focus", () => {
    render(<XFlowEditor value={workflow} />);
    selectNode("api");

    // A focused AntD tab carries a hidden "Tab n of N" live region, so follow
    // the focused element instead of re-querying it by name.
    const focused = (): HTMLElement => document.activeElement as HTMLElement;
    const press = (key: string) => fireEvent.keyDown(focused(), { key, code: key });
    const parameters = tab("配置");
    parameters.focus();

    press("ArrowRight");
    expect(focused().textContent).toContain("连接");
    expect(focused().getAttribute("aria-selected")).toBe("true");
    expect(parameters.getAttribute("aria-selected")).toBe("false");
    press("ArrowRight");
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

describe("connections tab groups (C02)", () => {
  const openConnections = (name: string): HTMLElement => {
    selectNode(name);
    fireEvent.click(tab("连接"));
    return inspector();
  };
  const connectionGroup = (name: string): HTMLElement =>
    within(inspector()).getByRole("group", { name });
  const groupCount = (name: string): string | undefined =>
    connectionGroup(name).querySelector(".xflow-editor-section-card__title em")?.textContent?.trim() ?? undefined;
  const withNode = (node: WorkflowNode): WorkflowDef => ({
    ...workflow,
    nodes: [...(workflow.nodes ?? []), node]
  });

  it("draws 输入端口 / 输出端口 / 错误策略 with in-and-out counts, and keeps empty groups on screen", () => {
    render(<XFlowEditor value={withNode({ name: "lonely", type: "xflow.function", position: { x: 720, y: 120 } })} />);
    openConnections("lonely");

    expect(connectionGroup("输入端口").querySelector(".xflow-editor-section-card__title strong")?.textContent).toBe(
      "输入端口"
    );
    expect(groupCount("输入端口")).toBe("0");
    expect(groupCount("输出端口")).toBe("0");
    expect(connectionGroup("错误策略").querySelector(".xflow-editor-section-card__title strong")?.textContent).toBe(
      "错误策略"
    );
    // An empty bucket keeps its card and says why; the tab does not collapse.
    expect(within(connectionGroup("输入端口")).getByText("暂无输入连接。")).toBeTruthy();
    expect(within(connectionGroup("输出端口")).getByText("暂无输出连接。")).toBeTruthy();
    expect(within(inspector()).getByText("在配置 tab 修改。")).toBeTruthy();
  });

  it("counts only the side that has edges", () => {
    render(<XFlowEditor value={workflow} />);

    openConnections("tail");
    expect(groupCount("输入端口")).toBe("1");
    expect(groupCount("输出端口")).toBe("0");
    expect(within(connectionGroup("输出端口")).getByText("暂无输出连接。")).toBeTruthy();

    openConnections("start");
    expect(groupCount("输入端口")).toBe("0");
    expect(groupCount("输出端口")).toBe("1");
    expect(within(connectionGroup("输入端口")).getByText("暂无输入连接。")).toBeTruthy();
  });

  it("groups incoming by target port and outgoing by source port, display name first", () => {
    const grouped: WorkflowDef = {
      ...workflow,
      nodes: [
        { name: "start", type: "xflow.start", kind: "trigger", ui: { label: "入口" } },
        { name: "feeder", type: "xflow.function" },
        { name: "api", type: "xflow.http", inputs: [{ name: "main" }, { name: "payload" }] },
        { name: "tail", type: "xflow.end", ui: { label: "结束" } },
        { name: "handler", type: "xflow.function", ui: { label: "错误处理" } }
      ],
      connections: {
        start: { main: [{ node: "api" }] },
        feeder: { main: [{ node: "api", input: "payload" }] },
        api: { main: [{ node: "tail" }], error: [{ node: "handler" }] }
      }
    };
    render(<XFlowEditor value={grouped} />);
    openConnections("api");

    const incoming = connectionGroup("输入端口");
    const inputMain = within(incoming).getByRole("group", { name: "输入端口 main" });
    const inputPayload = within(incoming).getByRole("group", { name: "输入端口 payload" });
    expect(inputMain.textContent).toContain("入口");
    expect(inputMain.textContent).toContain("start");
    expect(inputPayload.textContent).toContain("feeder");
    expect(inputPayload.textContent).not.toContain("入口");

    const outgoing = connectionGroup("输出端口");
    const outputMain = within(outgoing).getByRole("group", { name: "输出端口 main" });
    const outputError = within(outgoing).getByRole("group", { name: "输出端口 error" });
    expect(outputMain.textContent).toContain("结束");
    expect(outputMain.textContent).toContain("tail");
    expect(outputError.textContent).toContain("错误处理");
    expect(outputError.textContent).toContain("handler");
  });

  it("keeps the 添加数据连接 form inside 输出端口 and writes through onChange", () => {
    const handleChange = vi.fn();
    const withSink: WorkflowDef = {
      ...workflow,
      nodes: [...(workflow.nodes ?? []), { name: "sink", type: "xflow.function", position: { x: 720, y: 120 } }]
    };
    render(<XFlowEditor value={withSink} onChange={handleChange} />);
    openConnections("api");

    const output = connectionGroup("输出端口");
    expect(within(output).getByText("添加数据连接")).toBeTruthy();
    fireEvent.click(within(output).getByRole("button", { name: "连接到 sink" }));

    expect(lastWorkflow(handleChange).connections?.api?.main).toEqual([{ node: "tail" }, { node: "sink" }]);
  });

  it("keeps deletion inside the grouped rows and empties the bucket", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    openConnections("api");

    const incoming = connectionGroup("输入端口");
    fireEvent.click(within(incoming).getByRole("button", { name: "删除连接 start main 到 api main" }));

    expect(lastWorkflow(handleChange).connections?.start).toEqual({ main: [] });
    expect(groupCount("输入端口")).toBe("0");
    expect(within(connectionGroup("输入端口")).getByText("暂无输入连接。")).toBeTruthy();
  });

  it("reports on_error and the node's retry policy read-only", () => {
    const configured: WorkflowDef = {
      ...workflow,
      settings: { retry: { max_attempts: 5, strategy: "exponential" } },
      nodes: [
        workflow.nodes![0],
        {
          name: "api",
          type: "xflow.http",
          position: { x: 240, y: 120 },
          on_error: "main_output",
          retry: { max_attempts: 3, strategy: "fixed" }
        },
        workflow.nodes![2]
      ]
    };
    render(<XFlowEditor value={configured} />);
    openConnections("api");

    const policy = connectionGroup("错误策略");
    expect(policy.textContent).toContain("主端口");
    expect(policy.textContent).toContain("重试 3 次");
    expect(policy.textContent).toContain("固定间隔");
    expect(policy.textContent).toContain("在配置 tab 修改");
    // Read-only by design in this batch: no write controls in the group.
    expect(within(policy).queryByRole("button")).toBeNull();
  });

  it("shows inherited defaults and 未启用, and honours a node-level disabled retry", () => {
    const inherited: WorkflowDef = {
      ...workflow,
      settings: { on_error: "continue", retry: { max_attempts: 5, strategy: "exponential" } },
      nodes: [workflow.nodes![0], { name: "api", type: "xflow.http" }, workflow.nodes![2]]
    };
    const { unmount } = render(<XFlowEditor value={inherited} />);
    openConnections("api");
    expect(connectionGroup("错误策略").textContent).toContain("继承工作流（继续执行）");
    expect(connectionGroup("错误策略").textContent).toContain("继承工作流 · 重试 5 次 · 指数退避");
    unmount();

    const disabled: WorkflowDef = {
      ...inherited,
      nodes: [
        inherited.nodes![0],
        { name: "api", type: "xflow.http", retry: { enabled: false, max_attempts: 2 } },
        inherited.nodes![2]!
      ]
    };
    render(<XFlowEditor value={disabled} />);
    openConnections("api");
    const policy = connectionGroup("错误策略");
    // engine/graph resolveRetry: enabled=false suppresses the workflow fallback.
    expect(policy.textContent).toContain("未启用");
    expect(policy.textContent).not.toContain("重试 2 次");
  });

  it("disables the add controls and every delete button on a read-only panel", () => {
    const registry = createNodeFormRegistry();
    const formHost = { registry, compiler: new NodeFormCompiler(registry), renameRef: { current: undefined } };
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
    fireEvent.click(tab("连接"));

    expect(
      (within(inspector()).getByRole("button", { name: "连接到 tail" }) as HTMLButtonElement).disabled
    ).toBe(true);
    expect(
      (within(inspector()).getByRole("button", { name: "删除连接 start main 到 api main" }) as HTMLButtonElement)
        .disabled
    ).toBe(true);
    expect((within(inspector()).getByRole("combobox", { name: "输出端口" }) as HTMLInputElement).disabled).toBe(true);
    expect((within(inspector()).getByRole("combobox", { name: "输入端口" }) as HTMLInputElement).disabled).toBe(true);
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
