// The left column's 变量 panel (params / context.vars / context.config /
// credential references), the ⌘K node search (jump by name, next to the outline), and
// the Inspector's workflow settings shown with no node selected.

import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";
import { NodeFormCompiler, createNodeFormRegistry } from "./node-form";
import { Inspector } from "./panels/inspector";
import { expressionReference } from "./panels/variables";

const workflow: WorkflowDef = {
  id: "wf-variables",
  name: "Variables",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "worker", type: "xflow.function", parameters: { a: 1 } }
  ],
  connections: { start: { main: [{ node: "worker" }] } },
  params: { amount: { type: "number", required: true } },
  context: { vars: { threshold: 10 }, config: { region: "cn" } },
  credentials: { github: { name: "gh-prod", type: "apiKey" } }
};

const lastWorkflow = (spy: ReturnType<typeof vi.fn>) => spy.mock.calls.at(-1)?.[0] as WorkflowDef;
const variablesPanel = (): HTMLElement => {
  if (!screen.queryByRole("region", { name: "变量" })) fireEvent.click(screen.getByRole("button", { name: "变量" }));
  return screen.getByRole("region", { name: "变量" });
};
const section = (name: string) => within(variablesPanel()).getByRole("group", { name });
const canvasNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} 节点 "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  return node;
};

const originalClipboard = Object.getOwnPropertyDescriptor(navigator, "clipboard");
afterEach(() => {
  if (originalClipboard) Object.defineProperty(navigator, "clipboard", originalClipboard);
  else Reflect.deleteProperty(navigator, "clipboard");
});

/** Opens an editor AntD Select and clicks the option titled `label`. */
const pickEditorOption = (combo: HTMLElement, label: string): void => {
  fireEvent.mouseDown(combo);
  const popups = [...document.querySelectorAll(".xflow-editor-select__popup")];
  const option = popups.at(-1)?.querySelector<HTMLElement>(`[title="${label}"]`);
  if (!option) throw new Error(`no option ${label}`);
  fireEvent.click(option);
};

describe("expressionReference", () => {
  it("uses dot paths for identifiers and bracket paths otherwise", () => {
    expect(expressionReference("$params", "amount")).toBe("${{ $params.amount }}");
    expect(expressionReference("$vars", "my-key")).toBe("${{ $vars['my-key'] }}");
    expect(expressionReference("$config", "region")).toBe("${{ $config.region }}");
  });
});

describe("VariablesPanel", () => {
  it("switches the left column between the node library and the variables panel", () => {
    render(<XFlowEditor value={workflow} />);
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
    expect(screen.queryByRole("region", { name: "变量" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "变量" }));
    expect(screen.getByRole("region", { name: "变量" })).toBeTruthy();
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    expect(screen.getByRole("button", { name: "变量" }).getAttribute("aria-pressed")).toBe("true");

    fireEvent.click(screen.getByRole("button", { name: "节点与大纲" }));
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
  });

  it("adds an input parameter and writes it to workflow.params", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={{ ...workflow, params: undefined }} onChange={handleChange} />);
    const params = section("输入参数");
    expect(within(params).getByText(/暂无输入参数/)).toBeTruthy();

    fireEvent.click(within(params).getByRole("button", { name: "添加输入参数" }));
    expect(lastWorkflow(handleChange).params).toEqual({ param_1: { type: "string" } });

    fireEvent.change(within(section("输入参数")).getByLabelText("输入参数 1 名称"), { target: { value: "amount" } });
    expect(lastWorkflow(handleChange).params).toEqual({ amount: { type: "string" } });

    fireEvent.change(within(section("输入参数")).getByLabelText("输入参数 1 显示名"), { target: { value: "金额" } });
    fireEvent.click(within(section("输入参数")).getByLabelText("输入参数 1 必填"));
    expect(lastWorkflow(handleChange).params).toEqual({ amount: { type: "string", display_name: "金额", required: true } });

    fireEvent.click(within(section("输入参数")).getByRole("button", { name: "删除输入参数 amount" }));
    expect(lastWorkflow(handleChange).params).toBeUndefined();
  });

  it("blocks a duplicate or empty parameter name without writing it", () => {
    const handleChange = vi.fn();
    render(
      <XFlowEditor
        value={{ ...workflow, params: { amount: { type: "number" }, currency: { type: "string" } } }}
        onChange={handleChange}
      />
    );
    const name = within(section("输入参数")).getByLabelText("输入参数 2 名称") as HTMLInputElement;

    fireEvent.change(name, { target: { value: "amount" } });
    expect(within(section("输入参数")).getByRole("alert").textContent).toBe("名称已存在: amount");
    expect(name.getAttribute("aria-invalid")).toBe("true");

    fireEvent.change(name, { target: { value: "  " } });
    expect(within(section("输入参数")).getByRole("alert").textContent).toBe("名称不能为空");
    expect(handleChange).not.toHaveBeenCalled();
  });

  it("edits context vars as key/value rows with JSON literal values", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.change(within(section("变量")).getByLabelText("变量 1 值"), { target: { value: "25" } });
    expect(lastWorkflow(handleChange).context).toEqual({ vars: { threshold: 25 }, config: { region: "cn" } });

    fireEvent.change(within(section("变量")).getByLabelText("变量 1 值"), { target: { value: "high" } });
    expect(lastWorkflow(handleChange).context?.vars).toEqual({ threshold: "high" });

    fireEvent.click(within(section("配置")).getByRole("button", { name: "添加配置" }));
    expect(lastWorkflow(handleChange).context?.config).toEqual({ region: "cn", config_2: "" });
  });

  it("toggles each section to its source view, keeping the workflow JSON aria-labels", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    const toggle = within(section("输入参数")).getByRole("button", { name: "以 JSON 编辑输入参数" });
    expect(toggle.getAttribute("aria-pressed")).toBe("false");
    fireEvent.click(toggle);
    expect(toggle.getAttribute("aria-pressed")).toBe("true");
    const source = within(section("输入参数")).getByLabelText("工作流输入参数 JSON") as HTMLTextAreaElement;
    expect(JSON.parse(source.value)).toEqual(workflow.params);
    fireEvent.change(source, { target: { value: '{ "limit": { "type": "number" } }' } });
    expect(lastWorkflow(handleChange).params).toEqual({ limit: { type: "number" } });

    // Back on the rows, they read what the source wrote.
    fireEvent.click(toggle);
    expect((within(section("输入参数")).getByLabelText("输入参数 1 名称") as HTMLInputElement).value).toBe("limit");

    // context has one source, shown in the 变量 card for both cards.
    fireEvent.click(within(section("配置")).getByRole("button", { name: "以 JSON 编辑配置" }));
    expect(within(section("变量")).getByLabelText("工作流变量配置 JSON")).toBeTruthy();
    expect(within(section("配置")).queryByLabelText("工作流变量配置 JSON")).toBeNull();

    fireEvent.click(within(section("密钥引用")).getByRole("button", { name: "以 JSON 编辑密钥引用" }));
    expect(within(section("密钥引用")).getByLabelText("工作流密钥 JSON")).toBeTruthy();
  });

  it("records credential references by name and type and never renders a secret value field", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const credentials = section("密钥引用");

    expect((within(credentials).getByLabelText("密钥引用 1 凭证名称") as HTMLInputElement).value).toBe("gh-prod");
    expect(within(credentials).queryByLabelText(/值|密文|secret|value/i)).toBeNull();
    expect(credentials.querySelector('input[type="password"]')).toBeNull();

    fireEvent.click(within(credentials).getByRole("button", { name: "添加密钥引用" }));
    expect(lastWorkflow(handleChange).credentials).toEqual({
      github: { name: "gh-prod", type: "apiKey" },
      credential_2: { name: "", type: "apiKey" }
    });
  });

  it("copies the expression reference of a row to the clipboard", async () => {
    const writeText = vi.fn(async () => undefined);
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    render(<XFlowEditor value={workflow} />);

    fireEvent.click(within(section("输入参数")).getByRole("button", { name: "复制 amount 的引用" }));
    fireEvent.click(within(section("配置")).getByRole("button", { name: "复制 region 的引用" }));
    fireEvent.click(within(section("密钥引用")).getByRole("button", { name: "复制 github 的引用" }));

    expect(writeText.mock.calls.map(([text]) => text)).toEqual(["${{ $params.amount }}", "${{ $config.region }}", "github"]);
    expect(await screen.findByText("已复制 ${{ $params.amount }}")).toBeTruthy();
  });

  it("disables the copy buttons where no clipboard is available", () => {
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: undefined });
    render(<XFlowEditor value={workflow} />);
    expect((within(section("输入参数")).getByRole("button", { name: "复制 amount 的引用" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("shows the variables read-only outside edit mode, reached from the rail", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} readOnlyReason="没有写权限" onChange={handleChange} />);
    // A read-only viewer opens on the outline alone; 变量 is one rail key away.
    expect(screen.getByRole("region", { name: "大纲" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "变量" }));

    const variables = screen.getByRole("region", { name: "变量" });
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    expect((within(variables).getByLabelText("输入参数 1 名称") as HTMLInputElement).readOnly).toBe(true);
    expect((within(variables).getByRole("button", { name: "添加输入参数" }) as HTMLButtonElement).disabled).toBe(true);
    expect((within(variables).getByRole("button", { name: "删除变量 threshold" }) as HTMLButtonElement).disabled).toBe(true);
    expect(handleChange).not.toHaveBeenCalled();
  });
});

describe("node search and workflow settings", () => {
  it("opens the node search with ⌘K and selects the chosen node", async () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.keyDown(window, { key: "k", ctrlKey: true });
    const search = screen.getByRole("combobox", { name: "定位节点" });
    await waitFor(() => expect(document.activeElement).toBe(search));
    fireEvent.change(search, { target: { value: "xflow.start" } });
    fireEvent.click(await screen.findByText("start", { selector: ".xflow-editor-node-search__option strong" }));

    await waitFor(() => expect(canvasNode("start").closest(".xflow-preview-node")?.getAttribute("data-selected")).toBe("true"));
    expect(handleChange).not.toHaveBeenCalled();
  });

  it("lays the settings out in §6.4 order, with 标识 and 时区 under 基本", () => {
    const handleChange = vi.fn();
    render(
      <XFlowEditor
        value={{ ...workflow, id: "wf-abcdefgh-1234", settings: { timezone: "Asia/Shanghai" } }}
        onChange={handleChange}
      />
    );
    fireEvent.keyDown(window, { key: "Escape" });
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });

    // 入口 sits between 基本 and 并发, and no dead 超时 row survives the split.
    const groups = within(inspector)
      .getAllByRole("group")
      .map((group) => group.getAttribute("aria-label"))
      .filter((name): name is string => Boolean(name));
    expect(groups.slice(0, 5)).toEqual(["基本", "入口", "并发", "错误与重试", "高级"]);
    expect(groups.at(-1)).toBe("Pin Data");
    expect(within(inspector).queryByLabelText("超时")).toBeNull();
    expect(within(inspector).queryByText("超时")).toBeNull();

    // The fixture's start node is the workflow's only entry.
    const entry = within(inspector).getByRole("group", { name: "入口" });
    expect(within(entry).getByText("start")).toBeTruthy();
    expect(within(entry).getByText("xflow.start")).toBeTruthy();
    // ED-19: the row reuses the outline's category icon + secondary type, and
    // the locate key is an icon (not the word 定位).
    expect(entry.querySelector(".xflow-editor-entry-row .xflow-editor-category-icon")?.getAttribute("data-cat")).toBe(
      "control"
    );
    const locate = within(entry).getByRole("button", { name: "定位入口 start" });
    expect(locate.classList.contains("xflow-editor-entry-row__locate")).toBe(true);
    expect(locate.querySelector("svg")).toBeTruthy();
    expect(within(entry).queryByText("定位")).toBeNull();

    const basic = within(inspector).getByRole("group", { name: "基本" });
    expect((within(basic).getByLabelText("执行时区") as HTMLInputElement).value).toBe("Asia/Shanghai");
    const idText = basic.querySelector(".xflow-editor-workflow-id__text");
    expect(idText?.textContent).toBe("wf-abcde…");
    expect(idText?.getAttribute("title")).toBe("wf-abcdefgh-1234");
    expect(idText?.getAttribute("aria-label")).toBe("wf-abcdefgh-1234");
    // The 24px hit-area rule hangs off Typography's semantic action hook, not
    // its generated `.ant-*` class.
    expect(
      within(basic).getByRole("button", { name: "复制工作流标识" }).classList.contains("xflow-editor-workflow-id__copy"),
    ).toBe(true);

    const errorRetry = within(inspector).getByRole("group", { name: "错误与重试" });
    expect(within(errorRetry).getByText("节点默认继承")).toBeTruthy();

    // 高级 is collapsed by default: no Runner row, no option, no source view.
    const advanced = within(inspector).getByRole("group", { name: "高级" });
    expect(within(advanced).queryByLabelText("Runner 模式")).toBeNull();
    expect(within(advanced).queryByLabelText("允许环路")).toBeNull();
    expect(within(advanced).queryByText(/FAF/i)).toBeNull();
    fireEvent.click(within(advanced).getByRole("button", { name: "展开高级" }));
    expect(within(advanced).getByLabelText("Runner 模式")).toBeTruthy();
    expect(within(advanced).getByLabelText("Pin 数据模式")).toBeTruthy();
    expect(within(advanced).getByLabelText("允许环路")).toBeTruthy();
    expect(within(advanced).getByLabelText("最大自动深度")).toBeTruthy();
    expect(within(advanced).getByLabelText("节点组协议（实验）")).toBeTruthy();
    expect(within(advanced).getByLabelText("Transient 执行")).toBeTruthy();
    // The open state is local: expanding and collapsing writes nothing.
    fireEvent.click(within(advanced).getByRole("button", { name: "收起高级" }));
    expect(within(advanced).queryByLabelText("Runner 模式")).toBeNull();
    expect(handleChange).not.toHaveBeenCalled();
  });

  it("keeps settings help to one line with a ? key and hides raw enums as secondary text (ED-18)", () => {
    render(<XFlowEditor value={workflow} />);
    fireEvent.keyDown(window, { key: "Escape" });
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });

    const errorRetry = within(inspector).getByRole("group", { name: "错误与重试" });
    expect(within(errorRetry).getByText("未单独配置的节点沿用这里的默认值。")).toBeTruthy();
    expect(within(errorRetry).queryByText(/节点未单独配置错误处理与重试时/)).toBeNull();
    expect(
      within(errorRetry).getByRole("button", { name: "帮助：未单独配置的节点沿用这里的默认值。" })
    ).toBeTruthy();

    const pinData = within(inspector).getByRole("group", { name: "Pin Data" });
    expect(within(pinData).getByText("按节点名固定输出，调试时跳过真实执行。")).toBeTruthy();
    expect(within(pinData).getByRole("button", { name: /^帮助：/ })).toBeTruthy();
    expect(within(pinData).queryByText(/键是任意节点名/)).toBeNull();

    // The list leads with the Chinese meaning; the wire enum follows as mono text.
    fireEvent.mouseDown(within(errorRetry).getByLabelText("执行出错时"));
    const popup = [...document.querySelectorAll<HTMLElement>(".xflow-editor-select__popup")].at(-1);
    expect(popup).toBeTruthy();
    expect(within(popup!).getByText("失败即停止")).toBeTruthy();
    // Every option carries its raw enum as the secondary mono value.
    const rawValues = [...popup!.querySelectorAll(".xflow-editor-select__option-value")].map((node) => node.textContent);
    expect(rawValues).toEqual(["stop", "error_output", "main_output", "continue"]);
    expect(within(popup!).queryByText("stop：失败即停止")).toBeNull();
  });

  it("lists start, trigger-typed and trigger-kind nodes in 入口, and 定位 selects the node", () => {
    const entryWorkflow: WorkflowDef = {
      ...workflow,
      nodes: [
        { name: "hook", type: "xflow.trigger.webhook", ui: { label: "客户回调" } },
        { name: "start", type: "xflow.start" },
        { name: "worker", type: "xflow.function", kind: "action" }
      ],
      connections: {}
    };
    render(<XFlowEditor value={entryWorkflow} />);
    fireEvent.keyDown(window, { key: "Escape" });
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    const entry = within(inspector).getByRole("group", { name: "入口" });

    // Only the webhook and the start node are entries; the action node is not.
    expect(within(entry).getAllByRole("listitem")).toHaveLength(2);
    // Each row's icon follows the type's own category (ED-19).
    const entryRows = within(entry).getAllByRole("listitem");
    expect(entryRows.map((row) => row.querySelector(".xflow-editor-category-icon")?.getAttribute("data-cat"))).toEqual([
      "trigger",
      "control"
    ]);
    // The display label wins over the node name, and the type keeps its own cell.
    expect(within(entry).getByText("客户回调")).toBeTruthy();
    expect(within(entry).getByText("xflow.trigger.webhook")).toBeTruthy();
    expect(within(entry).getByText("xflow.start")).toBeTruthy();
    expect(within(entry).queryByText("worker")).toBeNull();

    fireEvent.click(within(entry).getByRole("button", { name: "定位入口 客户回调" }));
    expect(canvasNode("hook").closest(".xflow-preview-node")?.getAttribute("data-selected")).toBe("true");
    // Selecting an entry hands the Inspector to that node's form.
    const nodePanel = screen.getByRole("region", { name: "属性" });
    expect(within(nodePanel).getByText("客户回调", { selector: ".xflow-editor-inspector-head__name" })).toBeTruthy();
  });

  it("draws 入口 rows without a 定位 key when no locate callback is passed, and an empty note with no entries", () => {
    const registry = createNodeFormRegistry();
    const formHost = { registry, compiler: new NodeFormCompiler(registry), renameRef: { current: undefined } };
    const { unmount } = render(
      <Inspector formHost={formHost} parameterTab="form" selectedIndex={-1} workflow={workflow} />
    );
    const entry = screen.getByRole("group", { name: "入口" });
    expect(within(entry).getByText("start")).toBeTruthy();
    expect(within(entry).queryByRole("button", { name: /^定位入口/ })).toBeNull();
    unmount();

    render(
      <Inspector
        formHost={formHost}
        parameterTab="form"
        selectedIndex={-1}
        workflow={{ ...workflow, nodes: [{ name: "worker", type: "xflow.function" }] }}
      />
    );
    const empty = screen.getByRole("group", { name: "入口" });
    expect(within(empty).getByText("该工作流没有入口节点。")).toBeTruthy();
    expect(within(empty).queryByRole("listitem")).toBeNull();
  });

  it("deselects from the Inspector header into the workflow settings, and back by selecting a node", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    expect(within(inspector()).queryByLabelText("工作流名称")).toBeNull();

    fireEvent.keyDown(window, { key: "Escape" });
    expect(within(inspector()).getByRole("group", { name: "基本" })).toBeTruthy();
    // Runner and the advanced options sit behind the collapsed 高级 section.
    expect(within(inspector()).queryByLabelText("Runner 模式")).toBeNull();
    expect(within(inspector()).queryByLabelText("允许环路")).toBeNull();
    fireEvent.click(within(inspector()).getByRole("button", { name: "展开高级" }));
    expect(within(inspector()).getByLabelText("Runner 模式")).toBeTruthy();
    expect(within(inspector()).getByLabelText("允许环路")).toBeTruthy();
    // 并发 and 错误与重试 stay open and reachable beside it.
    expect(within(inspector()).getByLabelText("并发上限")).toBeTruthy();
    expect(within(inspector()).getByLabelText("自动重试")).toBeTruthy();
    expect(within(inspector()).queryByLabelText("工作流 Runner 选择 JSON")).toBeNull();
    expect(within(inspector()).getByLabelText("工作流 Pin Data JSON")).toBeTruthy();

    fireEvent.click(within(inspector()).getByRole("button", { name: "以 JSON 编辑 Runner 选择" }));
    expect(within(inspector()).getByLabelText("工作流 Runner 选择 JSON")).toBeTruthy();
    fireEvent.click(within(inspector()).getByRole("button", { name: "以 JSON 编辑执行设置" }));
    expect(within(inspector()).getByLabelText("工作流执行设置 JSON")).toBeTruthy();
    // Params / context / credentials moved to the left column.
    expect(within(inspector()).queryByLabelText("工作流输入参数 JSON")).toBeNull();

    fireEvent.change(within(inspector()).getByLabelText("工作流描述"), { target: { value: "Edited" } });
    expect(lastWorkflow(handleChange).description).toBe("Edited");

    fireEvent.click(canvasNode("worker"));
    expect(within(inspector()).queryByLabelText("工作流名称")).toBeNull();
    expect(screen.getByRole("region", { name: "属性" })).toBeTruthy();
  });

  it("writes the structured Runner, settings and advanced option rows into the workflow", () => {
    const handleChange = vi.fn();
    render(
      <XFlowEditor
        value={{ ...workflow, runner_selector: { mode: "required", match_labels: { env: "prod" } } }}
        onChange={handleChange}
      />
    );
    const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.keyDown(window, { key: "Escape" });
    // The Runner rows and every advanced option live inside the collapsed 高级 card.
    fireEvent.click(within(inspector()).getByRole("button", { name: "展开高级" }));

    // Runner: a mode select plus one key/value row per match label.
    const mode = within(inspector()).getByLabelText("Runner 模式").closest(".xflow-editor-select__root");
    expect(mode?.textContent).toContain("必须匹配标签");
    pickEditorOption(within(inspector()).getByLabelText("Runner 模式"), "任意可用 Runner");
    expect(lastWorkflow(handleChange).runner_selector).toEqual({ mode: "default", match_labels: { env: "prod" } });

    fireEvent.change(within(inspector()).getByLabelText("匹配标签 1 值"), { target: { value: "staging" } });
    expect(lastWorkflow(handleChange).runner_selector).toEqual({ mode: "default", match_labels: { env: "staging" } });

    fireEvent.click(within(inspector()).getByRole("button", { name: "删除匹配标签 env" }));
    expect(lastWorkflow(handleChange).runner_selector).toEqual({ mode: "default" });

    // 并发: each row writes exactly its own key of workflow.settings.
    // A bare limit alone keeps the int shorthand the server preserves.
    fireEvent.change(within(inspector()).getByLabelText("并发上限"), { target: { value: "4" } });
    expect(lastWorkflow(handleChange).settings).toEqual({ concurrency: 4 });

    // Reject mode forces the object form; queue controls only exist in queue mode.
    fireEvent.click(within(inspector()).getByRole("radio", { name: "拒绝" }));
    expect(lastWorkflow(handleChange).settings).toEqual({ concurrency: { limit: 4, mode: "reject" } });
    expect(within(inspector()).queryByLabelText("队列上限")).toBeNull();
    expect(within(inspector()).queryByLabelText("排队超时（分钟）")).toBeNull();

    // Back to the default queue mode the limit collapses to the shorthand again.
    fireEvent.click(within(inspector()).getByRole("radio", { name: "排队" }));
    expect(lastWorkflow(handleChange).settings).toEqual({ concurrency: 4 });
    fireEvent.change(within(inspector()).getByLabelText("队列上限"), { target: { value: "9" } });
    expect(lastWorkflow(handleChange).settings).toEqual({
      concurrency: { limit: 4, mode: "queue", max_queued: 9 }
    });
    // Minutes are stored as a nanosecond integer.
    fireEvent.change(within(inspector()).getByLabelText("排队超时（分钟）"), { target: { value: "2" } });
    expect(lastWorkflow(handleChange).settings).toEqual({
      concurrency: { limit: 4, mode: "queue", max_queued: 9, queue_timeout: 120000000000 }
    });

    // 基本 carries the timezone and the execution timeout; 错误与重试 carries the error policy and retry.
    // The timeout is the Go time.Duration wire form: integer nanoseconds, never a "30s" string.
    fireEvent.change(within(inspector()).getByLabelText("执行超时"), { target: { value: "1800000000000" } });
    expect(lastWorkflow(handleChange).settings).toEqual({
      timeout: 1800000000000,
      concurrency: { limit: 4, mode: "queue", max_queued: 9, queue_timeout: 120000000000 }
    });
    fireEvent.change(within(inspector()).getByLabelText("执行时区"), { target: { value: "Asia/Shanghai" } });
    pickEditorOption(within(inspector()).getByLabelText("执行出错时"), "继续执行");
    pickEditorOption(within(inspector()).getByLabelText("Pin 数据模式"), "总是使用");
    expect(lastWorkflow(handleChange).settings).toEqual({
      timeout: 1800000000000,
      concurrency: { limit: 4, mode: "queue", max_queued: 9, queue_timeout: 120000000000 },
      timezone: "Asia/Shanghai",
      on_error: "continue",
      pin_data_mode: "always"
    });

    // Retry keeps its nested object, and its rows only appear once it is on.
    expect(within(inspector()).queryByLabelText("重试最大尝试次数")).toBeNull();
    fireEvent.click(within(inspector()).getByLabelText("自动重试"));
    expect(lastWorkflow(handleChange).settings?.retry).toEqual({ enabled: true });
    fireEvent.change(within(inspector()).getByLabelText("重试最大尝试次数"), { target: { value: "3" } });
    expect(lastWorkflow(handleChange).settings?.retry).toEqual({ enabled: true, max_attempts: 3 });

    // 高级 options write to workflow.options under their DSL keys, never into settings.
    fireEvent.click(within(inspector()).getByLabelText("允许环路"));
    expect(lastWorkflow(handleChange).options).toEqual({ allow_cycles: true });
    fireEvent.change(within(inspector()).getByLabelText("最大自动深度"), { target: { value: "4" } });
    expect(lastWorkflow(handleChange).options).toEqual({ allow_cycles: true, max_auto_depth: 4 });
    fireEvent.click(within(inspector()).getByLabelText("节点组协议（实验）"));
    expect(lastWorkflow(handleChange).options).toEqual({
      allow_cycles: true,
      max_auto_depth: 4,
      experimental_node_group: true
    });
    fireEvent.click(within(inspector()).getByLabelText("Transient 执行"));
    expect(lastWorkflow(handleChange).options).toEqual({
      allow_cycles: true,
      max_auto_depth: 4,
      experimental_node_group: true,
      transient: true
    });
    // The TTL row only appears while transient is on, and seconds become nanoseconds.
    fireEvent.change(within(inspector()).getByLabelText("Transient 保留时长（秒）"), { target: { value: "60" } });
    expect(lastWorkflow(handleChange).options).toEqual({
      allow_cycles: true,
      max_auto_depth: 4,
      experimental_node_group: true,
      transient: true,
      transient_ttl: 60000000000
    });
    expect(lastWorkflow(handleChange).settings?.timezone).toBe("Asia/Shanghai");

    // An emptied row leaves the DSL rather than writing an empty value.
    fireEvent.change(within(inspector()).getByLabelText("执行时区"), { target: { value: "" } });
    expect(lastWorkflow(handleChange).settings?.timezone).toBeUndefined();
    fireEvent.change(within(inspector()).getByLabelText("执行超时"), { target: { value: "" } });
    expect(lastWorkflow(handleChange).settings?.timeout).toBeUndefined();
  });

  it("rejects a Go duration string in 执行超时 instead of writing it", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.keyDown(window, { key: "Escape" });

    // The server decodes settings.timeout as integer nanoseconds and rejects "30s".
    fireEvent.change(within(inspector()).getByLabelText("执行超时"), { target: { value: "30s" } });
    expect(handleChange).not.toHaveBeenCalled();
    expect(within(inspector()).getByRole("alert")).toBeTruthy();
  });

  it("reads the bare-int concurrency shorthand into the structured rows", () => {
    render(<XFlowEditor value={{ ...workflow, settings: { concurrency: 4 } }} />);
    fireEvent.keyDown(window, { key: "Escape" });
    const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });

    expect((within(inspector()).getByLabelText("并发上限") as HTMLInputElement).value).toBe("4");
    expect((within(inspector()).getByRole("radio", { name: "排队" }) as HTMLInputElement).checked).toBe(true);
    // Queue mode is the shorthand's meaning, so its controls are visible.
    expect(within(inspector()).getByLabelText("队列上限")).toBeTruthy();
  });

  it("rejects 执行超时 values that are not non-negative integer nanoseconds", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.keyDown(window, { key: "Escape" });
    const timeout = () => within(inspector()).getByLabelText("执行超时") as HTMLInputElement;
    const message = "需要非负整数纳秒，例如 1800000000000；0 或留空不限";

    // A JSON string, a negative number, a fraction, a boolean, null, an array and
    // an object all parse as JSON yet cannot be a Go time.Duration. Numbers past
    // the safe-integer range are rejected too, even when JSON parses them as integers
    // (9007199254740993 rounds to 9007199254740992).
    for (const text of ['"30s"', "-5", "1.5", "true", "null", "[]", "{}", "1e21", "9007199254740993"]) {
      fireEvent.change(timeout(), { target: { value: text } });
      expect(timeout().getAttribute("aria-invalid")).toBe("true");
      expect(within(inspector()).getByRole("alert").textContent).toBe(message);
    }
    expect(handleChange).not.toHaveBeenCalled();

    // A legal value after a rejected one commits and clears the message.
    fireEvent.change(timeout(), { target: { value: "0" } });
    expect(within(inspector()).queryByRole("alert")).toBeNull();
    expect(lastWorkflow(handleChange).settings).toEqual({ timeout: 0 });
  });

  it("accepts non-negative integer nanoseconds and a cleared 执行超时", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.keyDown(window, { key: "Escape" });
    const timeout = () => within(inspector()).getByLabelText("执行超时");

    // 0 is "no limit" and stays a legal value.
    fireEvent.change(timeout(), { target: { value: "0" } });
    expect(within(inspector()).queryByRole("alert")).toBeNull();
    expect(lastWorkflow(handleChange).settings).toEqual({ timeout: 0 });

    // Beyond 2^31 still lies in the int64 nanosecond range.
    fireEvent.change(timeout(), { target: { value: "3000000000" } });
    expect(within(inspector()).queryByRole("alert")).toBeNull();
    expect(lastWorkflow(handleChange).settings).toEqual({ timeout: 3000000000 });

    // An emptied row leaves the DSL rather than writing an empty value.
    fireEvent.change(timeout(), { target: { value: "" } });
    expect(handleChange).toHaveBeenCalledTimes(3);
    expect(lastWorkflow(handleChange).settings?.timeout).toBeUndefined();
  });

  it("keeps the other 执行设置 rows on their existing permissive literals", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.keyDown(window, { key: "Escape" });

    // Only 超时 is a Go duration; the integer check must not leak into the other rows,
    // which keep accepting whatever their literal parses to (a fraction included).
    fireEvent.click(within(inspector()).getByLabelText("自动重试"));
    fireEvent.change(within(inspector()).getByLabelText("重试最大尝试次数"), { target: { value: "1.5" } });
    expect(within(inspector()).queryByRole("alert")).toBeNull();
    expect(lastWorkflow(handleChange).settings).toEqual({ retry: { enabled: true, max_attempts: 1.5 } });

    fireEvent.change(within(inspector()).getByLabelText("执行时区"), { target: { value: "Asia/Shanghai" } });
    expect(lastWorkflow(handleChange).settings).toEqual({
      retry: { enabled: true, max_attempts: 1.5 },
      timezone: "Asia/Shanghai"
    });
  });

  it("round-trips the settings source view and blocks save on a broken draft", () => {
    const handleChange = vi.fn();
    const onSave = vi.fn();
    render(<XFlowEditor value={workflow} onSave={onSave} onChange={handleChange} />);
    const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.keyDown(window, { key: "Escape" });

    // The settings source lives in 高级; opening it expands the section.
    const toggle = within(inspector()).getByRole("button", { name: "以 JSON 编辑执行设置" });
    fireEvent.click(toggle);
    const source = within(inspector()).getByLabelText("工作流执行设置 JSON") as HTMLTextAreaElement;
    expect(JSON.parse(source.value)).toEqual({});
    fireEvent.change(source, {
      target: {
        value:
          '{ "timeout": 60000000000, "concurrency": { "limit": 4, "mode": "reject" }, "retry": { "enabled": true } }'
      }
    });
    expect(lastWorkflow(handleChange).settings).toEqual({
      timeout: 60000000000,
      concurrency: { limit: 4, mode: "reject" },
      retry: { enabled: true }
    });

    // Back on the rows, they read what the source wrote.
    fireEvent.click(toggle);
    expect((within(inspector()).getByLabelText("执行超时") as HTMLInputElement).value).toBe("60000000000");
    expect((within(inspector()).getByLabelText("并发上限") as HTMLInputElement).value).toBe("4");
    expect((within(inspector()).getByRole("radio", { name: "拒绝" }) as HTMLInputElement).checked).toBe(true);
    expect(within(inspector()).queryByLabelText("队列上限")).toBeNull();
    expect((within(inspector()).getByLabelText("自动重试") as HTMLButtonElement).getAttribute("aria-checked")).toBe(
      "true"
    );

    // A draft that does not parse is kept on screen, names its field, and stops save.
    fireEvent.click(toggle);
    fireEvent.change(within(inspector()).getByLabelText("工作流执行设置 JSON"), { target: { value: "{ broken" } });
    expect(within(inspector()).getByText("JSON 格式错误")).toBeTruthy();
    expect(screen.getByRole("button", { name: "保存" }).getAttribute("data-blocked")).toBe("true");
    // The broken source stays on screen and 高级 stays open while it blocks save.
    expect(within(inspector()).getByLabelText("工作流执行设置 JSON")).toBeTruthy();
    expect(within(inspector()).getByRole("button", { name: "收起高级" }).getAttribute("aria-expanded")).toBe("true");
    // The parseable commit before it is still what the workflow holds.
    expect(lastWorkflow(handleChange).settings).toEqual({
      timeout: 60000000000,
      concurrency: { limit: 4, mode: "reject" },
      retry: { enabled: true }
    });
  });
});
