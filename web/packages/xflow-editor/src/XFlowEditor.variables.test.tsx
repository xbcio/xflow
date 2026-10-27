// The left column's 变量 panel (params / context.vars / context.config /
// credential references), the ⌘K node search (jump by name, next to the outline), and
// the Inspector's workflow settings shown with no node selected.

import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";
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
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} node "]`);
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

  it("deselects from the Inspector header into the workflow settings, and back by selecting a node", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    expect(within(inspector()).queryByLabelText("工作流名称")).toBeNull();

    fireEvent.keyDown(window, { key: "Escape" });
    expect(within(inspector()).getByRole("group", { name: "基本信息" })).toBeTruthy();
    // Runner and 执行设置 are structured rows; their JSON source view is one
    // `</>` click away. Pin Data has no rows to build, so it stays source-only.
    expect(within(inspector()).getByLabelText("Runner 模式")).toBeTruthy();
    expect(within(inspector()).getByLabelText("执行超时")).toBeTruthy();
    expect(within(inspector()).getByLabelText("启用重试")).toBeTruthy();
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

  it("writes the structured Runner and 执行设置 rows into the workflow", () => {
    const handleChange = vi.fn();
    render(
      <XFlowEditor
        value={{ ...workflow, runner_selector: { mode: "required", match_labels: { env: "prod" } } }}
        onChange={handleChange}
      />
    );
    const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.keyDown(window, { key: "Escape" });

    // Runner: a mode select plus one key/value row per match label.
    const mode = within(inspector()).getByLabelText("Runner 模式").closest(".xflow-editor-select__root");
    expect(mode?.textContent).toContain("必须匹配标签");
    pickEditorOption(within(inspector()).getByLabelText("Runner 模式"), "默认：任意可用 Runner");
    expect(lastWorkflow(handleChange).runner_selector).toEqual({ mode: "default", match_labels: { env: "prod" } });

    fireEvent.change(within(inspector()).getByLabelText("匹配标签 1 值"), { target: { value: "staging" } });
    expect(lastWorkflow(handleChange).runner_selector).toEqual({ mode: "default", match_labels: { env: "staging" } });

    fireEvent.click(within(inspector()).getByRole("button", { name: "删除匹配标签 env" }));
    expect(lastWorkflow(handleChange).runner_selector).toEqual({ mode: "default" });

    // 执行设置: each row writes exactly its own key of workflow.settings.
    // The timeout is the Go time.Duration wire form: integer nanoseconds, never a "30s" string.
    fireEvent.change(within(inspector()).getByLabelText("执行超时"), { target: { value: "1800000000000" } });
    expect(lastWorkflow(handleChange).settings).toEqual({ timeout: 1800000000000 });

    fireEvent.change(within(inspector()).getByLabelText("执行并发"), { target: { value: "4" } });
    expect(lastWorkflow(handleChange).settings).toEqual({ timeout: 1800000000000, concurrency: 4 });

    fireEvent.change(within(inspector()).getByLabelText("执行时区"), { target: { value: "Asia/Shanghai" } });
    pickEditorOption(within(inspector()).getByLabelText("执行错误策略"), "continue：继续执行");
    pickEditorOption(within(inspector()).getByLabelText("Pin 数据模式"), "always：总是使用");
    expect(lastWorkflow(handleChange).settings).toEqual({
      timeout: 1800000000000,
      concurrency: 4,
      timezone: "Asia/Shanghai",
      on_error: "continue",
      pin_data_mode: "always"
    });

    // Retry keeps its nested object, and its rows only appear once it is on.
    expect(within(inspector()).queryByLabelText("重试最大尝试次数")).toBeNull();
    fireEvent.click(within(inspector()).getByLabelText("启用重试"));
    expect(lastWorkflow(handleChange).settings?.retry).toEqual({ enabled: true });
    fireEvent.change(within(inspector()).getByLabelText("重试最大尝试次数"), { target: { value: "3" } });
    expect(lastWorkflow(handleChange).settings?.retry).toEqual({ enabled: true, max_attempts: 3 });

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

  it("round-trips the 执行设置 source view and blocks save on a broken draft", () => {
    const handleChange = vi.fn();
    const onSave = vi.fn();
    render(<XFlowEditor value={workflow} onSave={onSave} onChange={handleChange} />);
    const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    fireEvent.keyDown(window, { key: "Escape" });

    const toggle = within(inspector()).getByRole("button", { name: "以 JSON 编辑执行设置" });
    fireEvent.click(toggle);
    const source = within(inspector()).getByLabelText("工作流执行设置 JSON") as HTMLTextAreaElement;
    expect(JSON.parse(source.value)).toEqual({});
    fireEvent.change(source, { target: { value: '{ "timeout": 60000000000, "retry": { "enabled": true } }' } });
    expect(lastWorkflow(handleChange).settings).toEqual({ timeout: 60000000000, retry: { enabled: true } });

    // Back on the rows, they read what the source wrote.
    fireEvent.click(toggle);
    expect((within(inspector()).getByLabelText("执行超时") as HTMLInputElement).value).toBe("60000000000");
    expect((within(inspector()).getByLabelText("启用重试") as HTMLButtonElement).getAttribute("aria-checked")).toBe("true");

    // A draft that does not parse is kept on screen, names its field, and stops save.
    fireEvent.click(toggle);
    fireEvent.change(within(inspector()).getByLabelText("工作流执行设置 JSON"), { target: { value: "{ broken" } });
    expect(within(inspector()).getByText("JSON 格式错误")).toBeTruthy();
    expect(screen.getByRole("button", { name: "保存" }).getAttribute("data-blocked")).toBe("true");
    // The parseable commit before it is still what the workflow holds.
    expect(lastWorkflow(handleChange).settings).toEqual({ timeout: 60000000000, retry: { enabled: true } });
  });
});
