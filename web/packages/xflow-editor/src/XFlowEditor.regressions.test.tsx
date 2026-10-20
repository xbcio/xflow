import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { NodeTypesResponse, WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";
import generated from "./node-form/testdata/node-types.generated.json";

/** The canvas node's own select button: the canvas cards stay a selectable list beside the outline tree. */
const canvasNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} node "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  return node;
};

const nodeTypes = generated as unknown as NodeTypesResponse;

const inspector = () => screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
const selectNode = (name: string) => {
  fireEvent.click(canvasNode(name));
  return inspector();
};
const jsonTab = (root: HTMLElement = inspector()) => {
  const toggle = within(root).getByRole("button", { name: "以 JSON 编辑参数" });
  if (toggle.getAttribute("aria-pressed") !== "true") fireEvent.click(toggle);
  return within(root).getByLabelText("参数 JSON") as HTMLTextAreaElement;
};
const historyButton = (name: "撤销" | "重做") =>
  within(screen.getByRole("group", { name: "常用画布与历史工具" })).getByRole("button", { name });
const lastWorkflow = (spy: ReturnType<typeof vi.fn>) => spy.mock.calls.at(-1)?.[0] as WorkflowDef;
/** How many Diagnostics lines are attributed to the node (each carries one "定位节点 X" button). */
const diagnosticCount = (name: string) => screen.queryAllByRole("button", { name: `定位节点 ${name}` }).length;
const openDiagnostics = () => {
  const toggle = screen.queryByRole("button", { name: "展开诊断台" });
  if (toggle) fireEvent.click(toggle);
};

const workflow: WorkflowDef = {
  id: "wf-regressions",
  name: "Regressions",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "worker", type: "xflow.function", parameters: { a: 1 } },
    { name: "tail", type: "xflow.http" }
  ],
  connections: { start: { main: [{ node: "worker" }] } }
};

describe("US-001 save in-flight guard", () => {
  it("ignores a second save (button, then shortcut) while the first is pending, and clears loading when it settles", async () => {
    let resolveSave: (() => void) | undefined;
    const handleSave = vi.fn((next: WorkflowDef) => new Promise<WorkflowDef>((resolve) => {
      resolveSave = () => resolve(next);
    }));
    render(<XFlowEditor value={workflow} onSave={handleSave} />);
    // A clean draft keeps 保存 disabled (baseline: the publish key is unusable
    // until there is something to save), so dirty the draft first.
    const settings = inspector();
    if (settings.querySelector(".xflow-editor-inspector-head")) fireEvent.keyDown(window, { key: "Escape" });
    fireEvent.change(within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByLabelText("工作流名称"), {
      target: { value: "Regressions edited" }
    });
    const save = screen.getByRole("button", { name: "保存" }) as HTMLButtonElement;
    expect(save.disabled).toBe(false);

    fireEvent.click(save);
    fireEvent.keyDown(document.body, { key: "s", metaKey: true });
    fireEvent.click(save);

    expect(handleSave).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(save.className).toContain("ant-btn-loading"));

    await act(async () => resolveSave?.());
    await waitFor(() => expect(save.className).not.toContain("ant-btn-loading"));
    expect(screen.getByText("已保存", { selector: ".xflow-editor-chip span" })).toBeTruthy();

    // Settled clean: the shortcut is a no-op, as on the disabled baseline key.
    fireEvent.keyDown(document.body, { key: "s", metaKey: true });
    expect(handleSave).toHaveBeenCalledTimes(1);

    // A fresh edit enables the next save again.
    fireEvent.change(within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByLabelText("工作流名称"), {
      target: { value: "Regressions edited again" }
    });
    fireEvent.keyDown(document.body, { key: "s", metaKey: true });
    expect(handleSave).toHaveBeenCalledTimes(2);
  });
});

describe("US-002 diagnostics attribution", () => {
  it("attributes a dangling-port error to the switch that lost the port", () => {
    const dangling: WorkflowDef = {
      id: "wf-dangling",
      name: "Dangling",
      nodes: [
        { name: "route", type: "xflow.switch", parameters: { outputs: ["a"] } },
        { name: "tail", type: "xflow.end" }
      ],
      // Port "b" was removed from the switch's outputs; its edge stays and dangles.
      connections: { route: { b: [{ node: "tail" }] } }
    };
    render(<XFlowEditor value={dangling} nodeTypes={nodeTypes} />);
    openDiagnostics();

    expect(screen.getByText("route.b 端口已不存在，连线悬空；请删除或改接连线")).toBeTruthy();
    expect(diagnosticCount("route")).toBeGreaterThan(0);
    expect(diagnosticCount("tail")).toBe(0);
  });

  it("attributes a diagnostic to the node it is about, not to a node whose name is a substring of the message", () => {
    const prefixed: WorkflowDef = {
      id: "wf-prefix",
      name: "Prefix",
      nodes: [
        { name: "start", type: "xflow.start", kind: "trigger" },
        { name: "a", type: "xflow.http" }
      ],
      // The missing target "start_2" contains the name "start".
      connections: { a: { main: [{ node: "start_2" }] } }
    };
    render(<XFlowEditor value={prefixed} />);
    openDiagnostics();

    expect(screen.getByText("a.main 连接目标不存在: start_2")).toBeTruthy();
    expect(diagnosticCount("start")).toBe(0);
    expect(diagnosticCount("a")).toBe(1);
  });
});

describe("US-003 workflow-level JSON", () => {
  /** The params source view of the left column's 变量 panel; opens panel and source as needed. */
  const paramsField = () => {
    if (!screen.queryByRole("region", { name: "变量" })) fireEvent.click(screen.getByRole("button", { name: "变量" }));
    const variables = screen.getByRole("region", { name: "变量" });
    const toggle = within(variables).getByRole("button", { name: "以 JSON 编辑输入参数" });
    if (toggle.getAttribute("aria-pressed") !== "true") fireEvent.click(toggle);
    return within(variables).getByLabelText("工作流输入参数 JSON") as HTMLTextAreaElement;
  };

  it("keeps parseable text as typed instead of reformatting it", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.change(paramsField(), { target: { value: '{ "amount": 1 }' } });

    expect(lastWorkflow(handleChange).params).toEqual({ amount: 1 });
    expect(paramsField().value).toBe('{ "amount": 1 }');
  });

  it("blocks save while a workflow JSON field does not parse, and keeps the draft across a panel unmount", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef) => next);
    render(<XFlowEditor value={workflow} onSave={handleSave} />);

    fireEvent.change(paramsField(), { target: { value: "{ broken" } });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    fireEvent.keyDown(document.body, { key: "s", metaKey: true });

    expect(handleSave).not.toHaveBeenCalled();
    expect(screen.getAllByText(/工作流 输入参数 JSON 未通过解析/).length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    expect(screen.queryByRole("region", { name: "变量" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "变量" }));
    // A broken draft reopens its section straight in the source view.
    const toggle = within(screen.getByRole("region", { name: "变量" })).getByRole("button", { name: "以 JSON 编辑输入参数" });
    expect(toggle.getAttribute("aria-pressed")).toBe("true");
    expect(paramsField().value).toBe("{ broken");
    expect(within(screen.getByRole("region", { name: "变量" })).getByText("JSON 格式错误")).toBeTruthy();

    fireEvent.change(paramsField(), { target: { value: '{ "amount": 2 }' } });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
    expect(handleSave.mock.calls[0]?.[0]?.params).toEqual({ amount: 2 });
  });
});

describe("US-004 undo/redo and draft debounce", () => {
  it("redo still applies when a parseable parameters draft is pending, and the draft stays one undo away", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const parameters = jsonTab(selectNode("worker"));

    fireEvent.change(parameters, { target: { value: '{ "a": 2 }' } });
    fireEvent.blur(parameters);
    fireEvent.click(historyButton("撤销"));
    expect(lastWorkflow(handleChange).nodes?.[1]?.parameters).toEqual({ a: 1 });

    // Typed, not blurred: the draft is still inside the debounce window.
    fireEvent.change(jsonTab(), { target: { value: '{ "a": 3 }' } });
    fireEvent.click(historyButton("重做"));
    expect(lastWorkflow(handleChange).nodes?.[1]?.parameters).toEqual({ a: 2 });

    fireEvent.click(historyButton("撤销"));
    expect(lastWorkflow(handleChange).nodes?.[1]?.parameters).toEqual({ a: 3 });
  });

  it("commits one node's valid draft even after another node gets an unparseable one", async () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);

    fireEvent.change(jsonTab(selectNode("worker")), { target: { value: '{ "a": 7 }' } });
    fireEvent.change(jsonTab(selectNode("tail")), { target: { value: "{ broken" } });

    await waitFor(() => expect(lastWorkflow(handleChange)?.nodes?.[1]?.parameters).toEqual({ a: 7 }), { timeout: 2000 });
    expect(jsonTab().value).toBe("{ broken");
  });
});
