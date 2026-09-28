import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";

// Pins the editor's write path (functional commits against the latest draft)
// and the parameters-JSON draft rules: an unparseable draft is never
// overwritten, survives re-selection, and blocks save/run.
describe("XFlowEditor state commits", () => {
  const workflow: WorkflowDef = {
    id: "wf-state",
    name: "State",
    nodes: [
      { name: "start", type: "xflow.start", kind: "trigger" },
      { name: "worker", type: "xflow.function", parameters: { a: 1 } },
      { name: "tail", type: "xflow.http" }
    ],
    connections: { start: { main: [{ node: "worker" }] } }
  };

  const lastWorkflow = (handleChange: ReturnType<typeof vi.fn>): WorkflowDef =>
    handleChange.mock.calls.at(-1)?.[0] as WorkflowDef;

  const selectNode = (name: string) => {
    fireEvent.click(screen.getByRole("button", { name: `选择节点 ${name}` }));
    return screen.getByRole("region", { name: "属性" });
  };
  /** The parameters JSON lives on the Inspector's "JSON" tab (the node form is the default). */
  const jsonTab = (inspector: HTMLElement) => {
    const tab = within(inspector).getByRole("tab", { name: "JSON" });
    if (tab.getAttribute("aria-selected") !== "true") fireEvent.click(tab);
    return within(inspector).getByLabelText("参数 JSON") as HTMLTextAreaElement;
  };
  const formTab = (inspector: HTMLElement) => fireEvent.click(within(inspector).getByRole("tab", { name: "表单" }));
  /** Types parseable JSON and blurs, committing it without waiting for the debounce. */
  const commitJson = (field: HTMLElement, text: string) => {
    fireEvent.change(field, { target: { value: text } });
    fireEvent.blur(field);
  };

  // These two pin that consecutive writers compose and that rename follows the
  // node. They do NOT reproduce the same-tick lost update the functional commit
  // fixes: React flushes discrete events (change, blur) synchronously, so the
  // Inspector re-renders between fireEvent calls even inside act(). No handler
  // in today's UI issues two writes, so that failure first becomes reachable
  // when composer delivers patch batches (doc C, C3); its regression test
  // belongs there.
  it("keeps both writes when two Inspector writers fire back to back", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const inspector = selectNode("worker");

    const parameters = jsonTab(inspector);
    act(() => {
      fireEvent.change(within(inspector).getByLabelText("工作流名称"), { target: { value: "Renamed flow" } });
      commitJson(parameters, '{ "a": 2 }');
    });

    const saved = lastWorkflow(handleChange);
    expect(saved.name).toBe("Renamed flow");
    expect(saved.nodes?.[1]?.parameters).toEqual({ a: 2 });
  });

  it("keeps a parameters edit made just before a rename, and follows the renamed node", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const inspector = selectNode("worker");

    // Typed but not blurred: leaving the JSON tab commits the pending draft.
    fireEvent.change(jsonTab(inspector), { target: { value: '{ "a": 3 }' } });
    formTab(inspector);
    const nodeName = within(inspector).getByLabelText("节点名称");
    fireEvent.change(nodeName, { target: { value: "executor" } });
    fireEvent.blur(nodeName);

    const saved = lastWorkflow(handleChange);
    expect(saved.nodes?.[1]).toMatchObject({ name: "executor", parameters: { a: 3 } });
    expect(saved.connections?.start?.main).toEqual([{ node: "executor" }]);
    // Selection is keyed by name for this id-less node; it must follow the rename.
    expect((within(screen.getByRole("region", { name: "属性" })).getByLabelText("节点名称") as HTMLInputElement).value)
      .toBe("executor");
  });

  it("does not overwrite an unparseable draft when the committed parameters change underneath it", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const inspector = selectNode("worker");
    const parameters = jsonTab(inspector);

    commitJson(parameters, '{ "a": 5 }');
    fireEvent.change(parameters, { target: { value: "{ broken" } });
    // Undo reverts the committed parameters to { a: 1 }, which re-runs the
    // Inspector's resync. The user's unsaved draft must survive it.
    fireEvent.click(within(screen.getByRole("group", { name: "常用画布与历史工具" })).getByRole("button", { name: "撤销" }));

    expect(lastWorkflow(handleChange).nodes?.[1]?.parameters).toEqual({ a: 1 });
    expect((within(screen.getByRole("region", { name: "属性" })).getByLabelText("参数 JSON") as HTMLTextAreaElement).value)
      .toBe("{ broken");
    expect(within(screen.getByRole("region", { name: "属性" })).getByText("参数 JSON 格式错误")).toBeTruthy();
  });

  it("restores an unparseable draft after selecting another node and coming back", () => {
    render(<XFlowEditor value={workflow} onChange={vi.fn()} />);
    fireEvent.change(jsonTab(selectNode("worker")), { target: { value: "{ broken" } });

    const other = selectNode("tail");
    expect((within(other).getByLabelText("参数 JSON") as HTMLTextAreaElement).value).not.toBe("{ broken");

    const back = selectNode("worker");
    expect((within(back).getByLabelText("参数 JSON") as HTMLTextAreaElement).value).toBe("{ broken");
    expect(within(back).getByText("参数 JSON 格式错误")).toBeTruthy();
  });

  it("blocks save and run while a node holds an unparseable draft, and unblocks once it parses", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef) => next);
    const handleRun = vi.fn(async () => ({ status: "success" as const, nodes: {} }));
    render(<XFlowEditor value={workflow} onSave={handleSave} onRun={handleRun} />);
    const parameters = jsonTab(selectNode("worker"));

    fireEvent.change(parameters, { target: { value: "{ broken" } });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    fireEvent.click(screen.getByRole("button", { name: "运行工作流" }));

    expect(handleSave).not.toHaveBeenCalled();
    expect(handleRun).not.toHaveBeenCalled();
    expect(screen.getAllByText(/节点 worker 的参数 JSON 未通过解析/).length).toBeGreaterThan(0);

    // Parseable but still inside the debounce window: save flushes it first.
    fireEvent.change(parameters, { target: { value: '{ "a": 9 }' } });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
    expect(handleSave.mock.calls[0]?.[0]?.nodes?.[1]?.parameters).toEqual({ a: 9 });
  });

  it("blocks the save shortcut fired from inside the parameters field, and honours it once the draft parses", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef) => next);
    render(<XFlowEditor value={workflow} onSave={handleSave} />);
    const parameters = jsonTab(selectNode("worker"));

    fireEvent.change(parameters, { target: { value: "{ broken" } });
    // The field never blurs: the global shortcut must still see the draft.
    fireEvent.keyDown(parameters, { key: "s", metaKey: true });
    expect(handleSave).not.toHaveBeenCalled();

    // Same shortcut, same field, valid draft: proves the keydown above did
    // reach the save handler rather than being ignored for some other reason.
    fireEvent.change(parameters, { target: { value: '{ "a": 4 }' } });
    fireEvent.keyDown(parameters, { key: "s", metaKey: true });
    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
    expect(handleSave.mock.calls[0]?.[0]?.nodes?.[1]?.parameters).toEqual({ a: 4 });
  });

  const renameSelected = (inspector: HTMLElement, name: string) => {
    formTab(inspector);
    const field = within(inspector).getByLabelText("节点名称");
    fireEvent.change(field, { target: { value: name } });
    fireEvent.blur(field);
  };
  const parametersText = () => jsonTab(screen.getByRole("region", { name: "属性" })).value;

  // With an unparseable draft the whole node form is read-only (Doc C §6.2),
  // rename included, so a draft can no longer be carried through a rename
  // from the UI (the editor still rekeys drafts on rename, defensively).
  it("makes the node form read-only, rename included, while the node holds an unparseable draft", () => {
    const handleChange = vi.fn();
    render(<XFlowEditor value={workflow} onChange={handleChange} />);
    const inspector = selectNode("worker");
    fireEvent.change(jsonTab(inspector), { target: { value: "{ broken" } });

    formTab(inspector);
    expect(within(inspector).getByText(/参数 JSON 未通过解析，表单暂为只读/)).toBeTruthy();
    const name = within(inspector).getByLabelText("节点名称") as HTMLInputElement;
    expect(name.readOnly).toBe(true);
    fireEvent.change(name, { target: { value: "executor" } });
    fireEvent.blur(name);
    expect(handleChange).not.toHaveBeenCalled();
    expect(parametersText()).toBe("{ broken");
  });

  it("drops a deleted node's draft so a node later given that name does not inherit it", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef) => next);
    render(<XFlowEditor value={workflow} onSave={handleSave} />);
    const inspector = selectNode("worker");
    fireEvent.change(jsonTab(inspector), { target: { value: "{ broken" } });
    fireEvent.click(within(inspector).getByRole("button", { name: "删除节点" }));

    renameSelected(selectNode("tail"), "worker");

    expect(parametersText()).not.toBe("{ broken");
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
  });

  it("drops a draft stranded by undoing a rename so the vacated name starts clean", async () => {
    const handleSave = vi.fn(async (next: WorkflowDef) => next);
    render(<XFlowEditor value={workflow} onSave={handleSave} />);
    const inspector = selectNode("worker");
    renameSelected(inspector, "executor");
    // A draft on the renamed node; undo puts the node back under "worker" and
    // the draft keyed "executor" then names no node.
    fireEvent.change(jsonTab(screen.getByRole("region", { name: "属性" })), { target: { value: "{ broken" } });
    fireEvent.click(within(screen.getByRole("group", { name: "常用画布与历史工具" })).getByRole("button", { name: "撤销" }));

    renameSelected(selectNode("tail"), "executor");

    expect(parametersText()).not.toBe("{ broken");
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(handleSave).toHaveBeenCalledTimes(1));
  });
});
