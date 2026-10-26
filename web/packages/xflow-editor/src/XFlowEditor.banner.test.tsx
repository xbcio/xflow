import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { WorkflowDef } from "@xflow/core";
import { XFlowEditor } from "./index";
import { editorBanners, isWorkflowConflictError } from "./panels/banner";

// Global DOM teardown for every case: a leftover editor from the previous case
// would otherwise satisfy (or shadow) the document-wide role queries below.
afterEach(cleanup);

// E4c: the three banners (§6.5.1 / §6.10): degraded node types, a lost save
// race and a read-only viewer.

const workflow: WorkflowDef = {
  id: "wf-banner",
  name: "Banner",
  version: "1.0.0",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "worker", type: "xflow.function" }
  ],
  connections: { start: { main: [{ node: "worker" }] } }
};

/** What an XFlowApiError carries, without importing @xflow/api: the editor reads it structurally. */
const apiError = (status: number, code: string, message = "request failed") =>
  Object.assign(new Error(message), { name: "XFlowApiError", status, code });

const banners = () => Array.from(document.querySelectorAll<HTMLElement>(".xflow-editor-banner"));
const bannerWith = (strong: string): HTMLElement => {
  const found = banners().find((banner) => banner.querySelector("strong")?.textContent === strong);
  if (!found) throw new Error(`banner ${strong} not shown`);
  return found;
};
const chipText = () => document.querySelector(".xflow-editor-chip")?.textContent;
/** A settled save: the button has left its loading state (it may stay disabled once the draft is clean). */
const saveSettled = () => expect(screen.getByRole("button", { name: "保存" }).className).not.toContain("ant-btn-loading");

/** An edit that makes the draft dirty, through the workflow settings in the Inspector. */
const renameWorkflow = (name: string) => {
  const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
  // A selected node shows its head; Esc deselects it (the spec's path back to the workflow settings).
  if (inspector.querySelector(".xflow-editor-inspector-head")) fireEvent.keyDown(window, { key: "Escape" });
  fireEvent.change(within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByLabelText("工作流名称"), {
    target: { value: name }
  });
};

describe("banner shell", () => {
  it("shows nothing when nothing is wrong", () => {
    render(<XFlowEditor value={workflow} onSave={vi.fn(async (next) => next)} />);

    expect(banners()).toHaveLength(0);
    expect(document.querySelector(".xflow-editor-banners")).toBeNull();
  });
});

describe("degraded node types banner", () => {
  it("warns with the baseline wording and an alert role", () => {
    render(<XFlowEditor value={workflow} nodeTypesError={{ message: "502 bad gateway" }} onRetryNodeTypes={() => {}} />);

    const banner = bannerWith("节点类型加载失败");
    expect(banner.getAttribute("data-tone")).toBe("warning");
    expect(banner.getAttribute("role")).toBe("alert");
    expect(banner.textContent).toContain("参数仅可按 JSON 编辑；画布与连线不受影响。");
    // The reason rides on the sentence as a tooltip, never in the visible text.
    expect(banner.textContent).not.toContain("502");
    expect(banner.querySelector(".xflow-editor-banner__text")?.getAttribute("title")).toBe("502 bad gateway");
  });

  it("offers 重试 only with a handler, and calls it", () => {
    const retry = vi.fn();
    const { rerender } = render(<XFlowEditor value={workflow} nodeTypesError={{}} />);
    expect(within(bannerWith("节点类型加载失败")).queryByRole("button")).toBeNull();

    rerender(<XFlowEditor value={workflow} nodeTypesError={{}} onRetryNodeTypes={retry} />);
    fireEvent.click(within(bannerWith("节点类型加载失败")).getByRole("button", { name: "重试" }));
    expect(retry).toHaveBeenCalledTimes(1);
  });

  it("goes away once the error clears, and leaves the canvas and its connections alone", () => {
    const { rerender } = render(<XFlowEditor value={workflow} nodeTypesError={{}} />);
    const nodeCount = () => document.querySelectorAll(".xflow-preview-node__select").length;
    const edgeCount = () => document.querySelectorAll(".xflow-preview-edge-hit, .react-flow__edge").length;
    expect(banners()).toHaveLength(1);
    expect(nodeCount()).toBe(2);
    const edgesBefore = edgeCount();

    rerender(<XFlowEditor value={workflow} />);
    expect(banners()).toHaveLength(0);
    expect(nodeCount()).toBe(2);
    expect(edgeCount()).toBe(edgesBefore);
  });

  it("sends parameters to the JSON editor in the inspector, with the schema-not-loaded notice", () => {
    render(<XFlowEditor value={workflow} nodeTypesError={{}} />);

    fireEvent.click(document.querySelector(".xflow-preview-node__select[aria-label^='worker 节点 ']") as HTMLElement);
    const inspector = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    expect(within(inspector).getByText(/节点类型 schema 未加载：参数请以 JSON 编辑/)).toBeTruthy();
    fireEvent.click(within(inspector).getByRole("button", { name: "以 JSON 编辑参数" }));
    expect(within(inspector).getByLabelText("参数 JSON")).toBeTruthy();
  });
});

describe("save conflict banner", () => {
  it("is the 409 workflow_conflict wording, danger, with both actions when both handlers exist", async () => {
    const onSave = vi.fn().mockRejectedValue(apiError(409, "workflow_conflict"));
    render(<XFlowEditor value={workflow} onSave={onSave} onReloadLatest={() => {}} onSaveAsCopy={() => {}} />);

    renameWorkflow("Edited");
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(banners()).toHaveLength(1));

    const banner = bannerWith("保存失败：工作流已被更新");
    expect(banner.getAttribute("data-tone")).toBe("danger");
    expect(banner.getAttribute("role")).toBe("alert");
    expect(banner.textContent).toContain("你基于的版本已不是最新。本地修改仍保留。");
    expect(within(banner).getAllByRole("button").map((button) => button.textContent)).toEqual(["重新加载", "另存为副本"]);
  });

  it("keeps the server's internal conflict message out of the diagnostics list", async () => {
    const onSave = vi.fn().mockRejectedValue(apiError(409, "workflow_conflict", "conflict"));
    render(<XFlowEditor value={workflow} onSave={onSave} />);

    renameWorkflow("Edited");
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(banners()).toHaveLength(1));

    const drawer = screen.getByRole("region", { name: "诊断台" });
    expect(within(drawer).getByText("保存失败：工作流已被更新")).toBeTruthy();
    expect(within(drawer).queryByText("conflict")).toBeNull();
  });

  it("shows only the buttons that have a handler", async () => {
    const onSave = vi.fn().mockRejectedValue(apiError(409, "workflow_conflict"));
    const { rerender } = render(<XFlowEditor value={workflow} onSave={onSave} onReloadLatest={() => {}} />);
    renameWorkflow("Edited");
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(banners()).toHaveLength(1));
    expect(within(banners()[0]).getAllByRole("button").map((button) => button.textContent)).toEqual(["重新加载"]);

    rerender(<XFlowEditor value={workflow} onSave={onSave} onSaveAsCopy={() => {}} />);
    expect(within(banners()[0]).getAllByRole("button").map((button) => button.textContent)).toEqual(["另存为副本"]);

    rerender(<XFlowEditor value={workflow} onSave={onSave} />);
    expect(within(banners()[0]).queryByRole("button")).toBeNull();
  });

  it("runs the handlers; 另存为副本 receives the current draft", async () => {
    const onSave = vi.fn().mockRejectedValue(apiError(409, "workflow_conflict"));
    const onReload = vi.fn();
    const onCopy = vi.fn();
    render(<XFlowEditor value={workflow} onSave={onSave} onReloadLatest={onReload} onSaveAsCopy={onCopy} />);

    renameWorkflow("Edited");
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(banners()).toHaveLength(1));

    fireEvent.click(within(banners()[0]).getByRole("button", { name: "重新加载" }));
    expect(onReload).toHaveBeenCalledTimes(1);
    fireEvent.click(within(banners()[0]).getByRole("button", { name: "另存为副本" }));
    expect(onCopy).toHaveBeenCalledTimes(1);
    expect(onCopy.mock.calls[0][0]).toMatchObject({ id: "wf-banner", name: "Edited" });
  });

  it("does not claim a conflict for other failures, including the sibling workflow_name_conflict", async () => {
    const failures = [
      apiError(409, "workflow_name_conflict", "name taken"),
      apiError(500, "internal_error", "boom"),
      apiError(409, "workflow_version_immutable", "published"),
      new Error("network down")
    ];
    for (const failure of failures) {
      const { unmount } = render(<XFlowEditor value={workflow} onSave={vi.fn().mockRejectedValue(failure)} />);
      renameWorkflow("Edited");
      fireEvent.click(screen.getByRole("button", { name: "保存" }));
      await waitFor(() => expect(chipText()).toBeTruthy());
      await waitFor(() => expect(document.querySelector("[aria-live='polite']")?.textContent).toContain("保存失败"));
      expect(banners()).toHaveLength(0);
      unmount();
    }
  });

  it("draws the clean version chip as plain text, like the baseline top-bar chip", () => {
    render(<XFlowEditor value={workflow} />);
    const chip = document.querySelector(".xflow-editor-title .xflow-editor-chip");
    expect(chip?.textContent).toBe("线上 v1.0.0");
    expect(chip?.getAttribute("data-tone")).toBe("success");
    expect(chip?.querySelector(".xflow-editor-chip__icon")).toBeNull();
  });

  it("keeps the local edits and the unsaved count while the banner is up", async () => {
    const onSave = vi.fn().mockRejectedValue(apiError(409, "workflow_conflict"));
    render(<XFlowEditor value={workflow} onSave={onSave} />);

    renameWorkflow("Edited");
    expect(chipText()).toBe("未保存 1 处 · 基于 v1.0.0");
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(banners()).toHaveLength(1));

    expect(chipText()).toBe("未保存 1 处 · 基于 v1.0.0");
    expect((within(screen.getByRole("region", { name: /^(属性|工作流设置)$/ })).getByLabelText("工作流名称") as HTMLInputElement).value).toBe(
      "Edited"
    );
  });

  it("survives a later save that fails some other way, until a save succeeds", async () => {
    const onSave = vi
      .fn()
      .mockRejectedValueOnce(apiError(409, "workflow_conflict"))
      .mockRejectedValueOnce(apiError(500, "internal_error", "boom"))
      .mockRejectedValueOnce(apiError(409, "workflow_name_conflict", "name taken"))
      .mockImplementationOnce(async (next: WorkflowDef) => next);
    render(<XFlowEditor value={workflow} onSave={onSave} />);
    const saveAndSettle = async (calls: number) => {
      fireEvent.click(screen.getByRole("button", { name: "保存" }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(calls));
      await waitFor(saveSettled);
    };

    renameWorkflow("Edited");
    await saveAndSettle(1);
    expect(banners()).toHaveLength(1);
    await saveAndSettle(2);
    expect(banners()).toHaveLength(1);
    await saveAndSettle(3);
    expect(banners()).toHaveLength(1);
    await saveAndSettle(4);
    await waitFor(() => expect(banners()).toHaveLength(0));
  });

  it("ignores a conflict that answers a save the host has already replaced the value under", async () => {
    let reject: (error: unknown) => void = () => {};
    const onSave = vi.fn(() => new Promise<WorkflowDef>((_resolve, fail) => { reject = fail; }));
    const { rerender } = render(<XFlowEditor value={workflow} onSave={onSave} />);
    renameWorkflow("Edited");
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));

    rerender(<XFlowEditor value={{ ...workflow, version: "2.0.0" }} onSave={onSave} />);
    // The rejection resolves the in-flight save outside any React event
    // handler, so its finally-block `setSaving(false)` is only queued for a
    // concurrent render. Coverage instrumentation (see vitest.config.ts) can
    // starve that render past waitFor's 1s budget, so flush it inside act.
    await act(async () => {
      reject(apiError(409, "workflow_conflict"));
    });
    await waitFor(saveSettled);
    expect(banners()).toHaveLength(0);
  });

  it("steps an expanded diagnostics drawer down so the banner is reachable", async () => {
    const onSave = vi.fn().mockRejectedValue(apiError(409, "workflow_conflict"));
    render(<XFlowEditor value={workflow} onSave={onSave} />);
    const dock = () => screen.getByRole("region", { name: "诊断台" }).getAttribute("data-dock");
    renameWorkflow("Edited");
    fireEvent.keyDown(window, { key: "j", code: "KeyJ", metaKey: true });
    fireEvent.click(screen.getByRole("button", { name: "扩展到画布高度" }));
    expect(dock()).toBe("expanded");

    fireEvent.keyDown(window, { key: "s", code: "KeyS", metaKey: true });
    await waitFor(() => expect(banners()).toHaveLength(1));
    expect(dock()).toBe("docked");
    expect(banners()[0].closest("[inert]")).toBeNull();
  });

  it("hands focus to the canvas when a pressed banner action takes the banner away", () => {
    const { rerender } = render(<XFlowEditor value={workflow} nodeTypesError={{}} onRetryNodeTypes={() => {}} />);
    const retry = within(bannerWith("节点类型加载失败")).getByRole("button", { name: "重试" });
    retry.focus();
    expect(document.activeElement).toBe(retry);

    rerender(<XFlowEditor value={workflow} onRetryNodeTypes={() => {}} />);
    expect(banners()).toHaveLength(0);
    expect(document.activeElement).toBe(document.querySelector(".xflow-editor-preview-frame"));
  });

  it("goes away when a later save succeeds", async () => {
    const onSave = vi
      .fn()
      .mockRejectedValueOnce(apiError(409, "workflow_conflict"))
      .mockImplementationOnce(async (next: WorkflowDef) => next);
    render(<XFlowEditor value={workflow} onSave={onSave} />);

    renameWorkflow("Edited");
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(banners()).toHaveLength(1));

    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(banners()).toHaveLength(0));
    expect(chipText()).toBe("线上 v1.0.0");
  });

  it("goes away when the host replaces the definition (重新加载)", async () => {
    const onSave = vi.fn().mockRejectedValue(apiError(409, "workflow_conflict"));
    const { rerender } = render(<XFlowEditor value={workflow} onSave={onSave} />);
    renameWorkflow("Edited");
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(banners()).toHaveLength(1));

    rerender(<XFlowEditor value={{ ...workflow, version: "1.1.0" }} onSave={onSave} />);
    expect(banners()).toHaveLength(0);
  });
});

describe("read-only banner", () => {
  const reason = "你在当前命名空间只有查看权限。可以查看定义与运行，不能保存或发布。";
  const handlers = { onSave: vi.fn(async (next: WorkflowDef) => next), onRun: vi.fn(async () => ({ status: "success" as const })) };

  it("is neutral, a status, and carries the host's reason", () => {
    render(<XFlowEditor value={workflow} readOnlyReason={reason} {...handlers} />);

    const banner = bannerWith("只读");
    expect(banner.getAttribute("data-tone")).toBe("neutral");
    expect(banner.getAttribute("role")).toBe("status");
    expect(banner.textContent).toContain(reason);
    expect(within(banner).queryByRole("button")).toBeNull();
  });

  it("hides 保存 / 发布 / 运行 instead of disabling them, even with handlers", () => {
    render(<XFlowEditor value={workflow} readOnlyReason={reason} {...handlers} />);

    for (const name of ["保存", "发布", "运行工作流", "运行选项"]) {
      expect(screen.queryByRole("button", { name }), name).toBeNull();
    }
  });

  it("keeps them when the viewer can edit", () => {
    render(<XFlowEditor value={workflow} {...handlers} />);

    for (const name of ["保存", "发布", "运行工作流"]) {
      expect(screen.getByRole("button", { name }), name).toBeTruthy();
    }
  });

  it("closes the keyboard routes to save and run too", async () => {
    const chords = () => {
      fireEvent.keyDown(window, { key: "s", code: "KeyS", metaKey: true });
      fireEvent.keyDown(window, { key: "Enter", code: "Enter", metaKey: true });
    };

    // Control: the same chords do reach the handlers of an editable editor, and
    // only after a tick, so "not called" below is checked after waiting for that.
    const liveSave = vi.fn(async (next: WorkflowDef) => next);
    const liveRun = vi.fn(async () => ({ status: "success" as const }));
    const live = render(<XFlowEditor value={workflow} onSave={liveSave} onRun={liveRun} />);
    renameWorkflow("Edited");
    chords();
    await waitFor(() => expect(liveSave).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(liveRun).toHaveBeenCalledTimes(1));
    live.unmount();

    const onSave = vi.fn(async (next: WorkflowDef) => next);
    const onRun = vi.fn(async () => ({ status: "success" as const }));
    render(<XFlowEditor value={workflow} readOnlyReason={reason} onSave={onSave} onRun={onRun} />);
    chords();
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(onSave).not.toHaveBeenCalled();
    expect(onRun).not.toHaveBeenCalled();
  });

  it("closes the canvas for editing in a read-only viewer", () => {
    render(<XFlowEditor value={workflow} readOnlyReason={reason} {...handlers} />);

    expect(screen.queryByRole("button", { name: "添加节点 (A)" })).toBeNull();
    expect(document.querySelector(".xflow-editor")?.getAttribute("data-view-mode")).toBe("edit");
    // Preview keeps the rail key live but shows the outline alone: no library.
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    expect(screen.getByRole("region", { name: "大纲" })).toBeTruthy();
  });

  it("shows the read-only banner alone, not on top of the replay notice", () => {
    render(<XFlowEditor value={workflow} readOnlyReason={reason} {...handlers} />);
    expect(screen.queryByText("预览模式")).toBeNull();
    expect(banners()).toHaveLength(1);

    // 运行记录 is a replay, not an edit session: no workflow banners, and the
    // canvas carries its own read-only notice.
    const runs = render(<XFlowEditor value={workflow} {...handlers} />);
    fireEvent.click(within(runs.container).getByRole("radio", { name: /运行记录/ }));
    expect(within(runs.container).getByText("只读回放")).toBeTruthy();
    expect(runs.container.querySelectorAll(".xflow-editor-banner")).toHaveLength(0);
  });
});

describe("archived-view banner", () => {
  const archived: WorkflowDef = { ...workflow, version: "1.1.4" };

  it("is neutral, a status, and takes the read-only banner's place", () => {
    render(
      <XFlowEditor
        historicalVersion="v1.1.4"
        readOnlyReason="你在当前命名空间只有查看权限。"
        value={archived}
      />
    );

    const banner = bannerWith("历史版本 v1.1.4");
    expect(banner.getAttribute("data-tone")).toBe("neutral");
    expect(banner.getAttribute("role")).toBe("status");
    expect(banner.textContent).toContain("正在查看归档版本，编辑器只读。回滚或退出历史后可继续编辑。");
    // The archived view owns the explanation, so the read-only banner is
    // replaced rather than stacked underneath it.
    expect(banners()).toHaveLength(1);
    expect(banners().some((item) => item.querySelector("strong")?.textContent === "只读")).toBe(false);
    // 查看 must never edit history.
    expect(screen.queryByRole("button", { name: "保存" })).toBeNull();
    expect(screen.queryByRole("button", { name: "发布" })).toBeNull();
    // No host handler, no way out drawn.
    expect(within(banner).queryByRole("button")).toBeNull();
  });

  it("stacks under the degraded-node-types warning and offers 退出历史 only with a handler", () => {
    const exit = vi.fn();
    const { rerender } = render(<XFlowEditor historicalVersion="v1.1.4" nodeTypesError={{}} value={archived} />);

    const stack = document.querySelector(".xflow-editor-banners") as HTMLElement;
    expect(Array.from(stack.querySelectorAll("strong")).map((strong) => strong.textContent)).toEqual([
      "节点类型加载失败",
      "历史版本 v1.1.4"
    ]);
    expect(within(bannerWith("历史版本 v1.1.4")).queryByRole("button")).toBeNull();

    rerender(
      <XFlowEditor historicalVersion="v1.1.4" nodeTypesError={{}} onExitHistory={exit} value={archived} />
    );
    fireEvent.click(within(bannerWith("历史版本 v1.1.4")).getByRole("button", { name: "退出历史" }));
    expect(exit).toHaveBeenCalledTimes(1);
  });
});

describe("capability-gated read-only", () => {
  const handlers = {
    onSave: vi.fn(async (next: WorkflowDef) => next),
    onRun: vi.fn(async () => ({ status: "success" as const }))
  };
  const all = { workflow: true, execution: true, executionControl: true, executionRerun: true };

  it("turns read-only when the credential has no workflow scope, naming it", () => {
    const view = render(<XFlowEditor value={workflow} capabilities={{ ...all, workflow: false }} {...handlers} />);

    const banner = bannerWith("只读");
    expect(banner.getAttribute("data-tone")).toBe("neutral");
    expect(banner.textContent).toContain("缺少 workflow 权限，编辑器为只读。");
    for (const name of ["保存", "发布", "运行工作流", "运行选项"]) {
      expect(within(view.container).queryByRole("button", { name }), name).toBeNull();
    }
    expect(within(view.container).queryByRole("button", { name: "添加节点 (A)" })).toBeNull();
  });

  it("names every missing scope, not only workflow, when the credential grants nothing", () => {
    render(
      <XFlowEditor
        value={workflow}
        capabilities={{ workflow: false, execution: false, executionControl: false, executionRerun: false }}
        {...handlers}
      />
    );

    expect(bannerWith("只读").textContent).toContain(
      "缺少 workflow、execution、execution.control、execution.rerun 权限，编辑器为只读。"
    );
  });

  it("keeps the editing surfaces when workflow is granted, whatever the execution bits say", () => {
    const view = render(
      <XFlowEditor
        value={workflow}
        capabilities={{ workflow: true, execution: false, executionControl: false, executionRerun: false }}
        {...handlers}
      />
    );

    for (const name of ["保存", "发布", "运行工作流"]) {
      expect(within(view.container).getByRole("button", { name }), name).toBeTruthy();
    }
    expect(document.querySelector(".xflow-editor-banners")).toBeNull();
  });

  it("lets the host's readOnlyReason win over the derived sentence", () => {
    const reason = "你在当前命名空间只有查看权限。";
    render(
      <XFlowEditor
        value={workflow}
        capabilities={{ ...all, workflow: false }}
        readOnlyReason={reason}
        {...handlers}
      />
    );

    expect(bannerWith("只读").textContent).toContain(reason);
    expect(bannerWith("只读").textContent).not.toContain("缺少 workflow");
  });

  it("keeps undo and redo out of the definition once write access is gone", () => {
    const onChange = vi.fn();
    const view = render(
      <XFlowEditor value={workflow} capabilities={all} onChange={onChange} {...handlers} />
    );
    renameWorkflow("Edited while still writable");
    const callsAfterEdit = onChange.mock.calls.length;
    expect(callsAfterEdit).toBeGreaterThan(0);

    // The same mounted editor loses the workflow bit (the host refreshed
    // current-user after a 403) while its history still holds the edit.
    view.rerender(
      <XFlowEditor
        value={workflow}
        capabilities={{ ...all, workflow: false }}
        onChange={onChange}
        {...handlers}
      />
    );
    const undo = within(view.container).getByRole("button", { name: "撤销" });
    const redo = within(view.container).getByRole("button", { name: "重做" });
    expect((undo as HTMLButtonElement).disabled).toBe(true);
    expect((redo as HTMLButtonElement).disabled).toBe(true);

    fireEvent.keyDown(window, { key: "z", code: "KeyZ", metaKey: true });
    fireEvent.keyDown(window, { key: "z", code: "KeyZ", metaKey: true, shiftKey: true });
    expect(onChange).toHaveBeenCalledTimes(callsAfterEdit);
  });
});

describe("shortcuts behind an expanded diagnostics drawer", () => {
  const expand = () => {
    fireEvent.keyDown(window, { key: "j", code: "KeyJ", metaKey: true });
    fireEvent.click(screen.getByRole("button", { name: "扩展到画布高度" }));
  };
  const nodeCount = () => document.querySelectorAll(".xflow-preview-node__select").length;

  it("leaves the covered workbench alone: no duplicate, no panel toggle, no node delete", () => {
    render(<XFlowEditor value={workflow} onSave={vi.fn(async (next) => next)} />);
    const editor = document.querySelector(".xflow-editor") as HTMLElement;
    const leftCollapsed = () => document.querySelector(".xflow-editor-main")?.getAttribute("data-left-collapsed");

    // Control: with the dock docked the same keys act.
    fireEvent.click(document.querySelector(".xflow-preview-node__select[aria-label^='worker 节点 ']") as HTMLElement);
    fireEvent.keyDown(window, { key: "b", code: "KeyB", metaKey: true });
    expect(leftCollapsed()).toBe("true");
    fireEvent.keyDown(window, { key: "b", code: "KeyB", metaKey: true });
    expect(leftCollapsed()).toBe("false");

    expand();
    expect(editor.querySelector(".xflow-editor-main")?.hasAttribute("inert")).toBe(true);
    fireEvent.keyDown(window, { key: "d", code: "KeyD", metaKey: true });
    fireEvent.keyDown(window, { key: "b", code: "KeyB", metaKey: true });
    fireEvent.keyDown(screen.getByRole("button", { name: "还原诊断台高度" }), { key: "Backspace", code: "Backspace" });
    expect(nodeCount()).toBe(2);
    expect(leftCollapsed()).toBe("false");
    expect(document.querySelector(".xflow-editor-compact-drawer")).toBeNull();
  });

  it("keeps save, find and the dock key live", async () => {
    const onSave = vi.fn(async (next: WorkflowDef) => next);
    render(<XFlowEditor value={workflow} onSave={onSave} />);
    renameWorkflow("Edited");
    expand();

    fireEvent.keyDown(window, { key: "s", code: "KeyS", metaKey: true });
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    fireEvent.keyDown(window, { key: "j", code: "KeyJ", metaKey: true });
    expect(screen.getByRole("region", { name: "诊断台" }).getAttribute("data-dock")).toBe("collapsed");
  });
});

describe("banner stacking", () => {
  it("lists the lost save race first, then degraded node types, then read-only", () => {
    const stack = editorBanners({
      nodeTypesError: {},
      saveConflict: true,
      readOnlyReason: "查看权限"
    });
    expect(stack.map((banner) => banner.key)).toEqual(["conflict", "node-types", "read-only"]);
  });

  it("renders several banners top-down in one stack", async () => {
    const onSave = vi.fn().mockRejectedValue(apiError(409, "workflow_conflict"));
    render(<XFlowEditor value={workflow} onSave={onSave} nodeTypesError={{}} />);
    renameWorkflow("Edited");
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() => expect(banners()).toHaveLength(2));

    const stack = document.querySelector(".xflow-editor-banners") as HTMLElement;
    expect(stack.children).toHaveLength(2);
    expect(Array.from(stack.querySelectorAll("strong")).map((strong) => strong.textContent)).toEqual([
      "保存失败：工作流已被更新",
      "节点类型加载失败"
    ]);
    // It floats inside the canvas section, so it takes no height from the canvas.
    expect(screen.getByRole("region", { name: "画布" }).contains(stack)).toBe(true);
  });
});

describe("409 recognition", () => {
  it("needs both the status and the code", () => {
    expect(isWorkflowConflictError(apiError(409, "workflow_conflict"))).toBe(true);
    expect(isWorkflowConflictError(apiError(409, "workflow_name_conflict"))).toBe(false);
    expect(isWorkflowConflictError(apiError(500, "workflow_conflict"))).toBe(false);
    expect(isWorkflowConflictError(new Error("workflow_conflict"))).toBe(false);
    expect(isWorkflowConflictError(undefined)).toBe(false);
    expect(isWorkflowConflictError("409")).toBe(false);
  });
});
