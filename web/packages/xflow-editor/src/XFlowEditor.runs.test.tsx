import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import * as React from "react";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { RuntimeSnapshot, WorkflowDef, XFlowEditorCapabilities } from "@xflow/core";
import type { XFlowPreview as RealXFlowPreview } from "@xflow/preview";

import { XFlowEditor, type XFlowExecution } from "./index";

type PreviewProps = {
  defaultViewport?: { x: number; y: number; zoom: number };
  onViewportChange?: (viewport: { x: number; y: number; zoom: number }) => void;
};
// React Flow never reports a viewport in jsdom; the pass-through wrapper hands
// the canvas props back so the replay round trip's remembered viewport can be
// asserted without a live canvas.
const preview = vi.hoisted(() => ({ lastProps: undefined as PreviewProps | undefined }));
vi.mock("@xflow/preview", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@xflow/preview")>();
  const Wrapped: typeof RealXFlowPreview = (props) => {
    preview.lastProps = props;
    return React.createElement(actual.XFlowPreview, props);
  };
  return { ...actual, XFlowPreview: Wrapped };
});

const originalResizeObserver = globalThis.ResizeObserver;
// @xyflow/system reads `new DOMMatrixReadOnly(transform).m22` whenever a resize
// observer fires with a mounted viewport; jsdom has neither. Remounting the
// preview (the mode switch) makes that path run, so the gap is filled here.
const originalDomMatrixReadOnly = window.DOMMatrixReadOnly;
class DomMatrixReadOnlyMock {
  m22 = 1;
}
class WidthResizeObserver {
  private readonly callback: ResizeObserverCallback;
  constructor(callback: ResizeObserverCallback) {
    this.callback = callback;
  }
  observe(target: Element): void {
    this.callback(
      [{ target, contentRect: { width: 1440 } } as ResizeObserverEntry],
      this as unknown as ResizeObserver
    );
  }
  unobserve(): void {}
  disconnect(): void {}
}
beforeAll(() => {
  globalThis.ResizeObserver = WidthResizeObserver as unknown as typeof ResizeObserver;
  window.DOMMatrixReadOnly = DomMatrixReadOnlyMock as unknown as typeof DOMMatrixReadOnly;
});
afterAll(() => {
  globalThis.ResizeObserver = originalResizeObserver;
  window.DOMMatrixReadOnly = originalDomMatrixReadOnly;
});
afterEach(() => {
  vi.unstubAllEnvs();
});

const workflow: WorkflowDef = {
  id: "wf-runs",
  name: "Runs",
  version: "4",
  spec: "1.0",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger", position: { x: 80, y: 80 } },
    { name: "approve", type: "xflow.http", position: { x: 320, y: 80 } },
    { name: "finish", type: "xflow.end", position: { x: 560, y: 80 } }
  ],
  connections: { start: { main: [{ node: "approve" }] }, approve: { main: [{ node: "finish" }] } }
};

/** The version the run was submitted with: an extra `audit` node the draft lacks. */
const snapshotWorkflow: WorkflowDef = {
  ...workflow,
  version: "3",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger", position: { x: 80, y: 80 } },
    { name: "approve", type: "xflow.http", position: { x: 320, y: 80 } },
    { name: "audit", type: "xflow.http", position: { x: 560, y: 80 } },
    { name: "finish", type: "xflow.end", position: { x: 800, y: 80 } }
  ],
  connections: {
    start: { main: [{ node: "approve" }] },
    approve: { main: [{ node: "audit" }] },
    audit: { main: [{ node: "finish" }] }
  }
};

const snapshotRuntime: RuntimeSnapshot = {
  nodes: {
    start: { status: "success" },
    approve: { status: "success" },
    audit: { status: "success" },
    finish: { status: "success" }
  }
};

const selectedExecution: XFlowExecution = {
  executionId: "exec-runs-1",
  status: "success",
  workflowId: "wf-runs",
  workflowVersion: "3",
  parentExecutionId: "exec-parent",
  rerunFrom: "approve",
  createdAt: "2026-01-02T03:04:05Z",
  updatedAt: "2026-01-02T03:04:15Z",
  nodes: [
    { name: "start", status: "success" },
    { name: "approve", status: "success" },
    { name: "audit", status: "success" },
    { name: "finish", status: "success" }
  ]
};

/** Rerun entry points are keyed on status: success no longer offers one. */
const failedExecution: XFlowExecution = {
  ...selectedExecution,
  executionId: "exec-runs-failed",
  status: "failed",
  nodes: [
    { name: "start", status: "success" },
    { name: "approve", status: "failed" },
    { name: "audit", status: "success" },
    { name: "finish", status: "pending" }
  ]
};

const allCapabilities: XFlowEditorCapabilities = {
  workflow: true,
  execution: true,
  executionControl: true,
  executionRerun: true
};

const canvasNode = (name: string): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} 节点 "]`);
  if (!node) throw new Error(`canvas node ${name} not found`);
  return node;
};

const runsProps = {
  capabilities: allCapabilities,
  executionSnapshotRuntime: snapshotRuntime,
  executionSnapshotWorkflow: snapshotWorkflow,
  mode: "runs" as const,
  selectedExecution,
  value: workflow
};

describe("运行记录 mode (§6.8)", () => {
  it("replaces 预览 with 编辑 | 运行记录 and swaps the library for the execution list", () => {
    const onLoadExecutions = vi.fn();
    render(<XFlowEditor {...runsProps} onLoadExecutions={onLoadExecutions} />);

    const modeSegmented = document.querySelector<HTMLElement>(".xflow-editor-mode-segmented");
    if (!modeSegmented) throw new Error("mode segmented not found");
    const radios = within(modeSegmented).getAllByRole("radio").map((radio) => radio.closest("label")?.textContent);
    expect(radios).toEqual(["编辑", "运行记录"]);
    // P2 ruling 2: 预览 is gone, not a third segment.
    expect(screen.queryByRole("radio", { name: "预览" })).toBeNull();

    const editor = screen.getByRole("region", { name: "Runs editor" });
    expect(editor.getAttribute("data-view-mode")).toBe("runs");
    expect(screen.getByRole("region", { name: "执行" })).toBeTruthy();
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();
    expect(screen.queryByRole("button", { name: "添加节点 (A)" })).toBeNull();
    expect(screen.getByText("只读回放")).toBeTruthy();
    expect(onLoadExecutions).toHaveBeenCalledTimes(1);
    expect(onLoadExecutions.mock.calls[0][0]).toMatchObject({ page: 1 });
  });

  it("drops the editing actions from the top bar while replaying, and restores them in 编辑", () => {
    const handlers = { onSave: vi.fn(), onRun: vi.fn(), onPublish: vi.fn() };
    const { rerender } = render(<XFlowEditor {...runsProps} {...handlers} />);

    // §6.8: the run-record top bar carries the execution identity and its
    // controls, never 保存 / 运行 / 发布 — the canvas is a snapshot, not a draft.
    expect(screen.queryByRole("button", { name: "保存" })).toBeNull();
    expect(screen.queryByRole("button", { name: "运行工作流" })).toBeNull();
    expect(screen.queryByRole("button", { name: "发布" })).toBeNull();

    rerender(<XFlowEditor {...runsProps} {...handlers} mode="edit" />);
    expect(screen.getByRole("button", { name: "保存" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "运行工作流" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "发布" })).toBeTruthy();
  });

  it("reopens a collapsed left column from the 执行 rail key", () => {
    render(<XFlowEditor capabilities={allCapabilities} value={workflow} />);
    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "执行记录" }));
    // Entering 运行记录 must not leave the execution list behind a collapsed
    // column: the rail key is the only control still on screen.
    expect(screen.getByRole("region", { name: "执行" })).toBeTruthy();
  });

  it("opens the execution list from the 运行记录 segment when the column is collapsed", () => {
    render(<XFlowEditor capabilities={allCapabilities} value={workflow} />);
    fireEvent.click(screen.getByRole("button", { name: "收起左栏" }));
    expect(screen.queryByRole("region", { name: "节点" })).toBeNull();

    fireEvent.click(screen.getByRole("radio", { name: "运行记录" }));
    expect(screen.getByRole("region", { name: "执行" })).toBeTruthy();
  });

  it("opens the compact execution drawer from the 运行记录 segment", () => {
    // Layout b has no left column: the execution list lives in the drawer, and
    // the segment must open it on 执行列表 rather than leaving 节点与大纲 in front.
    class CompactResizeObserver {
      private readonly callback: ResizeObserverCallback;
      constructor(callback: ResizeObserverCallback) {
        this.callback = callback;
      }
      observe(target: Element): void {
        this.callback(
          [{ target, contentRect: { width: 1200, height: 900 } } as ResizeObserverEntry],
          this as unknown as ResizeObserver
        );
      }
      unobserve(): void {}
      disconnect(): void {}
    }
    const previous = globalThis.ResizeObserver;
    globalThis.ResizeObserver = CompactResizeObserver as unknown as typeof ResizeObserver;
    try {
      render(<XFlowEditor capabilities={allCapabilities} value={workflow} />);
      fireEvent.click(screen.getByRole("radio", { name: "运行记录" }));
      expect(screen.getByRole("dialog", { name: "执行列表" })).toBeTruthy();
    } finally {
      globalThis.ResizeObserver = previous;
    }
  });

  it("closes the compact execution drawer once a row is picked, so the canvas controls are free", async () => {
    class CompactResizeObserver {
      private readonly callback: ResizeObserverCallback;
      constructor(callback: ResizeObserverCallback) {
        this.callback = callback;
      }
      observe(target: Element): void {
        this.callback(
          [{ target, contentRect: { width: 1200, height: 900 } } as ResizeObserverEntry],
          this as unknown as ResizeObserver
        );
      }
      unobserve(): void {}
      disconnect(): void {}
    }
    const previous = globalThis.ResizeObserver;
    globalThis.ResizeObserver = CompactResizeObserver as unknown as typeof ResizeObserver;
    try {
      const onSelectExecution = vi.fn();
      render(
        <XFlowEditor
          {...runsProps}
          executionList={{
            list: [
              { executionId: "exec-a", status: "success", createdAt: "2026-01-02T03:04:05Z", updatedAt: "2026-01-02T03:04:15Z" },
              { executionId: "exec-b", status: "failed", createdAt: "2026-01-02T04:04:05Z", updatedAt: "2026-01-02T04:04:15Z" }
            ],
            total: 2
          }}
          onLoadExecutions={vi.fn()}
          onSelectExecution={onSelectExecution}
        />
      );
      const rail = screen.getByRole("complementary", { name: "紧凑导航" });
      const key = within(rail).getByRole("button", { name: "执行记录" });
      key.focus();
      fireEvent.click(key);
      const dialog = await screen.findByRole("dialog", { name: "执行列表" });

      fireEvent.click(within(dialog).getByRole("button", { name: /exec-b/ }));

      expect(onSelectExecution).toHaveBeenCalledWith("exec-b");
      await waitFor(() => expect(screen.queryByRole("dialog", { name: "执行列表" })).toBeNull());
      // The row unmounted with the drawer; focus goes back to the key that opened it.
      expect(document.activeElement).toBe(key);
    } finally {
      globalThis.ResizeObserver = previous;
    }
  });

  it("leaves the docked execution list open after a row is picked on the desktop layout", () => {
    const onSelectExecution = vi.fn();
    render(
      <XFlowEditor
        {...runsProps}
        executionList={{
          list: [{ executionId: "exec-a", status: "success", createdAt: "2026-01-02T03:04:05Z", updatedAt: "2026-01-02T03:04:15Z" }],
          total: 1
        }}
        onLoadExecutions={vi.fn()}
        onSelectExecution={onSelectExecution}
      />
    );

    fireEvent.click(within(screen.getByRole("region", { name: "执行" })).getByRole("button", { name: /exec-a/ }));

    expect(onSelectExecution).toHaveBeenCalledWith("exec-a");
    expect(screen.getByRole("region", { name: "执行" })).toBeTruthy();
  });

  it("expands the left column when 节点 returns from the replay", () => {
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        executionSnapshotWorkflow={snapshotWorkflow}
        value={workflow}
      />
    );
    fireEvent.click(screen.getByRole("radio", { name: "运行记录" }));
    expect(screen.getByRole("region", { name: "执行" })).toBeTruthy();

    // The replay shows the execution list in the column, so 节点 must expand
    // it rather than toggle the remembered edit section off screen.
    fireEvent.click(screen.getByRole("button", { name: "节点与大纲" }));
    expect(screen.getByRole("region", { name: "节点" })).toBeTruthy();
  });

  it("leaves an expanded diagnostics drawer before entering the replay", () => {
    render(<XFlowEditor capabilities={allCapabilities} value={workflow} />);
    fireEvent.click(screen.getByRole("button", { name: "展开诊断台" }));
    fireEvent.click(screen.getByRole("button", { name: "扩展到画布高度" }));
    const main = () => document.querySelector(".xflow-editor-main");
    expect(main()?.hasAttribute("inert")).toBe(true);

    fireEvent.click(screen.getByRole("radio", { name: "运行记录" }));
    // The replay swaps the dock for its status bar; keeping `inert` would make
    // the whole workbench, execution list included, unusable with no control
    // left on screen to collapse the (unrendered) drawer.
    expect(main()?.hasAttribute("inert")).toBe(false);
    expect(screen.getByRole("region", { name: "执行" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "收起诊断台" })).toBeNull();

    fireEvent.click(screen.getByRole("radio", { name: "编辑" }));
    // The drawer was converged to 停靠 (not collapsed): it comes back with the
    // edit workbench instead of reappearing over it.
    expect(document.querySelector(".xflow-editor-bottom")?.getAttribute("data-dock")).toBe("docked");
  });

  it("hides the run-record entries without the execution scope", () => {
    const onModeChange = vi.fn();
    render(
      <XFlowEditor
        capabilities={{ ...allCapabilities, execution: false }}
        mode="runs"
        onModeChange={onModeChange}
        value={workflow}
      />
    );

    expect(screen.queryByRole("radio", { name: "运行记录" })).toBeNull();
    expect(screen.queryByRole("button", { name: "执行记录" })).toBeNull();
    // §6.11 hides rather than disables. A controlled host (a shared ?exec=
    // link) cannot leave the editor stuck in a mode it no longer offers.
    expect(onModeChange).toHaveBeenCalledWith("edit");
  });

  it("searches the replay snapshot in 运行记录 instead of the draft", async () => {
    render(<XFlowEditor {...runsProps} />);
    fireEvent.keyDown(window, { key: "k", metaKey: true });

    const dialog = await screen.findByRole("dialog", { name: "定位节点" });
    const options = within(dialog).getAllByRole("option").map((option) => option.textContent ?? "");
    // The draft has three nodes; the run's version has four (audit only exists
    // in the snapshot). Searching the draft would miss the node on screen.
    expect(options).toHaveLength(4);
    expect(options.some((text) => text.includes("audit"))).toBe(true);
  });

  it("keeps the replay canvas out of reach of dragging and Delete", () => {
    const onChange = vi.fn();
    const { rerender } = render(<XFlowEditor {...runsProps} onChange={onChange} />);
    const flowNode = () => document.querySelector<HTMLElement>(".react-flow__node");
    if (!flowNode()) throw new Error("canvas node not rendered");
    expect(flowNode()?.classList.contains("draggable")).toBe(false);

    fireEvent.click(canvasNode("audit"));
    fireEvent.keyDown(window, { key: "Delete" });
    fireEvent.keyDown(window, { key: "Backspace" });
    expect(onChange).not.toHaveBeenCalled();
    expect(canvasNode("audit")).toBeTruthy();

    rerender(<XFlowEditor {...runsProps} onChange={onChange} mode="edit" />);
    expect(flowNode()?.classList.contains("draggable")).toBe(true);
  });

  it("disables 校验 in the compact ⋯ menu while replaying", () => {
    // Layout b/c renders 校验 only in the ⋯ menu, and the replay has neither a
    // draft nor a diagnostics dock for it to act on.
    class CompactResizeObserver {
      private readonly callback: ResizeObserverCallback;
      constructor(callback: ResizeObserverCallback) {
        this.callback = callback;
      }
      observe(target: Element): void {
        this.callback(
          [{ target, contentRect: { width: 1200, height: 900 } } as ResizeObserverEntry],
          this as unknown as ResizeObserver
        );
      }
      unobserve(): void {}
      disconnect(): void {}
    }
    const previous = globalThis.ResizeObserver;
    globalThis.ResizeObserver = CompactResizeObserver as unknown as typeof ResizeObserver;
    try {
      render(<XFlowEditor {...runsProps} />);
      const toolbar = screen.getByRole("toolbar", { name: "编辑器工具栏" });
      fireEvent.click(within(toolbar).getByRole("button", { name: "更多操作" }));
      expect(screen.getByRole("menuitem", { name: "校验" }).getAttribute("aria-disabled")).toBe("true");
    } finally {
      globalThis.ResizeObserver = previous;
    }
  });

  it("jumps to the source execution from the rerun lineage", () => {
    const onSelectExecution = vi.fn();
    render(<XFlowEditor {...runsProps} onSelectExecution={onSelectExecution} />);

    const top = screen.getByRole("region", { name: "Runs editor" });
    fireEvent.click(within(top).getByRole("button", { name: /重跑自 exec-parent/ }));
    expect(onSelectExecution).toHaveBeenCalledWith("exec-parent");
  });

  it("replays the captured definition, annotates the rerun lineage and counts the run", () => {
    render(<XFlowEditor {...runsProps} />);

    // The snapshot's `audit` node only exists in version 3; the draft has none.
    expect(canvasNode("audit")).toBeTruthy();
    expect(screen.getByText("沿用原执行结果")).toBeTruthy();
    expect(screen.getByText("重跑起点")).toBeTruthy();

    const top = screen.getByRole("region", { name: "Runs editor" });
    // EL-19: the id is shown once, in the status bar; the top bar keeps only
    // the state chip and the lineage.
    expect(within(top).getByLabelText("当前执行").textContent).not.toContain("exec-runs-1");
    expect(within(top).getByLabelText("当前执行").textContent).toContain("重跑自 exec-parent · 起点 approve");

    const statusbar = screen.getByLabelText("执行状态栏");
    expect(statusbar.textContent).toContain("exec-runs-1");
    expect(statusbar.textContent).toContain("4 节点 · 4 成功");
    expect(statusbar.textContent).toContain("只读回放 · 定义为当次运行版本");

    // The title chip names the run's version, not the draft's; the leading `v`
    // matches the version chip and the /ui-preview runs baseline.
    expect(screen.getByText("运行版本 v3")).toBeTruthy();
  });

  it("draws the run summary bar and keeps the section boxes over the replay (EL-11)", () => {
    const onModeChange = vi.fn();
    render(
      <XFlowEditor
        {...runsProps}
        executionSnapshotRuntime={{ ...snapshotRuntime, traceId: "trace-runs-1" }}
        onModeChange={onModeChange}
        sections={[{ id: "sec-approval", title: "审批", node_keys: ["start"], x: 0, y: 0, width: 420, height: 220 }]}
        traceHref={(traceId) => `/traces/${traceId}`}
      />
    );

    const summary = screen.getByRole("region", { name: "本次运行" });
    expect(summary.textContent).toContain("成功");
    expect(summary.textContent).toContain("开始 ");
    expect(summary.textContent).toContain("耗时 10.0 s");
    expect(summary.textContent).toContain("成功 4 · 失败 0 · 跳过 0");
    expect(within(summary).getByRole("link", { name: "查看 trace" }).getAttribute("href")).toBe(
      "/traces/trace-runs-1"
    );

    // The replay keeps the edit canvas's visual landmarks, read-only.
    const boxes = [...document.querySelectorAll<HTMLElement>(".xflow-preview-section")];
    expect(boxes.map((box) => box.querySelector(".xflow-preview-section__title")?.textContent)).toEqual(["审批"]);
    expect(document.querySelector(".xflow-preview-section__title-action")).toBeNull();
    expect(document.querySelector(".xflow-preview-section__delete")).toBeNull();

    fireEvent.click(within(summary).getByRole("button", { name: "返回编辑" }));
    expect(onModeChange).toHaveBeenCalledWith("edit");
  });

  it("falls back to the current definition and marks it 定义已变化", () => {
    render(
      <XFlowEditor
        {...runsProps}
        executionDefinitionStale
        executionSnapshotWorkflow={workflow}
        executionSnapshotRuntime={{ nodes: { start: { status: "success" } } }}
      />
    );

    expect(screen.getByLabelText("执行状态栏").textContent).toContain("定义已变化，仅供参考");
  });

  it("offers the top-bar 重跑… entry only for a failed terminal execution", async () => {
    const { rerender } = render(<XFlowEditor {...runsProps} onRerun={vi.fn()} />);
    const top = screen.getByRole("region", { name: "Runs editor" });
    // Ruling 2026-10-21: a successful run offers no rerun action.
    expect(within(top).queryByRole("button", { name: /重跑…/ })).toBeNull();

    rerender(<XFlowEditor {...runsProps} selectedExecution={failedExecution} onRerun={vi.fn()} />);
    fireEvent.click(within(top).getByRole("button", { name: /重跑…/ }));

    const dialog = await screen.findByRole("dialog", { name: "重跑执行" });
    expect(within(dialog).getByText("exec-runs-failed")).toBeTruthy();
  });

  it("opens the Inspector on 运行 and reruns from the selected snapshot node", async () => {
    const onRerun = vi.fn(async () => ({ executionId: "exec-runs-2", status: "queued" as const }));
    render(<XFlowEditor {...runsProps} selectedExecution={failedExecution} onRerun={onRerun} />);

    fireEvent.click(canvasNode("approve"));
    const inspector = screen.getByRole("region", { name: /属性/ });
    expect(within(inspector).getByRole("tab", { name: "运行" }).getAttribute("aria-selected")).toBe("true");

    fireEvent.click(within(inspector).getByRole("button", { name: "从此节点重跑" }));
    const dialog = await screen.findByRole("dialog", { name: "重跑执行" });
    // A failed source needs no include_success confirmation.
    expect(within(dialog).queryByRole("checkbox")).toBeNull();
    fireEvent.click(within(dialog).getByRole("button", { name: "从这里重跑" }));

    await waitFor(() => expect(onRerun).toHaveBeenCalledTimes(1));
    const request = onRerun.mock.calls[0][0];
    expect(request.executionId).toBe("exec-runs-failed");
    // The Inspector entry names the node the user selected, not the default.
    expect(request.fromNode).toBe("approve");
    expect(request.includeSuccess).toBe(false);
    expect(typeof request.idempotencyKey).toBe("string");
    expect(request.idempotencyKey.length).toBeGreaterThan(8);
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "重跑执行" })).toBeNull());
  });

  it("drops the Inspector footer in a replay whose status offers no rerun", () => {
    // Ruling 2026-10-21: success/canceled no longer carry `rerun`, so a replay
    // must not fall back to the editing-only, disabled 测试此节点 footer.
    const { rerender } = render(<XFlowEditor {...runsProps} />);

    fireEvent.click(canvasNode("approve"));
    const successInspector = screen.getByRole("region", { name: /属性/ });
    expect(successInspector.querySelector(".xflow-editor-inspector-foot")).toBeNull();
    expect(within(successInspector).queryByRole("button", { name: "测试此节点" })).toBeNull();
    expect(within(successInspector).queryByRole("button", { name: "从此节点重跑" })).toBeNull();

    // A failed source keeps exactly the rerun entry.
    rerender(<XFlowEditor {...runsProps} selectedExecution={failedExecution} onRerun={vi.fn()} />);
    fireEvent.click(canvasNode("approve"));
    const failedInspector = screen.getByRole("region", { name: /属性/ });
    expect(within(failedInspector).getByRole("button", { name: "从此节点重跑" })).toBeTruthy();
    expect(within(failedInspector).queryByRole("button", { name: "测试此节点" })).toBeNull();
  });

  it("hides the floating run bar in the replay and brings it back in 编辑", () => {
    const running: XFlowExecution = { executionId: "exec-live", status: "running", nodes: [] };
    const { rerender } = render(<XFlowEditor capabilities={allCapabilities} execution={running} value={workflow} />);
    expect(screen.getByLabelText("执行 exec-live 状态")).toBeTruthy();

    rerender(<XFlowEditor capabilities={allCapabilities} execution={running} mode="runs" value={workflow} />);
    expect(screen.queryByLabelText("执行 exec-live 状态")).toBeNull();

    rerender(<XFlowEditor capabilities={allCapabilities} execution={running} mode="edit" value={workflow} />);
    expect(screen.getByLabelText("执行 exec-live 状态")).toBeTruthy();
  });

  it("keeps the draft when the user visits the replay and comes back", () => {
    const { container } = render(<XFlowEditor value={workflow} />);
    // ED-14 folds every group but the first; open 动作 so HTTP is on screen.
    fireEvent.click(within(screen.getByRole("region", { name: "节点" })).getByRole("button", { name: /动作/ }));
    const nodeCount = () => container.querySelectorAll(".xflow-preview-node").length;
    expect(nodeCount()).toBe(3);

    fireEvent.click(within(screen.getByRole("region", { name: "节点" })).getByRole("button", { name: "HTTP" }));
    expect(nodeCount()).toBe(4);

    fireEvent.click(screen.getByRole("radio", { name: "运行记录" }));
    expect(nodeCount()).toBe(0);
    expect(screen.getByText("从左侧选择一条执行")).toBeTruthy();

    fireEvent.click(screen.getByRole("radio", { name: "编辑" }));
    expect(nodeCount()).toBe(4);
  });

  it("restores the edit viewport after a replay round trip", () => {
    const onViewportChange = vi.fn();
    const initial = { x: 0, y: 0, zoom: 1 };
    const panned = { x: 111, y: 222, zoom: 0.5 };
    const { rerender } = render(
      <XFlowEditor defaultViewport={initial} onViewportChange={onViewportChange} value={workflow} />
    );
    // A pan on the edit canvas: the editor remembers the settled viewport.
    act(() => preview.lastProps?.onViewportChange?.(panned));
    expect(onViewportChange).toHaveBeenCalledWith(panned);

    rerender(<XFlowEditor mode="runs" onViewportChange={onViewportChange} value={workflow} />);
    rerender(<XFlowEditor mode="edit" onViewportChange={onViewportChange} value={workflow} />);

    // The remounted edit canvas opens at the remembered viewport, not the
    // host's initial one.
    expect(preview.lastProps?.defaultViewport).toEqual(panned);
  });

  it("keeps the run bar's 重跑 entry for terminal runs and drops it without a host handler", async () => {
    // rc hands every overlay the constant id "test-id" in NODE_ENV=test. The
    // merged 配置 tab keeps duration Selects in the DOM, so the dialog's
    // aria-labelledby would resolve to one of those instead of its title. A
    // real build numbers each overlay; this test needs the same.
    vi.stubEnv("NODE_ENV", "development");
    const failed: XFlowExecution = {
      executionId: "exec-failed",
      status: "failed",
      nodes: [
        { name: "start", status: "success" },
        { name: "approve", status: "failed" },
        { name: "finish", status: "pending" }
      ]
    };
    const { rerender } = render(
      <XFlowEditor capabilities={allCapabilities} execution={failed} onRerun={vi.fn()} value={workflow} />
    );
    fireEvent.click(within(screen.getByLabelText("执行 exec-failed 状态")).getByRole("button", { name: /重跑/ }));
    const dialog = await screen.findByRole("dialog", { name: "重跑执行" });
    // The default start node is the first failure, and a failed source needs no
    // success confirmation.
    expect(within(dialog).getByText("approve")).toBeTruthy();
    expect(within(dialog).queryByRole("checkbox")).toBeNull();

    rerender(<XFlowEditor capabilities={allCapabilities} execution={failed} value={workflow} />);
    expect(within(screen.getByLabelText("执行 exec-failed 状态")).queryByRole("button", { name: /重跑/ })).toBeNull();
  });

  it("reuses one Idempotency-Key across a failed submission's retry", async () => {
    vi.stubEnv("NODE_ENV", "development");
    const onRerun = vi.fn()
      .mockRejectedValueOnce(new Error("工作流执行不可重跑"))
      .mockResolvedValueOnce({ executionId: "exec-retry", status: "queued" as const });
    const failed: XFlowExecution = {
      executionId: "exec-retry-src",
      status: "failed",
      nodes: [{ name: "start", status: "failed" }]
    };
    render(<XFlowEditor capabilities={allCapabilities} execution={failed} onRerun={onRerun} value={workflow} />);

    fireEvent.click(within(screen.getByLabelText("执行 exec-retry-src 状态")).getByRole("button", { name: /重跑/ }));
    const dialog = await screen.findByRole("dialog", { name: "重跑执行" });
    fireEvent.click(within(dialog).getByRole("button", { name: "从这里重跑" }));
    expect(await within(dialog).findByRole("alert")).toBeTruthy();

    // Re-query: the error re-render can replace the modal's DOM node.
    const retry = await screen.findByRole("dialog", { name: "重跑执行" });
    fireEvent.click(within(retry).getByRole("button", { name: "从这里重跑" }));
    await waitFor(() => expect(onRerun).toHaveBeenCalledTimes(2));
    expect(onRerun.mock.calls[0][0].idempotencyKey).toBe(onRerun.mock.calls[1][0].idempotencyKey);
  });
});
