import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { RuntimeSnapshot, WorkflowDef, XFlowEditorCapabilities } from "@xflow/core";
import { XFlowEditor, type XFlowExecution } from "./index";

// jsdom has no layout: the editor learns its width from a ResizeObserver, and
// the run-bar stacking test below drives the DOM rects directly. Seeding 1440
// keeps every other test on the same wide layout a 0 width already produced
// (1280 is compact since EL-02).
const originalResizeObserver = globalThis.ResizeObserver;
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
});
afterAll(() => {
  globalThis.ResizeObserver = originalResizeObserver;
});

const workflow: WorkflowDef = {
  id: "wf-runbar",
  name: "Run bar",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger" },
    { name: "approve", type: "xflow.http", position: { x: 240, y: 120 } },
    { name: "fail", type: "xflow.end", position: { x: 480, y: 120 } }
  ],
  connections: { start: { main: [{ node: "approve" }] }, approve: { main: [{ node: "fail" }] } }
};

const allCapabilities: XFlowEditorCapabilities = {
  workflow: true,
  execution: true,
  executionControl: true,
  executionRerun: true
};

const runBar = (): HTMLElement => {
  const bar = document.querySelector<HTMLElement>(".xflow-editor-runbar");
  if (!bar) throw new Error("run bar not found");
  return bar;
};

const runningExecution: XFlowExecution = {
  executionId: "exec-8432",
  status: "running",
  nodes: [
    { name: "start", status: "success" },
    { name: "approve", status: "suspended" },
    { name: "fail", status: "failed" }
  ]
};

afterEach(() => {
  vi.useRealTimers();
});

describe("canvas run bar (§6.3)", () => {
  it("draws the status chip, the copyable execution id and the status table's operations", () => {
    render(<XFlowEditor capabilities={allCapabilities} execution={runningExecution} value={workflow} />);

    expect(within(runBar()).getByRole("status").textContent).toContain("运行中");
    // C05: the bar no longer prints the full id; the group's accessible name does.
    expect(runBar().getAttribute("aria-label")).toBe("执行 exec-8432 状态");
    // workflowStatusMeta.running = [pause, cancel]; rerun never appears (Q7=a).
    expect(within(runBar()).getByRole("button", { name: /暂停/ })).toBeTruthy();
    expect(within(runBar()).getByRole("button", { name: /取消/ })).toBeTruthy();
    expect(within(runBar()).queryByRole("button", { name: /继续/ })).toBeNull();
    expect(within(runBar()).queryByRole("button", { name: /重跑/ })).toBeNull();
    // The id keeps AntD's own copy control, and only the text child may
    // truncate: a clipped Typography root would cut the copy button off.
    const idRoot = runBar().querySelector(".xflow-editor-runbar__id");
    const idText = idRoot?.querySelector(".xflow-editor-runbar__id-text");
    const copy = idRoot?.querySelector(".ant-typography-copy");
    // C05: nine characters already take the short form; the full id stays on
    // hover and in the accessible name.
    expect(idText?.textContent).toBe("exec-843…");
    expect(idText?.getAttribute("title")).toBe("exec-8432");
    expect(idText?.getAttribute("aria-label")).toBe("exec-8432");
    expect(copy).not.toBeNull();
    expect(idText?.contains(copy)).toBe(false);
  });

  it("floats as a canvas-top toast in running, success and failed states", () => {
    const toast = () => document.querySelector<HTMLElement>(".xflow-editor-run-toast");
    const { rerender } = render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-toast", status: "running" }}
        value={workflow}
      />
    );

    // §6.3: the toast is an overlay owned by the canvas, not a toolbar row.
    expect(toast()).not.toBeNull();
    expect(toast()?.closest(".xflow-editor-canvas")).not.toBeNull();
    expect(document.querySelector(".xflow-editor-toolbar .xflow-editor-runbar")).toBeNull();
    expect(document.querySelector(".xflow-editor-runbar-row")).toBeNull();
    expect(toast()?.querySelector(".xflow-editor-runbar")).not.toBeNull();

    // Running: polite, with the live controls; no terminal chrome.
    const runningChip = within(runBar()).getByRole("status");
    expect(runningChip.getAttribute("aria-live")).toBe("polite");
    expect(runningChip.textContent).toContain("运行中");
    expect(within(runBar()).getByRole("button", { name: /暂停/ })).toBeTruthy();
    expect(within(runBar()).queryByRole("button", { name: "查看运行记录" })).toBeNull();
    expect(toast()?.querySelector(".xflow-editor-runbar__tool")).toBeNull();

    rerender(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-toast", status: "success" }}
        value={workflow}
      />
    );
    // Success: still polite, terminal chrome replaces the operations.
    const successChip = within(runBar()).getByRole("status");
    expect(successChip.getAttribute("aria-live")).toBe("polite");
    expect(successChip.textContent).toContain("成功");
    expect(within(runBar()).getByRole("button", { name: "查看运行记录" })).toBeTruthy();
    expect(within(runBar()).queryByRole("button", { name: /暂停/ })).toBeNull();
    expect(toast()?.querySelector(".xflow-editor-runbar__tool")).not.toBeNull();

    rerender(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-toast", status: "failed" }}
        value={workflow}
      />
    );
    // Failed terminal: assertive alert, the one state the user must not miss.
    const failedChip = within(runBar()).getByRole("alert");
    expect(failedChip.getAttribute("aria-live")).toBe("assertive");
    expect(failedChip.textContent).toContain("失败");
    expect(within(runBar()).getByRole("button", { name: "查看运行记录" })).toBeTruthy();
    expect(toast()?.querySelector(".xflow-editor-runbar__tool")).not.toBeNull();
  });

  it("opens the §6.9 rerun dialog from the toast", () => {
    // Rerun is the status table's op for failed / timeout; the host owns the
    // request, so the toast only has to hand the source execution to the dialog.
    const onRerun = vi.fn();
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-rerun-bar", status: "failed", nodes: [{ name: "approve", status: "failed" }] }}
        onRerun={onRerun}
        runBarDismissMs={null}
        value={workflow}
      />
    );

    fireEvent.click(within(runBar()).getByRole("button", { name: /重跑/ }));

    const dialog = screen.getByRole("dialog");
    expect(dialog.textContent).toContain("重跑执行");
    expect(dialog.textContent).toContain("exec-rerun-bar");
    // Opening the dialog is not a request: the host's onSubmit has not run.
    expect(onRerun).not.toHaveBeenCalled();
  });

  it("copies the full execution id even though the bar shows the short one", async () => {
    const fullId = "exec-0123456789abcdef";
    const writeText = vi.fn(async () => undefined);
    const originalClipboard = Object.getOwnPropertyDescriptor(navigator, "clipboard");
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    try {
      render(
        <XFlowEditor
          capabilities={allCapabilities}
          execution={{ executionId: fullId, status: "running" }}
          value={workflow}
        />
      );

      const idRoot = runBar().querySelector(".xflow-editor-runbar__id");
      const idText = idRoot?.querySelector(".xflow-editor-runbar__id-text");
      expect(idText?.textContent).toBe("exec-012…");
      expect(idText?.getAttribute("title")).toBe(fullId);
      expect(idText?.getAttribute("aria-label")).toBe(fullId);

      const copy = idRoot?.querySelector<HTMLElement>(".ant-typography-copy");
      if (!copy) throw new Error("copy control not found");
      fireEvent.click(copy);
      // AntD's copyable writes `copyable.text`, which the bar keeps full.
      await waitFor(() => expect(writeText).toHaveBeenCalledWith(fullId));
    } finally {
      if (originalClipboard) Object.defineProperty(navigator, "clipboard", originalClipboard);
      else Reflect.deleteProperty(navigator, "clipboard");
    }
  });

  it("runs pause and resume immediately and cancels only after confirmation", async () => {
    const onExecutionAction = vi.fn();
    const { rerender } = render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={runningExecution}
        onExecutionAction={onExecutionAction}
        value={workflow}
      />
    );

    fireEvent.click(within(runBar()).getByRole("button", { name: /暂停/ }));
    expect(onExecutionAction).toHaveBeenCalledWith("exec-8432", "pause");

    rerender(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ ...runningExecution, status: "paused" }}
        onExecutionAction={onExecutionAction}
        value={workflow}
      />
    );
    fireEvent.click(within(runBar()).getByRole("button", { name: /继续/ }));
    expect(onExecutionAction).toHaveBeenCalledWith("exec-8432", "resume");

    fireEvent.click(within(runBar()).getByRole("button", { name: /取消/ }));
    // Nothing is sent until the Popconfirm is accepted.
    expect(onExecutionAction).not.toHaveBeenCalledWith("exec-8432", "cancel");
    fireEvent.click(await screen.findByRole("button", { name: "取消执行" }));
    await waitFor(() => expect(onExecutionAction).toHaveBeenCalledWith("exec-8432", "cancel"));
  });

  it("locates the first waiting-approval and failed node from the counts", () => {
    const onLocateNode = vi.fn();
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={runningExecution}
        onLocateNode={onLocateNode}
        value={workflow}
      />
    );

    fireEvent.click(within(runBar()).getByRole("button", { name: /1 等待审批/ }));
    expect(onLocateNode).toHaveBeenCalledWith("approve");
    fireEvent.click(within(runBar()).getByRole("button", { name: /1 失败/ }));
    expect(onLocateNode).toHaveBeenCalledWith("fail");
  });

  it("shows the queue position in the queued chip", () => {
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-q", status: "queued", queuePosition: 2 }}
        value={workflow}
      />
    );

    expect(within(runBar()).getByRole("status").textContent).toContain("排队 · 第 2 位");
  });

  it("counts the run time from created_at on the wall clock and freezes at the terminal timestamp", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-21T10:00:00Z"));
    const createdAt = new Date(Date.now()).toISOString();
    const duration = () => runBar().querySelector<HTMLElement>(".xflow-editor-runbar__duration");
    const { rerender } = render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-timer", status: "running", createdAt }}
        value={workflow}
      />
    );

    expect(duration()?.textContent).toBe("0s");
    // WB-03: the title says the clock is wall time, so a paused run keeps counting.
    expect(duration()?.getAttribute("title")).toContain("墙上时间");
    act(() => {
      vi.advanceTimersByTime(5000);
    });
    expect(duration()?.textContent).toBe("5s");

    // Paused is still live: the wall clock keeps running.
    rerender(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-timer", status: "paused", createdAt }}
        value={workflow}
      />
    );
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(duration()?.textContent).toBe("6s");

    // Terminal: updated_at is the total, and the bar stops ticking.
    rerender(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{
          executionId: "exec-timer",
          status: "success",
          createdAt,
          updatedAt: new Date(Date.parse(createdAt) + 12_000).toISOString()
        }}
        runBarDismissMs={null}
        value={workflow}
      />
    );
    expect(duration()?.textContent).toBe("12s");
    expect(duration()?.getAttribute("title")).toContain("总耗时");
    act(() => {
      vi.advanceTimersByTime(30_000);
    });
    expect(duration()?.textContent).toBe("12s");
  });

  it("formats a minute-long run as 1m 05s", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-21T10:00:00Z"));
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{
          executionId: "exec-minute",
          status: "running",
          createdAt: new Date(Date.now() - 65_000).toISOString()
        }}
        value={workflow}
      />
    );

    expect(runBar().querySelector(".xflow-editor-runbar__duration")?.textContent).toBe("1m 05s");
  });

  it("freezes a terminal run at its first terminal render when updated_at is absent", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-21T10:00:10Z"));
    const createdAt = new Date(Date.now() - 4000).toISOString();
    const duration = () => runBar().querySelector<HTMLElement>(".xflow-editor-runbar__duration");
    const { rerender } = render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-freeze", status: "running", createdAt }}
        value={workflow}
      />
    );
    expect(duration()?.textContent).toBe("4s");
    act(() => {
      vi.advanceTimersByTime(2000);
    });
    expect(duration()?.textContent).toBe("6s");

    rerender(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-freeze", status: "failed", createdAt }}
        runBarDismissMs={null}
        value={workflow}
      />
    );
    expect(duration()?.textContent).toBe("6s");
    act(() => {
      vi.advanceTimersByTime(60_000);
    });
    expect(duration()?.textContent).toBe("6s");
  });

  it("shows no duration when the snapshot has no created_at", () => {
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-no-clock", status: "running" }}
        value={workflow}
      />
    );
    expect(runBar().querySelector(".xflow-editor-runbar__duration")).toBeNull();
  });

  it("locates the node by its canvas key when the snapshot only names it", () => {
    // nodeKey() is `node.id ?? node.name`: a bare name selects nothing when the
    // node has an id, and the previous fallback picked the default node.
    const withIds: WorkflowDef = {
      ...workflow,
      nodes: [
        { id: "node-start", name: "start", type: "xflow.start", kind: "trigger" },
        { id: "node-approve", name: "approve", type: "xflow.http", position: { x: 240, y: 120 } },
        { id: "node-fail", name: "fail", type: "xflow.end", position: { x: 480, y: 120 } }
      ]
    };
    const onLocateNode = vi.fn();
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={runningExecution}
        onLocateNode={onLocateNode}
        value={withIds}
      />
    );

    fireEvent.click(within(runBar()).getByRole("button", { name: /1 失败/ }));

    expect(onLocateNode).toHaveBeenCalledWith("fail");
    const type = document.querySelector(".xflow-editor-inspector-head__type")?.textContent;
    expect(type).toBe("xflow.end");
  });

  it("clears a rejected action's error once the next action succeeds", async () => {
    const onExecutionAction = vi.fn()
      .mockRejectedValueOnce(new Error("暂停失败：执行已结束"))
      .mockResolvedValueOnce(undefined);
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={runningExecution}
        onExecutionAction={onExecutionAction}
        value={workflow}
      />
    );

    fireEvent.click(within(runBar()).getByRole("button", { name: /暂停/ }));
    await waitFor(() => expect(screen.getByText(/暂停失败：执行已结束/)).toBeTruthy());

    // A later success must not leave the previous failure in the dock/status bar.
    fireEvent.click(within(runBar()).getByRole("button", { name: /暂停/ }));
    await waitFor(() => expect(screen.queryByText(/暂停失败：执行已结束/)).toBeNull());
    expect(screen.queryByText("操作失败")).toBeNull();
    expect(screen.getByText("操作已提交")).toBeTruthy();
  });

  it("drops below the preview float row when the toast would cover it", () => {
    const { rerender } = render(
      <XFlowEditor capabilities={allCapabilities} execution={runningExecution} value={workflow} />
    );
    const bar = document.querySelector<HTMLElement>(".xflow-editor-runbar")!;
    const canvas = document.querySelector<HTMLElement>(".xflow-editor-preview-frame")!;
    const float = document.querySelector<HTMLElement>(".xflow-preview-canvas-float")!;
    // A 500px toast centred in a 580px canvas starts at 40 and would reach the
    // 添加节点 float (its right edge at 153); 1600px leaves it clear.
    Object.defineProperty(bar, "clientWidth", { configurable: true, value: 500 });
    canvas.getBoundingClientRect = () => ({ left: 0, width: 580 } as DOMRect);
    float.getBoundingClientRect = () => ({ right: 153 } as DOMRect);

    rerender(
      <XFlowEditor capabilities={allCapabilities} execution={{ ...runningExecution }} value={workflow} />
    );
    const toast = () => document.querySelector(".xflow-editor-run-toast");
    expect(toast()?.getAttribute("data-stacked")).toBe("true");

    canvas.getBoundingClientRect = () => ({ left: 0, width: 1600 } as DOMRect);
    rerender(
      <XFlowEditor capabilities={allCapabilities} execution={{ ...runningExecution }} value={workflow} />
    );
    expect(toast()?.getAttribute("data-stacked")).toBeNull();
  });

  it("shows 查看运行记录 and a close control on a terminal run, and hides after 5s", () => {
    vi.useFakeTimers();
    const onOpenRunHistory = vi.fn();
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-8406", status: "success", nodes: [{ name: "start", status: "success" }] }}
        onOpenRunHistory={onOpenRunHistory}
        value={workflow}
      />
    );

    expect(within(runBar()).getByRole("status").textContent).toContain("成功");
    expect(within(runBar()).getByRole("button", { name: "查看运行记录" })).toBeTruthy();
    expect(within(runBar()).queryByRole("button", { name: /重跑/ })).toBeNull();
    expect(runBar().textContent).toContain("5s 后收起");
    fireEvent.click(within(runBar()).getByRole("button", { name: "查看运行记录" }));
    expect(onOpenRunHistory).toHaveBeenCalledTimes(1);

    act(() => {
      vi.advanceTimersByTime(4999);
    });
    expect(document.querySelector(".xflow-editor-runbar")).not.toBeNull();
    act(() => {
      vi.advanceTimersByTime(1);
    });
    // E-C: the bar plays its 120ms exit before it unmounts.
    expect(document.querySelector(".xflow-editor-runbar")?.getAttribute("data-leaving")).toBe("true");
    act(() => {
      vi.advanceTimersByTime(120);
    });
    expect(document.querySelector(".xflow-editor-runbar")).toBeNull();
  });

  it("lets the host override the dismissal delay, or keep the bar until closed", () => {
    vi.useFakeTimers();
    const onExecutionAction = vi.fn();
    const { rerender } = render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-1", status: "failed" }}
        onExecutionAction={onExecutionAction}
        runBarDismissMs={1000}
        value={workflow}
      />
    );

    expect(runBar().textContent).toContain("1s 后收起");
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(document.querySelector(".xflow-editor-runbar")?.getAttribute("data-leaving")).toBe("true");
    act(() => {
      vi.advanceTimersByTime(120);
    });
    expect(document.querySelector(".xflow-editor-runbar")).toBeNull();

    rerender(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-2", status: "failed" }}
        runBarDismissMs={null}
        value={workflow}
      />
    );
    act(() => {
      vi.advanceTimersByTime(60000);
    });
    expect(document.querySelector(".xflow-editor-runbar")).not.toBeNull();
    expect(runBar().textContent).not.toContain("后收起");
    fireEvent.click(within(runBar()).getByRole("button", { name: "关闭运行条" }));
    expect(document.querySelector(".xflow-editor-runbar")).toBeNull();
    // The float layer outlives the execution but holds no toast once dismissed.
    expect(document.querySelector(".xflow-editor-run-toast")?.childElementCount).toBe(0);
    // Closing is pure UI state: it sends no execution control and is not
    // remembered for the next run.
    expect(onExecutionAction).not.toHaveBeenCalled();
  });

  it("filters pause / resume behind executionControl and cancel behind execution", () => {
    const { rerender } = render(
      <XFlowEditor
        capabilities={{ ...allCapabilities, executionControl: false }}
        execution={runningExecution}
        value={workflow}
      />
    );
    expect(within(runBar()).queryByRole("button", { name: /暂停/ })).toBeNull();
    expect(within(runBar()).getByRole("button", { name: /取消/ })).toBeTruthy();

    rerender(
      <XFlowEditor
        capabilities={{ ...allCapabilities, execution: false }}
        execution={runningExecution}
        value={workflow}
      />
    );
    expect(within(runBar()).queryByRole("button", { name: /取消/ })).toBeNull();
    expect(within(runBar()).getByRole("button", { name: /暂停/ })).toBeTruthy();
  });

  it("reports a rejected host action in the diagnostics dock", async () => {
    const onExecutionAction = vi.fn().mockRejectedValue(new Error("暂停失败：执行已结束"));
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={runningExecution}
        onExecutionAction={onExecutionAction}
        value={workflow}
      />
    );

    fireEvent.click(within(runBar()).getByRole("button", { name: /暂停/ }));
    await waitFor(() => expect(screen.getByText(/暂停失败：执行已结束/)).toBeTruthy());
  });

  it("draws no bar without an execution prop", () => {
    render(<XFlowEditor capabilities={allCapabilities} value={workflow} />);
    expect(document.querySelector(".xflow-editor-runbar")).toBeNull();
  });
});

describe("run-record top bar lineage (RN-01)", () => {
  /** A sub-workflow child: it came from another workflow's parent execution. */
  const subflowChild: XFlowExecution = {
    executionId: "exec-child",
    status: "success",
    workflowId: "wf-child",
    parentExecutionId: "exec-parent",
    parentNode: "call_child",
    nodes: [{ name: "start", status: "success" }]
  };
  /** A rerun whose source execution the host has not wired for selection. */
  const rerunChild: XFlowExecution = {
    executionId: "exec-rerun",
    status: "running",
    parentExecutionId: "exec-parent",
    rerunFrom: "enrich_score",
    nodes: [{ name: "enrich_score", status: "running" }]
  };

  it("labels a subflow child with its parent execution, never as a link", () => {
    const onSelectExecution = vi.fn();
    render(
      <XFlowEditor
        capabilities={allCapabilities}
        mode="runs"
        onSelectExecution={onSelectExecution}
        selectedExecution={subflowChild}
        value={workflow}
      />
    );

    const lineage = screen.getByLabelText("当前执行").querySelector<HTMLElement>(".xflow-editor-runmeta__lineage");
    expect(lineage?.textContent).toBe("子流程 · 父执行 exec-parent");
    // The parent belongs to another workflow's record, so even with a select
    // handler the subflow lineage stays a label rather than a button.
    expect(lineage?.tagName).toBe("SPAN");
    expect(screen.queryByRole("button", { name: /子流程/ })).toBeNull();
    expect(onSelectExecution).not.toHaveBeenCalled();
  });

  it("renders the rerun lineage as a plain label when the host cannot select the source", () => {
    render(
      <XFlowEditor capabilities={allCapabilities} mode="runs" selectedExecution={rerunChild} value={workflow} />
    );

    const lineage = screen.getByLabelText("当前执行").querySelector<HTMLElement>(".xflow-editor-runmeta__lineage");
    expect(lineage?.textContent).toBe("重跑自 exec-parent · 起点 enrich_score");
    expect(lineage?.tagName).toBe("SPAN");
    expect(screen.queryByRole("button", { name: /重跑自/ })).toBeNull();
  });
});

describe("inspector 运行 tab", () => {
  const selectNode = (name: string): HTMLElement => {
    const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} node "]`);
    if (!node) throw new Error(`canvas node ${name} not found`);
    fireEvent.click(node);
    return screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
  };

  it("draws the status/attempt/duration kvlist over collapsible error and output cards", () => {
    const runtime: RuntimeSnapshot = {
      nodes: {
        approve: {
          status: "failed",
          attempts: 2,
          port: "rejected",
          error: "审批被拒绝",
          errorDetails: { code: "approval_rejected", at: "manager" },
          output: { approved: false },
          childExecutionId: "exec-sw-1"
        }
      }
    };
    const onOpenExecution = vi.fn();
    render(<XFlowEditor onOpenExecution={onOpenExecution} runtime={runtime} value={workflow} />);
    const panel = selectNode("approve");
    fireEvent.click(within(panel).getByRole("tab", { name: /运行/ }));
    const runPanel = panel.querySelector<HTMLElement>(".xflow-editor-run-panel");
    if (!runPanel) throw new Error("run panel not found");

    // The kvlist keeps every field the old list had.
    expect(within(runPanel).getByText("状态")).toBeTruthy();
    expect(within(runPanel).getByText("失败")).toBeTruthy();
    expect(within(runPanel).getByText("尝试")).toBeTruthy();
    expect(within(runPanel).getByText("2")).toBeTruthy();
    expect(within(runPanel).getByText("输出端口")).toBeTruthy();
    expect(within(runPanel).getByText("rejected")).toBeTruthy();
    const durationRow = within(runPanel).getByText("耗时").parentElement;
    expect(durationRow?.textContent).toContain("-");

    // 错误 and 输出 are composer Sections: a header toggle with a summary slot.
    const errorSection = within(runPanel).getByRole("button", { name: "错误" });
    const errorBody = errorSection.closest("section");
    expect(errorBody?.textContent).toContain("审批被拒绝");
    expect(errorBody?.textContent).toContain("code: approval_rejected");
    expect(errorBody?.textContent).toContain("错误详情为服务端脱敏后的扁平字段。");

    const outputSection = within(runPanel).getByRole("button", { name: "输出" });
    expect(outputSection.closest("section")?.textContent).toContain('"approved": false');

    const child = within(runPanel).getByRole("button", { name: "打开子执行" });
    fireEvent.click(child);
    expect(onOpenExecution).toHaveBeenCalledWith("exec-sw-1");
  });

  it("collapses the output card with a 无 summary when the node has no output", () => {
    const runtime: RuntimeSnapshot = { nodes: { approve: { status: "success" } } };
    render(<XFlowEditor runtime={runtime} value={workflow} />);
    const panel = selectNode("approve");
    fireEvent.click(within(panel).getByRole("tab", { name: /运行/ }));
    const runPanel = panel.querySelector<HTMLElement>(".xflow-editor-run-panel");
    if (!runPanel) throw new Error("run panel not found");

    const output = within(runPanel).getByRole("button", { name: "输出" });
    expect(output.getAttribute("aria-expanded")).toBe("false");
    expect(output.closest("section")?.querySelector(".xflow-composer-section-summary")?.textContent).toBe("无");
    // No error data: the error card is not drawn.
    expect(within(runPanel).queryByRole("button", { name: "错误" })).toBeNull();
    // Empty output writes nothing into the body.
    expect(within(runPanel).queryByText("{}")).toBeNull();
  });

  it("keeps 打开子执行 disabled when the host cannot open the child", () => {
    const runtime: RuntimeSnapshot = {
      nodes: { approve: { status: "success", childExecutionId: "exec-sw-2" } }
    };
    render(<XFlowEditor runtime={runtime} value={workflow} />);
    const panel = selectNode("approve");
    fireEvent.click(within(panel).getByRole("tab", { name: /运行/ }));

    expect((within(panel).getByRole("button", { name: "打开子执行" }) as HTMLButtonElement).disabled).toBe(true);
  });
});
