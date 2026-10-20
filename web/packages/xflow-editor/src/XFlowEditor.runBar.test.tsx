import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { RuntimeSnapshot, WorkflowDef, XFlowEditorCapabilities } from "@xflow/core";
import { XFlowEditor, type XFlowExecution } from "./index";

// jsdom has no layout: the editor learns its width from a ResizeObserver, and
// the run-bar stacking test below drives the DOM rects directly. Seeding 1280
// keeps every other test on the same wide layout a 0 width already produced.
const originalResizeObserver = globalThis.ResizeObserver;
class WidthResizeObserver {
  private readonly callback: ResizeObserverCallback;
  constructor(callback: ResizeObserverCallback) {
    this.callback = callback;
  }
  observe(target: Element): void {
    this.callback(
      [{ target, contentRect: { width: 1280 } } as ResizeObserverEntry],
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
    expect(runBar().textContent).toContain("exec-8432");
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
    expect(idText?.textContent).toBe("exec-8432");
    expect(copy).not.toBeNull();
    expect(idText?.contains(copy)).toBe(false);
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
    expect(type).toContain("fail · xflow.end");
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

  it("drops below the preview float row when the bar would cover it", () => {
    const { rerender } = render(
      <XFlowEditor capabilities={allCapabilities} execution={runningExecution} value={workflow} />
    );
    const bar = document.querySelector<HTMLElement>(".xflow-editor-runbar")!;
    const canvas = document.querySelector<HTMLElement>(".xflow-editor-preview-frame")!;
    const float = document.querySelector<HTMLElement>(".xflow-preview-canvas-float")!;
    // A 500px bar centred in a 580px canvas starts at 40 and would reach the
    // 添加节点 float (its right edge at 153); 1600px leaves it clear.
    Object.defineProperty(bar, "clientWidth", { configurable: true, value: 500 });
    canvas.getBoundingClientRect = () => ({ left: 0, width: 580 } as DOMRect);
    float.getBoundingClientRect = () => ({ right: 153 } as DOMRect);

    rerender(
      <XFlowEditor capabilities={allCapabilities} execution={{ ...runningExecution }} value={workflow} />
    );
    const row = () => document.querySelector(".xflow-editor-runbar-row");
    expect(row()?.getAttribute("data-stacked")).toBe("true");

    canvas.getBoundingClientRect = () => ({ left: 0, width: 1600 } as DOMRect);
    rerender(
      <XFlowEditor capabilities={allCapabilities} execution={{ ...runningExecution }} value={workflow} />
    );
    expect(row()?.getAttribute("data-stacked")).toBeNull();
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
    expect(document.querySelector(".xflow-editor-runbar")).toBeNull();
  });

  it("lets the host override the dismissal delay, or keep the bar until closed", () => {
    vi.useFakeTimers();
    const { rerender } = render(
      <XFlowEditor
        capabilities={allCapabilities}
        execution={{ executionId: "exec-1", status: "failed" }}
        runBarDismissMs={1000}
        value={workflow}
      />
    );

    expect(runBar().textContent).toContain("1s 后收起");
    act(() => {
      vi.advanceTimersByTime(1000);
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

describe("inspector 运行 tab", () => {
  const selectNode = (name: string): HTMLElement => {
    const node = document.querySelector<HTMLElement>(`.xflow-preview-node__select[aria-label^="${name} node "]`);
    if (!node) throw new Error(`canvas node ${name} not found`);
    fireEvent.click(node);
    return screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
  };

  it("shows the port, flat error details, output JSON, the child-execution link and a dash for duration", () => {
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

    expect(within(panel).getByText("输出端口")).toBeTruthy();
    expect(within(panel).getByText("rejected")).toBeTruthy();
    expect(within(panel).getByText("错误详情")).toBeTruthy();
    expect(within(panel).getByText("code")).toBeTruthy();
    expect(within(panel).getByText("approval_rejected")).toBeTruthy();
    expect(within(panel).getByText("输出")).toBeTruthy();
    expect(within(panel).getByText(/"approved": false/)).toBeTruthy();
    const durationRow = within(panel).getByText("耗时").parentElement;
    expect(durationRow?.textContent).toContain("-");

    const child = within(panel).getByRole("button", { name: "打开子执行" });
    fireEvent.click(child);
    expect(onOpenExecution).toHaveBeenCalledWith("exec-sw-1");
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
