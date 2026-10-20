import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import {
  EXECUTION_LIST_PAGE_SIZE,
  ExecutionListPanel,
  executionClock,
  executionDuration,
  executionRangeStart,
  type XFlowExecutionListItem,
  type XFlowExecutionListState
} from "./executionList";

const startedAt = new Date(2026, 0, 2, 3, 4, 5);

function row(overrides: Partial<XFlowExecutionListItem> = {}): XFlowExecutionListItem {
  return {
    executionId: "exec-1",
    status: "success",
    error: "",
    createdAt: startedAt.toISOString(),
    updatedAt: new Date(startedAt.getTime() + 4100).toISOString(),
    workflowName: "Purchase approval",
    ...overrides
  };
}

function state(list: XFlowExecutionListItem[], total = list.length): XFlowExecutionListState {
  return { list, total };
}

describe("executionRangeStart / executionClock / executionDuration", () => {
  it("turns a range into an exclusive RFC3339 lower bound", () => {
    const now = Date.parse("2026-01-02T03:04:05.000Z");
    expect(executionRangeStart("24h", now)).toBe("2026-01-01T03:04:05.000Z");
    expect(executionRangeStart("7d", now)).toBe("2025-12-26T03:04:05.000Z");
    expect(executionRangeStart("all", now)).toBeUndefined();
  });

  it("draws HH:mm:ss in the viewer's zone and passes garbage through", () => {
    expect(executionClock(startedAt.toISOString())).toBe("03:04:05");
    expect(executionClock("not-a-date")).toBe("not-a-date");
  });

  it("sizes only terminal runs, from created to updated", () => {
    expect(executionDuration(row())).toBe("4.1 s");
    expect(executionDuration(row({ status: "running" }))).toBeUndefined();
    expect(executionDuration(row({
      createdAt: startedAt.toISOString(),
      updatedAt: new Date(startedAt.getTime() + 500).toISOString()
    }))).toBe("500 ms");
    expect(executionDuration(row({
      createdAt: startedAt.toISOString(),
      updatedAt: new Date(startedAt.getTime() + 90_000).toISOString()
    }))).toBe("1m 30s");
    expect(executionDuration(row({
      createdAt: startedAt.toISOString(),
      updatedAt: new Date(startedAt.getTime() - 1).toISOString()
    }))).toBeUndefined();
  });
});

describe("ExecutionListPanel (§6.8)", () => {
  it("asks for page 1 of this workflow's last 24 hours on mount", () => {
    const onLoad = vi.fn();
    render(<ExecutionListPanel state={state([])} onLoad={onLoad} />);

    expect(screen.getByRole("region", { name: "执行" })).toBeTruthy();
    expect(onLoad).toHaveBeenCalledTimes(1);
    const query = onLoad.mock.calls[0][0];
    expect(query).toMatchObject({ page: 1, pageSize: EXECUTION_LIST_PAGE_SIZE });
    expect(query.status).toBeUndefined();
    const windowMs = Date.parse(query.createdAfter);
    expect(Number.isFinite(windowMs)).toBe(true);
    expect(Math.abs(Date.now() - windowMs - 24 * 60 * 60 * 1000)).toBeLessThan(5_000);
  });

  it("narrows the server request by status and resets to page 1", () => {
    const onLoad = vi.fn();
    render(<ExecutionListPanel state={state([])} onLoad={onLoad} />);

    fireEvent.click(screen.getByRole("radio", { name: "失败" }));

    expect(onLoad).toHaveBeenCalledTimes(2);
    expect(onLoad.mock.calls[1][0]).toMatchObject({ page: 1, status: "failed" });
  });

  it("maps the range menu onto created_after rather than filtering rows locally", () => {
    const onLoad = vi.fn();
    render(<ExecutionListPanel state={state([row()])} onLoad={onLoad} />);

    fireEvent.click(screen.getByRole("button", { name: /最近 24 小时/ }));
    fireEvent.click(screen.getByRole("menuitem", { name: "全部时间" }));

    expect(onLoad).toHaveBeenCalledTimes(2);
    expect(onLoad.mock.calls[1][0].createdAfter).toBeUndefined();
  });

  it("draws id, status, time, duration and the first error line", () => {
    render(
      <ExecutionListPanel
        selectedId="exec-1"
        state={state([row({ error: "upstream unavailable\n at gateway.go:42" })])}
        onLoad={vi.fn()}
      />
    );

    const region = screen.getByRole("region", { name: "执行" });
    expect(within(region).getByText("exec-1")).toBeTruthy();
    expect(within(region).getByText("成功")).toBeTruthy();
    const sub = region.querySelector(".xflow-editor-exec-item__sub")?.textContent ?? "";
    expect(sub).toContain("03:04:05");
    expect(sub).toContain("4.1 s");
    expect(sub).toContain("upstream unavailable");
    expect(sub).not.toContain("gateway.go");
    expect(region.querySelector(".xflow-editor-exec-item__button")?.getAttribute("aria-current")).toBe("true");
  });

  it("puts the queue position on the status chip, as §4's 排队 label does", () => {
    render(<ExecutionListPanel state={state([row({ status: "queued", queuePosition: 3 })])} onLoad={vi.fn()} />);

    const region = screen.getByRole("region", { name: "执行" });
    const chip = region.querySelector(".xflow-editor-chip");
    expect(chip?.textContent).toContain("排队 · 第 3 位");
    expect(region.querySelector(".xflow-editor-exec-item__sub")?.textContent).not.toContain("排队");
  });

  it("does not claim an empty result before the host has answered", () => {
    render(<ExecutionListPanel onLoad={vi.fn()} />);

    expect(screen.queryByText("没有匹配的执行。")).toBeNull();
    expect(screen.getByText("正在加载…")).toBeTruthy();
  });

  it("shows the empty state when there is no host loader to wait for", () => {
    render(<ExecutionListPanel />);

    expect(screen.getByText("没有匹配的执行。")).toBeTruthy();
    expect(screen.queryByText("正在加载…")).toBeNull();
  });

  it("shows the first row without a selection and keeps 还有 N 条 scrollable", () => {
    render(<ExecutionListPanel state={state([row()], 42)} onLoad={vi.fn()} />);

    expect(screen.queryByText("没有匹配的执行。")).toBeNull();
    expect(screen.getByText("还有 41 条")).toBeTruthy();
    const first = screen.getByText("exec-1").closest("button");
    expect(first?.getAttribute("aria-current")).toBeNull();
  });

  it("selects a row through the host callback", () => {
    const onSelect = vi.fn();
    render(
      <ExecutionListPanel
        selectedId="exec-1"
        state={state([row()])}
        onLoad={vi.fn()}
        onSelect={onSelect}
      />
    );

    const button = screen.getByText("exec-1").closest("button");
    if (!button) throw new Error("execution row button not found");
    expect(button.getAttribute("aria-current")).toBe("true");
    fireEvent.click(button);
    expect(onSelect).toHaveBeenCalledWith("exec-1");
  });

  it("requests the next page only when the scrolled rows near their end", () => {
    const onLoad = vi.fn();
    const { container } = render(<ExecutionListPanel state={state([row()], 41)} onLoad={onLoad} />);
    const rows = container.querySelector(".xflow-editor-exec-rows") as HTMLDivElement;
    Object.defineProperty(rows, "scrollHeight", { configurable: true, value: 400 });
    Object.defineProperty(rows, "clientHeight", { configurable: true, value: 300 });

    rows.scrollTop = 10;
    fireEvent.scroll(rows);
    expect(onLoad).toHaveBeenCalledTimes(1);

    rows.scrollTop = 80;
    fireEvent.scroll(rows);
    expect(onLoad).toHaveBeenCalledTimes(2);
    expect(onLoad.mock.calls[1][0]).toMatchObject({ page: 2, pageSize: EXECUTION_LIST_PAGE_SIZE });
  });

  it("keeps the loaded rows on a failed page and re-asks for it on 重试", () => {
    const onLoad = vi.fn();
    render(
      <ExecutionListPanel
        state={{ list: [row()], total: 1, error: "列表加载失败" }}
        onLoad={onLoad}
      />
    );

    expect(screen.getByText("exec-1")).toBeTruthy();
    expect(screen.getByRole("alert").textContent).toContain("列表加载失败");
    fireEvent.click(screen.getByRole("button", { name: "重试" }));
    expect(onLoad).toHaveBeenCalledTimes(2);
    expect(onLoad.mock.calls[1][0]).toMatchObject({ page: 1 });
  });

  it("does not skip a failed page when the list is scrolled again", () => {
    const onLoad = vi.fn();
    const { container, rerender } = render(
      <ExecutionListPanel state={state([row()], 41)} onLoad={onLoad} />
    );
    const rows = container.querySelector(".xflow-editor-exec-rows") as HTMLDivElement;
    Object.defineProperty(rows, "scrollHeight", { configurable: true, value: 400 });
    Object.defineProperty(rows, "clientHeight", { configurable: true, value: 300 });

    rows.scrollTop = 80;
    fireEvent.scroll(rows);
    expect(onLoad).toHaveBeenCalledTimes(2);
    expect(onLoad.mock.calls[1][0]).toMatchObject({ page: 2 });

    // Page 2 failed: scrolling again must not ask for page 3, which would
    // leave the failed page permanently missing from the list.
    rerender(
      <ExecutionListPanel
        state={{ list: [row()], total: 41, error: "第 2 页失败" }}
        onLoad={onLoad}
      />
    );
    rows.scrollTop = 90;
    fireEvent.scroll(rows);
    expect(onLoad).toHaveBeenCalledTimes(2);
    // The banner owns the next step; "还有 N 条" would promise a scroll that
    // can no longer advance past the failed page.
    expect(screen.queryByText("还有 40 条")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "重试" }));
    expect(onLoad).toHaveBeenCalledTimes(3);
    expect(onLoad.mock.calls[2][0]).toMatchObject({ page: 2 });
  });
});
