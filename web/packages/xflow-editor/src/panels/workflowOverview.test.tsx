import { fireEvent, render, screen, within } from "@testing-library/react";
import * as React from "react";
import { describe, expect, it, vi } from "vitest";
import type { WorkflowDef } from "@xflow/core";

import { WorkflowOverview, type WorkflowOverviewProps } from "./workflowOverview";
import type { WorkflowDiagnostic } from "./diagnostics";

const workflow: WorkflowDef = {
  id: "wf-overview",
  name: "Overview",
  version: "2.1.0",
  spec: "1.0",
  nodes: [
    { name: "start", type: "xflow.start", kind: "trigger", ui: { label: "定时触发" } },
    { name: "request", type: "xflow.http" },
    { name: "finish", type: "xflow.end" }
  ],
  connections: {
    start: { main: [{ node: "request" }] },
    request: { main: [{ node: "finish" }] }
  }
};

const diagnostics: WorkflowDiagnostic[] = [
  { status: "error", area: "connection", message: "连接目标不存在" },
  { status: "warn", area: "parameters", message: "缺少默认值" },
  { status: "pass", area: "workflow", message: "结构有效" }
];

const props = (): WorkflowOverviewProps => ({
  workflow,
  sectionCount: 2,
  diagnostics,
  version: "2.1.0",
  persisted: true,
  unsavedCount: 0
});

describe("WorkflowOverview (EL-13)", () => {
  it("states the counts, trigger, validation, version and save state the editor holds", () => {
    render(<WorkflowOverview {...props()} />);

    const overview = screen.getByRole("region", { name: "工作流概览" });
    const stats = [...overview.querySelectorAll(".xflow-editor-overview__stat-value")].map(
      (value) => value.textContent
    );
    expect(stats).toEqual(["3", "2", "2"]);
    expect(within(overview).getByText("定时触发")).toBeTruthy();
    expect(within(overview).getByText("1 错误 · 1 警告")).toBeTruthy();
    expect(overview.textContent).toContain("v2.1.0");
    expect(overview.textContent).toContain("已保存");
    // No run was supplied: the row stays absent rather than becoming a placeholder.
    expect(within(overview).queryByText("最近运行")).toBeNull();
  });

  it("jumps to the diagnostics dock and into the replay from the validation and run rows", () => {
    const onOpenDiagnostics = vi.fn();
    const onOpenRun = vi.fn();
    render(
      <WorkflowOverview
        {...props()}
        latestRun={{
          executionId: "exec-overview-1",
          status: "success",
          createdAt: "2026-01-02T03:04:05Z",
          updatedAt: "2026-01-02T03:04:15Z"
        }}
        onOpenDiagnostics={onOpenDiagnostics}
        onOpenRun={onOpenRun}
      />
    );

    const overview = screen.getByRole("region", { name: "工作流概览" });
    fireEvent.click(within(overview).getByRole("button", { name: /1 错误 · 1 警告/ }));
    expect(onOpenDiagnostics).toHaveBeenCalledTimes(1);

    const run = within(overview).getByTitle("在回放中打开 exec-overview-1");
    expect(run.textContent).toContain("成功");
    expect(run.textContent).toContain("10.0 s");
    expect(run.textContent).toContain("回放");
    fireEvent.click(run);
    expect(onOpenRun).toHaveBeenCalledWith("exec-overview-1");
  });

  it("counts unsaved edits even when the baseline has a version", () => {
    render(<WorkflowOverview {...props()} unsavedCount={3} />);
    expect(screen.getByText("3 处未保存")).toBeTruthy();
    expect(screen.queryByText("已保存")).toBeNull();
  });
});
