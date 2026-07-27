import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { App } from "./App";

describe("xflow-admin App", () => {
  it("creates and runs a workflow from the admin workbench", async () => {
    render(<App />);

    expect(await screen.findByRole("region", { name: "Dashboard" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "概览" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "工作流" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Runner" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "当前用户" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "工作流" }));

    const workflowPage = await screen.findByRole("region", { name: "工作流管理" });
    expect(within(workflowPage).getByText("purchase-approval")).toBeTruthy();
    expect(within(workflowPage).getByText("incident-triage")).toBeTruthy();

    fireEvent.click(within(workflowPage).getByRole("button", { name: "新建工作流" }));

    await waitFor(() => {
      expect(screen.getByRole("region", { name: "工作流编辑器" })).toBeTruthy();
    });

    fireEvent.click(screen.getByRole("button", { name: "运行工作流" }));

    await waitFor(() => {
      expect(screen.getByText("运行完成")).toBeTruthy();
    });
    const debugDialog = screen.getByRole("dialog", { name: /调试/ });
    expect(debugDialog).toBeTruthy();
    expect(within(debugDialog).getByText("输出")).toBeTruthy();
    expect(within(debugDialog).getByText(/Node executed successfully/)).toBeTruthy();
  });
});
