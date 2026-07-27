import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { XFlowPreview } from "./index";

describe("XFlowPreview", () => {
  it("renders workflow nodes with runtime status", () => {
    const { container } = render(
      <XFlowPreview
        workflow={{
          id: "wf-1",
          name: "Payment flow",
          nodes: [
            { name: "start", type: "xflow.start" },
            {
              name: "charge",
              type: "xflow.http",
              notes: "Calls payment gateway",
              inputs: [{ name: "main", required: true }]
            },
            { name: "approval", type: "xflow.wait", disabled: true }
          ],
          connections: {
            start: {
              main: [{ node: "charge" }]
            },
            charge: {
              main: [{ node: "approval" }]
            }
          }
        }}
        runtime={{
          status: "running",
          nodes: {
            start: { status: "success", durationMs: 12 },
            charge: { status: "running", attempts: 2 },
            approval: { status: "skipped" }
          }
        }}
      />
    );

    expect(screen.getByRole("region", { name: "Payment flow preview" })).toBeTruthy();
    expect(container.querySelector(".react-flow")).toBeTruthy();
    expect(screen.getByText("Payment flow")).toBeTruthy();
    expect(screen.getAllByText("running")).toHaveLength(2);
    expect(screen.getByText("3 nodes")).toBeTruthy();
    expect(screen.getByText("1 running")).toBeTruthy();
    expect(screen.getByText("1 success")).toBeTruthy();
    expect(screen.getByText("1 skipped")).toBeTruthy();
    expect(screen.getAllByText("start")).toHaveLength(2);
    expect(screen.getByText("success")).toBeTruthy();
    expect(screen.getAllByText("charge")).toHaveLength(3);
    expect(screen.getByLabelText("xflow.http icon")).toBeTruthy();
    expect(screen.getAllByText("action").length).toBeGreaterThan(0);
    expect(screen.getByText("input: main")).toBeTruthy();
    expect(screen.getByText("2 attempts")).toBeTruthy();
    expect(screen.getByLabelText("start main to charge main")).toBeTruthy();

    const chargeNode = container.querySelector('[aria-label="charge node running"]');
    expect(chargeNode).toBeTruthy();
    fireEvent.click(chargeNode!);

    expect(screen.getByRole("region", { name: "Selected node details" })).toBeTruthy();
    expect(screen.getByText("Calls payment gateway")).toBeTruthy();
    expect(screen.getByText("Inputs 1")).toBeTruthy();
    expect(screen.getByText("Attempts 2")).toBeTruthy();
  });

  it("shows an empty state when the workflow has no nodes", () => {
    render(<XFlowPreview workflow={{ name: "Empty flow", nodes: [] }} />);

    expect(screen.getByText("No nodes to preview")).toBeTruthy();
  });

  it("notifies the owner when a canvas node is selected", () => {
    const handleSelectNode = vi.fn();
    const { container } = render(
      <XFlowPreview
        workflow={{
          name: "Selectable flow",
          nodes: [
            { name: "start", type: "xflow.start" },
            { name: "http_1", type: "xflow.http" }
          ],
          connections: {
            start: {
              main: [{ node: "http_1" }]
            }
          }
        }}
        onSelectNode={handleSelectNode}
      />
    );

    const httpNode = container.querySelector('[aria-label="http_1 node pending"]');
    expect(httpNode).toBeTruthy();

    fireEvent.click(httpNode!);

    expect(handleSelectNode).toHaveBeenCalledWith("http_1");
  });
});
