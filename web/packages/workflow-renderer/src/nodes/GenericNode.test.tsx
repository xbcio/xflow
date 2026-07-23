// @vitest-environment jsdom

import { describe, expect, it } from "vitest";
import { render } from "@testing-library/react";
import { ReactFlowProvider } from "@xyflow/react";
import type { NodeData } from "../types";
import { GenericNode } from "./GenericNode";

function data(partial: Partial<NodeData> & { nodeDef: NodeData["nodeDef"] }): NodeData {
  return { ...partial };
}

function renderWithProvider(element: React.ReactElement) {
  return render(<ReactFlowProvider>{element}</ReactFlowProvider>);
}

function sourceHandleTestIds(container: HTMLElement, nodeId: string): string[] {
  return Array.from(
    container.querySelectorAll(`[data-testid^="source-handle-${nodeId}-"]`)
  ).map((el) => el.getAttribute("data-testid") ?? "");
}

function targetHandleTestIds(container: HTMLElement, nodeId: string): string[] {
  return Array.from(
    container.querySelectorAll(`[data-testid^="target-handle-${nodeId}-"]`)
  ).map((el) => el.getAttribute("data-testid") ?? "");
}

describe("GenericNode", () => {
  it("renders a single default source Handle when sourcePorts is absent", () => {
    const { container } = renderWithProvider(
      <GenericNode
        id="a"
        data={data({ nodeDef: { name: "a", type: "xflow.start" } })}
      />
    );
    const ids = sourceHandleTestIds(container, "a");
    expect(ids).toHaveLength(1);
    expect(ids).toContain("source-handle-a-default");
  });

  it("renders one source Handle per named source port", () => {
    const { container } = renderWithProvider(
      <GenericNode
        id="a"
        data={data({
          nodeDef: { name: "a", type: "xflow.if" },
          sourcePorts: ["yes", "no"],
        })}
      />
    );
    const ids = sourceHandleTestIds(container, "a");
    expect(ids).toHaveLength(2);
    expect(ids).toContain("source-handle-a-yes");
    expect(ids).toContain("source-handle-a-no");
  });

  it("renders default source Handle alongside named source ports", () => {
    const { container } = renderWithProvider(
      <GenericNode
        id="a"
        data={data({
          nodeDef: { name: "a", type: "xflow.if" },
          sourcePorts: ["approved"],
          hasDefaultSourcePort: true,
        })}
      />
    );
    const ids = sourceHandleTestIds(container, "a");
    expect(ids).toHaveLength(2);
    expect(ids).toContain("source-handle-a-approved");
    expect(ids).toContain("source-handle-a-default");
  });

  it("renders one target Handle per named input", () => {
    const { container } = renderWithProvider(
      <GenericNode
        id="a"
        data={data({
          nodeDef: {
            name: "a",
            type: "xflow.log",
            inputs: [{ name: "message", required: true }],
          },
        })}
      />
    );
    const ids = targetHandleTestIds(container, "a");
    expect(ids).toHaveLength(1);
    expect(ids).toContain("target-handle-a-message");
  });

  it("renders a single default target Handle when inputs are unnamed", () => {
    const { container } = renderWithProvider(
      <GenericNode
        id="a"
        data={data({ nodeDef: { name: "a", type: "xflow.start" } })}
      />
    );
    const ids = targetHandleTestIds(container, "a");
    expect(ids).toHaveLength(1);
    expect(ids).toContain("target-handle-a-default");
  });
});
