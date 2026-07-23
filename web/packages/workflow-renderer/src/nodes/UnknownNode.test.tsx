// @vitest-environment jsdom

import { describe, expect, it } from "vitest";
import { render } from "@testing-library/react";
import { ReactFlowProvider } from "@xyflow/react";
import type { NodeData } from "../types";
import { UnknownNode } from "./UnknownNode";

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

describe("UnknownNode", () => {
  it("renders a single default source Handle when sourcePorts is absent", () => {
    const { container } = renderWithProvider(
      <UnknownNode
        id="a"
        data={data({ nodeDef: { name: "a", type: "acme.unknown" } })}
      />
    );
    const ids = sourceHandleTestIds(container, "a");
    expect(ids).toHaveLength(1);
    expect(ids).toContain("source-handle-a-default");
  });

  it("renders one source Handle per named source port", () => {
    const { container } = renderWithProvider(
      <UnknownNode
        id="a"
        data={data({
          nodeDef: { name: "a", type: "acme.unknown" },
          sourcePorts: ["out", "alt"],
        })}
      />
    );
    const ids = sourceHandleTestIds(container, "a");
    expect(ids).toHaveLength(2);
    expect(ids).toContain("source-handle-a-out");
    expect(ids).toContain("source-handle-a-alt");
  });

  it("renders default source Handle alongside named source ports", () => {
    const { container } = renderWithProvider(
      <UnknownNode
        id="a"
        data={data({
          nodeDef: { name: "a", type: "acme.unknown" },
          sourcePorts: ["out"],
          hasDefaultSourcePort: true,
        })}
      />
    );
    const ids = sourceHandleTestIds(container, "a");
    expect(ids).toHaveLength(2);
    expect(ids).toContain("source-handle-a-out");
    expect(ids).toContain("source-handle-a-default");
  });

  it("renders one target Handle per named input", () => {
    const { container } = renderWithProvider(
      <UnknownNode
        id="a"
        data={data({
          nodeDef: {
            name: "a",
            type: "acme.unknown",
            inputs: [{ name: "value", required: true }],
          },
        })}
      />
    );
    const ids = targetHandleTestIds(container, "a");
    expect(ids).toHaveLength(1);
    expect(ids).toContain("target-handle-a-value");
  });
});
