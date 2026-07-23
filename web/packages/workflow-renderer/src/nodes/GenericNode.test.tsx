// @vitest-environment jsdom

import { describe, expect, it } from "vitest";
import { render } from "@testing-library/react";
import { ReactFlowProvider } from "@xyflow/react";
import type { NodeData } from "../types";
import { GenericNode } from "./GenericNode";

function data(partial: Partial<NodeData> & { nodeDef: NodeData["nodeDef"] }): NodeData {
  return {
    ...partial,
    nodeDef: partial.nodeDef,
  };
}

function renderWithProvider(element: React.ReactElement) {
  return render(<ReactFlowProvider>{element}</ReactFlowProvider>);
}

describe("GenericNode", () => {
  it("renders a single default source Handle when sourcePorts is absent", () => {
    const { container } = renderWithProvider(
      <GenericNode
        id="a"
        data={data({ nodeDef: { name: "a", type: "xflow.start" } })}
      />
    );
    const handles = container.querySelectorAll(
      '.react-flow__handle[data-handlepos="right"]'
    );
    expect(handles).toHaveLength(1);
    expect(handles[0].getAttribute("data-handleid")).toBeNull();
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
    const handles = container.querySelectorAll(
      '.react-flow__handle[data-handlepos="right"]'
    );
    expect(handles).toHaveLength(2);
    const ids = Array.from(handles).map((h) =>
      h.getAttribute("data-handleid")
    );
    expect(ids).toContain("yes");
    expect(ids).toContain("no");
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
    const handles = container.querySelectorAll(
      '.react-flow__handle[data-handlepos="left"]'
    );
    expect(handles).toHaveLength(1);
    expect(handles[0].getAttribute("data-handleid")).toBe("message");
  });

  it("renders a single default target Handle when inputs are unnamed", () => {
    const { container } = renderWithProvider(
      <GenericNode
        id="a"
        data={data({ nodeDef: { name: "a", type: "xflow.start" } })}
      />
    );
    const handles = container.querySelectorAll(
      '.react-flow__handle[data-handlepos="left"]'
    );
    expect(handles).toHaveLength(1);
    expect(handles[0].getAttribute("data-handleid")).toBeNull();
  });
});
