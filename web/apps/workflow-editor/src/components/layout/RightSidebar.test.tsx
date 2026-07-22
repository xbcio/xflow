// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { useEffect } from "react";
import type { WorkflowDef } from "@xflow/workflow-core";
import { EditorProvider, useEditor } from "../../context/EditorContext";
import { RightSidebar } from "./RightSidebar";

function SelectionController({ nodeId }: { nodeId: string }) {
  const { selectNode } = useEditor();
  useEffect(() => {
    selectNode(nodeId);
  }, [selectNode, nodeId]);
  return null;
}

const fixtureDefinition: WorkflowDef = {
  id: "wf-1",
  name: "Test Workflow",
  nodes: [
    { id: "node-1", name: "Fetch", type: "http", kind: "action" },
    { id: "node-2", name: "Done", type: "end", kind: "action" },
  ],
};

afterEach(cleanup);

describe("RightSidebar", () => {
  it("renders the catalog and inspector panels", () => {
    render(
      <EditorProvider>
        <RightSidebar />
      </EditorProvider>
    );

    expect(screen.getByTestId("right-sidebar")).toBeDefined();
    expect(screen.getByTestId("node-catalog")).toBeDefined();
    expect(screen.getByTestId("node-inspector")).toBeDefined();
    expect(screen.getByTestId("right-sidebar-collapse")).toBeDefined();
  });

  it("passes the selected node to the inspector", () => {
    render(
      <EditorProvider initialDefinition={fixtureDefinition}>
        <SelectionController nodeId="node-1" />
        <RightSidebar />
      </EditorProvider>
    );

    expect(screen.getByTestId("inspector-name").textContent).toBe("Fetch");
    expect(screen.getByTestId("inspector-type").textContent).toBe("http");
  });

  it("collapses the sidebar when the collapse button is clicked", () => {
    render(
      <EditorProvider>
        <RightSidebar />
      </EditorProvider>
    );

    expect(screen.getByTestId("right-sidebar")).toBeDefined();
    const collapseButton = screen.getByTestId("right-sidebar-collapse");
    fireEvent.click(collapseButton);
    expect(screen.queryByTestId("right-sidebar")).toBeNull();
  });
});
