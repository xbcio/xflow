// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import type { WorkflowDef } from "@xflow/workflow-core";
import { EditorProvider } from "../../context/EditorContext";
import { LeftSidebar } from "./LeftSidebar";

const mockDefinition: WorkflowDef = {
  id: "wf-test",
  name: "test-workflow",
  nodes: [
    { id: "start", name: "Start", type: "start" },
    { id: "http", name: "HTTP Request", type: "http" },
  ],
};

afterEach(cleanup);

describe("LeftSidebar", () => {
  it("renders outline header and node list", () => {
    render(
      <EditorProvider initialDefinition={mockDefinition}>
        <LeftSidebar />
      </EditorProvider>,
    );

    expect(screen.getByTestId("left-sidebar")).toBeDefined();
    expect(screen.getByText("Outline")).toBeDefined();
    expect(screen.getByTestId("node-outline")).toBeDefined();
    expect(screen.getByText("Start")).toBeDefined();
    expect(screen.getByText("HTTP Request")).toBeDefined();
  });

  it("collapses when the collapse button is clicked", () => {
    render(
      <EditorProvider initialDefinition={mockDefinition}>
        <LeftSidebar />
      </EditorProvider>,
    );

    expect(screen.getByTestId("node-outline")).toBeDefined();

    const collapseButton = screen.getByTestId("left-sidebar-collapse-button");
    fireEvent.click(collapseButton);

    expect(screen.queryByTestId("node-outline")).toBeNull();
    expect(document.querySelector(".left-sidebar.collapsed")).not.toBeNull();
  });

  it("expands when the collapse button is clicked while collapsed", () => {
    render(
      <EditorProvider initialDefinition={mockDefinition}>
        <LeftSidebar />
      </EditorProvider>,
    );

    const collapseButton = screen.getByTestId("left-sidebar-collapse-button");
    fireEvent.click(collapseButton);
    fireEvent.click(collapseButton);

    expect(screen.getByTestId("node-outline")).toBeDefined();
    expect(document.querySelector(".left-sidebar.collapsed")).toBeNull();
  });

  it("selects a node when a row is clicked", () => {
    render(
      <EditorProvider initialDefinition={mockDefinition}>
        <LeftSidebar />
      </EditorProvider>,
    );

    fireEvent.click(screen.getByText("HTTP Request"));

    const selectedItem = document.querySelector(".node-outline-item.selected");
    expect(selectedItem).not.toBeNull();
    expect(selectedItem?.getAttribute("data-testid")).toBe(
      "node-outline-item-http",
    );
  });
});
