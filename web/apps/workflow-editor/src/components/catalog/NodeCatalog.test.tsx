// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { EditorProvider } from "../../context/EditorContext";
import { NodeCatalog } from "./NodeCatalog";
import { DEFAULT_CATALOG } from "../../utils/catalog";

afterEach(cleanup);

describe("NodeCatalog", () => {
  it("renders catalog items grouped by category", () => {
    render(
      <EditorProvider>
        <NodeCatalog />
      </EditorProvider>
    );

    expect(screen.getByTestId("node-catalog")).toBeDefined();
    expect(screen.getByTestId("catalog-search")).toBeDefined();

    for (const item of DEFAULT_CATALOG) {
      expect(screen.getByTestId(`catalog-item-${item.type}`)).toBeDefined();
    }
  });

  it("filters catalog items by keyword", () => {
    render(
      <EditorProvider>
        <NodeCatalog />
      </EditorProvider>
    );

    const searchInput = screen.getByTestId("catalog-search");
    fireEvent.change(searchInput, { target: { value: "http" } });

    expect(screen.getByTestId("catalog-item-http")).toBeDefined();
    expect(screen.queryByTestId("catalog-item-grpc")).toBeNull();
    expect(screen.queryByTestId("catalog-item-start")).toBeNull();
  });

  it("shows empty state when no items match", () => {
    render(
      <EditorProvider>
        <NodeCatalog />
      </EditorProvider>
    );

    const searchInput = screen.getByTestId("catalog-search");
    fireEvent.change(searchInput, { target: { value: "nonexistent" } });

    expect(screen.getByTestId("catalog-empty")).toBeDefined();
    expect(screen.queryByTestId("catalog-item-http")).toBeNull();
  });
});
