// @vitest-environment jsdom
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { createRuntimeConfig } from "../config/runtime";
import { EditorPage } from "./EditorPage";

vi.mock("../mocks", () => ({
  loadMockWorkflow: vi.fn().mockResolvedValue({
    id: "wf-test",
    namespace: "default",
    name: "test-workflow",
    version: "1.0.0",
    description: "Test fixture",
    spec: "1.0",
    nodes: [],
    connections: {},
  }),
}));

describe("EditorPage", () => {
  it("does not render fixture-json or placeholder text when mock is disabled", () => {
    const config = createRuntimeConfig("Workflow Editor", "0.1.0");
    config.mockEnabled = false;
    render(<EditorPage config={config} />);

    expect(screen.queryByTestId("fixture-json")).toBeNull();
    expect(
      screen.queryByText(/read-only placeholder editor/i),
    ).toBeNull();
    expect(screen.getByTestId("empty-state")).toBeDefined();
  });

  it("renders fixture summary when mock is enabled", async () => {
    const config = createRuntimeConfig("Workflow Editor", "0.1.0");
    config.mockEnabled = true;
    render(<EditorPage config={config} />);

    await waitFor(() => {
      expect(screen.getByTestId("fixture-summary")).toBeDefined();
    });
    expect(screen.getByText(/test-workflow/)).toBeDefined();
    expect(screen.queryByTestId("fixture-json")).toBeNull();
  });
});
