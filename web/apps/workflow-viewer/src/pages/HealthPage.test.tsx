// @vitest-environment jsdom
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { createRuntimeConfig } from "../config/runtime";
import { HealthPage } from "./HealthPage";

describe("HealthPage", () => {
  it("renders app metadata and fixture nodes/connections", async () => {
    const config = createRuntimeConfig("Workflow Viewer", "0.1.0");
    render(<HealthPage config={config} />);

    expect(screen.getByText("Workflow Viewer")).toBeDefined();
    expect(screen.getByText(/Version: 0.1.0/)).toBeDefined();

    await waitFor(() => {
      expect(screen.getByTestId("fixture-nodes").children.length).toBe(2);
    });
    expect(screen.getByTestId("fixture-connections").children.length).toBe(1);
  });

  it("does not render fixture nodes/connections when mock is disabled", () => {
    const config = createRuntimeConfig("Workflow Viewer", "0.1.0");
    config.mockEnabled = false;
    render(<HealthPage config={config} />);

    expect(screen.getByText("Workflow Viewer")).toBeDefined();
    expect(screen.getByText(/Mock: disabled/)).toBeDefined();
    expect(screen.queryByTestId("fixture-nodes")).toBeNull();
    expect(screen.queryByTestId("fixture-connections")).toBeNull();
  });
});
