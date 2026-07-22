// @vitest-environment jsdom
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { createRuntimeConfig } from "../config/runtime";
import { HealthPage } from "./HealthPage";

afterEach(cleanup);

describe("HealthPage", () => {
  it("renders app metadata and fixture nodes/connections", async () => {
    const config = createRuntimeConfig("Workflow Editor", "0.1.0");
    render(<HealthPage config={config} />);

    expect(screen.getByText("Workflow Editor")).toBeDefined();
    expect(screen.getByText(/Version: 0.1.0/)).toBeDefined();

    await waitFor(() => {
      expect(screen.getByTestId("fixture-nodes").children.length).toBe(7);
    });
    expect(screen.getByTestId("fixture-connections").children.length).toBe(7);
  });

  it("does not render fixture nodes/connections when mock is disabled", () => {
    const config = createRuntimeConfig("Workflow Editor", "0.1.0");
    config.mockEnabled = false;
    render(<HealthPage config={config} />);

    expect(screen.getByText("Workflow Editor")).toBeDefined();
    expect(screen.getByText(/Mock: disabled/)).toBeDefined();
    expect(screen.queryByTestId("fixture-nodes")).toBeNull();
    expect(screen.queryByTestId("fixture-connections")).toBeNull();
  });
});
