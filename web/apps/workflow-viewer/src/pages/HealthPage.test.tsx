// @vitest-environment jsdom
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { createRuntimeConfig } from "../config/runtime";
import { HealthPage } from "./HealthPage";

describe("HealthPage", () => {
  it("renders app metadata and fixture nodes/connections", () => {
    const config = createRuntimeConfig("Workflow Viewer", "0.1.0");
    render(<HealthPage config={config} />);

    expect(screen.getByText("Workflow Viewer")).toBeDefined();
    expect(screen.getByText(/Version: 0.1.0/)).toBeDefined();
    expect(screen.getByTestId("fixture-nodes").children).toHaveLength(2);
    expect(screen.getByTestId("fixture-connections").children).toHaveLength(1);
  });
});
