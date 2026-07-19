// @vitest-environment jsdom

import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { render } from "@testing-library/react";
import type { WorkflowDef } from "@xflow/workflow-core";
import { selectionOverlay, executionOverlay, diagnosticOverlay } from "./";

const definition: WorkflowDef = {
  name: "demo",
  nodes: [
    { name: "a", type: "xflow.start" },
    { name: "b", type: "xflow.end" },
  ],
  connections: {
    a: { default: [{ node: "ghost" }] },
  },
};

describe("selectionOverlay", () => {
  it("renders selected count and ids", () => {
    const { container } = render(
      selectionOverlay({ definition, selectedNodeIds: ["a", "b"] }) as ReactNode
    );
    expect(container.querySelector('[data-testid="selection-count"]')?.textContent).toBe("2");
    expect(container.querySelector('[data-testid="selection-names"]')?.textContent).toBe("a, b");
  });

  it("renders zero when nothing is selected", () => {
    const { container } = render(selectionOverlay({ definition, selectedNodeIds: [] }) as ReactNode);
    expect(container.querySelector('[data-testid="selection-count"]')?.textContent).toBe("0");
  });
});

describe("executionOverlay", () => {
  it("renders execution status breakdown", () => {
    const { container } = render(
      executionOverlay({
        definition,
        executionSnapshot: {
          nodeStatuses: {
            a: "completed",
            b: "failed",
          },
        },
      }) as ReactNode
    );
    expect(container.querySelector('[data-testid="execution-total"]')?.textContent).toBe("2");
    const breakdown = container.querySelector('[data-testid="execution-breakdown"]');
    expect(breakdown?.textContent).toContain("C:1");
    expect(breakdown?.textContent).toContain("F:1");
  });

  it("renders zero total when snapshot is empty", () => {
    const { container } = render(
      executionOverlay({ definition, executionSnapshot: {} }) as ReactNode
    );
    expect(container.querySelector('[data-testid="execution-total"]')?.textContent).toBe("0");
  });
});

describe("diagnosticOverlay", () => {
  it("renders diagnostic severity breakdown", () => {
    const { container } = render(
      diagnosticOverlay({
        definition,
        diagnostics: [
          { code: "E1", severity: "error", message: "bad" },
          { code: "W1", severity: "warning", message: "warn" },
          { code: "I1", severity: "info", message: "info" },
        ],
      }) as ReactNode
    );
    expect(container.querySelector('[data-testid="diagnostic-total"]')?.textContent).toBe("3");
    const breakdown = container.querySelector('[data-testid="diagnostic-breakdown"]');
    expect(breakdown?.textContent).toContain("E:1");
    expect(breakdown?.textContent).toContain("W:1");
    expect(breakdown?.textContent).toContain("I:1");
  });

  it("renders dangling targets with UnknownPort", () => {
    const { container } = render(diagnosticOverlay({ definition, diagnostics: [] }) as ReactNode);
    const list = container.querySelector('[data-testid="dangling-targets"]');
    expect(list).not.toBeNull();
    expect(list?.querySelector('[data-testid="port-default"]')).not.toBeNull();
    expect(list?.textContent).toContain("a → ghost");
  });
});
