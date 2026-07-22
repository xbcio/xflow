// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { useEffect, type ReactNode } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { EditorProvider, useEditor } from "../../context/EditorContext";
import { BottomPanel } from "./BottomPanel";
import type { Diagnostic } from "@xflow/workflow-core";

const diagnostics: Diagnostic[] = [
  {
    code: "ERR_MISSING_INPUT",
    severity: "error",
    message: "Missing required input",
    nodeId: "node-1",
  },
  {
    code: "WARN_DEPRECATED_PORT",
    severity: "warning",
    message: "Port is deprecated",
    nodeId: "node-2",
  },
  {
    code: "INFO_OPTIMIZATION",
    severity: "info",
    message: "Optimization available",
  },
];

function DiagnosticsSetter({
  children,
  diagnostics,
}: {
  children: ReactNode;
  diagnostics?: Diagnostic[];
}) {
  const { setDiagnostics } = useEditor();

  useEffect(() => {
    setDiagnostics(diagnostics ?? []);
  }, [diagnostics, setDiagnostics]);

  return children;
}

afterEach(cleanup);

describe("BottomPanel", () => {
  it("renders with diagnostics and collapse toggle", () => {
    render(
      <EditorProvider initialDefinition={{ name: "test" }}>
        <DiagnosticsSetter diagnostics={diagnostics}>
          <BottomPanel />
        </DiagnosticsSetter>
      </EditorProvider>,
    );

    expect(screen.getByText("Diagnostics / Logs")).toBeDefined();
    expect(screen.getByTestId("diag-count-error").textContent).toBe("1 errors");
    expect(screen.getByTestId("diag-count-warning").textContent).toBe("1 warnings");
    expect(screen.getByTestId("diag-count-info").textContent).toBe("1 info");

    const items = screen.getAllByTestId("diagnostics-item");
    expect(items).toHaveLength(3);
    expect(items[0].textContent).toContain("ERR_MISSING_INPUT");
    expect(items[0].textContent).toContain("Missing required input");
    expect(items[0].textContent).toContain("node-1");

    expect(screen.getByTestId("execution-log")).toBeDefined();

    const collapse = screen.getByTestId("bottom-panel-collapse");
    expect(collapse).toBeDefined();
    expect(collapse.getAttribute("aria-label")).toBe("Collapse panel");
  });

  it("toggles collapse when clicked", () => {
    render(
      <EditorProvider initialDefinition={{ name: "test" }}>
        <BottomPanel />
      </EditorProvider>,
    );

    const collapse = screen.getByTestId("bottom-panel-collapse");
    expect(collapse.getAttribute("aria-expanded")).toBe("false");

    fireEvent.click(collapse);

    expect(collapse.getAttribute("aria-expanded")).toBe("true");
  });

  it("renders empty state when no diagnostics", () => {
    render(
      <EditorProvider initialDefinition={{ name: "test" }}>
        <BottomPanel />
      </EditorProvider>,
    );

    expect(screen.getByTestId("diagnostics-empty")).toBeDefined();
    expect(screen.queryByTestId("diagnostics-item")).toBeNull();
    expect(screen.getByTestId("diag-count-error").textContent).toBe("0 errors");
  });
});
