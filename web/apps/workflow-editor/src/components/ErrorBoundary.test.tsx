// @vitest-environment jsdom
import { render, screen } from "@testing-library/react";
import { useState } from "react";
import { describe, expect, it } from "vitest";
import { ErrorBoundary } from "../components/ErrorBoundary";

function Bomb({ shouldThrow }: { shouldThrow: boolean }) {
  if (shouldThrow) {
    throw new Error("Intentional test error");
  }
  return <div>safe</div>;
}

function ToggleBomb() {
  const [shouldThrow, setShouldThrow] = useState(false);
  return (
    <div>
      <button onClick={() => setShouldThrow(true)}>Trigger</button>
      <Bomb shouldThrow={shouldThrow} />
    </div>
  );
}

describe("ErrorBoundary", () => {
  it("renders a generic fallback when a child throws", () => {
    render(
      <ErrorBoundary>
        <Bomb shouldThrow />
      </ErrorBoundary>
    );

    expect(screen.getByText(/Something went wrong/i)).toBeDefined();
    // Ensure no stack trace or error details leak into the UI.
    expect(screen.queryByText(/Intentional test error/i)).toBeNull();
  });

  it("renders children when there is no error", () => {
    render(
      <ErrorBoundary>
        <ToggleBomb />
      </ErrorBoundary>
    );

    expect(screen.getByText("safe")).toBeDefined();
  });
});
