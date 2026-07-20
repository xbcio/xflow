import { test, expect } from "playwright/test";

test.describe("Workflow Viewer", () => {
  test("health page renders app info and fixture", async ({ page }) => {
    await page.goto("http://localhost:5174/");

    await expect(
      page.getByRole("heading", { name: "Workflow Viewer" }),
    ).toBeVisible();
    await expect(page.getByText("Version: 0.1.0")).toBeVisible();
    await expect(page.getByText("Environment: development")).toBeVisible();
    await expect(page.getByText("Mock: enabled")).toBeVisible();
    await expect(
      page.getByTestId("fixture-nodes"),
    ).toContainText("start (xflow.start) — trigger");
    await expect(
      page.getByTestId("fixture-nodes"),
    ).toContainText("end (xflow.end) — action");
    await expect(
      page.getByTestId("fixture-connections"),
    ).toContainText("start:default → end:default");
  });

  test("navigates to viewer workflow page and renders the canvas", async ({ page }) => {
    await page.goto("http://localhost:5174/view/some-workflow-id");

    await expect(
      page.getByRole("heading", { name: "Viewer: some-workflow-id" }),
    ).toBeVisible();
    await expect(page.getByTestId("viewer-page")).toBeVisible();

    const canvas = page.locator(".xflow-root");
    await expect(canvas).toBeVisible();
    await expect(canvas.locator(".react-flow__node").first()).toBeVisible();
    await expect(page.getByTestId("fixture-json")).toHaveCount(0);
  });

  test("renders at least one edge and emits no React Flow handle warnings", async ({ page }) => {
    const warnings: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() === "warning" || msg.type() === "error") {
        warnings.push(msg.text());
      }
    });

    await page.goto("http://localhost:5174/view/some-workflow-id");
    await expect(page.locator(".xflow-root")).toBeVisible();

    // The static fixture has a start → end connection; at least one edge
    // path must be present in the DOM.
    await expect(page.locator(".react-flow__edge")).toHaveCount(1);

    // Give React Flow a tick to finish initializing edges and emit any
    // handle-mismatch warnings it would emit.
    await page.waitForTimeout(500);

    const handleWarnings = warnings.filter((text) =>
      /couldn't create edge|handle not found|Couldn't create edge|Handle not found/i.test(text),
    );
    expect(handleWarnings).toEqual([]);
  });

  test("overlay panels do not overlap the search input", async ({ page }) => {
    await page.goto("http://localhost:5174/view/some-workflow-id");
    await expect(page.locator(".xflow-root")).toBeVisible();

    const searchInput = page.getByLabel("Search nodes");
    await expect(searchInput).toBeVisible();
    const searchBox = await searchInput.boundingBox();
    expect(searchBox).not.toBeNull();

    const selectionOverlay = page.locator('[data-testid="selection-overlay"]');
    if ((await selectionOverlay.count()) > 0) {
      const overlayBox = await selectionOverlay.boundingBox();
      expect(overlayBox).not.toBeNull();
      // The selection overlay must not vertically overlap the search input.
      const verticallyOverlapping =
        overlayBox!.y < searchBox!.y + searchBox!.height &&
        overlayBox!.y + overlayBox!.height > searchBox!.y;
      expect(verticallyOverlapping).toBe(false);
    }

    const diagnosticOverlay = page.locator('[data-testid="diagnostic-overlay"]');
    if ((await diagnosticOverlay.count()) > 0) {
      const diagBox = await diagnosticOverlay.boundingBox();
      expect(diagBox).not.toBeNull();
      // Diagnostic overlay sits at the bottom-right or top-right but must not
      // overlap the search input (top-left).
      const horizontallyOverlapping =
        diagBox!.x < searchBox!.x + searchBox!.width &&
        diagBox!.x + diagBox!.width > searchBox!.x;
      const verticallyOverlapping =
        diagBox!.y < searchBox!.y + searchBox!.height &&
        diagBox!.y + diagBox!.height > searchBox!.y;
      expect(horizontallyOverlapping && verticallyOverlapping).toBe(false);
    }
  });

  test("error boundary catches render errors without leaking stack traces", async ({
    page,
  }) => {
    await page.goto("http://localhost:5174/__error");

    await expect(
      page.getByRole("heading", { name: "Something went wrong" }),
    ).toBeVisible();

    const bodyText = await page.locator("body").innerText();
    expect(bodyText).not.toContain("at ");
    expect(bodyText).not.toContain(".tsx");
    expect(bodyText).not.toContain(".ts");
    expect(bodyText).not.toContain("Error:");
    expect(bodyText).not.toContain("ErrorTriggerPage");
  });

  test("viewer workflow page exposes no write operations", async ({ page }) => {
    await page.goto("http://localhost:5174/view/some-workflow-id");

    await expect(page.getByRole("button")).toHaveCount(0);

    const bodyText = await page.locator("body").innerText();
    expect(bodyText).not.toMatch(/Save|Publish|Edit|Update|Delete|Create/i);
  });
});
