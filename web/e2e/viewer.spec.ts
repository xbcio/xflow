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
    ).toContainText("Start (xflow.start) — trigger");
    await expect(
      page.getByTestId("fixture-nodes"),
    ).toContainText("End (xflow.end) — action");
    await expect(
      page.getByTestId("fixture-connections"),
    ).toContainText("start:default → end:default");
  });

  test("navigates to viewer workflow page", async ({ page }) => {
    await page.goto("http://localhost:5174/view/some-workflow-id");

    await expect(
      page.getByRole("heading", { name: "Viewer: some-workflow-id" }),
    ).toBeVisible();
    await expect(page.getByTestId("viewer-page")).toBeVisible();
    await expect(page.getByTestId("fixture-json")).toContainText(
      '"name": "health-check"',
    );
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
