import { test, expect } from "playwright/test";

test.describe("Workflow Editor", () => {
  test("health page renders app info and fixture", async ({ page }) => {
    await page.goto("http://localhost:5173/");

    await expect(
      page.getByRole("heading", { name: "Workflow Editor" }),
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

  test("navigates to editor workflow page", async ({ page }) => {
    await page.goto("http://localhost:5173/editor/some-workflow-id");

    await expect(
      page.getByRole("heading", { name: "Editor: some-workflow-id" }),
    ).toBeVisible();
    await expect(page.getByTestId("editor-page")).toBeVisible();
    await expect(page.getByTestId("fixture-json")).toContainText(
      '"name": "health-check"',
    );
  });

  test("error boundary catches render errors without leaking stack traces", async ({
    page,
  }) => {
    await page.goto("http://localhost:5173/__error");

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
});
