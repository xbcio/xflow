import { test, expect } from "playwright/test";

test.describe("Production Preview", () => {
  test("viewer shows empty state and no canvas nodes", async ({ page }) => {
    await page.goto("http://localhost:4174/view/some-workflow-id");

    await expect(
      page.getByRole("heading", { name: "Viewer: some-workflow-id" }),
    ).toBeVisible();
    await expect(page.getByTestId("viewer-page")).toBeVisible();
    await expect(page.getByTestId("empty-state")).toBeVisible();
    await expect(page.locator(".react-flow__node")).toHaveCount(0);
  });

  test("viewer exposes no write operations", async ({ page }) => {
    await page.goto("http://localhost:4174/view/some-workflow-id");

    await expect(page.getByRole("button")).toHaveCount(0);

    const bodyText = await page.locator("body").innerText();
    expect(bodyText).not.toMatch(/Save|Publish|Edit|Update|Delete|Create/i);
  });

  test("editor shows configuration empty state", async ({ page }) => {
    await page.goto("http://localhost:4173/editor/some-workflow-id");

    await expect(
      page.getByText("Editor: some-workflow-id"),
    ).toBeVisible();
    await expect(page.getByTestId("editor-page")).toBeVisible();
    await expect(page.getByTestId("fixture-nodes")).toHaveCount(0);
    await expect(page.getByTestId("fixture-connections")).toHaveCount(0);
  });

  test("health pages do not show fixture node lists", async ({ page }) => {
    await page.goto("http://localhost:4173/");

    await expect(
      page.getByRole("heading", { name: "Workflow Editor" }),
    ).toBeVisible();
    await expect(page.getByText(/Mock: disabled/)).toBeVisible();
    await expect(page.getByTestId("fixture-nodes")).toHaveCount(0);
    await expect(page.getByTestId("fixture-connections")).toHaveCount(0);

    await page.goto("http://localhost:4174/");

    await expect(
      page.getByRole("heading", { name: "Workflow Viewer" }),
    ).toBeVisible();
    await expect(page.getByText(/Mock: disabled/)).toBeVisible();
    await expect(page.getByTestId("fixture-nodes")).toHaveCount(0);
    await expect(page.getByTestId("fixture-connections")).toHaveCount(0);
  });
});
