import { expect, type Page } from "@playwright/test";

export async function assertAdminSmoke(page: Page): Promise<void> {
  await page.goto("/workflows");
  await expect(page).toHaveURL(/\/workflows$/);

  const heading = page.getByTestId("style-probe-heading");
  await expect(heading).toBeVisible();
  await expect(heading).toHaveText("XFlow Admin");
  await expect(heading).toHaveCSS("font-weight", "700");

  const styleBlock = page.getByTestId("style-probe-block");
  await expect(styleBlock).toBeVisible();
  const backgroundColor = await styleBlock.evaluate(
    (element) => window.getComputedStyle(element).backgroundColor
  );
  expect(backgroundColor).not.toBe("rgba(0, 0, 0, 0)");

  await page.getByTestId("message-probe").click();
  await expect(page.locator(".ant-message").getByText("ok", { exact: true })).toBeVisible();
}
