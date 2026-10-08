import { expect, type Page } from "@playwright/test";

export async function assertAdminSmoke(page: Page): Promise<void> {
  await page.goto("/workflows");
  await expect(page).toHaveURL(/\/workflows$/);

  // This is the first navigation to the route, so the dev server's bundler
  // (utoo/Vite) still has to compile the /workflows chunk on demand — the
  // webServer readiness probe only confirms the HTTP server answers, not that
  // this chunk is pre-compiled. On a loaded host that compile has been
  // observed eating most of the default 20s expect timeout (down to ~2s of
  // margin at load average ~60 on an 8-core host), so give first visibility
  // its own, wider budget. Every assertion after this one only re-queries an
  // already-rendered page and keeps the suite's default timeout, so a real
  // rendering regression is still caught promptly.
  const heading = page.getByTestId("style-probe-heading");
  await expect(heading).toBeVisible({ timeout: 45_000 });
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
