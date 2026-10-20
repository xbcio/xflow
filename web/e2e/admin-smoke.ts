import { expect, type Page } from "@playwright/test";

/** Namespace the stubbed identity reports; `?ns=ghost` is outside it on purpose. */
const SERVER_NAMESPACE = "ns-smoke";

/**
 * Resolve the workflow table card's background next to a probe carrying the
 * same `--xflow-panel` variable, so equality proves the surface is derived and
 * not a literal. Callers run it per colour scheme: a hard-coded `#ffffff`
 * passes the light pass and fails the dark one (`--xflow-panel: #1b1c1f`).
 */
async function tableSurface(page: Page): Promise<{ token: string; card: string }> {
  return page.locator('[data-table="workflows"]').evaluate((element) => {
    const probe = document.createElement("div");
    probe.style.color = "var(--xflow-panel)";
    (document.querySelector(".xflow-admin") ?? document.body).appendChild(probe);
    const token = window.getComputedStyle(probe).color;
    const card = window.getComputedStyle(element).backgroundColor;
    probe.remove();
    return { token, card };
  });
}

/**
 * Shared admin smoke for the dev server (admin.spec) and the production preview
 * (preview.spec). `/workflows` is the workflow centre now, not the old
 * placeholder, so the probes attach to its real chrome while keeping the
 * original three intents:
 *
 *   1. The Tailwind layer still wins: the centre's master grid must compute to
 *      a real two-track grid with the `gap-4` gutter (16px), and the CSSOM
 *      layer order plus a cascade probe must show AntD's reset is layered
 *      before `utilities` instead of leaking out unlayered.
 *   2. Design tokens drive surfaces: the table card's background must be the
 *      resolved `--xflow-panel` value from tokens.css, not a literal — checked
 *      in both light and dark so a hard-coded light colour cannot pass.
 *   3. AntD 6 + React 19 runtime: AntD's table must render, and the
 *      `App.useApp()` message instance must really mount DOM. The namespace
 *      guard is a genuine user path that emits one: a `?ns=` outside the
 *      server-issued list warns and falls back to the server namespace (the
 *      URL value is never trusted on its own). That toast is asserted before
 *      the lazily compiled page chunk is awaited, inside its 3s lifetime.
 *
 * The first assertion keeps a wider budget: this is the first navigation to
 * the route, so the dev bundler may still have to compile the chunk on demand.
 */
export async function assertAdminSmoke(page: Page): Promise<void> {
  // The shell only fetches `current-user` when a token exists; seed one and
  // stub the identity so the namespace guard has a server-issued list. The
  // catch-all is registered first: Playwright prefers the newest matching
  // route, so `/v1/current-user` below wins over it.
  await page.addInitScript(() => {
    try {
      sessionStorage.setItem("xflow.token", "e2e-smoke");
    } catch {
      // about:blank documents have no storage; the real navigation has one.
    }
  });
  await page.route("**/v1/**", (route) =>
    route.fulfill({ json: { success: false, code: "404", message: "not stubbed" } }),
  );
  await page.route("**/v1/current-user", (route) =>
    route.fulfill({
      json: {
        success: true,
        code: "200",
        message: "",
        data: { subject: "e2e", namespace: SERVER_NAMESPACE, scopes: ["workflow", "execution"] },
      },
    }),
  );

  await page.goto("/workflows?ns=ghost");

  // 3 (first, deliberately). App.useApp() message: the shell mounts and warns
  // before the lazily compiled page chunk resolves, and antd dismisses a
  // message after 3s — waiting for the page-owned layout below can outlive the
  // toast on the dev server. The guard strips `?ns=` in the same effect, so
  // check the URL here too, while no page chunk is involved.
  await expect(page.locator(".ant-message").getByText(/不在服务端返回的可用列表/)).toBeVisible({
    timeout: 45_000,
  });
  await expect(page).toHaveURL(/\/workflows$/);

  const layout = page.getByTestId("workflow-center-layout");
  await expect(layout).toBeVisible({ timeout: 45_000 });

  // 1. Tailwind utilities, computed, plus the layer order they depend on.
  await expect
    .poll(() =>
      layout.evaluate((element) => {
        const style = window.getComputedStyle(element);
        return `${style.display}|${style.columnGap}|${style.gridTemplateColumns}`;
      }),
    )
    .toMatch(/^grid\|16px\|.+ 320px$/);
  const cascade = await page.evaluate(() => {
    const layers: string[] = [];
    const walk = (rules: CSSRuleList) => {
      for (const rule of Array.from(rules)) {
        const ctor = rule.constructor.name;
        if (ctor === "CSSLayerBlockRule") layers.push((rule as CSSLayerBlockRule).name);
        else if (ctor === "CSSLayerStatementRule") layers.push(...(rule as CSSLayerStatementRule).nameList);
        const nested = (rule as CSSRule & { cssRules?: CSSRuleList }).cssRules;
        if (nested) walk(nested);
      }
    };
    for (const sheet of Array.from(document.styleSheets)) {
      try {
        walk(sheet.cssRules);
      } catch {
        // Cross-origin sheets cannot be inspected; the app's sheets are local.
      }
    }
    // A bare h2 inherits AntD reset's `font-weight: 500`. Preflight's
    // `font-weight: inherit` (tailwind layer base, later in the order) must
    // win and compute to 400 — which only happens if the reset really sits in
    // its layer instead of leaking out unlayered, where it would beat every
    // layered rule regardless of order.
    const host = document.querySelector(".xflow-admin") ?? document.body;
    const probe = document.createElement("h2");
    host.appendChild(probe);
    const weight = window.getComputedStyle(probe).fontWeight;
    probe.remove();
    return { layers, weight };
  });
  const resetAt = cascade.layers.indexOf("antd-reset");
  expect(resetAt).toBeGreaterThanOrEqual(0);
  expect(cascade.layers.indexOf("utilities")).toBeGreaterThan(resetAt);
  expect(cascade.weight).toBe("400");

  // 2. The card surface resolves from the token, not from a hard-coded colour.
  // Light first, then dark: a literal `#ffffff` passes the light half and must
  // fail the dark half, where the token resolves to `#1b1c1f`.
  const light = await tableSurface(page);
  expect(light.token).not.toBe("");
  expect(light.card).toBe(light.token);
  await page.emulateMedia({ colorScheme: "dark" });
  await expect
    .poll(() => page.evaluate(() => document.querySelector(".xflow-admin")?.getAttribute("data-theme")))
    .toBe("dark");
  const dark = await tableSurface(page);
  expect(dark.token).not.toBe(light.token);
  expect(dark.card).toBe(dark.token);

  // 3. AntD runtime: the table renders; the App.useApp() message was already
  // asserted right after navigation, inside its 3s lifetime.
  await expect(page.locator(".ant-table")).toBeVisible();
  await expect(page.getByRole("heading", { name: "工作流", exact: true })).toBeVisible();
  await expect(page).toHaveURL(/\/workflows$/);
}
