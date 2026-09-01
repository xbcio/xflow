import { test } from "@playwright/test";

import { assertAdminSmoke } from "./admin-smoke";

test("admin production bundle renders the style and Ant Design probes", async ({ page }) => {
  await assertAdminSmoke(page);
});
