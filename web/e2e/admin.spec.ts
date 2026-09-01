import { test } from "@playwright/test";

import { assertAdminSmoke } from "./admin-smoke";

test("admin development server renders the style and Ant Design probes", async ({ page }) => {
  await assertAdminSmoke(page);
});
