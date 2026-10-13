import { defineConfig, devices } from "@playwright/test";

// One-off smoke for the composer default-kernel swap; mirrors
// playwright.preview.config.ts but runs only e2e/kernel-smoke.spec.ts and
// refuses to reuse a server that happens to sit on 4173 (a stale bundle would
// make the smoke meaningless).
export default defineConfig({
  testDir: "./e2e",
  testMatch: "kernel-smoke.spec.ts",
  fullyParallel: false,
  workers: 1,
  reporter: [["list"]],
  outputDir: "test-results/kernel-smoke",
  timeout: 60_000,
  expect: { timeout: 5_000 },
  use: {
    baseURL: "http://127.0.0.1:4173",
    screenshot: "only-on-failure",
    trace: "retain-on-failure"
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] }
    }
  ],
  webServer: {
    command: "pnpm build && PORT=4173 node scripts/serve-static.mjs apps/xflow-admin/dist",
    url: "http://127.0.0.1:4173/workflows",
    reuseExistingServer: false,
    timeout: 300_000
  }
});
