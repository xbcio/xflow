import { defineConfig, devices } from "playwright/test";

const isCI = Boolean(process.env.CI);

export default defineConfig({
  testDir: "./e2e",
  testMatch: "preview.spec.ts",
  fullyParallel: true,
  forbidOnly: isCI,
  retries: isCI ? 1 : 0,
  workers: isCI ? 1 : undefined,
  reporter: [["list"], ["html", { outputFolder: "playwright-report" }]],
  outputDir: "test-results",
  timeout: 30 * 1000,
  use: {
    trace: "on-first-retry",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: [
    {
      command:
        "pnpm --filter @xflow/app-workflow-editor preview --port 4173 --strictPort",
      url: "http://localhost:4173",
      reuseExistingServer: !isCI,
      timeout: 120 * 1000,
    },
    {
      command:
        "pnpm --filter @xflow/app-workflow-viewer preview --port 4174 --strictPort",
      url: "http://localhost:4174",
      reuseExistingServer: !isCI,
      timeout: 120 * 1000,
    },
  ],
});
