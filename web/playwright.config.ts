import { defineConfig, devices } from "playwright/test";

const isCI = Boolean(process.env.CI);

export default defineConfig({
  testDir: "./e2e",
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
      command: "pnpm --filter @xflow/app-workflow-editor dev",
      url: "http://localhost:5173",
      reuseExistingServer: !isCI,
      timeout: 120 * 1000,
    },
    {
      command: "pnpm --filter @xflow/app-workflow-viewer dev",
      url: "http://localhost:5174",
      reuseExistingServer: !isCI,
      timeout: 120 * 1000,
    },
  ],
});
