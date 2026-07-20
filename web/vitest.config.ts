import { defineConfig } from "vitest/config";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const workspacePackages = [
  "api-client",
  "embed-sdk",
  "node-registry",
  "workflow-components",
  "workflow-core",
  "workflow-editor",
  "workflow-provider",
  "workflow-renderer",
  "workflow-viewer",
];

const alias = Object.fromEntries(
  workspacePackages.map((name) => [
    `@xflow/${name}`,
    path.resolve(__dirname, "packages", name, "src", "index.ts"),
  ])
);

export default defineConfig({
  resolve: { alias },
  test: {
    globals: true,
    environment: "node",
    include: ["packages/*/src/**/*.test.ts", "packages/*/src/**/*.test.tsx", "apps/*/src/**/*.test.ts", "apps/*/src/**/*.test.tsx"],
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      reportsDirectory: "./coverage",
      // all: true counts every file matching `include`, not only files that
      // were imported during the test run. This prevents a test gap from
      // silently hiding uncovered modules (e.g. a new src file added without
      // any test import would not lower coverage under all: false).
      all: true,
      include: ["packages/*/src/**/*.ts", "packages/*/src/**/*.tsx", "apps/*/src/**/*.ts", "apps/*/src/**/*.tsx"],
      exclude: [
        "**/*.test.ts",
        "**/*.test.tsx",
        "**/*.d.ts",
        "**/node_modules/**",
        "**/dist/**",
      ],
      // thresholds are a global regression baseline, NOT a quality target.
      // vitest v8 coverage thresholds support only global metrics (not
      // per-package overrides), so we set a single conservative floor that
      // is below the current real coverage and will catch large drops.
      // Calibrated from `pnpm test:coverage` on 2026-07-20:
      //   Stmts 93.94 | Branches 78.58 | Funcs 97.14 | Lines 93.94
      // Core packages (workflow-core / workflow-renderer / workflow-viewer)
      // are expected to maintain non-zero coverage above this floor; the
      // placeholder version constants in workflow-core/src/types.ts are
      // excluded from quality measurement (uncovered by design, see comment
      // in that file).
      thresholds: {
        statements: 85,
        branches: 65,
        functions: 85,
        lines: 85,
      },
    },
  },
});
