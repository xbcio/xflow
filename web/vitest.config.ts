import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const root = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "@xflow/core": path.resolve(root, "packages/xflow-core/src/index.ts"),
      "@xflow/api": path.resolve(root, "packages/xflow-api/src/index.ts"),
      "@xflow/preview": path.resolve(root, "packages/xflow-preview/src/index.tsx"),
      "@xflow/editor": path.resolve(root, "packages/xflow-editor/src/index.tsx")
    }
  },
  test: {
    environment: "jsdom",
    // The editor suite performs several full React/AntD rerenders. Running the
    // four DOM-heavy files concurrently can starve Vitest's 5s per-test timer
    // on a cold CI worker, so keep file execution deterministic and serial.
    fileParallelism: false,
    globals: true,
    setupFiles: ["./vitest.setup.ts"],
    include: [
      "apps/*/src/**/*.test.ts",
      "apps/*/src/**/*.test.tsx",
      "packages/*/src/**/*.test.ts",
      "packages/*/src/**/*.test.tsx"
    ],
    coverage: {
      provider: "v8",
      reporter: ["text", "html", "json-summary"],
      include: ["packages/*/src/**/*.{ts,tsx}"],
      exclude: ["**/*.test.{ts,tsx}", "**/*.d.ts"],
      thresholds: {
        statements: 90,
        branches: 70,
        functions: 80,
        lines: 90
      }
    }
  }
});
