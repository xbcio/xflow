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
    globals: true,
    setupFiles: ["./vitest.setup.ts"],
    include: [
      "apps/*/src/**/*.test.ts",
      "apps/*/src/**/*.test.tsx",
      "packages/*/src/**/*.test.ts",
      "packages/*/src/**/*.test.tsx"
    ]
  }
});
