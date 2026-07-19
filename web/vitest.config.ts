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
  },
});
