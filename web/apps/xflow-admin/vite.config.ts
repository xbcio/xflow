import react from "@vitejs/plugin-react";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@xflow/core": path.resolve(root, "packages/xflow-core/src/index.ts"),
      "@xflow/api": path.resolve(root, "packages/xflow-api/src/index.ts"),
      "@xflow/preview": path.resolve(root, "packages/xflow-preview/src/index.tsx"),
      "@xflow/editor": path.resolve(root, "packages/xflow-editor/src/index.tsx")
    }
  }
});
