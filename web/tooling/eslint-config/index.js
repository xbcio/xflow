import js from "@eslint/js";
import tsParser from "@typescript-eslint/parser";
import tsPlugin from "@typescript-eslint/eslint-plugin";
import importPlugin from "eslint-plugin-import";

const sourceFiles = [
  "packages/*/src/**/*.ts",
  "packages/*/src/**/*.tsx",
  "apps/*/src/**/*.ts",
  "apps/*/src/**/*.tsx",
];

const configFiles = [
  "eslint.config.js",
  "vitest.config.ts",
  "tooling/**/*.js",
  "tooling/**/*.ts",
  "scripts/**/*.mjs",
];

export default [
  { name: "xflow/ignores", ignores: ["**/dist/**", "**/node_modules/**", "**/.turbo/**", "**/coverage/**", "**/*.d.ts", "**/*.test.*"] },
  js.configs.recommended,
  {
    name: "xflow/typescript",
    files: sourceFiles,
    languageOptions: {
      parser: tsParser,
      parserOptions: {
        ecmaVersion: "latest",
        sourceType: "module",
        projectService: true,
      },
    },
    plugins: {
      "@typescript-eslint": tsPlugin,
      import: importPlugin,
    },
    settings: {
      "import/resolver": {
        typescript: {
          project: "./tsconfig.json",
        },
      },
    },
    rules: {
      ...tsPlugin.configs.recommended.rules,
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
      "@typescript-eslint/no-explicit-any": "warn",
    },
  },
  {
    name: "xflow/boundaries",
    files: sourceFiles,
    rules: {
      "import/no-restricted-paths": [
        "error",
        {
          zones: [
            {
              target: "./packages/**/*",
              from: "./apps/**/*",
              message: "Public packages cannot import from apps.",
            },
            {
              target: "./packages/**/*",
              from: ["@umijs/max", "@ant-design/pro-layout"],
              message: "Public packages cannot import Umi or ProLayout.",
            },
            {
              target: "./packages/workflow-core/**/*",
              from: ["react", "react-dom", "@xyflow/react", "@umijs/max", "@ant-design/pro-layout"],
              message: "workflow-core must not depend on React/DOM/ReactFlow/AntD/Umi.",
            },
            {
              target: "./packages/workflow-renderer/**/*",
              from: ["@umijs/max"],
              message: "workflow-renderer must not depend on Umi.",
            },
            {
              target: "./packages/api-client/**/*",
              from: ["@umijs/max"],
              message: "api-client must not depend on Umi.",
            },
          ],
        },
      ],
      "no-restricted-imports": [
        "error",
        {
          paths: [
            { name: "@umijs/max", message: "Umi is only allowed in apps/admin." },
            { name: "@ant-design/pro-layout", message: "ProLayout is only allowed in apps/admin." },
          ],
        },
      ],
    },
  },
  {
    name: "xflow/tooling",
    files: configFiles,
    languageOptions: {
      parser: tsParser,
      parserOptions: {
        ecmaVersion: "latest",
        sourceType: "module",
      },
      globals: {
        console: "readonly",
        process: "readonly",
      },
    },
    plugins: {
      "@typescript-eslint": tsPlugin,
    },
    rules: {
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
    },
  },
];
