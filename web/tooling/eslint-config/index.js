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

const packageFiles = [
  "packages/*/src/**/*.ts",
  "packages/*/src/**/*.tsx",
];

const workflowCoreFiles = ["packages/workflow-core/src/**/*.ts"];

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
      "no-undef": "off",
    },
  },
  {
    name: "xflow/boundaries-direction",
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
          ],
        },
      ],
    },
  },
  {
    name: "xflow/boundaries-public-packages",
    files: packageFiles,
    rules: {
      "no-restricted-imports": [
        "error",
        {
          paths: [
            { name: "@umijs/max", message: "Umi is only allowed in apps/admin." },
            { name: "@ant-design/pro-layout", message: "ProLayout is only allowed in apps/admin." },
          ],
          patterns: [
            { group: ["@umijs/*"], message: "Umi packages are only allowed in apps/admin." },
            { group: ["@ant-design/pro-*"], message: "ProComponents are only allowed in apps/admin." },
          ],
        },
      ],
    },
  },
  {
    name: "xflow/boundaries-workflow-core",
    files: workflowCoreFiles,
    rules: {
      "no-restricted-imports": [
        "error",
        {
          paths: [
            { name: "react", message: "workflow-core must not depend on React." },
            { name: "react-dom", message: "workflow-core must not depend on React DOM." },
            { name: "@xyflow/react", message: "workflow-core must not depend on React Flow." },
            { name: "@umijs/max", message: "workflow-core must not depend on Umi." },
            { name: "@ant-design/pro-layout", message: "workflow-core must not depend on ProLayout." },
          ],
          patterns: [
            { group: ["@umijs/*"], message: "workflow-core must not depend on Umi packages." },
            { group: ["@xyflow/*"], message: "workflow-core must not depend on React Flow packages." },
            { group: ["@ant-design/pro-*"], message: "workflow-core must not depend on ProComponents." },
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
