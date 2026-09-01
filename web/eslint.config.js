import eslint from "@eslint/js";
import tseslint from "@typescript-eslint/eslint-plugin";
import tsParser from "@typescript-eslint/parser";
import globals from "globals";

const packageBoundaryPatterns = [
  {
    group: ["@xflow/admin", "@xflow/admin/*", "**/apps/*"],
    message: "Public packages must not import application code."
  },
  {
    group: ["@umijs/*", "@ant-design/pro-*", "@ant-design/pro-*/*"],
    message: "Umi and Ant Design Pro are application-layer dependencies."
  }
];

const coreRestrictedImports = {
  paths: [
    { name: "react", message: "@xflow/core must remain framework-free." },
    { name: "react-dom", message: "@xflow/core must remain framework-free." },
    { name: "antd", message: "@xflow/core must remain framework-free." },
    { name: "@xyflow/react", message: "@xflow/core must remain framework-free." }
  ],
  patterns: [
    ...packageBoundaryPatterns,
    {
      group: ["react/*", "react-dom/*", "antd/*", "@xyflow/react/*"],
      message: "@xflow/core must remain framework-free."
    }
  ]
};

export default [
  {
    ignores: [
      "**/node_modules/**",
      "**/dist/**",
      "**/.turbo/**",
      "**/.turbopack/**",
      "**/coverage/**",
      "**/playwright-report/**",
      "**/test-results/**",
      "**/src/.umi/**",
      "**/src/.umi-production/**",
      "**/src/.umi-test/**",
      "**/*.d.ts"
    ]
  },
  {
    files: ["**/*.{js,mjs,cjs}"],
    ...eslint.configs.recommended,
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: {
        ...globals.node
      }
    }
  },
  {
    files: ["**/*.{ts,tsx}"],
    languageOptions: {
      parser: tsParser,
      ecmaVersion: "latest",
      sourceType: "module",
      parserOptions: {
        ecmaFeatures: { jsx: true }
      },
      globals: {
        ...globals.browser,
        ...globals.node
      }
    },
    plugins: {
      "@typescript-eslint": tseslint
    },
    rules: {
      ...eslint.configs.recommended.rules,
      ...tseslint.configs.recommended.rules,
      "no-undef": "off",
      "no-unused-vars": "off",
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", caughtErrorsIgnorePattern: "^_", varsIgnorePattern: "^_" }
      ]
    }
  },
  {
    files: ["packages/**/*.{js,mjs,cjs,ts,tsx}"],
    rules: {
      "no-restricted-imports": ["error", { patterns: packageBoundaryPatterns }]
    }
  },
  {
    files: ["packages/xflow-core/**/*.{js,mjs,cjs,ts,tsx}"],
    rules: {
      "no-restricted-imports": ["error", coreRestrictedImports]
    }
  }
];
