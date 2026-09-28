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

const composerCoreRestrictedImports = {
  paths: ["react", "react-dom", "antd", "@xyflow/react"].map((name) => ({
    name,
    message: "@xflow/composer/core must stay free of React and renderers (Doc B §2)."
  })),
  patterns: [
    ...packageBoundaryPatterns,
    {
      group: ["react/*", "react-dom/*", "antd/*", "@xyflow/react/*", "@json-render/*"],
      message: "@xflow/composer/core must stay free of React and renderers (Doc B §2)."
    }
  ]
};

const jsonRenderPattern = {
  group: ["@json-render/*"],
  message: "Only composer/kernel-json-render and composer/react/defaultKernel may import @json-render/* (Doc B §7.2)."
};

const composerRestrictedImports = {
  patterns: [...packageBoundaryPatterns, jsonRenderPattern]
};

const composerReactRestrictedImports = {
  patterns: [
    ...packageBoundaryPatterns,
    jsonRenderPattern,
    {
      group: ["../kernel-*", "../kernel-*/**", "@xflow/composer/kernel-*"],
      message: "composer/react reaches a kernel only through react/defaultKernel (Doc B §7.2)."
    }
  ]
};

const composerFormRestrictedImports = {
  patterns: [
    ...packageBoundaryPatterns,
    jsonRenderPattern,
    {
      group: ["../kernel-*", "../kernel-*/**", "@xflow/composer/kernel-*"],
      message: "composer/form depends only on the component contract, never on a kernel (Doc B §5)."
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
  },
  {
    files: ["packages/composer/src/**/*.{js,mjs,cjs,ts,tsx}"],
    rules: {
      "no-restricted-imports": ["error", composerRestrictedImports]
    }
  },
  {
    files: ["packages/composer/src/react/**/*.{js,mjs,cjs,ts,tsx}"],
    ignores: ["packages/composer/src/react/**/*.test.{ts,tsx}"],
    rules: {
      "no-restricted-imports": ["error", composerReactRestrictedImports]
    }
  },
  {
    files: ["packages/composer/src/form/**/*.{js,mjs,cjs,ts,tsx}"],
    ignores: ["packages/composer/src/form/**/*.test.{ts,tsx}", "packages/composer/src/form/**/*.testkit.tsx"],
    rules: {
      "no-restricted-imports": ["error", composerFormRestrictedImports]
    }
  },
  {
    files: [
      "packages/composer/src/kernel-json-render/**/*.{js,mjs,cjs,ts,tsx}",
      "packages/composer/src/react/defaultKernel.ts"
    ],
    rules: {
      "no-restricted-imports": ["error", { patterns: packageBoundaryPatterns }]
    }
  },
  {
    files: ["packages/composer/src/core/**/*.{js,mjs,cjs,ts,tsx}"],
    rules: {
      "no-restricted-imports": ["error", composerCoreRestrictedImports]
    }
  }
];
