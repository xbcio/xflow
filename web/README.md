# @xflow/web

pnpm/Turbo workspace for the xflow frontend workflow management system.

## Structure

```
web/
├── apps/
│   ├── workflow-editor/   # Vite editor app (M1.2)
│   └── workflow-viewer/   # Vite viewer app (M1.2)
├── packages/
│   ├── workflow-core/     # DSL/types/DAG utilities (no React/HTTP)
│   ├── workflow-renderer/ # React Flow based canvas renderer
│   ├── workflow-provider/ # Capability provider contracts
│   ├── workflow-editor/   # Editor component package
│   ├── workflow-viewer/   # Viewer component package
│   ├── workflow-components/ # Shared workflow UI components
│   ├── node-registry/     # Node plugin registry
│   ├── api-client/        # HTTP transport client
│   └── embed-sdk/         # iframe embed SDK
└── tooling/
    ├── eslint-config/     # Shared flat ESLint config with boundary rules
    ├── typescript-config/ # Shared tsconfig bases
    └── tailwind-config/   # Shared Tailwind preset
```

## Commands

```bash
pnpm install
pnpm typecheck
pnpm lint
pnpm test
pnpm check:boundaries
pnpm build
```

## Constraints

- Node `22.15.0`, pnpm `10.10.0`.
- Public packages use `@xflow/*` scope and keep `react`/`react-dom` as peer dependencies where applicable.
- Public packages must not depend on `@umijs/max`, `@ant-design/pro-layout`, Admin `@/` alias, or Umi runtime APIs.
- `apps/admin` is not implemented in this milestone.
