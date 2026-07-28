# XFlow Web

This workspace keeps the frontend small on purpose.

## Packages

- `apps/xflow-admin` - admin app that assembles API, preview, and later editor flows
- `packages/xflow-core` - React-free workflow types and `WorkflowDef -> GraphModel`
- `packages/xflow-api` - request client for XFlow backend APIs
- `packages/xflow-preview` - `XFlowPreview`, the read-only workflow/runtime preview component
- `packages/xflow-editor` - `XFlowEditor`, built on top of preview

## Commands

```bash
pnpm install
pnpm test
pnpm typecheck
pnpm build
pnpm dev
```

