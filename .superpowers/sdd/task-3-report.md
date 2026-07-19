# Task 3 (M1.1) Report: web/ pnpm/Turbo Workspace Scaffold

## Status

DONE — all 6 verification commands pass.

## File Tree

```
web/
├── .gitignore
├── .npmrc
├── .nvmrc
├── README.md
├── eslint.config.js
├── package.json
├── pnpm-lock.yaml
├── pnpm-workspace.yaml
├── tsconfig.json
├── turbo.json
├── vitest.config.ts
├── scripts/
│   └── check-boundary.mjs
├── apps/
│   ├── workflow-editor/
│   │   ├── package.json
│   │   ├── README.md
│   │   ├── tsconfig.json
│   │   └── src/
│   │       ├── index.ts
│   │       └── index.test.ts
│   └── workflow-viewer/
│       ├── package.json
│       ├── README.md
│       ├── tsconfig.json
│       └── src/
│           ├── index.ts
│           └── index.test.ts
├── packages/
│   ├── api-client/
│   ├── embed-sdk/
│   ├── node-registry/
│   ├── workflow-components/
│   ├── workflow-core/
│   ├── workflow-editor/
│   ├── workflow-provider/
│   ├── workflow-renderer/
│   └── workflow-viewer/
│       (each: package.json, README.md, tsconfig.json, src/index.ts, src/index.test.ts)
└── tooling/
    ├── eslint-config/
    │   ├── index.js
    │   └── package.json
    ├── tailwind-config/
    │   ├── package.json
    │   └── tailwind.config.js
    └── typescript-config/
        ├── base.json
        ├── node.json
        ├── package.json
        └── react.json
```

## Package Key Fields

| Package | Version | Dependencies | Peer Dependencies |
|---------|---------|--------------|-------------------|
| `@xflow/workflow-core` | 0.1.0 | — | — |
| `@xflow/workflow-provider` | 0.1.0 | `workflow-core` | `react`, `react-dom` |
| `@xflow/node-registry` | 0.1.0 | `workflow-core` | `react`, `react-dom` |
| `@xflow/api-client` | 0.1.0 | `workflow-core` | — |
| `@xflow/workflow-renderer` | 0.1.0 | `workflow-core`, `workflow-provider`, `node-registry` | `react`, `react-dom`, `@xyflow/react` (optional) |
| `@xflow/workflow-components` | 0.1.0 | `workflow-core`, `workflow-provider`, `node-registry`, `workflow-renderer` | `react`, `react-dom`, `@xyflow/react` (optional) |
| `@xflow/workflow-editor` | 0.1.0 | `workflow-core`, `workflow-provider`, `node-registry`, `workflow-renderer`, `workflow-components`, `api-client` | `react`, `react-dom`, `@xyflow/react` (opt), `@monaco-editor/react` (opt), `zustand` (opt), `@tanstack/react-query` (opt) |
| `@xflow/workflow-viewer` | 0.1.0 | same six `@xflow/*` packages as editor | `react`, `react-dom`, `@xyflow/react` (opt), `zustand` (opt), `@tanstack/react-query` (opt) |
| `@xflow/embed-sdk` | 0.1.0 | `workflow-editor`, `workflow-viewer` | — |
| `@xflow/app-workflow-editor` | 0.1.0 | `workflow-editor`, `react`, `react-dom` | — |
| `@xflow/app-workflow-viewer` | 0.1.0 | `workflow-viewer`, `react`, `react-dom` | — |
| `@xflow/eslint-config` | 0.1.0 | `@eslint/js`, `@typescript-eslint/*`, `eslint-plugin-import`, `eslint-import-resolver-typescript` | `eslint` |
| `@xflow/typescript-config` | 0.1.0 | — | — |
| `@xflow/tailwind-config` | 0.1.0 | `tailwindcss` | — |

All internal dependencies use `workspace:*`.

## Version Lock Summary (vs ADR D1)

| Item | Locked Value | Location |
|------|--------------|----------|
| Node.js | `22.15.0` | `.nvmrc`, `package.json#engines` |
| pnpm | `10.10.0` | `package.json#packageManager` |
| TypeScript | `5.8.3` | root + package devDependencies |
| React / React DOM | `19.1.0` | peerDependencies of React packages; root devDependencies |
| `@types/react` | `19.1.17` | root devDependencies |
| `@types/react-dom` | `19.1.10` | root devDependencies |
| tailwindcss | `3.4.17` | `tooling/tailwind-config` dependencies |
| `@xyflow/react` | `12.5.0` | peerDependency (optional) only |
| `@monaco-editor/react` | `4.7.0` | peerDependency (optional) only |
| zustand | `5.0.5` | peerDependency (optional) only |
| `@tanstack/react-query` | `5.75.0` | peerDependency (optional) only |
| vitest | `3.2.0` | root + package devDependencies |
| playwright | `1.52.0` | declared in root devDependencies |
| ESLint | `9.25.0` | root + `eslint-config` peerDependencies |
| `@changesets/cli` | `2.29.0` | root devDependencies |
| turbo | `2.5.0` | root devDependencies |

Umi/AntD/ProComponents are not installed. `apps/admin` is not created.

## Verification Outputs

### 1. `pnpm install`

Resolved 522 packages, added 441. Completed with only an engine warning (system runs Node 22.18.0 while `engines` declares 22.15.0, as expected by the environment note in the brief).

### 2. `pnpm typecheck`

`tsc --build` across root references succeeds; emits `dist/` for all 11 code packages. No type errors.

### 3. `pnpm lint`

ESLint flat config runs over source and tooling files with no errors. Boundary rules (`import/no-restricted-paths`, `no-restricted-imports`) are active.

### 4. `pnpm test`

Vitest runs 11 test files / 11 tests, all passing.

```
Test Files  11 passed (11)
     Tests  11 passed (11)
```

### 5. `pnpm check:boundaries`

```
Boundary check passed: no public package depends on Umi/ProLayout.
```

### 6. `pnpm build`

Turbo runs `tsc --build` in 11 code packages with correct topological order:

```
Tasks:    11 successful, 11 total
Cached:   0 cached, 11 total
Time:     ~2s
```

## Commits

- `8f62656` — feat(web): scaffold pnpm/turbo workspace with boundary tooling

## Notes / Concerns

- `pnpm install` emits an `Unsupported engine` warning because the host Node is `22.18.0` while the workspace declares `22.15.0`. This is expected per the task environment note and does not block any command.
- `pnpm typecheck` emits `dist/` (it uses `tsc --build`, not `--noEmit`), which is required for the subsequent `pnpm test` to resolve workspace package entries. `pnpm build` then re-runs the same `tsc --build` via Turbo.
- `@xyflow/react`, `@monaco-editor/react`, `zustand`, and `@tanstack/react-query` are declared only as optional peer dependencies in their consumer packages; they are not installed in M1.
