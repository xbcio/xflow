# Task 4 (M1.2) Report: Vite App Shells

## Status

Completed. Both `apps/workflow-editor` and `apps/workflow-viewer` have been upgraded from placeholders to real Vite applications with routing, ErrorBoundary, runtime config, dev mock toggle, production mock disable, static fixtures, and component tests. All six validation commands pass.

## File Trees

### `web/apps/workflow-editor`

```
index.html
package.json
src/App.tsx
src/main.tsx
vite.config.ts
tsconfig.json
src/components/
  ErrorBoundary.tsx
  ErrorBoundary.test.tsx
src/config/
  runtime.ts
src/mocks/
  fixtures.ts
src/pages/
  EditorPage.tsx
  HealthPage.tsx
  HealthPage.test.tsx
src/styles/
  index.css
```

### `web/apps/workflow-viewer`

```
index.html
package.json
src/App.tsx
src/main.tsx
vite.config.ts
tsconfig.json
src/components/
  ErrorBoundary.tsx
  ErrorBoundary.test.tsx
src/config/
  runtime.ts
src/mocks/
  fixtures.ts
src/pages/
  ViewerPage.tsx
  HealthPage.tsx
  HealthPage.test.tsx
src/styles/
  index.css
```

## Key Dependencies (pinned, per ADR D1)

| Package | Version | Scope | Note |
|---------|---------|-------|------|
| `vite` | `6.3.5` | app devDep | latest stable 6.x at install time |
| `@vitejs/plugin-react` | `4.5.0` | app devDep | matches Vite 6 |
| `react` / `react-dom` | `19.1.0` | app dep / root devDep | pinned per ADR |
| `react-router-dom` | `7.6.1` | app dep | React 19 compatible |
| `@testing-library/react` | `16.1.0` | root devDep | React 19 compatible |
| `@testing-library/dom` | `10.4.0` | root devDep | peer of testing-library/react |
| `jsdom` | `26.0.0` | root devDep | Vitest jsdom environment |
| `vitest` | `3.2.0` | root/app devDep | existing workspace version |

Excluded (per brief): Umi, AntD, ProComponents, React Flow, Monaco, zustand, TanStack Query.

## Configuration Highlights

### Vite

Each app has its own `vite.config.ts`:

- `@vitejs/plugin-react` for JSX / Fast Refresh.
- `resolve.alias["@"]` points to `src`.
- `build.outDir` is `dist`.
- Editor dev server port: `5173`; Viewer dev server port: `5174`.

### Routing (`react-router-dom`)

- Editor: `/` health page, `/editor/:workflowId` placeholder editor page.
- Viewer: `/` health page, `/view/:workflowId` read-only viewer page.
- Both apps use `BrowserRouter` wrapped in `ErrorBoundary`.

### ErrorBoundary

- Class-based React ErrorBoundary catching render errors.
- Displays a generic message (`Something went wrong`) and never exposes stack traces, paths, or error details to the UI.

### Runtime Config / Mock Toggle

`src/config/runtime.ts` reads Vite env vars:

- `apiBaseUrl`: placeholder, defaults to empty string (`VITE_API_BASE_URL`).
- `mockEnabled`:
  - Development default: `true` unless `VITE_MOCK_ENABLED=false`.
  - Production (`import.meta.env.PROD`): forced to `false`.
- `environment`: `import.meta.env.MODE`.

This satisfies the requirement that production builds disable mock fixtures at runtime.

### Static Fixture

`src/mocks/fixtures.ts` defines a minimal `WorkflowDef` (start → end) with `id`, `namespace`, `name`, `version`, `spec`, `nodes`, and `connections`, matching the shape of `types/workflow.go`. No HTTP calls are made.

### Health Page

Renders app name, version, environment, mock status, and a textual list of fixture nodes/connections without React Flow.

## Validation Summary (all green)

```bash
cd <repo-root>/.worktrees/frontend-mgmt/web
pnpm install          # OK (network available)
pnpm typecheck        # OK — tsc --noEmit passes
pnpm lint             # OK — ESLint passes
pnpm test             # OK — 13 files / 15 tests pass
pnpm check:boundaries # OK — no public package depends on Umi/ProLayout
pnpm build            # OK — Turbo build of all packages + both Vite apps succeeds
```

Additional independent app builds:

```bash
pnpm --filter @xflow/app-workflow-editor build  # OK — dist/index.html + assets generated
pnpm --filter @xflow/app-workflow-viewer build   # OK — dist/index.html + assets generated
```

Production bundle verification:

- `grep -o 'mockEnabled:[^,]*' apps/*/dist/assets/index-*.js` returns `mockEnabled:!1` (i.e., `false`), confirming production mock disablement.

## Test Coverage

- Editor HealthPage rendering and fixture node/connection counts.
- Editor ErrorBoundary fallback and no stack-leak assertion.
- Viewer HealthPage rendering and fixture node/connection counts.
- Viewer ErrorBoundary fallback and no stack-leak assertion.

Total: **15 tests** across **13 test files** (9 package placeholder tests + 4 app tests).

## Commit Hash

Code commit: `1bfba95`

## Open Issues / Notes

1. **Engine warning**: local Node is `v22.18.0`, while `package.json` pins `22.15.0`. This produces a pnpm warning but does not fail any command.
2. **Placeholder public packages**: the 9 `@xflow/*` public packages remain empty skeletons; the apps deliberately do not import them in this milestone.
3. **No real API calls**: all workflow data comes from `src/mocks/fixtures.ts`.
4. **Styles are intentionally minimal**: `.xflow-root` scope is in place; Tailwind integration is deferred to a later milestone.
5. **Dist directories** are generated by Vite and covered by `.gitignore`; they are not committed.
