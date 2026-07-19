# @xflow/workflow-viewer

Viewer component package: read-only display and execution overlay.

## Dependency constraints

- Depends on `@xflow/workflow-core` and `@xflow/workflow-renderer`.
- Declares `react`, `react-dom` and `@xyflow/react` as peer dependencies.
- No Umi / Ant Design / ProLayout dependencies.
- No `ahooks` or `zustand` in the public package.
- Does not depend on `@xflow/workflow-editor`.

## Exports

- `WorkflowViewer` — read-only workflow canvas with node search, focus and a read-only detail sidebar.
- `ExecutionViewer` — `WorkflowViewer` plus an execution snapshot summary bar.
- `ExecutionSnapshot` type — re-exported from `@xflow/workflow-renderer` (placeholder, to be replaced in M2/M3).
