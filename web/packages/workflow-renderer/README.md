# @xflow/workflow-renderer

React Flow based canvas renderer, overlays and unknown-node fallback.

## Dependency constraints

- Depends on `@xflow/workflow-core`, React, React DOM and `@xyflow/react` (peer).
- `@xflow/node-registry` and `@xflow/workflow-provider` are intentionally not declared as dependencies for M4.1; they are empty skeletons and renderer uses a built-in `type -> component` mapping.
- Real node-registry/provider integration is reserved for M4.2 / M3.3. Custom node renderers can still be passed via the `nodeTypes` prop of `WorkflowCanvas`.
- No API calls, no save/publish logic.
- No Umi runtime, no `ahooks`/`zustand`.
- Required CSS: import `@xflow/workflow-renderer/styles.css` and wrap the canvas in `.xflow-root`.
