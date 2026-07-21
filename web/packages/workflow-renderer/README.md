# @xflow/workflow-renderer

React Flow based canvas renderer, overlays and unknown-node fallback.

## Dependency constraints

- Depends on `@xflow/workflow-core`, React, React DOM and `@xyflow/react` (peer).
- `@xflow/node-registry` and `@xflow/workflow-provider` are intentionally not declared as dependencies for M4.1; they are empty skeletons and renderer uses a built-in `type -> component` mapping.
- Real node-registry/provider integration is reserved for M4.2 / M3.3. Custom node renderers can still be passed via the `nodeTypes` prop of `WorkflowCanvas`.
- No API calls, no save/publish logic.
- No Umi runtime, no `ahooks`/`zustand`.
- Required CSS: import `@xflow/workflow-renderer/styles.css` and wrap the canvas in `.xflow-root`.

## CSS isolation

All renderer-defined `.xf-*` classes are scoped under `.xflow-root` (via
`:where(.xflow-root) .xf-*`) so a host application cannot accidentally pick
them up, and any `.xf-*` classes the host defines cannot leak into the
renderer. Overlay positions are configurable through CSS variables exposed
on `.xflow-root` (e.g. `--xf-overlay-selection-top`,
`--xf-overlay-diag-bottom`); see `src/styles.css` for the full list.

### Known limitation: React Flow base CSS is global

`src/styles.css` begins with `@import url("@xyflow/react/dist/style.css")`.
That import is a build-time side effect: it injects the upstream
`.react-flow__*` rules into the **global** stylesheet, not under
`.xflow-root`. F0-B guarantees our own `.xf-*` classes are fully scoped, but
**does not** scope the third-party `.react-flow__*` rules. Hosts that need
full isolation today must wrap the renderer in a Shadow DOM or apply a
build-time prefix to `@xyflow/react` styles; that work is deferred to Embed
hardening. Do not assume the renderer is fully CSS-isolated from the host
today.
