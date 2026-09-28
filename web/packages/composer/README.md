# @xflow/composer

A general low-code rendering engine: **Spec + component registry + controlled
value → UI**, with user edits handed back to the host as patches. All
semantics (visibility, repeat, expressions, checks, write rules) live in
`core`; a kernel only turns the resolved tree into React.

| Subpath | Content |
|---|---|
| `@xflow/composer/core` | Spec types + JSON Schema, `validateSpec`, `resolve`, `write`, `applyPatches`, checks/expressions, `zodProps` (no React) |
| `@xflow/composer/react` | `<Composer>`, `createRegistry`, component/kernel contracts, `defaultKernel` |
| `@xflow/composer/form` | built-in AntD form components, `formComponents`, `createFormRegistry(extra?)`; styles in `@xflow/composer/form/styles.css` |
| `@xflow/composer/kernel-json-render` | default kernel (on `@json-render/*` 0.21.0) |
| `@xflow/composer/kernel-native` | reference / fallback kernel |
| `@xflow/composer/testing` | `kernelConformance(kernel)` and test helpers (needs `vitest`, `@testing-library/react`) |

`react` (and `react-dom`) are peer dependencies (`^19.2.3`).

## Using `<Composer>`

```tsx
import { applyPatches } from "@xflow/composer/core";
import { Composer, createRegistry } from "@xflow/composer/react";

const registry = createRegistry([Input, Select /* , ... */]);

function Panel({ spec }) {
  const [value, setValue] = useState({});
  return (
    <Composer
      spec={spec}
      registry={registry}
      value={value}
      context={{ nodeKind: "http" }}          // read-only, visible at /$ctx
      externalIssues={{ "/parameters/url": [{ message: "unreachable", severity: "error" }] }}
      onChange={(patches) => setValue((v) => applyPatches(v, patches))}
      onValidate={(result) => setCanSave(result.valid)}
    />
  );
}
```

- Apply patches with core `applyPatches` (it keeps row identity). A host with
  its own reducer must call `adoptRowIdentity(prev, next)` for edited rows.
- Writes of one microtask arrive as **one** `onChange` call (same path: last
  wins). Until the host passes a new `value` reference, Composer renders
  "host value + pending patches" (the overlay), so fast typing and IME input
  never see stale values. A new `value` always wins (a host may reject patches
  by not applying them). If no new `value` arrives within 1s, the overlay is
  dropped with an `overlay-timeout` warning.
- Warnings (dropped writes, `/$ctx` writes, overlay timeout, `emit`) go to
  `onWarning`, default `console.warn`.

## Writing a component

Components depend only on the contract exported from `@xflow/composer/react`
and must never import a kernel.

```tsx
import { z } from "zod";
import { zodProps } from "@xflow/composer/core";
import type { ComposerComponent } from "@xflow/composer/react";

export const Input: ComposerComponent<{ label?: string; placeholder?: string }> = {
  type: "Input",
  props: zodProps(z.object({ label: z.string().optional(), placeholder: z.string().optional() })),
  // bindings: { value: "scalar" }    // default
  // emptyAs: "keep"                  // write "" instead of unsetting
  render: ({ node, props, value, onChange, defaultHint, issues, readOnly }) => (
    <label className="xflow-composer-input">
      {props.label}
      <input
        id={node.id}
        value={typeof value === "string" ? value : ""}
        placeholder={value === undefined && defaultHint !== undefined ? `Default: ${defaultHint}` : props.placeholder}
        readOnly={readOnly}
        aria-invalid={issues.some((i) => i.severity === "error")}
        onChange={(e) => onChange?.(e.target.value)}
      />
    </label>
  )
};
```

`render` receives `ComposerComponentProps<P>`:

| Field | Meaning |
|---|---|
| `node` | `{ id, type }`; `id` is the instance id (`el@repeat#uid` inside repeats) |
| `props` | evaluated props, already validated by `props` (the schema). Invalid props never reach `render`; the element shows `ElementError` instead |
| `bindings` | every bound prop: `{ value, onChange(next) }` |
| `value` / `onChange` | shorthand for `bindings.value` |
| `defaultHint` | display-only default |
| `issues` | check results + external issues for this element |
| `children` / `slots` | rendered default slot / named slots |
| `rows` | repeat containers only: `{ id, index, children }` per row |
| `readOnly` | render read-only; writes are ignored anyway |
| `emit` | reserved for v2 events; v1 only logs a warning |

`render` is mounted as a React component (one stable component per
registration), so hooks are allowed.

### Bindings

Any prop can be two-way bound with `{ "$bindState": "/path" }` (or
`{ "$bindItem": "field" }` inside a repeat). Declare the value kind of each
bindable prop in `bindings` (`"scalar" | "array" | "object" | "none"`,
default `{ value: "scalar" }`); it decides what "empty" means:

- scalar: `undefined` or `""`; array: `[]`; object: `{}`.
- Writing an empty value produces `unset`, never `set null`.
  `emptyAs: "keep"` makes `""` a real value for scalar bindings.
- After an unset, empty ancestors collapse only while each ancestor is itself
  bound by a visible `valueKind: "object"` element; never across an array row.
- Unchanged values produce no patch. Writes to `/$ctx` are rejected.

A component may have several bindings (e.g. a range with `min` and `max`);
by convention the main value is `value`.

### Slots

`children` is the default slot. Named slots in the Spec
(`"slots": { "header": ["a"], "footer": ["b"] }`) arrive as
`slots.header` / `slots.footer`, already rendered.

### Rows (repeat containers)

An element with `repeat` gets one synthetic `$row` per array item. The
container receives `rows` in order; render each `row.children` under
`key={row.id}` so rows keep identity (and focus/local state) across insert,
delete and reorder:

```tsx
render: ({ rows = [], value, onChange }) => (
  <table><tbody>
    {rows.map((row) => <tr key={row.id}>{row.children}</tr>)}
  </tbody></table>
)
```

Adding, removing and reordering rows is done only by the container, by
writing the whole array through its `value` binding (`bindings: { value: "array" }`).
Row fields write through their own `$bindItem` bindings; a write for a row
that no longer exists (e.g. a blur fired while a deleted row unmounts) is
dropped, never applied to a neighbour.

### Rules

- **Never write on mount** — not in render, not in effects, not to "normalise"
  a value. The conformance suite checks zero patches on mount under
  StrictMode.
- **`defaultHint` is display only.** When the value is unset, show the
  effective default ("On (default)", a placeholder, ...); do not write it.
- Use `xflow-composer-` class names and `--xflow-*` CSS variables for styling.
- Need a host capability that produces no patch (e.g. rename)? Close over the
  callback when registering the component (v1); `emit` + actions come in v2.

## Built-in form components (`@xflow/composer/form`)

`Form`, `FieldGroup`, `Input`, `TextArea`, `Password`, `InputNumber`, `Switch`,
`Select`, `Radio`, `MultiSelect`, `Tags`, `KeyValue`, `ObjectGroup`,
`ArrayTable`, `JsonEditor`, `CodeEditor`, `Unsupported`, `ElementError`.
`antd` / `@ant-design/icons` are (optional) peers needed by this subpath only.

```ts
import { createFormRegistry } from "@xflow/composer/form";
import "@xflow/composer/form/styles.css";
const registry = createFormRegistry([ExpressionInput]); // extra host components; same type overrides a built-in
```

- Props are strict: an unknown prop is a props error (ElementError placeholder).
  Common field props: `label`, `description`, `required` (marker only — add a
  `required` check for validation), `disabled`.
- A value whose shape does not match the control (including `null`, except
  in `JsonEditor` / `ArrayTable`) is shown read-only as raw JSON with a
  notice; it is never coerced. "清除此值" unsets it on request.
- Unset + `defaultHint`: text controls use the placeholder (`默认：X`), Switch
  shows `开（默认）` / `关（默认）`, the others show a hint line.
- `KeyValue.valueType` names a registered component used to edit values
  (looked up through `useComposerRegistry()`); incomplete / duplicate rows
  stay local drafts.
- `ArrayTable` needs `repeat`; it adds / removes / reorders by writing the
  whole array. `JsonEditor` never writes text that does not parse.
- Styling: `xflow-composer-*` classes, colours from `--xflow-*` variables
  (with fallbacks); AntD itself follows the host `ConfigProvider`.

## Registry

```ts
const base = createRegistry([Form, Input, Select]);          // duplicate type in one call: throws
const host = createRegistry([MyInput], { extends: base });  // later wins, logged via console.debug
const withChecks = createRegistry([], { extends: host, checks: [defineCheck("even", fn)] });
```

Register components named `Unsupported` / `ElementError` to replace the
built-in placeholders; they receive `props: { type, message, error? }` plus
the node's children and slots. `$row` is reserved.

## Kernels

A kernel implements `RenderKernel.render(tree, env)`: walk the
`ResolvedTree`, render each node's children and slots first, then call
`env.renderNode(node, { children, slots })`, keyed by `node.id` and with
`children` as an array aligned with `node.children`. Everything else
(component lookup, bindings, placeholders, error boundaries, memoisation) is
done by `renderNode`.

- `kernel-json-render` (default): converts the tree to json-render's flat
  spec with a single `ComposerNode` type and static `{ id }` props; none of
  json-render's state/visibility/validation/action semantics are used.
- `kernel-native`: ~50-line recursive renderer; reference and fallback.

Pass `kernel={nativeKernel}` to use another kernel for one Composer. To
switch the default, change the single re-export in
`src/react/defaultKernel.ts`. A kernel qualifies only if it passes the
conformance suite:

```ts
// my-kernel.test.tsx
import { kernelConformance } from "@xflow/composer/testing";
kernelConformance(myKernel);
```

Only `kernel-json-render` and `react/defaultKernel` may import
`@json-render/*` (ESLint rule + `src/boundary.test.ts`).
