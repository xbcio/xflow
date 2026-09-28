// renderNode (Doc B §7.1): everything between a ResolvedNode and the
// registered component, independent of the kernel:
//   component lookup, props injection, bindings -> core write, the
//   value/onChange shorthands, defaultHint, `rows` for repeat containers,
//   slots, Unsupported/ElementError placeholders and a per-element error
//   boundary that resets when the element's inputs (its node) change.
//
// Output is memoised per ResolvedNode reference: resolve() shares unchanged
// subtrees, so an unchanged node yields the identical React element and
// React skips the whole subtree (one keystroke re-renders one leaf).

import { Component, Fragment, memo, useMemo, useRef, type ErrorInfo, type ReactElement, type ReactNode } from "react";
import type { BindTarget, ResolvedNode, ResolvedTree, ValueKind, Warning } from "../core";
import type { ComposerBinding, ComposerComponent, ComposerComponentProps, ComposerRow, KernelContent, KernelEnv, Registry } from "./contract";

export const ROW_TYPE = "$row";
const EMPTY_SLOTS: Record<string, ReactNode> = Object.freeze({}) as Record<string, ReactNode>;

export interface NodeRendererOptions {
  registry: Registry;
  readOnly: boolean;
  /** Latest resolve result (read lazily; used to tell repeat containers apart). */
  getTree(): ResolvedTree | undefined;
  write(target: BindTarget, next: unknown, valueKind: ValueKind, emptyAs: "unset" | "keep"): void;
  warn(warning: Warning): void;
}

/** Builds a memoising renderNode. Create a new one whenever an option changes. */
export function createNodeRenderer(options: NodeRendererOptions): KernelEnv["renderNode"] {
  const cache = new WeakMap<ResolvedNode, ReactElement>();
  return (node, content) => {
    const hit = cache.get(node);
    if (hit) return hit;
    const element =
      node.type === ROW_TYPE ? (
        <Fragment>{content.children}</Fragment>
      ) : (
        <NodeHost
          node={node}
          content={content}
          rows={rowsOf(node, content, options.getTree())}
          options={options}
        />
      );
    cache.set(node, element);
    return element;
  };
}

function rowsOf(node: ResolvedNode, content: KernelContent, tree: ResolvedTree | undefined): ComposerRow[] | undefined {
  const isRepeat = tree?.repeats[node.id] !== undefined || node.children.some((child) => child.type === ROW_TYPE);
  if (!isRepeat) return undefined;
  const rendered = Array.isArray(content.children) ? (content.children as ReactNode[]) : [];
  const rows: ComposerRow[] = [];
  node.children.forEach((child, i) => {
    if (child.type !== ROW_TYPE) return;
    const index = typeof child.props.index === "number" ? child.props.index : rows.length;
    rows.push({ id: child.id, index, children: rendered[i] ?? null });
  });
  return rows;
}

// ------------------------------------------------------------- NodeHost

interface NodeHostProps {
  node: ResolvedNode;
  content: KernelContent;
  rows: ComposerRow[] | undefined;
  options: NodeRendererOptions;
}

const NodeHost = memo(function NodeHost({ node, content, rows, options }: NodeHostProps) {
  const { registry, readOnly } = options;
  const component = registry.get(node.type);

  // Keep the latest node reachable from stable callbacks (e.g. a blur write
  // fired while the element unmounts uses the row uid it was bound to).
  const latest = useRef(node);
  latest.current = node;

  const bindingKeys = Object.keys(node.bindings).join("\u0000");
  const handlers = useMemo(() => {
    const out: Record<string, (next: unknown) => void> = {};
    for (const key of bindingKeys === "" ? [] : bindingKeys.split("\u0000")) {
      out[key] = (next: unknown) => {
        const binding = latest.current.bindings[key];
        if (!binding) return;
        if (options.readOnly) {
          options.warn({ code: "read-only", message: `write to ${latest.current.id}.${key} ignored: composer is read-only` });
          return;
        }
        options.write(binding.target, next, binding.valueKind, component?.emptyAs ?? "unset");
      };
    }
    return out;
  }, [bindingKeys, options, component]);

  const emit = useMemo(
    () => (event: string) =>
      options.warn({
        code: "emit-reserved",
        message: `emit(${JSON.stringify(event)}) from ${latest.current.id} ignored: events are reserved for a later version`
      }),
    [options]
  );

  const bindings: Record<string, ComposerBinding> = {};
  for (const [key, binding] of Object.entries(node.bindings)) {
    bindings[key] = { value: binding.value, onChange: handlers[key] };
  }

  const base = {
    node: { id: node.id, type: node.type },
    bindings,
    value: bindings.value?.value,
    onChange: bindings.value?.onChange,
    defaultHint: node.defaultHint,
    issues: node.issues,
    children: content.children,
    slots: content.slots ?? EMPTY_SLOTS,
    rows,
    readOnly,
    emit
  };

  const placeholder = (kind: "Unsupported" | "ElementError", message: string, extra: Record<string, unknown> = {}) => {
    const props = { type: node.type, message, ...extra };
    const registered = registry.get(kind);
    const Render = registered ? renderable(registered) : kind === "Unsupported" ? UnsupportedPlaceholder : ElementErrorPlaceholder;
    return <Render {...base} props={props} />;
  };

  let body: ReactNode;
  if (!component || node.error?.code === "unknown-type") {
    body = placeholder("Unsupported", node.error?.message ?? `component type ${node.type} is not registered`);
  } else if (node.error) {
    body = placeholder("ElementError", node.error.message, { error: node.error });
  } else {
    const Render = renderable(component);
    body = <Render {...base} props={node.props} />;
  }

  return (
    <ElementBoundary node={node} fallback={(error) => placeholder("ElementError", error.message, { error: { code: "render", message: error.message } })}>
      {body}
    </ElementBoundary>
  );
});

// ---------------------------------------------------------- components

const renderables = new WeakMap<ComposerComponent<unknown>, (p: ComposerComponentProps<unknown>) => ReactNode>();

/** A stable React component per ComposerComponent, so its render may use hooks. */
function renderable(component: ComposerComponent<unknown>): (p: ComposerComponentProps<unknown>) => ReactNode {
  let Render = renderables.get(component);
  if (!Render) {
    Render = (p) => component.render(p);
    Object.defineProperty(Render, "name", { value: `Composer(${component.type})` });
    renderables.set(component, Render);
  }
  return Render;
}

export interface PlaceholderProps {
  type: string;
  message: string;
  error?: { code: string; message: string };
}

function UnsupportedPlaceholder(p: ComposerComponentProps<unknown>) {
  const props = p.props as PlaceholderProps;
  return (
    <div className="xflow-composer-unsupported" data-composer-placeholder="unsupported" data-composer-node={p.node.id} role="note">
      <span className="xflow-composer-placeholder-message">{props.message}</span>
      {p.children}
      {Object.entries(p.slots).map(([name, slot]) => (
        <Fragment key={name}>{slot}</Fragment>
      ))}
    </div>
  );
}

function ElementErrorPlaceholder(p: ComposerComponentProps<unknown>) {
  const props = p.props as PlaceholderProps;
  return (
    <div className="xflow-composer-element-error" data-composer-placeholder="error" data-composer-node={p.node.id} role="alert">
      <span className="xflow-composer-placeholder-message">{props.message}</span>
      {p.children}
      {Object.entries(p.slots).map(([name, slot]) => (
        <Fragment key={name}>{slot}</Fragment>
      ))}
    </div>
  );
}

// -------------------------------------------------------- error boundary

interface BoundaryProps {
  node: ResolvedNode;
  fallback(error: Error): ReactNode;
  children: ReactNode;
}

interface BoundaryState {
  node: ResolvedNode;
  error: Error | null;
}

/** Per-element boundary; resets as soon as the element's node (its inputs) changes. */
class ElementBoundary extends Component<BoundaryProps, BoundaryState> {
  state: BoundaryState = { node: this.props.node, error: null };

  static getDerivedStateFromProps(props: BoundaryProps, state: BoundaryState): Partial<BoundaryState> | null {
    return props.node !== state.node ? { node: props.node, error: null } : null;
  }

  static getDerivedStateFromError(error: unknown): Partial<BoundaryState> {
    return { error: error instanceof Error ? error : new Error(String(error)) };
  }

  componentDidCatch(_error: Error, _info: ErrorInfo): void {
    // React already reports the error; the placeholder carries the message.
  }

  render(): ReactNode {
    return this.state.error ? this.props.fallback(this.state.error) : this.props.children;
  }
}
