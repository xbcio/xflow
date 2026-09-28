// @xflow/composer/kernel-json-render — default kernel on @json-render 0.21.0
// (Doc B §7.2).
//
// json-render only renders the tree. Every render converts the ResolvedTree
// into json-render's flat spec: one element per ResolvedNode, keyed by the
// node id, of the single generic type `ComposerNode`, whose only (static)
// prop is that id. Children and named slots map onto json-render's
// `children` / `slots` element fields. `ComposerNode` looks the ResolvedNode
// up by id in a React context and calls env.renderNode.
//
// None of json-render's semantics are used: the spec never carries
// `visible`, `repeat`, `on`, `watch`, `$state`/`$bind*` expressions or
// validation, and the StateProvider / VisibilityProvider / ActionProvider
// below are the empty providers its Renderer requires to mount.

import { ActionProvider, Renderer, StateProvider, VisibilityProvider, type ComponentRegistry, type ComponentRenderProps, type Spec } from "@json-render/react";
import { createContext, useContext, useMemo, type ReactNode } from "react";
import type { ResolvedNode, ResolvedTree } from "../core";
import type { KernelEnv, RenderKernel } from "../react/contract";

const NODE_TYPE = "ComposerNode";
const EMPTY_STATE: Record<string, unknown> = Object.freeze({}) as Record<string, unknown>;
const EMPTY_SLOTS: Record<string, ReactNode> = Object.freeze({}) as Record<string, ReactNode>;

interface TreeContextValue {
  nodes: ReadonlyMap<string, ResolvedNode>;
  env: KernelEnv;
}

const TreeContext = createContext<TreeContextValue | null>(null);

interface ComposerNodeProps {
  id: string;
}

function ComposerNode({ element, children, slots }: ComponentRenderProps<ComposerNodeProps>) {
  const tree = useContext(TreeContext);
  const node = tree?.nodes.get(element.props.id);
  if (!tree || !node) return null;
  return tree.env.renderNode(node, { children, slots: slots ?? EMPTY_SLOTS });
}

const registry: ComponentRegistry = { [NODE_TYPE]: ComposerNode };

/** ResolvedTree -> json-render flat spec (static props only). */
export function toJsonRenderSpec(tree: ResolvedTree): Spec | null {
  if (!tree.root) return null;
  const elements: Spec["elements"] = {};
  const visit = (node: ResolvedNode) => {
    const element: Spec["elements"][string] = { type: NODE_TYPE, props: { id: node.id } };
    if (node.children.length > 0) element.children = node.children.map((child) => child.id);
    const slotNames = Object.keys(node.slots);
    if (slotNames.length > 0) {
      element.slots = {};
      for (const name of slotNames) element.slots[name] = node.slots[name].map((child) => child.id);
    }
    elements[node.id] = element;
    node.children.forEach(visit);
    for (const list of Object.values(node.slots)) list.forEach(visit);
  };
  visit(tree.root);
  return { root: tree.root.id, elements };
}

function indexNodes(tree: ResolvedTree): Map<string, ResolvedNode> {
  const nodes = new Map<string, ResolvedNode>();
  const visit = (node: ResolvedNode) => {
    nodes.set(node.id, node);
    node.children.forEach(visit);
    for (const list of Object.values(node.slots)) list.forEach(visit);
  };
  if (tree.root) visit(tree.root);
  return nodes;
}

function JsonRenderHost({ tree, env }: { tree: ResolvedTree; env: KernelEnv }) {
  const spec = useMemo(() => toJsonRenderSpec(tree), [tree]);
  const context = useMemo(() => ({ nodes: indexNodes(tree), env }), [tree, env]);
  return (
    <StateProvider initialState={EMPTY_STATE}>
      <VisibilityProvider>
        <ActionProvider>
          <TreeContext.Provider value={context}>
            <Renderer spec={spec} registry={registry} />
          </TreeContext.Provider>
        </ActionProvider>
      </VisibilityProvider>
    </StateProvider>
  );
}

export const jsonRenderKernel: RenderKernel = {
  name: "json-render",
  render(tree: ResolvedTree, env: KernelEnv): ReactNode {
    return <JsonRenderHost tree={tree} env={env} />;
  }
};

export default jsonRenderKernel;
