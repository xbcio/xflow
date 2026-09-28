// @xflow/composer/kernel-native — reference/fallback kernel (Doc B §7.3).
//
// A plain recursive renderer: for every node it renders the default-slot
// children and each named slot first, then hands them to env.renderNode.
// Keys are node ids, so repeat rows keep their React identity across
// insert/delete/reorder. `$row` nodes are walked like any other node.

import { Fragment, type ReactNode } from "react";
import type { ResolvedNode, ResolvedTree } from "../core";
import type { KernelEnv, RenderKernel } from "../react/contract";

function renderList(nodes: readonly ResolvedNode[], env: KernelEnv): ReactNode[] | undefined {
  return nodes.length === 0 ? undefined : nodes.map((node) => renderTree(node, env));
}

// resolve() shares unchanged subtrees by reference, so a subtree already
// rendered for this env is reused without walking it again.
const rendered = new WeakMap<KernelEnv, WeakMap<ResolvedNode, ReactNode>>();

function renderTree(node: ResolvedNode, env: KernelEnv): ReactNode {
  let cache = rendered.get(env);
  if (!cache) rendered.set(env, (cache = new WeakMap()));
  const hit = cache.get(node);
  if (hit !== undefined) return hit;
  const slots: Record<string, ReactNode> = {};
  for (const [name, list] of Object.entries(node.slots)) slots[name] = renderList(list, env) ?? [];
  const children = renderList(node.children, env);
  const element = <Fragment key={node.id}>{env.renderNode(node, { children, slots })}</Fragment>;
  cache.set(node, element);
  return element;
}

export const nativeKernel: RenderKernel = {
  name: "native",
  render(tree: ResolvedTree, env: KernelEnv): ReactNode {
    return tree.root ? renderTree(tree.root, env) : null;
  }
};

export default nativeKernel;
