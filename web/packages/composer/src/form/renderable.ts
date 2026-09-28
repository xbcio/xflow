// A stable React component per ComposerComponent, so a component rendered
// inside another one (KeyValue `valueType`) may use hooks — the same
// approach renderNode takes for registered components.

import type { ReactNode } from "react";
import type { ComposerComponent, ComposerComponentProps } from "../react";

type Renderable = (p: ComposerComponentProps<unknown>) => ReactNode;

const renderables = new WeakMap<ComposerComponent<unknown>, Renderable>();

export function renderableOf(component: ComposerComponent<unknown>): Renderable {
  let Render = renderables.get(component);
  if (!Render) {
    Render = (p) => component.render(p);
    Object.defineProperty(Render, "name", { value: `ComposerInline(${component.type})` });
    renderables.set(component, Render);
  }
  return Render;
}
