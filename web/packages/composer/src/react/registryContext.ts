// The registry of the enclosing <Composer>, for components that compose
// other registered components by type name (Doc B §5 "组件组合", e.g.
// KeyValue `valueType`). Specs carry only the type name; the component
// looks it up here, so hosts that extend the registry get their own types.

import { createContext, useContext } from "react";
import type { Registry } from "./contract";

export const RegistryContext = createContext<Registry | null>(null);

/** The registry of the enclosing <Composer>, or null outside one. */
export function useComposerRegistry(): Registry | null {
  return useContext(RegistryContext);
}
