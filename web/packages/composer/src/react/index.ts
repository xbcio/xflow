// @xflow/composer/react — <Composer>, the component/kernel contracts,
// createRegistry and renderNode (Doc B §5–§7).

export { Composer, validationResult } from "./Composer";
export type {
  ComposerBinding,
  ComposerComponent,
  ComposerComponentProps,
  ComposerProps,
  ComposerRow,
  KernelContent,
  KernelEnv,
  Registry,
  RenderKernel,
  ValidationResult
} from "./contract";
export { defaultKernel } from "./defaultKernel";
export { OVERLAY_TIMEOUT_MS } from "./overlay";
export { createRegistry, type CreateRegistryOptions } from "./registry";
export { createNodeRenderer, ROW_TYPE, type NodeRendererOptions, type PlaceholderProps } from "./renderNode";
