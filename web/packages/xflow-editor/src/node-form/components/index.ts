// Node-form host components (Doc C §3, deliverable C2) and the full node-form
// registry: composer/form built-ins + host components + nodeFormChecks.

import { createFormRegistry } from "@xflow/composer/form";
import type { ComposerComponent, Registry } from "@xflow/composer/react";
import { nodeFormChecks } from "../checks";
import { Base64Input } from "./Base64Input";
import { DurationInput } from "./DurationInput";
import { ExpressionInput } from "./ExpressionInput";
import { CredentialSelect, PortSelect } from "./Selects";
import { createNodeNameInput, FormNotice, ShapeGuard, type RenameCallback } from "./structural";
import { CronInput, DateTimeInput } from "./TextFormatInputs";

export { Base64Input, bytesToBase64, formatBytes, summarizeBase64, type Base64Summary } from "./Base64Input";
export { DurationInput, type DurationInputProps } from "./DurationInput";
export { ExpressionInput, type ExpressionInputProps } from "./ExpressionInput";
export { CredentialSelect, PortSelect, type CredentialSelectProps, type PortSelectProps } from "./Selects";
export {
  createNodeNameInput,
  FormNotice,
  ShapeGuard,
  type FormNoticeProps,
  type NodeNameInputProps,
  type RenameCallback,
  type ShapeGuardProps
} from "./structural";
export { CronInput, DateTimeInput, isRfc3339, toRfc3339 } from "./TextFormatInputs";
export { formatGoDuration, humanizeNs, parseGoDuration } from "./duration";
export { isExpressionString, matchesShape } from "./shared";

export interface NodeFormRegistryOptions {
  /**
   * Rename callback of `NodeNameInput` (Doc C §4.2, Doc B §5 v1: closed over
   * at registration). Called on blur / Enter with the requested name. The
   * registry is created once, so pass a stable function (e.g. one reading a
   * ref to the editor's current `commitNodeRename`). Without it the name is
   * shown read-only.
   */
  onRename?: RenameCallback;
  /** Debug sink for registry override notices; defaults to `console.debug`. */
  debug?: (message: string) => void;
}

/** The host components, in registration order. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- heterogeneous prop types
export function createNodeFormComponents(options: NodeFormRegistryOptions = {}): ComposerComponent<any>[] {
  return [
    ExpressionInput,
    DurationInput,
    DateTimeInput,
    CronInput,
    Base64Input,
    CredentialSelect,
    PortSelect,
    createNodeNameInput(options.onRename),
    ShapeGuard,
    FormNotice
  ];
}

/** composer/form built-ins + node-form host components + node-form checks. */
export function createNodeFormRegistry(options: NodeFormRegistryOptions = {}): Registry {
  return createFormRegistry(createNodeFormComponents(options), {
    checks: nodeFormChecks,
    ...(options.debug && { debug: options.debug })
  });
}
