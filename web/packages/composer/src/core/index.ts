// @xflow/composer/core — framework-free semantics of the composer engine
// (Doc B §2–§6). Must never import React or any renderer.

export * from "./types";
export {
  builtinChecks,
  createCheckRegistry,
  defineCheck,
  type CheckContext,
  type CheckDefinition,
  type CheckFn,
  type CheckIssue,
  type CheckRegistry,
  type OneOfMode
} from "./checks";
export { evalCond, type CondResult, type CondScope } from "./cond";
export {
  builtinExpressions,
  createExpressionRegistry,
  defineExpression,
  evaluateValue,
  ExpressionError,
  type ExpressionContext,
  type ExpressionDefinition,
  type ExpressionRegistry,
  type RowScope
} from "./expressions";
export { FORMATS, containsExpression, isCron, isGoDuration, isHostPort, isUrl, type FormatName } from "./formats";
export { deepEqual, formatPointer, getPointer, isSet, jsTruthy, parsePointer } from "./pointer";
export { parseProps, zodProps, type PropsParseResult, type PropsSchema } from "./props";
export { resolve, type ResolveInput } from "./resolve";
export { adoptRowIdentity, rowUids } from "./rows";
export { resolveTarget, sameTarget } from "./targets";
export { isFutureMinor, validateSpec, type SpecRegistries } from "./validate";
export {
  applyPatches,
  createPatchBatch,
  isEmptyValue,
  mergePatches,
  write,
  type PatchBatch,
  type WriteState
} from "./write";
