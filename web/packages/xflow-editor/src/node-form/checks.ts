// Host check types emitted by compileNodeForm (registered via composer
// defineCheck, Doc B §8). The host MUST register `nodeFormChecks` with the
// Composer's check registry; validateSpec rejects unregistered check types.

import { builtinChecks, containsExpression, defineCheck, type CheckDefinition } from "@xflow/composer/core";

const builtinByType = new Map(builtinChecks.map((def) => [def.type, def]));

function innerArgs(args: Record<string, unknown>): Record<string, unknown> {
  const inner = args.args;
  return inner !== null && typeof inner === "object" && !Array.isArray(inner) ? (inner as Record<string, unknown>) : {};
}

function validateWrapped(args: Record<string, unknown>): string[] {
  const def = typeof args.check === "string" ? builtinByType.get(args.check) : undefined;
  if (!def) return [`args.check must name a builtin check`];
  if (def.type === "required" || def.type === "oneOf") return [`args.check ${def.type} cannot be wrapped`];
  const unexpected = Object.keys(args).filter((key) => key !== "check" && key !== "args");
  return [...unexpected.map((key) => `unexpected args.${key}`), ...(def.validateArgs?.(innerArgs(args)) ?? [])];
}

/**
 * Template-mode rule gates (Doc C §4.1: "切到表达式后只保留 required，其他规则降为
 * warning"). The compiler emits a rule twice: `literalOnly` with the rule's own
 * severity, and `expressionOnly` with severity warning. Each delegates to the
 * builtin named by `args.check` with `args.args`.
 */
export const literalOnlyCheck = defineCheck(
  "literalOnly",
  (value, args, ctx) => {
    if (containsExpression(value)) return [];
    return builtinByType.get(args.check as string)?.fn(value, innerArgs(args), ctx) ?? [];
  },
  { validateArgs: validateWrapped }
);

export const expressionOnlyCheck = defineCheck(
  "expressionOnly",
  (value, args, ctx) => {
    if (!containsExpression(value)) return [];
    return builtinByType.get(args.check as string)?.fn(value, innerArgs(args), ctx) ?? [];
  },
  { validateArgs: validateWrapped }
);

const noArgs = (args: Record<string, unknown>) => Object.keys(args).map((key) => `unexpected args.${key}`);

/** `literal` mode (host source, e.g. script.code): `${{ }}` in code is not rendered (Doc C §4.1). */
export const noTemplateInCodeCheck = defineCheck(
  "noTemplateInCode",
  (value) => (containsExpression(value) ? [{}] : []),
  { validateArgs: noArgs }
);

/** `none` mode (trigger params, map.body, common fields): the value is never evaluated (Doc C §4.1). */
export const notEvaluatedCheck = defineCheck(
  "notEvaluated",
  (value) => (containsExpression(value) ? [{}] : []),
  { validateArgs: noArgs }
);

// Credential-looking literals in free maps (Doc C §1 rule 4). Advisory only.
const SECRET_PATTERNS: readonly RegExp[] = [
  /^\s*Bearer\s+[A-Za-z0-9._~+/=-]{8,}/i,
  /^\s*Basic\s+[A-Za-z0-9+/=]{8,}\s*$/i,
  /\bAKIA[0-9A-Z]{16}\b/,
  /-----BEGIN [A-Z ]*PRIVATE KEY-----/,
  /\bgh[pousr]_[A-Za-z0-9]{36,}\b/,
  /\bxox[abprs]-[A-Za-z0-9-]{10,}\b/
];

export function looksLikeSecret(text: string): boolean {
  if (containsExpression(text)) return false;
  return SECRET_PATTERNS.some((pattern) => pattern.test(text));
}

export const secretLikeCheck = defineCheck(
  "secretLike",
  (value) => {
    if (value === null || typeof value !== "object" || Array.isArray(value)) return [];
    const hits = Object.entries(value as Record<string, unknown>)
      .filter(([, entry]) => typeof entry === "string" && looksLikeSecret(entry))
      .map(([key]) => key);
    return hits.length > 0 ? [{ detail: hits.join(", ") }] : [];
  },
  { validateArgs: noArgs }
);

/** Every check type compileNodeForm may emit beyond the composer builtins. */
export const nodeFormChecks: readonly CheckDefinition[] = [
  literalOnlyCheck,
  expressionOnlyCheck,
  noTemplateInCodeCheck,
  notEvaluatedCheck,
  secretLikeCheck
];
