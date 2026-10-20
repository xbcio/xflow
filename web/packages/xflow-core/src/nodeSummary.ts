import type { Condition, NodeFormField, NodeFormSchema } from "./nodeForm";

/**
 * The node card's configuration summary (spec §6.1.1). A descriptor names the
 * parameters it wants on the card (`card_summary`); this module turns those
 * names plus the node's runtime parameters into the rows the preview draws.
 *
 * One entry may join two parameters with "+" (`method+url`): they share one
 * row, and a blank half is dropped rather than leaving a dangling separator.
 * An optional empty value produces no row ("空值不显示"); a required value that
 * is missing draws the spec's danger line instead ("<label> 未填写"). A type
 * that declares no card_summary falls back to its first three required
 * parameters that carry a short scalar value (spec §6.1.1).
 */
export type CardSummaryValueKind = "plain" | "mono" | "expr" | "ref" | "secret" | "missing";

/** One segment of a joined row; each keeps the formatting of its own parameter. */
export interface CardSummarySegment {
  text: string;
  kind: CardSummaryValueKind;
}

export interface CardSummaryRow {
  /** The parameter's display name, as the (localized) schema labels it. */
  label: string;
  /** The runtime value, formatted for one card line. */
  value: string;
  /** How the preview draws it: expression, credential reference, masked secret, missing, or text. */
  kind: CardSummaryValueKind;
  /** Present for an a+b row: the values in order, each retaining its own kind. */
  parts?: CardSummarySegment[];
  /** JSON Pointer of the field the row points at (the §6.1.1 click target). */
  path?: string;
}

/** Longest value a card line carries before it is clipped; the full value is a click away. */
const MAX_VALUE_LENGTH = 80;

/** Widgets whose value is a payload, not a label: the card prints its size, never its body. */
const payloadWidgets = new Set(["code", "textarea", "json"]);

function isPresent(value: unknown): boolean {
  if (value === undefined || value === null) return false;
  if (typeof value === "string") return value.trim() !== "";
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === "object") return Object.keys(value as Record<string, unknown>).length > 0;
  return true;
}

function clip(text: string): string {
  return text.length > MAX_VALUE_LENGTH ? `${text.slice(0, MAX_VALUE_LENGTH - 1)}…` : text;
}

/**
 * Whether a `required_when` / `visible_when` condition holds for the node's
 * parameters. The card needs the same answer the form's validation used, so
 * the clause semantics mirror the wire contract (all present clauses AND).
 */
function conditionHolds(
  condition: Condition | null | undefined,
  parameters: Record<string, unknown> | undefined
): boolean {
  if (!condition) return true;
  const holds = (clause: Condition): boolean => {
    if (clause.param !== undefined) {
      const value = parameters?.[clause.param];
      if (clause.eq !== undefined && !Object.is(value, clause.eq)) return false;
      if (clause.in !== undefined && !clause.in.some((candidate) => Object.is(candidate, value))) return false;
      if (clause.truthy !== undefined && Boolean(value) !== clause.truthy) return false;
    }
    if (clause.all_of && !clause.all_of.every(holds)) return false;
    if (clause.any_of && !clause.any_of.some(holds)) return false;
    if (clause.not && holds(clause.not)) return false;
    return true;
  };
  return holds(condition);
}

function isRequired(field: NodeFormField, parameters: Record<string, unknown> | undefined): boolean {
  if (field.required) return true;
  if (field.required_when === undefined || field.required_when === null) return false;
  return conditionHolds(field.required_when, parameters);
}

function isVisible(field: NodeFormField, parameters: Record<string, unknown> | undefined): boolean {
  return conditionHolds(field.visible_when, parameters);
}

function lineCount(text: string): number {
  return text.split(/\r?\n/).length;
}

interface FormattedValue {
  text: string;
  kind: CardSummaryValueKind;
}

/** Go duration strings (`5s`, `1m30s`, `72h`), drawn as the spec's `5 s` / `72h`. */
const DURATION_UNITS = /^\s*(\d+(?:\.\d+)?)(ms|s|m|h)\s*$/;

function formatDuration(value: string): string {
  const match = DURATION_UNITS.exec(value);
  if (!match) return clip(value);
  const amount = Number(match[1]);
  if (match[2] === "h") return `${amount}h`;
  if (match[2] === "m") return amount % 60 === 0 ? `${amount / 60}h` : `${amount}m`;
  if (match[2] === "s") return amount >= 60 && amount % 60 === 0 ? `${amount / 60}m` : `${amount} s`;
  return `${amount} ms`;
}

/** The language a code payload is written in, for "JavaScript · 42 行". */
function codeLanguage(
  fields: readonly NodeFormField[],
  parameters: Record<string, unknown> | undefined,
  nodeType: string | undefined
): string | undefined {
  const raw = parameters?.language;
  if (isPresent(raw)) {
    const field = fields.find((candidate) => candidate.name === "language");
    const option = field?.options?.find((entry) => Object.is(entry.value, raw));
    if (option?.label) return option.label;
    if (typeof raw === "string") return raw;
  }
  return nodeType === "xflow.function" ? "JavaScript" : undefined;
}

function formatValue(field: NodeFormField | undefined, value: unknown, language?: string): FormattedValue | undefined {
  if (!isPresent(value)) return undefined;
  if (field?.secret) return { text: "••••••", kind: "secret" };
  if (field?.widget === "credential-select") {
    // A credential is referenced by name; its value never reaches the card.
    const referenceOf = (candidate: unknown): string | undefined => {
      if (typeof candidate === "string" || typeof candidate === "number") return String(candidate);
      if (candidate && typeof candidate === "object") {
        const reference = (candidate as { ref?: unknown }).ref ?? (candidate as { name?: unknown }).name;
        return typeof reference === "string" || typeof reference === "number" ? String(reference) : undefined;
      }
      return undefined;
    };
    if (Array.isArray(value)) {
      const names = value.map(referenceOf).filter((name): name is string => name !== undefined);
      return { text: clip(names.length > 0 ? names.join(" → ") : `${value.length} 个凭证`), kind: "ref" };
    }
    const reference = referenceOf(value);
    // An object that names no credential has nothing to show; never print `undefined`.
    return reference === undefined ? undefined : { text: clip(reference), kind: "ref" };
  }
  if (typeof value === "boolean") return { text: value ? "是" : "否", kind: "plain" };
  if (typeof value === "number") return { text: String(value), kind: "plain" };
  if (typeof value === "string") {
    // §6.1.1: an expression shows itself. This comes before the payload check:
    // xflow.function's inline body is a `code` parameter whose widget is an expression.
    if (field?.expression?.mode === "pure" || field?.widget === "expression") {
      return { text: clip(value), kind: "expr" };
    }
    if (field?.widget && payloadWidgets.has(field.widget)) {
      return { text: `${language ? `${language} · ` : ""}${lineCount(value)} 行`, kind: "plain" };
    }
    if (field?.widget === "duration") return { text: formatDuration(value), kind: "plain" };
    const option = field?.options?.find((entry) => Object.is(entry.value, value));
    if (option?.label) return { text: option.label, kind: "plain" };
    if (value.includes("{{")) return { text: clip(value), kind: "mono" };
    return { text: clip(value), kind: "plain" };
  }
  if (Array.isArray(value)) {
    const scalars = value.every(
      (item) => typeof item === "string" || typeof item === "number" || typeof item === "boolean"
    );
    // A chain reads as a chain ("ops → finance"); structured items only count.
    return scalars
      ? { text: clip(value.map((item) => String(item)).join(" → ")), kind: "plain" }
      : { text: `${value.length} 条`, kind: "plain" };
  }
  if (typeof value === "object") {
    const text = JSON.stringify(value);
    return text ? { text: clip(text), kind: "mono" } : undefined;
  }
  return undefined;
}

function rowFor(
  entry: string,
  fields: readonly NodeFormField[],
  parameters: Record<string, unknown> | undefined,
  language?: string
): CardSummaryRow | undefined {
  const names = entry.split("+").map((name) => name.trim()).filter(Boolean);
  const parts: { field: NodeFormField; formatted: FormattedValue }[] = [];
  for (const name of names) {
    const value = parameters?.[name];
    // A declared name without a projected field still shows the parameter, under
    // its wire name: a runner schema may describe fewer fields than it accepts.
    const field = fields.find((candidate) => candidate.name === name)
      ?? { name, path: `/parameters/${name}`, type: "string" as const };
    // §6.1.1: a required value that is missing is itself the card line, in
    // danger text — the same statement the form's validation makes.
    if (!isPresent(value)) {
      if (isRequired(field, parameters) && isVisible(field, parameters)) {
        const label = field.label ?? field.name;
        return { label, value: `${label} 未填写`, kind: "missing", path: field.path };
      }
      continue;
    }
    const formatted = formatValue(field, value, language);
    if (formatted) parts.push({ field, formatted });
  }
  if (parts.length === 0) return undefined;
  // Each half of an a+b row keeps its own formatting (a URL stays mono beside a
  // plain method); the preview draws the segments when there is more than one.
  const segments = parts.map((part) => ({ text: part.formatted.text, kind: part.formatted.kind }));
  const row: CardSummaryRow = {
    label: parts.map((part) => part.field.label ?? part.field.name).join(" + "),
    value: segments.map((segment) => segment.text).join(" "),
    kind: parts.length === 1 ? parts[0].formatted.kind : "plain",
    path: parts[0].field.path
  };
  if (segments.length > 1) row.parts = segments;
  return row;
}

/** §6.1.1's fallback: the first three required parameters with a short scalar value. */
function fallbackEntries(
  fields: readonly NodeFormField[],
  parameters: Record<string, unknown> | undefined
): string[] {
  const names: string[] = [];
  for (const field of fields) {
    if (names.length >= 3) break;
    if (!field.name || !isRequired(field, parameters) || !isVisible(field, parameters)) continue;
    const value = parameters?.[field.name];
    if (!isPresent(value)) continue;
    if (field.type === "object" || field.type === "array") continue;
    if (field.widget && payloadWidgets.has(field.widget)) continue;
    if (typeof value === "string" && (value.length > MAX_VALUE_LENGTH || value.includes("\n"))) continue;
    names.push(field.name);
  }
  return names;
}

/**
 * The card's summary rows for one node: `card_summary` entries in declaration
 * order, values from the node's parameters. An optional empty value is dropped;
 * a required one that is missing draws the spec's danger row. A schema that
 * omits `card_summary` falls back to its first three required parameters with a
 * short scalar value, while an explicit empty array turns the summary off. The
 * card caps the list at three lines; this returns them all so the caller can
 * render its own "+n 项" row.
 */
export function cardSummaryRows(
  schema: Pick<NodeFormSchema, "fields" | "card_summary"> & { node_type?: string },
  parameters: Record<string, unknown> | undefined
): CardSummaryRow[] {
  const declared = Array.isArray(schema.card_summary) ? schema.card_summary : undefined;
  if (declared && declared.length === 0) return [];
  const fields = Array.isArray(schema.fields) ? schema.fields : [];
  const language = codeLanguage(fields, parameters, schema.node_type);
  const entries = declared ?? fallbackEntries(fields, parameters);
  const rows: CardSummaryRow[] = [];
  for (const entry of entries) {
    if (typeof entry !== "string") continue;
    const row = rowFor(entry, fields, parameters, language);
    if (row) rows.push(row);
  }
  return rows;
}
