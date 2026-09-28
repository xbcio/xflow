// Built-in `format` validators (Doc B §3.5, Doc A §2.2 "Format 的执行范围").

import { isPlainObject } from "./pointer";

export type FormatName = "duration" | "cron" | "json" | "expression" | "sha256-digest" | "url" | "host-port";

// Go time.ParseDuration: optional sign, then "0" or one or more
// <decimal><unit> groups, units ns|us|µs|μs|ms|s|m|h.
const DURATION = /^[-+]?(?:0|(?:(?:\d+(?:\.\d*)?|\.\d+)(?:ns|us|\u00b5s|\u03bcs|ms|s|m|h))+)$/;

export function isGoDuration(value: string): boolean {
  return DURATION.test(value);
}

const CRON_DESCRIPTORS = new Set(["@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly"]);
const MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
const DAYS = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

interface CronField {
  min: number;
  max: number;
  names?: string[];
  nameBase?: number;
  question?: boolean;
}

const MINUTE: CronField = { min: 0, max: 59 };
const HOUR: CronField = { min: 0, max: 23 };
const DOM: CronField = { min: 1, max: 31, question: true };
const MONTH: CronField = { min: 1, max: 12, names: MONTHS, nameBase: 1 };
const DOW: CronField = { min: 0, max: 7, names: DAYS, nameBase: 0, question: true };

/** Basic 5-field (m h dom mon dow) or 6-field (s m h dom mon dow) cron, plus descriptors. */
export function isCron(value: string): boolean {
  const trimmed = value.trim();
  if (CRON_DESCRIPTORS.has(trimmed)) return true;
  if (trimmed.startsWith("@every ")) return isGoDuration(trimmed.slice(7).trim());
  const fields = trimmed.split(/\s+/);
  const layout =
    fields.length === 5
      ? [MINUTE, HOUR, DOM, MONTH, DOW]
      : fields.length === 6
        ? [MINUTE /* seconds share 0-59 */, MINUTE, HOUR, DOM, MONTH, DOW]
        : null;
  if (!layout) return false;
  return fields.every((field, index) => cronField(field, layout[index]));
}

function cronField(field: string, spec: CronField): boolean {
  if (field === "?") return spec.question === true;
  return field.split(",").every((part) => {
    const [range, step, ...rest] = part.split("/");
    if (rest.length > 0) return false;
    if (step !== undefined && !/^[1-9]\d*$/.test(step)) return false;
    if (range === "*") return true;
    const bounds = range.split("-");
    if (bounds.length > 2) return false;
    const numbers = bounds.map((bound) => cronValue(bound, spec));
    if (numbers.some((n) => n === null)) return false;
    return bounds.length === 1 || (numbers[0] as number) <= (numbers[1] as number);
  });
}

function cronValue(token: string, spec: CronField): number | null {
  if (/^\d+$/.test(token)) {
    const n = Number(token);
    return n >= spec.min && n <= spec.max ? n : null;
  }
  const index = spec.names?.indexOf(token.toUpperCase()) ?? -1;
  return index >= 0 ? index + (spec.nameBase ?? 0) : null;
}

export function isJsonText(value: string): boolean {
  try {
    JSON.parse(value);
    return true;
  } catch {
    return false;
  }
}

const TEMPLATE = /\$?\{\{[\s\S]*?\}\}/;

/** "Contains counts" (Doc A §2.2): any `${{ }}` or `{{ }}` anywhere, recursively. */
export function containsExpression(value: unknown): boolean {
  if (typeof value === "string") return TEMPLATE.test(value);
  if (Array.isArray(value)) return value.some(containsExpression);
  if (isPlainObject(value)) return Object.values(value).some(containsExpression);
  return false;
}

const SHA256_DIGEST = /^sha256:[0-9a-f]{64}$/;

export function isUrl(value: string): boolean {
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\/\S+$/.test(value)) return false;
  try {
    const url = new URL(value);
    return url.host !== "";
  } catch {
    return false;
  }
}

const HOSTNAME = /^(?=.{1,253}$)[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/;
const IPV6 = /^[0-9a-fA-F:.]+(?:%[\w.-]+)?$/;

export function isHostPort(value: string): boolean {
  let host: string;
  let port: string;
  if (value.startsWith("[")) {
    const close = value.indexOf("]");
    if (close < 0 || value[close + 1] !== ":") return false;
    host = value.slice(1, close);
    port = value.slice(close + 2);
    if (!host.includes(":") || !IPV6.test(host)) return false;
  } else {
    const colon = value.lastIndexOf(":");
    if (colon <= 0) return false;
    host = value.slice(0, colon);
    port = value.slice(colon + 1);
    if (!HOSTNAME.test(host)) return false;
  }
  if (!/^\d{1,5}$/.test(port)) return false;
  const n = Number(port);
  return n >= 1 && n <= 65535;
}

export const FORMATS: Record<FormatName, (value: unknown) => boolean> = {
  duration: (value) => typeof value === "string" && isGoDuration(value),
  cron: (value) => typeof value === "string" && isCron(value),
  json: (value) => typeof value !== "string" || isJsonText(value),
  expression: containsExpression,
  "sha256-digest": (value) => typeof value === "string" && SHA256_DIGEST.test(value),
  url: (value) => typeof value === "string" && isUrl(value),
  "host-port": (value) => typeof value === "string" && isHostPort(value)
};

export function isFormatName(name: unknown): name is FormatName {
  return typeof name === "string" && Object.prototype.hasOwnProperty.call(FORMATS, name);
}
