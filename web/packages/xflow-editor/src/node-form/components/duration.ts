// Go time.Duration helpers for DurationInput: parse the text form, and
// render nanoseconds for display. Display only — nothing here is written.

const UNIT_NS: Readonly<Record<string, number>> = {
  ns: 1,
  us: 1e3,
  "\u00b5s": 1e3,
  "\u03bcs": 1e3,
  ms: 1e6,
  s: 1e9,
  m: 6e10,
  h: 3.6e12
};

const PART = /(\d+(?:\.\d*)?|\.\d+)(ns|us|\u00b5s|\u03bcs|ms|s|m|h)/y;

/** Nanoseconds of a Go duration string (time.ParseDuration syntax), or null. */
export function parseGoDuration(text: string): number | null {
  let rest = text.trim();
  let sign = 1;
  if (rest.startsWith("-") || rest.startsWith("+")) {
    if (rest[0] === "-") sign = -1;
    rest = rest.slice(1);
  }
  if (rest === "0") return 0;
  if (rest === "") return null;
  let total = 0;
  PART.lastIndex = 0;
  while (PART.lastIndex < rest.length) {
    const match = PART.exec(rest);
    if (!match) return null;
    total += Number(match[1]) * UNIT_NS[match[2]];
  }
  return sign * total;
}

/** "1 小时 30 分 1.5 秒", "500 毫秒", "0 秒". */
export function humanizeNs(ns: number): string {
  if (!Number.isFinite(ns)) return String(ns);
  if (ns === 0) return "0 秒";
  const sign = ns < 0 ? "-" : "";
  let rest = Math.abs(ns);
  if (rest < 1e3) return `${sign}${round(rest)} 纳秒`;
  if (rest < 1e6) return `${sign}${round(rest / 1e3)} 微秒`;
  if (rest < 1e9) return `${sign}${round(rest / 1e6)} 毫秒`;
  const hours = Math.floor(rest / 3.6e12);
  rest -= hours * 3.6e12;
  const minutes = Math.floor(rest / 6e10);
  rest -= minutes * 6e10;
  const parts: string[] = [];
  if (hours > 0) parts.push(`${hours} 小时`);
  if (minutes > 0) parts.push(`${minutes} 分`);
  if (rest > 0) parts.push(`${round(rest / 1e9)} 秒`);
  return sign + parts.join(" ");
}

/** Go-style compact text of nanoseconds: "1h30m", "1.5s", "250ms". */
export function formatGoDuration(ns: number): string {
  if (!Number.isFinite(ns)) return String(ns);
  if (ns === 0) return "0s";
  const sign = ns < 0 ? "-" : "";
  let rest = Math.abs(ns);
  if (rest < 1e3) return `${sign}${round(rest)}ns`;
  if (rest < 1e6) return `${sign}${round(rest / 1e3)}µs`;
  if (rest < 1e9) return `${sign}${round(rest / 1e6)}ms`;
  let out = "";
  const hours = Math.floor(rest / 3.6e12);
  rest -= hours * 3.6e12;
  const minutes = Math.floor(rest / 6e10);
  rest -= minutes * 6e10;
  if (hours > 0) out += `${hours}h`;
  if (minutes > 0) out += `${minutes}m`;
  if (rest > 0) out += `${round(rest / 1e9)}s`;
  return sign + out;
}

function round(value: number): string {
  return String(Math.round(value * 1000) / 1000);
}

export interface DisplayUnit {
  key: string;
  label: string;
  /** Size of the unit in wire units (ns or ms). */
  factor: number;
}

const NS_UNITS: readonly DisplayUnit[] = [
  { key: "ns", label: "纳秒", factor: 1 },
  { key: "us", label: "微秒", factor: 1e3 },
  { key: "ms", label: "毫秒", factor: 1e6 },
  { key: "s", label: "秒", factor: 1e9 },
  { key: "m", label: "分", factor: 6e10 },
  { key: "h", label: "小时", factor: 3.6e12 }
];

const MS_UNITS: readonly DisplayUnit[] = [
  { key: "ms", label: "毫秒", factor: 1 },
  { key: "s", label: "秒", factor: 1e3 },
  { key: "m", label: "分", factor: 6e4 },
  { key: "h", label: "小时", factor: 3.6e6 }
];

export function displayUnits(wire: "ns" | "ms"): readonly DisplayUnit[] {
  return wire === "ns" ? NS_UNITS : MS_UNITS;
}

/** Nanoseconds per wire unit. */
export function wireToNs(wire: "ns" | "ms"): number {
  return wire === "ns" ? 1 : 1e6;
}

/** Largest unit that shows `value` as a whole number; seconds for 0 / unset. */
export function bestUnit(value: number | undefined, wire: "ns" | "ms"): DisplayUnit {
  const units = displayUnits(wire);
  const seconds = units.find((unit) => unit.key === "s")!;
  if (value === undefined || value === 0 || !Number.isFinite(value)) return seconds;
  for (let index = units.length - 1; index >= 0; index--) {
    if (Number.isInteger(value / units[index].factor)) return units[index];
  }
  return units[0];
}
