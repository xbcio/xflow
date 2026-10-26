/**
 * Formats a node timeout given in nanoseconds. The value is divided down to the
 * largest of h/m/s that represents it exactly; when it is not a whole number of
 * seconds the raw nanosecond count is kept (`1500000000ns`) instead of rounding.
 */
export function formatNodeTimeout(ns: number): string {
  const seconds = ns / 1_000_000_000;
  if (seconds >= 3600 && Number.isInteger(seconds / 3600)) return `${seconds / 3600}h`;
  if (seconds >= 60 && Number.isInteger(seconds / 60)) return `${seconds / 60}m`;
  if (Number.isInteger(seconds)) return `${seconds}s`;
  return `${ns}ns`;
}
