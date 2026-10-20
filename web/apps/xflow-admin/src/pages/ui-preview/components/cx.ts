/** Joins class names, dropping falsy parts. Keeps the preview free of a clsx dep. */
export function cx(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(' ');
}
