/**
 * Layout breakpoints (spec §7), the single source for JS and CSS, measured on
 * the workbench itself rather than the window: at or below `compact` both side
 * panels collapse to drawers (the comparison is inclusive, so a 1280-wide host
 * is compact). `narrow` marks the narrower band, kept for callers that need it.
 */
export const breakpoints = { compact: 1280, narrow: 1024 } as const;
