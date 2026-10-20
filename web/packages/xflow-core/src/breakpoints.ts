/**
 * Layout breakpoints (spec §7), the single source for JS and CSS: the editor
 * collapses the left panel under `compact` and both side panels under `narrow`,
 * measured on the workbench itself rather than the window.
 */
export const breakpoints = { compact: 1280, narrow: 1024 } as const;
