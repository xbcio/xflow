/**
 * Level of detail (§6.2): below 75% zoom a card keeps only its category icon,
 * name and status icon, and the remaining pieces are scaled by the inverse of
 * the viewport zoom so they stay legible.
 *
 * The threshold is where the full card stops being readable: at 62% an 11px
 * summary row renders at ~6.8px and a 13px title at ~8px, which is why manual
 * zooming below the threshold drops to low detail. Automatic fits never go
 * there: READABLE_FIT_ZOOM (0.8) keeps the first screen at full detail. Below
 * the threshold the inverse scale lifts the title back to ~13 screen px.
 *
 * The inverse has a ceiling. Without one the factor grows without bound as the
 * user zooms out (1/0.14 = 7.1), so the card's flow-space box outgrows the
 * layout and same-column cards overlap. The design's 45% state sizes the low
 * detail card at roughly 264x74 flow units (icon 44, title 22), which is 1.6x
 * the full card's type scale; that is the ceiling.
 */
export const LOD_ZOOM = 0.75;

/**
 * The zoom at which a card is fully readable, and the lowest zoom an automatic
 * fit may pick. It sits above {@link LOD_ZOOM} so the first screen still renders
 * the full card: parameter summaries, run badges and port labels stay readable
 * instead of dropping to the icon-and-title state. A graph too large for this
 * zoom is anchored at its top-left (its entry column) and panned rather than
 * being shrunk to the 11–14% an "always fit everything" fit would choose.
 */
export const READABLE_FIT_ZOOM = 0.8;

/**
 * Where an already-full card drops its summary after the zoom leaves the
 * readable floor. Re-entering the full card exactly at
 * {@link READABLE_FIT_ZOOM} and dropping it at the same line made a trackpad
 * wobble across 80% flicker every card between 84 and 124 flow px. The one
 * percent hold keeps the decision one-way: a compact card still becomes full at
 * 80% (so an auto fit is always fully readable), but a full card does not drop
 * until the zoom goes below 79%, and comes back only at 80% again. An
 * oscillation around the line therefore changes level once, not once per step.
 *
 * Full detail is never drawn below this floor: with the 1.25 glyph cap an 11px
 * summary row still measures 10.86 screen px there. Written as a literal, not
 * `0.8 - 0.01` (0.7900000000000000355 in binary floating point), so a slider
 * that lands exactly on 79% holds.
 */
export const LOD_HOLD_ZOOM = 0.79;

/** How much of the card is drawn at a given zoom (see `lodLevel`). */
export type LodLevel = "low" | "compact" | "full";

/**
 * The card's level of detail: full at and above {@link READABLE_FIT_ZOOM},
 * compact down to {@link LOD_ZOOM}, and low below it. The compact band is the
 * one where the title, type name and badges are still legible but the summary
 * rows are not.
 *
 * `previous` is the level the card showed on the last frame. Passing it adds
 * the {@link LOD_HOLD_ZOOM} hysteresis: a full card holds its summary for the
 * last percent above the compact band, while a card coming up from compact
 * needs the full {@link READABLE_FIT_ZOOM}. A fresh card (no previous level)
 * uses the documented thresholds, so an automatic fit to the readable floor
 * always starts full.
 */
export function lodLevel(zoom: number, previous?: LodLevel): LodLevel {
  if (!Number.isFinite(zoom)) return "full";
  if (zoom >= READABLE_FIT_ZOOM) return "full";
  if (previous === "full" && zoom >= LOD_HOLD_ZOOM) return "full";
  return zoom < LOD_ZOOM ? "low" : "compact";
}

/** Largest inverse factor low detail may apply. */
export const LOD_MAX_SCALE = 1.6;

/**
 * The factor the low-detail CSS multiplies its base sizes by. Outside low
 * detail the variable is 1 (no rule reads it); inside, it is `1 / zoom` capped
 * at {@link LOD_MAX_SCALE}. Because the threshold is 0.5, `1 / zoom` is always
 * above the ceiling in that range, so the low-detail card has one stable size.
 */
export function lodScale(zoom: number): number {
  if (!Number.isFinite(zoom) || zoom <= 0) return 1;
  return zoom < LOD_ZOOM ? Math.min(1 / zoom, LOD_MAX_SCALE) : 1;
}

/**
 * Largest inverse factor a screen-space hit target may apply (WCAG 2.5.8). At
 * 33% the factor would be 3; below that the transparent target stops growing so
 * neighbouring controls do not swallow each other.
 */
export const HIT_MAX_INVERSE = 3;

/**
 * The factor a canvas control multiplies its 24 screen-px target by, written
 * onto the canvas as a CSS variable. At or above 100% no compensation is
 * needed (the visual box is already 24+ screen px); zoomed out it is `1 / zoom`,
 * capped at {@link HIT_MAX_INVERSE} so the expanded targets stay local.
 */
export function hitInverseZoom(zoom: number): number {
  if (!Number.isFinite(zoom) || zoom <= 0) return 1;
  return Math.min(Math.max(1 / zoom, 1), HIT_MAX_INVERSE);
}
