import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { HIT_MAX_INVERSE, LOD_HOLD_ZOOM, LOD_MAX_SCALE, LOD_ZOOM, READABLE_FIT_ZOOM, hitInverseZoom, lodLevel, lodScale } from "./lod";

describe("lodLevel", () => {
  it("keeps the full card at and above the readable fit zoom", () => {
    expect(READABLE_FIT_ZOOM).toBe(0.8);
    for (const zoom of [0.8, 0.9, 1, 1.6, 2]) expect(lodLevel(zoom)).toBe("full");
  });

  it("drops the summary but keeps the title, type and badges in the compact band", () => {
    for (const zoom of [0.75, 0.76, 0.79]) expect(lodLevel(zoom)).toBe("compact");
  });

  it("drops to icon-and-title below 75%", () => {
    for (const zoom of [0.74, 0.62, 0.3, 0.14]) expect(lodLevel(zoom)).toBe("low");
    expect(LOD_ZOOM).toBe(0.75);
  });

  it("treats an invalid zoom as full detail", () => {
    expect(lodLevel(Number.NaN)).toBe("full");
    expect(lodLevel(Number.POSITIVE_INFINITY)).toBe("full");
  });

  it("holds a full card for the last percent above the compact band", () => {
    expect(LOD_HOLD_ZOOM).toBe(0.79);
    expect(lodLevel(0.8, "full")).toBe("full");
    expect(lodLevel(0.79, "full")).toBe("full");
    expect(lodLevel(0.789, "full")).toBe("compact");
    expect(lodLevel(0.789, "low")).toBe("compact");
  });

  it("re-enters full exactly at the readable floor, so an auto fit is full", () => {
    expect(lodLevel(0.8, "compact")).toBe("full");
    expect(lodLevel(0.79, "compact")).toBe("compact");
    // An oscillation around the floor changes level once: from compact at
    // 80.5% the card becomes full and the held 79.5% keeps it full; from full
    // the held 79.5% stays full, and only a step below 79% drops it.
    expect(lodLevel(0.795, lodLevel(0.805, "compact"))).toBe("full");
    expect(lodLevel(0.795, "full")).toBe("full");
    expect(lodLevel(0.789, lodLevel(0.805, "compact"))).toBe("compact");
  });

  it("keeps the compact/low boundary a plain threshold", () => {
    expect(lodLevel(0.74, "compact")).toBe("low");
    expect(lodLevel(0.76, "low")).toBe("compact");
  });
});

describe("lodScale", () => {
  it("caps the inverse scale so a small zoom cannot inflate the card", () => {
    expect(lodScale(0.45)).toBe(LOD_MAX_SCALE);
    expect(lodScale(0.14)).toBe(LOD_MAX_SCALE);
    // The threshold is 0.75, so 1/zoom is above the cap inside low detail.
    for (const zoom of [0.74, 0.5, 0.3, 0.2, 0.1]) {
      expect(lodScale(zoom)).toBeLessThanOrEqual(LOD_MAX_SCALE);
    }
  });

  it("switches to low detail where the full card stops being readable", () => {
    // A manual zoom to 62%: an 11px summary row would render at ~6.8 screen
    // px, so the low-detail state must already be on. Automatic fits stay above.
    expect(lodScale(0.62)).toBeCloseTo(1.6, 10);
    expect(lodScale(0.75)).toBe(1);
    expect(lodScale(1)).toBe(1);
    expect(lodScale(2)).toBe(1);
  });

  it("stays finite for a zero or invalid transform", () => {
    expect(lodScale(0)).toBe(1);
    expect(lodScale(Number.NaN)).toBe(1);
    expect(lodScale(Number.POSITIVE_INFINITY)).toBe(1);
  });
});

describe("hitInverseZoom", () => {
  it("grows a 24px target back to 24 screen px as the canvas zooms out", () => {
    expect(hitInverseZoom(0.62)).toBeCloseTo(1 / 0.62, 10);
    expect(hitInverseZoom(1)).toBe(1);
    // At the LOD threshold a 16px control is compensated by 1.33.
    expect(hitInverseZoom(0.75)).toBeCloseTo(4 / 3, 10);
  });

  it("never shrinks a target and stops growing it below 33%", () => {
    expect(hitInverseZoom(2)).toBe(1);
    expect(hitInverseZoom(0.4)).toBeCloseTo(2.5, 10);
    expect(hitInverseZoom(0.3)).toBe(HIT_MAX_INVERSE);
    expect(hitInverseZoom(0.1)).toBe(HIT_MAX_INVERSE);
    expect(hitInverseZoom(0)).toBe(1);
    expect(hitInverseZoom(Number.NaN)).toBe(1);
  });
});

describe("card type readability", () => {
  it("keeps the 11px card rows at 10 screen px or more down to the compact floor", () => {
    // styles.css multiplies full/compact glyphs by clamp(1, 1/zoom, 1.25); this
    // mirrors the formula so the threshold and the cap cannot drift apart. At
    // 80% the old card rendered 11px rows at 8.8 screen px; the cap lifts 75% to
    // 10.31 screen px.
    const textScale = (zoom: number) => Math.min(Math.max(1 / zoom, 1), 1.25);
    expect(READABLE_FIT_ZOOM).toBe(0.8);
    // 0.79 is the hysteresis floor where a full card may still show its rows.
    for (const zoom of [READABLE_FIT_ZOOM, 0.85, 0.9, 1, 1.5, LOD_HOLD_ZOOM, LOD_ZOOM]) {
      expect(11 * textScale(zoom) * zoom).toBeGreaterThanOrEqual(10);
    }
  });
});

describe("low-detail stylesheet", () => {
  const css = readFileSync(`${import.meta.dirname}/styles.css`, "utf8").replace(/\/\*[\s\S]*?\*\//g, "");

  it("multiplies by the capped scale instead of dividing by the raw zoom", () => {
    expect(css).toContain("var(--xflow-preview-lod-scale)");
    expect(css).not.toContain("/ var(--xflow-preview-lod)");
  });

  it("keeps the low-detail card inside its 264px slot", () => {
    // Header horizontal padding, icon, gap and icon-only chip at the cap must
    // leave room for the name; the sums below are the CSS bases times 1.6.
    const padding = 12 * LOD_MAX_SCALE * 2;
    const icon = 28 * LOD_MAX_SCALE;
    const gap = 8 * LOD_MAX_SCALE;
    const chip = 12 * LOD_MAX_SCALE + 4 * LOD_MAX_SCALE * 2;
    expect(264 - padding - icon - gap - chip).toBeGreaterThan(60);
  });

  it("compensates the overlay hit targets with the inverse-zoom variable", () => {
    // Section delete/rename and the node handles still size themselves to 24
    // screen px from JS (1/zoom). The summary row does not: its 30 flow px are
    // already 24 screen px at the 0.8 full-detail floor and must not resize a
    // card on every zoom step (P2-2).
    expect(css).toContain("var(--xflow-preview-inverse-zoom, 1)");
    for (const selector of [
      ".xflow-preview-section__delete::after",
      ".xflow-preview-section__title-action::after",
      ".xflow-preview-node__handle::after"
    ]) {
      expect(css, `${selector} must carry the 24px screen target`).toContain(selector);
    }
  });

  it("keeps the card's summary geometry in flow units across zoom steps", () => {
    expect(css).toMatch(/--xflow-preview-summary-row-h:\s*30px;/);
    expect(css).not.toMatch(/--xflow-preview-summary-row-h:\s*calc\([^;]*inverse-zoom/);
  });
});
