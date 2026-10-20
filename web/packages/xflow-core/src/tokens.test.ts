import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  FG_MIX_PERCENT,
  cssVarName,
  derivedTokens,
  foundationTokens,
  paletteTokens,
  renderTokensCss,
  semanticColors,
  type Appearance,
  type Palette,
} from "./tokens";

/** WCAG 2.x relative luminance and contrast ratio. Test-only: core ships no colour maths. */
function channel(value: number): number {
  const c = value / 255;
  return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
}

function rgbOf(hex: string): [number, number, number] {
  expect(hex, "contrast checks need an opaque #rrggbb colour").toMatch(/^#[0-9a-f]{6}$/i);
  return [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16)) as [number, number, number];
}

function luminance([r, g, b]: [number, number, number]): number {
  return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b);
}

function contrast(a: [number, number, number], b: [number, number, number]): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

/** `color-mix(in srgb, <base> p%, <other>)` with opaque inputs. */
function mix(base: [number, number, number], other: [number, number, number], percent: number): [number, number, number] {
  return base.map((v, i) => Math.round((v * percent) / 100 + (other[i] * (100 - percent)) / 100)) as [number, number, number];
}

const appearances: Appearance[] = ["light", "dark"];

describe("palette contrast (spec §3.3)", () => {
  const floors = { text: 12, textSecondary: 7, textTertiary: 4.5 } as const;
  for (const appearance of appearances) {
    for (const ground of ["panel", "app"] as const) {
      for (const [key, floor] of Object.entries(floors)) {
        it(`${appearance}: ${key} on ${ground} is at least ${floor}:1`, () => {
          const palette = paletteTokens[appearance];
          const ratio = contrast(rgbOf(palette[key as keyof typeof floors]), rgbOf(palette[ground]));
          expect(ratio).toBeGreaterThanOrEqual(floor);
        });
      }
    }
  }

  for (const appearance of appearances) {
    it(`${appearance}: every derived -fg stays above 6:1 on panel`, () => {
      const palette = paletteTokens[appearance];
      const text = rgbOf(palette.text);
      const panel = rgbOf(palette.panel);
      const ratios = semanticColors.map((color) => ({
        color,
        ratio: contrast(mix(rgbOf(palette[color]), text, FG_MIX_PERCENT), panel),
      }));
      expect(ratios.filter(({ ratio }) => ratio < 6)).toEqual([]);
    });
  }
});

describe("token vocabulary", () => {
  it("has the same palette keys in both appearances", () => {
    expect(Object.keys(paletteTokens.dark)).toEqual(Object.keys(paletteTokens.light));
  });

  it("keeps the 23 colours and three elevations of the accepted preview", () => {
    const previewColours: Array<keyof Palette> = [
      "app", "panel", "panelRaised", "field", "canvas", "canvasDot", "border", "borderStrong", "hover",
      "text", "textSecondary", "textTertiary", "primary", "onPrimary", "info", "success", "warning",
      "danger", "suspend", "neutral", "accent", "orange", "cyan",
    ];
    const keys = Object.keys(paletteTokens.light);
    for (const key of [...previewColours, "elev1", "elev2", "elev3"]) {
      expect(keys, key).toContain(key);
    }
  });

  it("adds exactly the four editor surface tokens the preview did not need", () => {
    // highlight / hoverSubtle / insetShadow / shadow replace the editor's own
    // translucent literals; each is a per-appearance value, not derivable by mixing.
    expect(Object.keys(paletteTokens.light)).toHaveLength(23 + 4 + 3);
    for (const key of ["highlight", "hoverSubtle", "insetShadow", "shadow"]) {
      expect(Object.keys(paletteTokens.light), key).toContain(key);
    }
  });

  it("renders camelCase keys as kebab-case custom properties", () => {
    expect(cssVarName("fontXs")).toBe("--xflow-font-xs");
    expect(cssVarName("panelRaised")).toBe("--xflow-panel-raised");
    expect(cssVarName("elev1")).toBe("--xflow-elev-1");
    expect(cssVarName("controlHeightSm")).toBe("--xflow-control-height-sm");
  });

  it("derives -fg, -bg-subtle and -border for every semantic colour", () => {
    const names = new Set(derivedTokens.map(([name]) => name));
    for (const color of semanticColors) {
      for (const suffix of ["fg", "bg-subtle", "border"]) {
        expect(names.has(`${color}-${suffix}`), `${color}-${suffix}`).toBe(true);
      }
    }
  });

  it("writes no colour literal in a derived expression", () => {
    const literal = derivedTokens.filter(([, value]) => /#[0-9a-f]{3,8}\b|rgba?\(|hsla?\(/i.test(value));
    expect(literal).toEqual([]);
  });
});

describe("tokens.css", () => {
  const onDisk = readFileSync(`${import.meta.dirname}/tokens.css`, "utf8");

  it("is exactly what renderTokensCss() produces (regenerate with `pnpm --filter @xflow/core tokens:generate`)", () => {
    expect(onDisk).toBe(renderTokensCss());
  });

  it("sits in the xflow.tokens layer", () => {
    expect(onDisk).toContain("@layer xflow.tokens {");
  });

  for (const appearance of appearances) {
    it(`declares color-scheme and every palette key for ${appearance}`, () => {
      const start = onDisk.indexOf(`[data-theme="${appearance}"] {`);
      expect(start).toBeGreaterThan(-1);
      const block = onDisk.slice(start, onDisk.indexOf("}", start));
      expect(block).toContain(`color-scheme: ${appearance};`);
      for (const key of Object.keys(paletteTokens[appearance])) {
        expect(block, key).toContain(`${cssVarName(key)}: ${paletteTokens[appearance][key as keyof Palette]};`);
      }
    });
  }

  it("declares every foundation value once, outside the appearance blocks", () => {
    for (const [key, value] of Object.entries(foundationTokens)) {
      expect(onDisk, key).toContain(`${cssVarName(key)}: ${value};`);
    }
  });
});
