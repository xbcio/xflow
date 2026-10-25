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
  // Spec §3.3 originally pinned #6b7280 for tertiary, but it only reached
  // 4.43:1 on `surfaceApp` and 4.23:1 on `surfaceSunken`, where labels,
  // statusbars and footnotes are painted; the palette now deepens it so the
  // 4.5 floor holds on every light surface step.
  const groundsByKey: Record<
    keyof typeof floors,
    ReadonlyArray<"surfacePanel" | "surfaceApp" | "surfaceSunken" | "surfaceOverlay">
  > = {
    text: ["surfacePanel", "surfaceApp"],
    textSecondary: ["surfacePanel", "surfaceApp"],
    textTertiary: ["surfacePanel", "surfaceApp", "surfaceSunken"],
  };
  for (const appearance of appearances) {
    for (const [key, floor] of Object.entries(floors)) {
      for (const ground of groundsByKey[key as keyof typeof floors]) {
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

  it("keeps the accepted preview colours and adds the surface/elevation scale", () => {
    const previewColours: Array<keyof Palette> = [
      "app", "panel", "panelRaised", "field", "canvas", "canvasDot", "border", "borderStrong", "hover",
      "text", "textSecondary", "textTertiary", "primary", "onPrimary", "info", "success", "warning",
      "danger", "suspend", "neutral", "accent", "orange", "cyan",
    ];
    const keys = Object.keys(paletteTokens.light);
    for (const key of [...previewColours, "elev1", "elev2", "elev3"]) {
      expect(keys, key).toContain(key);
    }
    for (const key of ["surfaceSunken", "surfaceApp", "surfacePanel", "surfaceRaised", "surfaceOverlay", "elev0", "elev4"]) {
      expect(keys, key).toContain(key);
    }
  });

  it("adds exactly the four editor surface tokens the preview did not need", () => {
    // highlight / hoverSubtle / insetShadow / shadow replace the editor's own
    // translucent literals; each is a per-appearance value, not derivable by mixing.
    // The solid-primary triad and the focus ring are opaque AA values for the
    // same reason, so they are palette keys rather than derived expressions.
    // Stage 1b adds the five surface steps plus elev0/elev4 (elev1-3 already existed);
    // stage 1c adds borderSubtle for separators.
    expect(Object.keys(paletteTokens.light)).toHaveLength(23 + 4 + 3 + 4 + 7 + 1);
    for (const key of ["highlight", "hoverSubtle", "insetShadow", "shadow", "borderSubtle"]) {
      expect(Object.keys(paletteTokens.light), key).toContain(key);
    }
  });

  for (const appearance of appearances) {
    it(`${appearance}: separators sit below the card/field outline`, () => {
      const palette = paletteTokens[appearance];
      const panel = rgbOf(palette.surfacePanel);
      expect(contrast(rgbOf(palette.borderSubtle), panel), appearance).toBeLessThan(
        contrast(rgbOf(palette.border), panel)
      );
      expect(palette.borderSubtle).not.toBe(palette.border);
    });
  }

  it("defines the weight scale and composite type styles (stage 1c)", () => {
    expect(foundationTokens.fontWeightRegular).toBe("400");
    expect(foundationTokens.fontWeightMedium).toBe("500");
    expect(foundationTokens.fontWeightSemibold).toBe("600");
    expect(foundationTokens.textCaption).toBe(
      `${foundationTokens.fontWeightRegular} ${foundationTokens.fontXs}/${foundationTokens.lineHeightXs} ${foundationTokens.fontSans}`
    );
    expect(foundationTokens.textBody).toBe(
      `${foundationTokens.fontWeightRegular} ${foundationTokens.fontMd}/${foundationTokens.lineHeightMd} ${foundationTokens.fontSans}`
    );
    expect(foundationTokens.textLabel).toBe(
      `${foundationTokens.fontWeightMedium} ${foundationTokens.fontSm}/${foundationTokens.lineHeightSm} ${foundationTokens.fontSans}`
    );
    expect(foundationTokens.textTitle).toBe(
      `${foundationTokens.fontWeightSemibold} ${foundationTokens.fontLg}/${foundationTokens.lineHeightLg} ${foundationTokens.fontSans}`
    );
    expect(foundationTokens.textHeading).toBe(
      `${foundationTokens.fontWeightSemibold} ${foundationTokens.fontXl}/${foundationTokens.lineHeightXl} ${foundationTokens.fontSans}`
    );
    expect(foundationTokens.textMono).toBe(
      `${foundationTokens.fontWeightRegular} ${foundationTokens.fontXs}/${foundationTokens.lineHeightXs} ${foundationTokens.fontMono}`
    );
    // Full replaces pill; the old name stays as an alias for callers that have
    // not migrated (composer's fallbacks).
    expect(foundationTokens.radiusFull).toBe("9999px");
    expect(foundationTokens.radiusPill).toBe(foundationTokens.radiusFull);
  });

  for (const appearance of appearances) {
    it(`${appearance}: solid primary carries white text and its states at AA`, () => {
      const palette = paletteTokens[appearance];
      for (const key of ["primarySolid", "primarySolidHover", "primarySolidActive"] as const) {
        expect(contrast(rgbOf(palette.onPrimary), rgbOf(palette[key])), key).toBeGreaterThanOrEqual(4.5);
      }
    });

    it(`${appearance}: the focus ring stays at 3:1 on every surface it is drawn on`, () => {
      const palette = paletteTokens[appearance];
      for (const ground of ["surfacePanel", "surfaceRaised", "surfaceOverlay", "surfaceSunken", "canvas", "surfaceApp", "field"] as const) {
        const ratio = contrast(rgbOf(palette.focusRing), rgbOf(palette[ground]));
        expect(ratio, ground).toBeGreaterThanOrEqual(3);
      }
    });
  }

  it("pairs every font step with a line height (ED-16)", () => {
    const pairs = [
      ["fontXs", "lineHeightXs", "16px"],
      ["fontSm", "lineHeightSm", "18px"],
      ["fontMd", "lineHeightMd", "20px"],
      ["fontLg", "lineHeightLg", "22px"],
      ["fontXl", "lineHeightXl", "24px"]
    ] as const;
    for (const [font, lineHeight, value] of pairs) {
      expect(foundationTokens[font], font).toBeDefined();
      expect(foundationTokens[lineHeight], lineHeight).toBe(value);
      expect(cssVarName(lineHeight)).toBe(`--xflow-line-height-${font.slice(4).toLowerCase()}`);
    }
  });

  it("renders camelCase keys as kebab-case custom properties", () => {
    expect(cssVarName("fontXs")).toBe("--xflow-font-xs");
    expect(cssVarName("panelRaised")).toBe("--xflow-panel-raised");
    expect(cssVarName("surfaceRaised")).toBe("--xflow-surface-raised");
    expect(cssVarName("elev0")).toBe("--xflow-elev-0");
    expect(cssVarName("elev1")).toBe("--xflow-elev-1");
    expect(cssVarName("elev4")).toBe("--xflow-elev-4");
    expect(cssVarName("controlHeightSm")).toBe("--xflow-control-height-sm");
    expect(cssVarName("motionDurationFast")).toBe("--xflow-motion-duration-fast");
    expect(cssVarName("motionEaseStandard")).toBe("--xflow-motion-ease-standard");
    expect(cssVarName("space0_5")).toBe("--xflow-space-0-5");
    expect(cssVarName("space1_5")).toBe("--xflow-space-1-5");
    expect(cssVarName("space2_5")).toBe("--xflow-space-2-5");
  });

  it("defines the spacing scale as four base steps plus two half steps (stage 1d)", () => {
    expect(foundationTokens).toMatchObject({
      space0: "0px",
      space0_5: "2px",
      space1: "4px",
      space1_5: "6px",
      space2: "8px",
      space2_5: "10px",
      space3: "12px",
      space4: "16px",
      space5: "20px",
      space6: "24px",
      space8: "32px",
      space10: "40px",
      space12: "48px",
    });
  });

  it("defines the motion scale as duration and easing tokens", () => {
    expect(foundationTokens).toMatchObject({
      motionDurationInstant: "80ms",
      motionDurationFast: "120ms",
      motionDurationBase: "180ms",
      motionDurationSlow: "260ms",
      motionEaseStandard: "cubic-bezier(.2, 0, 0, 1)",
      motionEaseEmphasized: "cubic-bezier(.3, 0, 0, 1)",
      motionEaseExit: "cubic-bezier(.4, 0, 1, 1)",
    });
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

  it("zeroes every motion duration under prefers-reduced-motion", () => {
    expect(onDisk).toContain("@media (prefers-reduced-motion: reduce) {");
    for (const key of ["motionDurationInstant", "motionDurationFast", "motionDurationBase", "motionDurationSlow"] as const) {
      expect(onDisk, key).toContain(`${cssVarName(key)}: 0ms;`);
    }
    // Easings keep their shape: a 0ms transition never plays them.
    expect(onDisk).toContain(`${cssVarName("motionEaseStandard")}: ${foundationTokens.motionEaseStandard};`);
    // Every hand-written duration is token-driven now, so the old wildcard
    // fallback (and its `!important`) is gone: the generated sheet carries no
    // `!important` at all.
    expect(onDisk).not.toContain("!important");
    expect(onDisk).not.toContain("*::before");
    expect(onDisk).not.toContain("transition-duration:");
    expect(onDisk).not.toContain("animation-duration:");
  });
});
