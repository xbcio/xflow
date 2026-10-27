import { describe, expect, it } from "vitest";
import { buildAntdTheme } from "./antdTheme";
import { foundationTokens, paletteTokens, type Appearance } from "./tokens";

/**
 * The shared map is the single source both ConfigProviders spread, so these
 * tests pin what cannot be mode-delegated: the AA variants, the foundation
 * numbers, the motion contract and the component tokens the two hosts share.
 */
const appearances: Appearance[] = ["light", "dark"];

/** WCAG 2.x contrast ratio between two opaque #rrggbb colours. */
function contrastRatio(a: string, b: string): number {
  const luminance = (hex: string) => {
    const [r, g, bl] = [1, 3, 5].map((i) => {
      const c = parseInt(hex.slice(i, i + 2), 16) / 255;
      return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
    });
    return 0.2126 * r + 0.7152 * g + 0.0722 * bl;
  };
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

describe("buildAntdTheme", () => {
  for (const mode of appearances) {
    describe(mode, () => {
      const palette = paletteTokens[mode];
      const { token, components } = buildAntdTheme(mode);

      it("takes colours, links and surfaces from the palette at the AA variant", () => {
        expect(token).toMatchObject({
          colorPrimary: palette.primarySolid,
          colorPrimaryHover: palette.primarySolidHover,
          colorPrimaryActive: palette.primarySolidActive,
          colorPrimaryBorder: palette.focusRing,
          colorLink: mode === "dark" ? palette.focusRing : palette.primarySolid,
          colorLinkHover: mode === "dark" ? palette.focusRing : palette.primarySolidHover,
          colorLinkActive: mode === "dark" ? palette.focusRing : palette.primarySolidActive,
          colorInfo: palette.info,
          colorSuccess: palette.success,
          colorWarning: palette.warning,
          colorError: palette.danger,
          colorText: palette.text,
          colorTextSecondary: palette.textSecondary,
          colorTextTertiary: palette.textTertiary,
          colorTextPlaceholder: palette.textTertiary,
          colorBgContainer: palette.surfacePanel,
          colorBgElevated: palette.surfaceOverlay,
          colorBgLayout: palette.surfaceApp,
          colorBorder: palette.border,
          colorBorderSecondary: palette.borderSubtle,
          colorSplit: palette.borderSubtle,
          colorFillTertiary: palette.hover,
          controlItemBgHover: palette.hover,
        });
      });

      it("paints the label of a solid danger button with onDanger, readable on the error fill", () => {
        expect(components.Button.dangerColor).toBe(palette.onDanger);
        // A solid danger button fills with colorError and takes dangerColor as its text.
        expect(token.colorError).toBe(palette.danger);
        expect(contrastRatio(components.Button.dangerColor, token.colorError)).toBeGreaterThanOrEqual(4.5);
      });

      it("parses focus, radius, type and elevation from the foundation scale", () => {
        const radius = (value: string) => Number.parseInt(value, 10);
        expect(token.controlOutline).toBe(palette.focusRing);
        expect(token.controlOutlineWidth).toBe(2);
        expect(token.lineWidthFocus).toBe(2);
        expect(token.borderRadius).toBe(radius(foundationTokens.radiusSm));
        expect(token.borderRadiusSM).toBe(radius(foundationTokens.radiusXs));
        expect(token.borderRadiusOuter).toBe(radius(foundationTokens.radiusXs));
        expect(token.borderRadiusLG).toBe(radius(foundationTokens.radiusMd));
        expect(token.borderRadiusXS).toBe(radius(foundationTokens.radiusXs));
        expect(token.controlHeight).toBe(radius(foundationTokens.controlHeight));
        expect(token.controlHeightSM).toBe(radius(foundationTokens.controlHeightSm));
        expect(token.fontFamily).toBe(foundationTokens.fontSans);
        expect(token.fontFamilyCode).toBe(foundationTokens.fontMono);
        expect(token.fontSize).toBe(radius(foundationTokens.fontMd));
        expect(token.fontSizeSM).toBe(radius(foundationTokens.fontSm));
        // AntD omits lineHeight by default, which renders 13px at 20.43px; the
        // ratio must land the body on the design's 20px rhythm.
        expect(token.fontSize * token.lineHeight).toBeCloseTo(20, 10);
        expect(token.fontSizeSM * token.lineHeightSM).toBeCloseTo(18, 10);
        expect(token.boxShadow).toBe(palette.elev4);
        expect(token.boxShadowSecondary).toBe(palette.elev3);
        expect(token.boxShadowTertiary).toBe(palette.elev1);
      });

      it("adopts the shared motion tokens", () => {
        expect(token.motion).toBe(true);
        expect(token.motionDurationFast).toBe(foundationTokens.motionDurationFast);
        expect(token.motionDurationMid).toBe(foundationTokens.motionDurationBase);
        expect(token.motionDurationSlow).toBe(foundationTokens.motionDurationSlow);
        expect(token.motionEaseInOut).toBe(foundationTokens.motionEaseStandard);
        expect(token.motionEaseOut).toBe(foundationTokens.motionEaseEmphasized);
      });

      it("carries the component tokens both hosts share", () => {
        expect(components.Button).toMatchObject({
          defaultBg: palette.surfacePanel,
          defaultBorderColor: palette.border,
          primaryColor: palette.onPrimary,
          solidTextColor: palette.onPrimary,
        });
        // The 25% primary soft ring is batch A behaviour: not the opaque ring.
        expect(components.Input.activeShadow).toBe(`0 0 0 3px ${palette.primary}40`);
        expect(components.Select).toMatchObject({
          activeOutlineColor: `${palette.primary}40`,
          optionSelectedFontWeight: 600,
          selectorBg: palette.field,
        });
        expect(components.Card).toEqual({
          bodyPaddingSM: 10,
          headerBg: palette.surfaceSunken,
          headerPaddingSM: 10,
        });
        expect(components.Table).toEqual({
          headerBg: palette.surfaceSunken,
          headerColor: palette.textSecondary,
          rowHoverBg: palette.hover,
        });
        expect(components.Tabs).toEqual({
          horizontalItemGutter: 16,
          horizontalMargin: "0",
          itemColor: palette.textSecondary,
        });
        expect(components.Tag).toMatchObject({ defaultBg: palette.surfaceSunken, solidTextColor: palette.onPrimary });
        expect(components.Switch).toMatchObject({ handleBg: palette.field, handleSizeSM: 12, trackHeightSM: 16 });
        expect(components.Popover.colorBgElevated).toBe(palette.surfaceOverlay);
        expect(components.Modal.headerBg).toBe(palette.surfaceOverlay);
      });
    });
  }

  it("switches AntD's own motion off when the OS asks for reduced motion", () => {
    expect(buildAntdTheme("light").token.motion).toBe(true);
    expect(buildAntdTheme("light", { reducedMotion: true }).token.motion).toBe(false);
    expect(buildAntdTheme("dark", { reducedMotion: true }).token.motion).toBe(false);
    // Component CSS transitions read the duration tokens directly, so every
    // step collapses to 0s; the eased curves stay for hosts that re-enable.
    const reduced = buildAntdTheme("light", { reducedMotion: true }).token;
    expect(reduced.motionDurationFast).toBe("0s");
    expect(reduced.motionDurationMid).toBe("0s");
    expect(reduced.motionDurationSlow).toBe("0s");
    expect(reduced.motionEaseInOut).toBe(foundationTokens.motionEaseStandard);
  });
});
