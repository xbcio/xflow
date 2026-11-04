import { describe, expect, it } from "vitest";
import { buildAntdTheme } from "./antdTheme";
import { FG_MIX_PERCENT, foundationTokens, paletteTokens, type Appearance } from "./tokens";

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

/**
 * `color-mix(in srgb, <base> p%, <other>)` with opaque inputs, rounded the way
 * a browser rounds the 8-bit result. Mirrors `renderTokensCss`'s `-fg` mix.
 */
function mixHex(base: string, other: string, percent: number): string {
  const channels = (hex: string) => [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));
  const mixed = channels(base).map((value, i) =>
    Math.round((value * percent) / 100 + (channels(other)[i] * (100 - percent)) / 100)
  );
  return `#${mixed.map((value) => value.toString(16).padStart(2, "0")).join("")}`;
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

      it("keeps the solid danger label at AA on the hover and pressed fills, and the fills distinct", () => {
        // AntD reads the solid button's hover/pressed fills from colorErrorHover/Active.
        const label = components.Button.dangerColor;
        expect(contrastRatio(label, token.colorErrorHover)).toBeGreaterThanOrEqual(4.5);
        expect(contrastRatio(label, token.colorErrorActive)).toBeGreaterThanOrEqual(4.5);
        expect(token.colorErrorHover).not.toBe(token.colorError);
        expect(token.colorErrorActive).not.toBe(token.colorError);
        expect(token.colorErrorActive).not.toBe(token.colorErrorHover);
      });

      it("paints the dangerous menu items with the -fg tone, readable in every state", () => {
        // AntD derives these from colorError, which the dark algorithm renders
        // as #dc5d56 (3.78:1 on the popup) and the light hover wash drops to
        // 4.38:1. The menu text takes the same 70/30 mix as
        // `--xflow-danger-fg`.
        const menu = components.Menu;
        const fg = mixHex(palette.danger, palette.text, FG_MIX_PERCENT);
        expect(menu.dangerItemColor).toBe(fg);
        expect(menu.dangerItemHoverColor).toBe(fg);
        // Popups render on the elevated surface; hover washes it with 6% text.
        const overlay = palette.surfaceOverlay;
        expect(contrastRatio(fg, overlay)).toBeGreaterThanOrEqual(4.5);
        expect(contrastRatio(fg, mixHex(palette.text, overlay, 6))).toBeGreaterThanOrEqual(4.5);
        // The pressed row fills with the 12% danger wash and keeps the -fg
        // label: AntD has no active text token, `:active` keeps the hover colour.
        expect(menu.dangerItemActiveBg).toBe(mixHex(palette.danger, overlay, 12));
        expect(contrastRatio(fg, menu.dangerItemActiveBg)).toBeGreaterThanOrEqual(4.5);
        // The selected row fills solid danger and takes the onDanger ink.
        expect(menu.dangerItemSelectedBg).toBe(palette.danger);
        expect(menu.dangerItemSelectedColor).toBe(palette.onDanger);
        expect(contrastRatio(menu.dangerItemSelectedColor, menu.dangerItemSelectedBg)).toBeGreaterThanOrEqual(4.5);
      });

      it("paints a nested dark menu from the dark appearance, whatever the host theme", () => {
        const dark = paletteTokens.dark;
        const menu = components.Menu;
        const fg = mixHex(dark.danger, dark.text, FG_MIX_PERCENT);
        expect(menu.darkDangerItemColor).toBe(fg);
        expect(menu.darkDangerItemHoverColor).toBe(fg);
        expect(menu.darkDangerItemActiveBg).toBe(mixHex(dark.danger, dark.surfaceOverlay, 12));
        expect(menu.darkDangerItemSelectedBg).toBe(dark.danger);
        expect(menu.darkDangerItemSelectedColor).toBe(dark.onDanger);
        expect(contrastRatio(fg, dark.surfaceOverlay)).toBeGreaterThanOrEqual(4.5);
        expect(contrastRatio(fg, mixHex(dark.text, dark.surfaceOverlay, 6))).toBeGreaterThanOrEqual(4.5);
        expect(contrastRatio(fg, menu.darkDangerItemActiveBg)).toBeGreaterThanOrEqual(4.5);
        expect(contrastRatio(menu.darkDangerItemSelectedColor, menu.darkDangerItemSelectedBg)).toBeGreaterThanOrEqual(4.5);
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
        // An outlined input's idle ground is the alias token, not a component
        // token; it must match the Select selector so neighbouring fields do
        // not show two shades. InputNumber, DatePicker and Mentions reuse the
        // same input variants under their own token scopes.
        expect(components.Input.colorBgContainer).toBe(palette.field);
        expect(components.InputNumber.colorBgContainer).toBe(palette.field);
        expect(components.DatePicker.colorBgContainer).toBe(palette.field);
        expect(components.Mentions.colorBgContainer).toBe(palette.field);
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
