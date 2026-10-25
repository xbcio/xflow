import { foundationTokens, paletteTokens } from "@xflow/core";
import { theme } from "antd";
import { describe, expect, it } from "vitest";
import { createEditorThemeConfig, type ResolvedXFlowEditorAppearance } from "./theme";

/**
 * The AntD ConfigProvider and the --xflow-* custom properties must show the
 * same colours. Both come from @xflow/core's palette record (tokens.css is
 * rendered from it, and core's own test pins the rendered file), so the only
 * thing left to guard here is that the editor maps the right palette key onto
 * each AntD token.
 */
const appearances: ResolvedXFlowEditorAppearance[] = ["light", "dark"];

describe("createEditorThemeConfig", () => {
  for (const appearance of appearances) {
    describe(appearance, () => {
      const palette = paletteTokens[appearance];
      const config = createEditorThemeConfig(appearance);
      const token = config.token ?? {};

      it("uses the matching AntD algorithm", () => {
        expect(config.algorithm).toBe(appearance === "dark" ? theme.darkAlgorithm : theme.defaultAlgorithm);
      });

      it("keys the css-var scope by appearance only", () => {
        expect(config.cssVar).toEqual({ prefix: "xflow-ant", key: `xflow-editor-${appearance}` });
      });

      it("takes surfaces, text and borders from the core palette", () => {
        expect(token).toMatchObject({
          colorBgBase: palette.surfaceApp,
          colorBgLayout: palette.surfaceApp,
          colorBgContainer: palette.surfacePanel,
          colorBgElevated: palette.surfaceOverlay,
          colorFillAlter: palette.field,
          colorFillTertiary: palette.hover,
          colorBorder: palette.border,
          colorSplit: palette.borderSubtle,
          colorText: palette.text,
          colorTextSecondary: palette.textSecondary,
          colorTextTertiary: palette.textTertiary,
          colorTextPlaceholder: palette.textTertiary,
          controlItemBgHover: palette.hover,
          boxShadowSecondary: palette.elev3
        });
      });

      it("takes the semantic colours from the core palette", () => {
        expect(token).toMatchObject({
          colorPrimary: palette.primarySolid,
          colorPrimaryHover: palette.primarySolidHover,
          colorPrimaryActive: palette.primarySolidActive,
          // The dark generator deepens a seed, so dark links are seeded with the
          // light interactive blue instead of the solid fill.
          colorLink: appearance === "dark" ? palette.focusRing : palette.primarySolid,
          colorLinkHover: appearance === "dark" ? palette.focusRing : palette.primarySolidHover,
          colorLinkActive: appearance === "dark" ? palette.focusRing : palette.primarySolidActive,
          colorInfo: palette.info,
          colorSuccess: palette.success,
          colorWarning: palette.warning,
          colorError: palette.danger
        });
      });

      it("draws the keyboard ring from the opaque focus token and keeps the soft ring for glows", () => {
        expect(token.controlOutline).toBe(palette.focusRing);
        expect(token.controlOutlineWidth).toBe(2);
        expect(token.colorPrimaryBorder).toBe(palette.focusRing);
        expect(token.lineWidthFocus).toBe(2);
        // 25% primary, the same wash as --xflow-primary-ring.
        expect(config.components?.Input?.activeShadow).toBe(`0 0 0 3px ${palette.primary}40`);
      });

      it("uses the shared 32 / 13 control scale", () => {
        expect(token).toMatchObject({
          fontFamily: foundationTokens.fontSans,
          fontFamilyCode: foundationTokens.fontMono,
          fontSize: 13,
          fontSizeSM: 12,
          borderRadius: Number.parseInt(foundationTokens.radiusSm, 10),
          borderRadiusSM: Number.parseInt(foundationTokens.radiusXs, 10),
          borderRadiusLG: Number.parseInt(foundationTokens.radiusMd, 10),
          borderRadiusXS: Number.parseInt(foundationTokens.radiusXs, 10),
          controlHeight: 32,
          controlHeightSM: 24
        });
      });

      it("keeps AntD body text on the 20px rhythm and the shared elevation scale", () => {
        expect(token.fontSize * (token.lineHeight ?? 0)).toBeCloseTo(20, 10);
        expect(token.fontSizeSM * (token.lineHeightSM ?? 0)).toBeCloseTo(18, 10);
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

      it("drops the 12px-era component overrides", () => {
        const components = config.components ?? {};
        expect(components.Button).not.toHaveProperty("fontWeight");
        expect(components.Button).not.toHaveProperty("paddingInlineSM");
        expect(components.Button).not.toHaveProperty("iconGap");
        expect(components.Input).not.toHaveProperty("inputFontSizeSM");
        expect(components.Input).not.toHaveProperty("paddingBlockSM");
        expect(components.Input).not.toHaveProperty("paddingInlineSM");
        expect(components.Select).not.toHaveProperty("optionFontSize");
        expect(components.Select).not.toHaveProperty("optionHeight");
        expect(components.Select).not.toHaveProperty("optionLineHeight");
        expect(components.Select).not.toHaveProperty("optionPadding");
        expect(components.Card).not.toHaveProperty("headerHeightSM");
      });

      it("maps component surfaces onto the same palette record", () => {
        const components = config.components ?? {};
        expect(components.Button).toMatchObject({ defaultBg: palette.surfacePanel });
        // Hover stays on AntD's default (the baseline sets only defaultBg); a
        // palette hover fill and primary text made every default button turn grey.
        expect(components.Button).not.toHaveProperty("defaultHoverBg");
        expect(components.Button).not.toHaveProperty("defaultHoverColor");
        expect(components.Button).not.toHaveProperty("defaultHoverBorderColor");
        expect(components.Segmented).toMatchObject({
          itemSelectedBg: palette.panel,
          trackBg: palette.hover,
          // The selected thumb carries the baseline elevation, not AntD's flatter default.
          boxShadowTertiary: palette.elev1
        });
        expect(components.Select).toMatchObject({ selectorBg: palette.field });
        expect(components.Card).toMatchObject({ headerBg: palette.surfaceSunken });
        expect(components.Popover).toMatchObject({ colorBgElevated: palette.surfaceOverlay });
      });

      it("sets the Inspector Tabs token to the baseline's: 16px gutter, no nav margin, secondary labels", () => {
        expect(config.components?.Tabs).toEqual({
          horizontalItemGutter: 16,
          horizontalMargin: "0",
          itemColor: palette.textSecondary
        });
      });

      it("reads the text on a solid button from the palette's on-primary colour", () => {
        expect(config.components?.Button).toMatchObject({
          primaryColor: palette.onPrimary,
          solidTextColor: palette.onPrimary
        });
        expect(config.components?.Tag?.solidTextColor).toBe(palette.onPrimary);
      });
    });
  }

  it("turns AntD's own motion off when the OS asks for reduced motion", () => {
    expect(createEditorThemeConfig("light").token?.motion).toBe(true);
    expect(createEditorThemeConfig("light", { reducedMotion: true }).token?.motion).toBe(false);
    expect(createEditorThemeConfig("dark", { reducedMotion: true }).token?.motion).toBe(false);
    // The host's ConfigProvider must carry the 0s durations, not the base scale:
    // AntD's component CSS transitions read them directly.
    const reduced = createEditorThemeConfig("light", { reducedMotion: true }).token;
    expect(reduced?.motionDurationFast).toBe("0s");
    expect(reduced?.motionDurationMid).toBe("0s");
    expect(reduced?.motionDurationSlow).toBe("0s");
  });

  it("gives light and dark distinct scopes and surfaces", () => {
    const light = createEditorThemeConfig("light");
    const dark = createEditorThemeConfig("dark");
    expect(light.cssVar).not.toEqual(dark.cssVar);
    expect(light.token?.colorBgContainer).not.toBe(dark.token?.colorBgContainer);
  });

  it("writes no colour literal of its own: every colour value traces to the palette", () => {
    for (const appearance of appearances) {
      const palette = paletteTokens[appearance];
      const known = new Set<string>([...Object.values(palette), `${palette.primary}40`]);
      const strays: string[] = [];
      const walk = (path: string, value: unknown) => {
        if (typeof value === "string" && /^(#[0-9a-f]{3,8}|rgba?\()/i.test(value) && !known.has(value)) {
          strays.push(`${path}=${value}`);
        } else if (value && typeof value === "object") {
          for (const [key, child] of Object.entries(value)) walk(`${path}.${key}`, child);
        }
      };
      const { token, components } = createEditorThemeConfig(appearance);
      walk("token", token);
      walk("components", components);
      expect(strays, appearance).toEqual([]);
    }
  });
});
