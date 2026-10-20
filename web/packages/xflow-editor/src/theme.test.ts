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
          colorBgBase: palette.app,
          colorBgLayout: palette.app,
          colorBgContainer: palette.field,
          colorBgElevated: palette.panel,
          colorFillAlter: palette.field,
          colorFillTertiary: palette.hover,
          colorBorder: palette.border,
          colorSplit: palette.border,
          colorText: palette.text,
          colorTextSecondary: palette.textSecondary,
          colorTextTertiary: palette.textTertiary,
          colorTextPlaceholder: palette.textTertiary,
          controlItemBgHover: palette.hover,
          boxShadowSecondary: palette.elev2
        });
      });

      it("takes the semantic colours from the core palette", () => {
        expect(token).toMatchObject({
          colorPrimary: palette.primary,
          colorInfo: palette.info,
          colorSuccess: palette.success,
          colorWarning: palette.warning,
          colorError: palette.danger
        });
      });

      it("derives the focus outline from the primary colour, not a literal", () => {
        expect(token.controlOutline).toBe(`${palette.primary}33`);
        expect(config.components?.Input?.activeShadow).toBe(`0 0 0 2px ${palette.primary}33`);
      });

      it("uses the shared 32 / 13 control scale", () => {
        expect(token).toMatchObject({
          fontFamily: foundationTokens.fontSans,
          fontFamilyCode: foundationTokens.fontMono,
          fontSize: 13,
          fontSizeSM: 12,
          borderRadius: 6,
          borderRadiusSM: 4,
          borderRadiusLG: 8,
          controlHeight: 32,
          controlHeightSM: 24
        });
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
        expect(components.Button).toMatchObject({ defaultBg: palette.panel });
        // Hover stays on AntD's default (the baseline sets only defaultBg); a
        // palette hover fill and primary text made every default button turn grey.
        expect(components.Button).not.toHaveProperty("defaultHoverBg");
        expect(components.Button).not.toHaveProperty("defaultHoverColor");
        expect(components.Button).not.toHaveProperty("defaultHoverBorderColor");
        expect(components.Segmented).toMatchObject({
          itemSelectedBg: palette.panel,
          trackBg: palette.hover
        });
        expect(components.Select).toMatchObject({ selectorBg: palette.field });
        expect(components.Card).toMatchObject({ headerBg: palette.panelRaised });
        expect(components.Popover).toMatchObject({ colorBgElevated: palette.panel });
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

  it("gives light and dark distinct scopes and surfaces", () => {
    const light = createEditorThemeConfig("light");
    const dark = createEditorThemeConfig("dark");
    expect(light.cssVar).not.toEqual(dark.cssVar);
    expect(light.token?.colorBgContainer).not.toBe(dark.token?.colorBgContainer);
  });

  it("writes no colour literal of its own: every colour value traces to the palette", () => {
    for (const appearance of appearances) {
      const palette = paletteTokens[appearance];
      const known = new Set<string>([...Object.values(palette), `${palette.primary}33`]);
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
