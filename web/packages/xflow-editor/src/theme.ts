import { foundationTokens, paletteTokens, type Appearance } from "@xflow/core";
import { theme, type ThemeConfig } from "antd";

/**
 * `system` follows `prefers-color-scheme`; the other two are explicit. The
 * resolved value is what `data-theme` carries and what picks the palette.
 * Palette values live in `@xflow/core` (`paletteTokens`); the editor keeps no
 * colour table of its own.
 */
export type XFlowEditorAppearance = Appearance | "system";
export type ResolvedXFlowEditorAppearance = Appearance;

/** The editor's AntD theme, generated from the same palette that renders tokens.css. */
export function createEditorThemeConfig(appearance: ResolvedXFlowEditorAppearance): ThemeConfig {
  // The palette is the same record @xflow/core renders into tokens.css, so the
  // AntD tokens and the --xflow-* custom properties cannot disagree.
  // ConfigProvider owns AntD's interaction states; the root CSS variables own
  // editor-only surfaces such as rulers and the React Flow canvas.
  const palette = paletteTokens[appearance];
  const focusOutline = `${palette.primary}33`;

  return {
    algorithm: appearance === "dark" ? theme.darkAlgorithm : theme.defaultAlgorithm,
    cssVar: {
      prefix: "xflow-ant",
      key: `xflow-editor-${appearance}`
    },
    token: {
      borderRadius: 6,
      borderRadiusSM: 4,
      borderRadiusLG: 8,
      colorBgBase: palette.app,
      colorBgContainer: palette.field,
      colorBgContainerDisabled: palette.panelRaised,
      colorBgElevated: palette.panel,
      colorBgLayout: palette.app,
      colorBgTextActive: palette.hover,
      colorBgTextHover: palette.hover,
      colorBorder: palette.border,
      colorBorderBg: palette.border,
      colorBorderSecondary: palette.border,
      colorError: palette.danger,
      colorFillAlter: palette.field,
      colorFillContent: palette.field,
      colorFillContentHover: palette.hover,
      colorFillSecondary: palette.field,
      colorFillTertiary: palette.hover,
      colorIcon: palette.textTertiary,
      colorIconHover: palette.text,
      colorInfo: palette.info,
      colorPrimary: palette.primary,
      colorSplit: palette.border,
      colorSuccess: palette.success,
      colorText: palette.text,
      colorTextBase: palette.text,
      colorTextDescription: palette.textTertiary,
      colorTextLabel: palette.textSecondary,
      colorTextPlaceholder: palette.textTertiary,
      colorTextSecondary: palette.textSecondary,
      colorTextTertiary: palette.textTertiary,
      colorWarning: palette.warning,
      boxShadowSecondary: palette.elev2,
      controlHeight: 32,
      controlHeightSM: 24,
      controlItemBgActive: palette.hover,
      controlItemBgActiveHover: palette.hover,
      controlItemBgHover: palette.hover,
      controlOutline: focusOutline,
      controlOutlineWidth: 2,
      fontFamily: foundationTokens.fontSans,
      fontFamilyCode: foundationTokens.fontMono,
      fontSize: 13,
      fontSizeSM: 12
    },
    components: {
      Button: {
        dangerColor: palette.danger,
        dangerShadow: "none",
        defaultActiveBg: palette.hover,
        defaultActiveBorderColor: palette.primary,
        defaultActiveColor: palette.text,
        defaultBg: palette.panel,
        defaultBorderColor: palette.border,
        defaultColor: palette.text,
        // No defaultHover* tokens: the baseline keeps AntD's default hover (field
        // fill, primary-hover border), so overriding them here made every default
        // button hover grey with a solid primary border.
        defaultShadow: "none",
        // The 24px tool buttons (spec §6.0) carry a 16px icon, the size the
        // baseline draws with `.xflow-pv-tool`; AntD would inherit fontSizeSM.
        onlyIconSizeSM: 16,
        primaryColor: palette.onPrimary,
        primaryShadow: "none",
        solidTextColor: palette.onPrimary
      },
      Card: {
        bodyPaddingSM: 10,
        headerBg: palette.panelRaised,
        headerPaddingSM: 10
      },
      Input: {
        activeBg: palette.field,
        activeBorderColor: palette.primary,
        activeShadow: `0 0 0 2px ${focusOutline}`,
        hoverBg: palette.field,
        hoverBorderColor: palette.primary
      },
      Modal: { borderRadiusLG: 12, contentBg: palette.panel, footerBg: palette.panel, headerBg: palette.panel },
      Popover: { colorBgElevated: palette.panel },
      Segmented: {
        itemActiveBg: palette.hover,
        itemColor: palette.textSecondary,
        itemHoverBg: palette.hover,
        itemHoverColor: palette.text,
        itemSelectedBg: palette.panel,
        itemSelectedColor: palette.text,
        trackBg: palette.hover,
        trackPadding: 2
      },
      Select: {
        activeBorderColor: palette.primary,
        activeOutlineColor: focusOutline,
        hoverBorderColor: palette.primary,
        optionActiveBg: palette.hover,
        optionSelectedBg: palette.hover,
        optionSelectedColor: palette.text,
        optionSelectedFontWeight: 600,
        selectorBg: palette.field
      },
      Switch: {
        handleBg: palette.field,
        handleShadow: "none",
        handleSizeSM: 12,
        trackHeightSM: 16,
        trackMinWidthSM: 28,
        trackPadding: 2
      },
      // Same values as the baseline's Tabs token (ui-preview/tokens.ts): 16px
      // between items, no gap under the nav, inactive labels in secondary text.
      Tabs: {
        horizontalItemGutter: 16,
        horizontalMargin: "0",
        itemColor: palette.textSecondary
      },
      Tag: {
        defaultBg: palette.panelRaised,
        defaultColor: palette.textSecondary,
        solidTextColor: palette.onPrimary
      }
    }
  };
}

