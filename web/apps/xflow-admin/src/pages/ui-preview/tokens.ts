import { foundationTokens as foundation, paletteTokens as palettes, type Appearance } from '@xflow/core';
import { theme, type ThemeConfig } from 'antd';

/** AntD tokens take numbers; the shared radius scale is the single source. */
const radiusPx = (token: string) => Number.parseInt(token, 10);

/**
 * The preview's AntD theme. Colour and size values come from @xflow/core — the
 * same record that generates `tokens.css` — so AntD and the `--xflow-*`
 * custom properties cannot drift apart. Only the AntD-specific mapping lives here.
 */
export function themeConfig(appearance: Appearance): ThemeConfig {
  const p = palettes[appearance];
  return {
    algorithm: appearance === 'dark' ? theme.darkAlgorithm : theme.defaultAlgorithm,
    cssVar: { prefix: 'xflow-ant', key: `xflow-ui-preview-${appearance}` },
    token: {
      fontFamily: foundation.fontSans,
      fontFamilyCode: foundation.fontMono,
      fontSize: 13,
      fontSizeSM: 12,
      borderRadius: radiusPx(foundation.radiusSm),
      borderRadiusSM: radiusPx(foundation.radiusSm),
      borderRadiusLG: radiusPx(foundation.radiusMd),
      borderRadiusXS: radiusPx(foundation.radiusXs),
      controlHeight: 32,
      controlHeightSM: 24,
      // Solid fills and text both take the AA variant; the raw `primary` stays
      // the accent for decorative bars, matching @xflow/core's buildAntdTheme.
      colorPrimary: p.primarySolid,
      colorPrimaryHover: p.primarySolidHover,
      colorPrimaryActive: p.primarySolidActive,
      colorInfo: p.info,
      colorSuccess: p.success,
      colorWarning: p.warning,
      colorError: p.danger,
      colorText: p.text,
      colorTextSecondary: p.textSecondary,
      colorTextTertiary: p.textTertiary,
      colorTextDescription: p.textTertiary,
      colorTextPlaceholder: p.textTertiary,
      colorBgLayout: p.app,
      colorBgContainer: p.field,
      colorBgElevated: p.panel,
      colorBorder: p.border,
      colorBorderSecondary: p.border,
      colorSplit: p.border,
      colorFillTertiary: p.hover,
      controlItemBgHover: p.hover,
      boxShadowSecondary: p.elev2,
    },
    components: {
      Button: { defaultBg: p.panel, defaultShadow: 'none', primaryShadow: 'none', dangerShadow: 'none' },
      Modal: { borderRadiusLG: radiusPx(foundation.radiusLg), contentBg: p.panel, headerBg: p.panel, footerBg: p.panel },
      Form: { itemMarginBottom: 12, verticalLabelPadding: '0 0 4px', labelFontSize: 12, labelColor: p.textSecondary },
      Tabs: { horizontalItemGutter: 16, horizontalMargin: '0', itemColor: p.textSecondary },
      Segmented: { itemSelectedBg: p.panel, trackBg: p.hover },
      Table: { headerBg: p.panelRaised, headerColor: p.textSecondary, rowHoverBg: p.hover, cellPaddingBlock: 10 },
      Popover: { colorBgElevated: p.panel },
    },
  };
}

/**
 * 编辑器密度档（D9，2026-10-30）：只罩住工作台节，演示 28 / 22 控件高；正式落地
 * 走编辑器自己的嵌套 ConfigProvider（计划 §2.5，W4 之后），core token 保持不动。
 */
export const workbenchDensityTheme: ThemeConfig = {
  token: { controlHeight: 28, controlHeightSM: 22 },
};
