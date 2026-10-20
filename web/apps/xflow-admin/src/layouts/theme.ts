import { foundationTokens, paletteTokens, type Appearance } from '@xflow/core';
import { theme, type ThemeConfig } from 'antd';
import { useEffect, useState } from 'react';

/**
 * System colour scheme. `systemAppearance` mirrors the editor's fallback when
 * the platform cannot answer (`prefers-color-scheme` unavailable): dark, the
 * editor's own default. The admin keeps no theme preference of its own — the
 * editor exposes light/dark only for the current editing session, so there is
 * no persisted key both hosts could share.
 */
export function systemAppearance(): Appearance {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
    return 'dark';
  }
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

/** Track `prefers-color-scheme` and return the appearance the OS asks for. */
export function useSystemAppearance(): Appearance {
  const [appearance, setAppearance] = useState<Appearance>(systemAppearance);

  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
      return undefined;
    }

    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
    const sync = () => setAppearance(mediaQuery.matches ? 'dark' : 'light');
    sync();

    if (typeof mediaQuery.addEventListener === 'function') {
      mediaQuery.addEventListener('change', sync);
      return () => mediaQuery.removeEventListener('change', sync);
    }

    mediaQuery.addListener?.(sync);
    return () => mediaQuery.removeListener?.(sync);
  }, []);

  return appearance;
}

/**
 * Admin AntD theme. Colour and type values come from `@xflow/core` — the same
 * record that generates `tokens.css` for the `.xflow-admin` root — so AntD and
 * the `--xflow-*` custom properties cannot drift apart. Only the AntD-specific
 * mapping lives here; component tokens match the design baseline.
 */
export function adminThemeConfig(appearance: Appearance): ThemeConfig {
  const palette = paletteTokens[appearance];
  return {
    algorithm: appearance === 'dark' ? theme.darkAlgorithm : theme.defaultAlgorithm,
    cssVar: { prefix: 'xflow-ant', key: `xflow-admin-${appearance}` },
    token: {
      fontFamily: foundationTokens.fontSans,
      fontFamilyCode: foundationTokens.fontMono,
      fontSize: 13,
      fontSizeSM: 12,
      borderRadius: 6,
      borderRadiusSM: 4,
      borderRadiusLG: 8,
      controlHeight: 32,
      controlHeightSM: 24,
      colorPrimary: palette.primary,
      colorInfo: palette.info,
      colorSuccess: palette.success,
      colorWarning: palette.warning,
      colorError: palette.danger,
      colorText: palette.text,
      colorTextSecondary: palette.textSecondary,
      colorTextTertiary: palette.textTertiary,
      colorTextDescription: palette.textTertiary,
      colorTextPlaceholder: palette.textTertiary,
      colorBgLayout: palette.app,
      colorBgContainer: palette.field,
      colorBgElevated: palette.panel,
      colorBorder: palette.border,
      colorBorderSecondary: palette.border,
      colorSplit: palette.border,
      colorFillTertiary: palette.hover,
      controlItemBgHover: palette.hover,
      boxShadowSecondary: palette.elev2,
    },
    components: {
      Button: { defaultBg: palette.panel, defaultShadow: 'none', primaryShadow: 'none', dangerShadow: 'none' },
      Modal: { borderRadiusLG: 12, contentBg: palette.panel, headerBg: palette.panel, footerBg: palette.panel },
      Form: { itemMarginBottom: 12, verticalLabelPadding: '0 0 4px', labelFontSize: 12, labelColor: palette.textSecondary },
      Tabs: { horizontalItemGutter: 16, horizontalMargin: '0', itemColor: palette.textSecondary },
      Segmented: { itemSelectedBg: palette.panel, trackBg: palette.hover },
      Table: { headerBg: palette.panelRaised, headerColor: palette.textSecondary, rowHoverBg: palette.hover, cellPaddingBlock: 10 },
      Popover: { colorBgElevated: palette.panel },
    },
  };
}
