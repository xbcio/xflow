import { buildAntdTheme, paletteTokens, type AntdThemeOptions, type Appearance } from '@xflow/core';
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

const REDUCED_MOTION_QUERY = '(prefers-reduced-motion: reduce)';

function systemReducedMotion(): boolean {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
    return false;
  }
  return window.matchMedia(REDUCED_MOTION_QUERY).matches;
}

/**
 * Track the OS "reduce motion" switch. tokens.css zeroes the duration tokens
 * on the same signal; this hook flips AntD's own `token.motion` off so its
 * collapsed transitions match the CSS side.
 */
export function useReducedMotion(): boolean {
  const [reduced, setReduced] = useState(systemReducedMotion);

  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
      return undefined;
    }

    const mediaQuery = window.matchMedia(REDUCED_MOTION_QUERY);
    const sync = () => setReduced(mediaQuery.matches);
    sync();

    if (typeof mediaQuery.addEventListener === 'function') {
      mediaQuery.addEventListener('change', sync);
      return () => mediaQuery.removeEventListener('change', sync);
    }

    mediaQuery.addListener?.(sync);
    return () => mediaQuery.removeListener?.(sync);
  }, []);

  return reduced;
}

/**
 * AL-30: semantic class hooks for the compact inline banner (admin.css). The
 * tone class carries the colour variables; AntD still draws the icon, so the
 * state is never colour alone. Passed to `ConfigProvider alert.classNames`, so
 * every Alert below the admin shell takes it without repeating the classes.
 */
export const adminAlertClassNames = ({ props }: { props: { type?: string } }) => ({
  root: `xflow-admin-alert xflow-admin-alert--${props.type ?? 'info'}`,
  icon: 'xflow-admin-alert-icon',
  title: 'xflow-admin-alert-title',
  description: 'xflow-admin-alert-description',
  actions: 'xflow-admin-alert-actions',
});

/**
 * Admin AntD theme. Colour, focus, radius, type, elevation and motion come from
 * `@xflow/core`'s `buildAntdTheme` — the same mapping the editor uses — so the
 * console and the editor cannot drift. Only the admin's own component tokens
 * (the vertical Form, its Segmented density) live here.
 */
export function adminThemeConfig(appearance: Appearance, options: AntdThemeOptions = {}): ThemeConfig {
  const palette = paletteTokens[appearance];
  const shared = buildAntdTheme(appearance, options);

  return {
    algorithm: appearance === 'dark' ? theme.darkAlgorithm : theme.defaultAlgorithm,
    cssVar: { prefix: 'xflow-ant', key: `xflow-admin-${appearance}` },
    token: shared.token,
    components: {
      ...shared.components,
      Form: { itemMarginBottom: 12, verticalLabelPadding: '0 0 4px', labelFontSize: 12, labelColor: palette.textSecondary },
      Segmented: { itemSelectedBg: palette.panel, trackBg: palette.hover },
    },
  };
}
