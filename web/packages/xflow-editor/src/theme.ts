import {
  buildAntdTheme,
  foundationTokens,
  paletteTokens,
  type AntdThemeOptions,
  type Appearance
} from "@xflow/core";
import { theme, type ThemeConfig } from "antd";
import { useEffect, useState } from "react";

/**
 * `system` follows `prefers-color-scheme`; the other two are explicit. The
 * resolved value is what `data-theme` carries and what picks the palette.
 * Palette values live in `@xflow/core` (`paletteTokens`); the editor keeps no
 * colour table of its own.
 */
export type XFlowEditorAppearance = Appearance | "system";
export type ResolvedXFlowEditorAppearance = Appearance;

const REDUCED_MOTION_QUERY = "(prefers-reduced-motion: reduce)";

function systemReducedMotion(): boolean {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") {
    return false;
  }
  return window.matchMedia(REDUCED_MOTION_QUERY).matches;
}

/**
 * Track the OS "reduce motion" switch. The CSS duration tokens already drop to
 * 0ms through tokens.css; this hook lets the ConfigProvider flip AntD's own
 * `token.motion` off in the same breath.
 */
export function useReducedMotion(): boolean {
  const [reduced, setReduced] = useState(systemReducedMotion);

  useEffect(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") {
      return undefined;
    }

    const mediaQuery = window.matchMedia(REDUCED_MOTION_QUERY);
    const sync = () => setReduced(mediaQuery.matches);
    sync();

    if (typeof mediaQuery.addEventListener === "function") {
      mediaQuery.addEventListener("change", sync);
      return () => mediaQuery.removeEventListener("change", sync);
    }

    mediaQuery.addListener?.(sync);
    return () => mediaQuery.removeListener?.(sync);
  }, []);

  return reduced;
}

/**
 * The top bar's density step (§6.3): one 28px control step under the 32px
 * foundation, with 22px small controls. It is nested inside the editor's own
 * ConfigProvider, which merges these tokens over the inherited theme, so the
 * palette, algorithm and components the editor already set stay in force.
 * Every other surface keeps the foundation height; only the `<header>` is
 * wrapped, so popups must restore this step where they need it.
 */
export const editorDensityTheme: ThemeConfig = {
  token: { controlHeight: 28, controlHeightSM: 22 }
};

/**
 * The foundation control step, for the popups that hang off the dense header.
 * A header popup is a React descendant of the density provider, so its menu
 * rows, segmented tracks and dialog buttons would all shrink with the bar;
 * wrapping the popup body in this restores the 32 / 24 step the rest of the
 * editor uses. Values come from `@xflow/core`'s foundation tokens, never a
 * literal (the CSS guard reads the same source).
 */
export const coreControlHeightTheme: ThemeConfig = {
  token: {
    controlHeight: Number.parseInt(foundationTokens.controlHeight, 10),
    controlHeightSM: Number.parseInt(foundationTokens.controlHeightSm, 10)
  }
};

/**
 * The editor's AntD theme. Everything the console shares — palette, focus rings,
 * radius, type, elevation and motion — comes from `@xflow/core`'s
 * `buildAntdTheme`; only the editor's own component overrides live here.
 */
export function createEditorThemeConfig(
  appearance: ResolvedXFlowEditorAppearance,
  options: AntdThemeOptions = {}
): ThemeConfig {
  const palette = paletteTokens[appearance];
  const shared = buildAntdTheme(appearance, options);

  return {
    algorithm: appearance === "dark" ? theme.darkAlgorithm : theme.defaultAlgorithm,
    cssVar: {
      prefix: "xflow-ant",
      key: `xflow-editor-${appearance}`
    },
    token: shared.token,
    components: {
      ...shared.components,
      Segmented: {
        itemActiveBg: palette.hover,
        itemColor: palette.textSecondary,
        itemHoverBg: palette.hover,
        itemHoverColor: palette.text,
        itemSelectedBg: palette.panel,
        itemSelectedColor: palette.text,
        // The selected thumb carries the shared elevation in the baseline; AntD's
        // boxShadowTertiary default is flatter and lighter.
        boxShadowTertiary: palette.elev1,
        trackBg: palette.hover,
        trackPadding: 2
      }
    }
  };
}
