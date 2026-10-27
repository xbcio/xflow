import { foundationTokens, paletteTokens, type Appearance, type Palette } from "./tokens";

/**
 * The AntD token map both ConfigProviders share. `@xflow/core` must not depend
 * on `antd`, so this module returns the plain `token`/`components` records and
 * each host owns the pieces AntD alone can supply: the palette algorithm, the
 * css-var scope key and host-specific component overrides. Everything colour,
 * radius, type and motion related is generated here from the same record that
 * renders `tokens.css`, so the two cannot drift.
 */
export type AntdTokenValue = string | number | boolean;

export interface SharedAntdToken {
  borderRadius: number;
  borderRadiusOuter: number;
  borderRadiusSM: number;
  borderRadiusLG: number;
  borderRadiusXS: number;
  boxShadow: string;
  boxShadowSecondary: string;
  boxShadowTertiary: string;
  colorBgBase: string;
  colorBgContainer: string;
  colorBgContainerDisabled: string;
  colorBgElevated: string;
  colorBgLayout: string;
  colorBgTextActive: string;
  colorBgTextHover: string;
  colorBorder: string;
  colorBorderBg: string;
  colorBorderSecondary: string;
  colorError: string;
  colorFillAlter: string;
  colorFillContent: string;
  colorFillContentHover: string;
  colorFillSecondary: string;
  colorFillTertiary: string;
  colorIcon: string;
  colorIconHover: string;
  colorInfo: string;
  colorLink: string;
  colorLinkActive: string;
  colorLinkHover: string;
  colorPrimary: string;
  colorPrimaryActive: string;
  colorPrimaryBorder: string;
  colorPrimaryHover: string;
  colorSplit: string;
  colorSuccess: string;
  colorText: string;
  colorTextBase: string;
  colorTextDescription: string;
  colorTextLabel: string;
  colorTextPlaceholder: string;
  colorTextSecondary: string;
  colorTextTertiary: string;
  colorWarning: string;
  controlHeight: number;
  controlHeightSM: number;
  controlItemBgActive: string;
  controlItemBgActiveHover: string;
  controlItemBgHover: string;
  controlOutline: string;
  controlOutlineWidth: number;
  fontFamily: string;
  fontFamilyCode: string;
  fontSize: number;
  fontSizeSM: number;
  fontWeightStrong: number;
  lineHeight: number;
  lineHeightSM: number;
  lineWidthFocus: number;
  motion: boolean;
  motionDurationFast: string;
  motionDurationMid: string;
  motionDurationSlow: string;
  motionEaseInOut: string;
  motionEaseOut: string;
}

/** Component tokens both hosts need; each host appends only its own extras. */
export interface SharedAntdComponents {
  Button: {
    dangerColor: string;
    dangerShadow: string;
    defaultActiveBg: string;
    defaultActiveBorderColor: string;
    defaultActiveColor: string;
    defaultBg: string;
    defaultBorderColor: string;
    defaultColor: string;
    defaultShadow: string;
    onlyIconSizeSM: number;
    primaryColor: string;
    primaryShadow: string;
    solidTextColor: string;
  };
  Card: {
    bodyPaddingSM: number;
    headerBg: string;
    headerPaddingSM: number;
  };
  Input: {
    activeBg: string;
    activeBorderColor: string;
    activeShadow: string;
    hoverBg: string;
    hoverBorderColor: string;
  };
  Modal: {
    borderRadiusLG: number;
    contentBg: string;
    footerBg: string;
    headerBg: string;
  };
  Popover: {
    colorBgElevated: string;
  };
  Select: {
    activeBorderColor: string;
    activeOutlineColor: string;
    hoverBorderColor: string;
    optionActiveBg: string;
    optionSelectedBg: string;
    optionSelectedColor: string;
    optionSelectedFontWeight: number;
    selectorBg: string;
  };
  Switch: {
    handleBg: string;
    handleShadow: string;
    handleSizeSM: number;
    trackHeightSM: number;
    trackMinWidthSM: number;
    trackPadding: number;
  };
  Table: {
    headerBg: string;
    headerColor: string;
    rowHoverBg: string;
  };
  Tabs: {
    horizontalItemGutter: number;
    horizontalMargin: string;
    itemColor: string;
  };
  Tag: {
    defaultBg: string;
    defaultColor: string;
    solidTextColor: string;
  };
}

export interface SharedAntdTheme {
  token: SharedAntdToken;
  components: SharedAntdComponents;
}

export interface AntdThemeOptions {
  /**
   * `prefers-reduced-motion: reduce`. AntD v6 reads `token.motion` for its
   * entrance/exit helpers, but its component CSS transitions read the duration
   * tokens directly, so `motion: false` alone leaves Select/Input/Segmented
   * animating; every duration drops to `0s` alongside it.
   */
  reducedMotion?: boolean;
}

/** AntD tokens take numbers; the shared pixel scale is the single source. */
const px = (token: string) => Number.parseInt(token, 10);

/** A unitless AntD line-height ratio that renders `fontSize` at exactly `lineHeight`. */
const lineHeightOf = (lineHeight: string, fontSize: string) => px(lineHeight) / px(fontSize);

/**
 * AntD runs its seed through the palette generator, and the dark generator
 * deepens and desaturates a blue seed (#2d6fdd renders #2961bf, 2.9:1 on the
 * panel). Each appearance therefore seeds the blue that lands on an AA link,
 * and the hover/active aliases pin the rendered values instead of letting the
 * generator pick a lighter shade.
 */
function linkColours(palette: Palette, mode: Appearance): { active: string; base: string; hover: string } {
  return mode === "dark"
    ? { active: palette.focusRing, base: palette.focusRing, hover: palette.focusRing }
    : { active: palette.primarySolidActive, base: palette.primarySolid, hover: palette.primarySolidHover };
}

export function buildAntdTheme(mode: Appearance, options: AntdThemeOptions = {}): SharedAntdTheme {
  const palette = paletteTokens[mode];
  const link = linkColours(palette, mode);
  // Two rings, two jobs. `focusRing` is the opaque keyboard ring (2px, AA on
  // every surface); `softRing` is the 25% primary wash the editor's input and
  // select glows keep using, matching `--xflow-primary-ring`.
  const softRing = `${palette.primary}40`;

  return {
    token: {
      borderRadius: px(foundationTokens.radiusSm),
      // 32px controls take `sm`, 24px controls `xs`, so a small control is not
      // as round as a full-height one; `outer` matches the small step.
      borderRadiusOuter: px(foundationTokens.radiusXs),
      borderRadiusSM: px(foundationTokens.radiusXs),
      borderRadiusLG: px(foundationTokens.radiusMd),
      borderRadiusXS: px(foundationTokens.radiusXs),
      // Modal/Notification carry the top step; secondary is the menu/popover
      // layer and tertiary the raised card/segmented thumb (stage 1b scale).
      boxShadow: palette.elev4,
      boxShadowSecondary: palette.elev3,
      boxShadowTertiary: palette.elev1,
      colorBgBase: palette.surfaceApp,
      colorBgContainer: palette.surfacePanel,
      colorBgContainerDisabled: palette.surfaceSunken,
      colorBgElevated: palette.surfaceOverlay,
      colorBgLayout: palette.surfaceApp,
      colorBgTextActive: palette.hover,
      colorBgTextHover: palette.hover,
      colorBorder: palette.border,
      colorBorderBg: palette.border,
      colorBorderSecondary: palette.borderSubtle,
      colorError: palette.danger,
      colorFillAlter: palette.field,
      colorFillContent: palette.field,
      colorFillContentHover: palette.hover,
      colorFillSecondary: palette.field,
      colorFillTertiary: palette.hover,
      colorIcon: palette.textTertiary,
      colorIconHover: palette.text,
      colorInfo: palette.info,
      colorLink: link.base,
      colorLinkActive: link.active,
      colorLinkHover: link.hover,
      // Solid fills and text both take the AA variant; the raw `primary` stays
      // the accent for selection glows and decorative bars.
      colorPrimary: palette.primarySolid,
      colorPrimaryActive: palette.primarySolidActive,
      colorPrimaryBorder: palette.focusRing,
      colorPrimaryHover: palette.primarySolidHover,
      colorSplit: palette.borderSubtle,
      colorSuccess: palette.success,
      colorText: palette.text,
      colorTextBase: palette.text,
      colorTextDescription: palette.textTertiary,
      colorTextLabel: palette.textSecondary,
      colorTextPlaceholder: palette.textTertiary,
      colorTextSecondary: palette.textSecondary,
      colorTextTertiary: palette.textTertiary,
      colorWarning: palette.warning,
      controlHeight: px(foundationTokens.controlHeight),
      controlHeightSM: px(foundationTokens.controlHeightSm),
      controlItemBgActive: palette.hover,
      controlItemBgActiveHover: palette.hover,
      controlItemBgHover: palette.hover,
      controlOutline: palette.focusRing,
      controlOutlineWidth: 2,
      fontFamily: foundationTokens.fontSans,
      fontFamilyCode: foundationTokens.fontMono,
      fontSize: px(foundationTokens.fontMd),
      fontSizeSM: px(foundationTokens.fontSm),
      fontWeightStrong: px(foundationTokens.fontWeightSemibold),
      // 13px body text gets the design's 20px rhythm instead of AntD's 1.5714
      // ratio (20.43px); the small step pairs 12px with 18px.
      lineHeight: lineHeightOf(foundationTokens.lineHeightMd, foundationTokens.fontMd),
      lineHeightSM: lineHeightOf(foundationTokens.lineHeightSm, foundationTokens.fontSm),
      lineWidthFocus: 2,
      motion: !options.reducedMotion,
      motionDurationFast: options.reducedMotion ? "0s" : foundationTokens.motionDurationFast,
      motionDurationMid: options.reducedMotion ? "0s" : foundationTokens.motionDurationBase,
      motionDurationSlow: options.reducedMotion ? "0s" : foundationTokens.motionDurationSlow,
      motionEaseInOut: foundationTokens.motionEaseStandard,
      motionEaseOut: foundationTokens.motionEaseEmphasized,
    },
    components: {
      Button: {
        dangerColor: palette.onDanger,
        dangerShadow: "none",
        defaultActiveBg: palette.hover,
        defaultActiveBorderColor: palette.primary,
        defaultActiveColor: palette.text,
        defaultBg: palette.surfacePanel,
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
        solidTextColor: palette.onPrimary,
      },
      Card: {
        bodyPaddingSM: 10,
        headerBg: palette.surfaceSunken,
        headerPaddingSM: 10,
      },
      Input: {
        activeBg: palette.field,
        activeBorderColor: palette.primary,
        activeShadow: `0 0 0 3px ${softRing}`,
        hoverBg: palette.field,
        hoverBorderColor: palette.primary,
      },
      Modal: {
        borderRadiusLG: px(foundationTokens.radiusLg),
        contentBg: palette.surfaceOverlay,
        footerBg: palette.surfaceOverlay,
        headerBg: palette.surfaceOverlay,
      },
      Popover: { colorBgElevated: palette.surfaceOverlay },
      Select: {
        activeBorderColor: palette.primary,
        activeOutlineColor: softRing,
        hoverBorderColor: palette.primary,
        optionActiveBg: palette.hover,
        optionSelectedBg: palette.hover,
        optionSelectedColor: palette.text,
        optionSelectedFontWeight: px(foundationTokens.fontWeightSemibold),
        selectorBg: palette.field,
      },
      Switch: {
        handleBg: palette.field,
        handleShadow: "none",
        handleSizeSM: 12,
        trackHeightSM: 16,
        trackMinWidthSM: 28,
        trackPadding: 2,
      },
      // AD-20: density lives in admin.css (td `8px 10px`); no cell padding here,
      // so there is one source for the row height.
      Table: {
        headerBg: palette.surfaceSunken,
        headerColor: palette.textSecondary,
        rowHoverBg: palette.hover,
      },
      // Same values as the baseline's Tabs token (ui-preview/tokens.ts): 16px
      // between items, no gap under the nav, inactive labels in secondary text.
      Tabs: {
        horizontalItemGutter: 16,
        horizontalMargin: "0",
        itemColor: palette.textSecondary,
      },
      Tag: {
        defaultBg: palette.surfaceSunken,
        defaultColor: palette.textSecondary,
        solidTextColor: palette.onPrimary,
      },
    },
  };
}
