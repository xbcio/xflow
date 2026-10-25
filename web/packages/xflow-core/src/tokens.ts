export type Appearance = "light" | "dark";

/**
 * Semantic palette (spec §3.3). This is the only place a colour value is
 * written: `renderTokensCss` turns it into the `--xflow-*` custom properties
 * every XFlow surface reads, and hosts feed the same record to AntD's
 * ConfigProvider, so the two cannot drift apart.
 */
export interface Palette {
  /**
   * The five-step surface scale (stage 1b). `sunken` is recessed below the app
   * ground (table/section headers, code blocks), `panel` is the docked surface,
   * `raised` lifts with `elev1`, and `overlay` lifts with `elev3`/`elev4`.
   * Light separates raised/overlay from panel by shadow alone; dark separates
   * every step by L*.
   */
  surfaceSunken: string;
  surfaceApp: string;
  surfacePanel: string;
  surfaceRaised: string;
  surfaceOverlay: string;
  /** @deprecated Alias of `surfaceApp`; migrate call sites to the surface scale. */
  app: string;
  /** @deprecated Alias of `surfacePanel`. */
  panel: string;
  /**
   * @deprecated Split by role: light resolves to `surfaceSunken` (the old value
   * was darker than panel and was used as a recessed header fill), dark to
   * `surfaceRaised`. Replace call sites with `surfaceSunken` or `surfaceRaised`.
   */
  panelRaised: string;
  field: string;
  canvas: string;
  canvasDot: string;
  /**
   * Separator lines: header/footer rules, list-row dividers, panel seams. The
   * default `border` is the outline of a card, field or floating layer; using
   * the same value for both made every divider read as a container edge
   * (stage 1c).
   */
  borderSubtle: string;
  /** Card/field/overlay outlines. */
  border: string;
  /** Hover outlines, group frames and dashed placeholders. */
  borderStrong: string;
  /**
   * @deprecated Alias of `--xflow-surface-hover`, the text-tinted wash. Hover
   * is now a wash so it works on raised and overlay surfaces, not just panel.
   */
  hover: string;
  text: string;
  textSecondary: string;
  textTertiary: string;
  primary: string;
  onPrimary: string;
  /**
   * Solid primary fill that carries `onPrimary` text at AA. `primary` stays the
   * brand accent (rings, glows, decorative bars); a fill with white text needs a
   * deeper blue because the brand value only reaches 3.85:1.
   */
  primarySolid: string;
  primarySolidHover: string;
  primarySolidActive: string;
  /** Opaque keyboard focus ring, at least 3:1 on every surface of the theme. */
  focusRing: string;
  info: string;
  success: string;
  warning: string;
  danger: string;
  suspend: string;
  neutral: string;
  accent: string;
  orange: string;
  cyan: string;
  /** Soft top-edge highlight on raised surfaces. */
  highlight: string;
  /** Hover wash for list rows that must not hide what is behind them. */
  hoverSubtle: string;
  /** Inner shadow under a pressed or inset control. */
  insetShadow: string;
  /**
   * @deprecated The single-colour drop shadow the editor's own popup/dock
   * variables used. Use the `elev*` scale instead.
   */
  shadow: string;
  /** No elevation: a flat surface or an element that already sits in flow. */
  elev0: string;
  /** Node cards at rest, inline cards: `surfaceRaised`. */
  elev1: string;
  /** Hovered node, floating bars, minimap: `surfaceOverlay` at rest. */
  elev2: string;
  /** Menus, popovers, tooltips, toasts, docks: `surfaceOverlay`. */
  elev3: string;
  /** Modals, drawers, approval dialogs, a node being dragged. */
  elev4: string;
}

/**
 * Surface scale values per appearance. Light separates raised/overlay from the
 * panel by shadow alone (all three are #ffffff, as the design draws them);
 * dark lifts each step in L* so adjacent surfaces stay ≥3 L* apart without
 * relying on the shadow.
 */
const lightSurfaces = {
  surfaceSunken: "#eef0f3",
  surfaceApp: "#f4f5f7",
  surfacePanel: "#ffffff",
  surfaceRaised: "#ffffff",
  surfaceOverlay: "#ffffff",
} as const;

const darkSurfaces = {
  surfaceSunken: "#0e0f10",
  surfaceApp: "#111213",
  surfacePanel: "#1b1c1f",
  surfaceRaised: "#222428",
  surfaceOverlay: "#2a2d32",
} as const;

/** Dark keeps the same steps but deepens the shadow and adds a 1px top highlight. */
const lightElevation = {
  elev0: "none",
  elev1: "0 1px 2px rgba(16,24,40,.06), 0 1px 3px rgba(16,24,40,.08)",
  elev2: "0 4px 12px rgba(16,24,40,.10), 0 1px 3px rgba(16,24,40,.06)",
  elev3: "0 12px 28px rgba(16,24,40,.14), 0 2px 6px rgba(16,24,40,.06)",
  elev4: "0 24px 56px rgba(16,24,40,.20), 0 4px 12px rgba(16,24,40,.08)",
} as const;

const darkElevation = {
  elev0: "none",
  elev1: "0 1px 2px rgba(0,0,0,.40)",
  elev2: "0 6px 16px rgba(0,0,0,.45), inset 0 1px 0 rgba(255,255,255,.04)",
  elev3: "0 12px 32px rgba(0,0,0,.55), inset 0 1px 0 rgba(255,255,255,.05)",
  elev4: "0 24px 64px rgba(0,0,0,.65), inset 0 1px 0 rgba(255,255,255,.06)",
} as const;

export const palettes: Record<Appearance, Palette> = {
  light: {
    ...lightSurfaces,
    app: lightSurfaces.surfaceApp,
    panel: lightSurfaces.surfacePanel,
    panelRaised: lightSurfaces.surfaceSunken,
    field: "#ffffff",
    canvas: "#f7f8fa",
    canvasDot: "#d4d8df",
    borderSubtle: "#eceef1",
    border: "#e2e5ea",
    borderStrong: "#c9ced6",
    // Text-tinted wash so hover reads on panel, raised and overlay alike.
    hover: "rgba(22, 25, 29, .06)",
    text: "#16191d",
    textSecondary: "#4a5260",
    // Spec §3.3 named #6b7280 (user ruling 2026-10-20), which held 4.83:1 on
    // `surfacePanel` but only 4.43:1 on the app ground and 4.23:1 on
    // `surfaceSunken` — tertiary labels are painted on all three, so stage 1b
    // deepened it until it clears 4.5:1 on the sunken surface too (4.73:1).
    textTertiary: "#616b79",
    primary: "#2f7cff",
    onPrimary: "#ffffff",
    // White on #1f69e0 measures 5.05:1; hover/active keep darkening instead of
    // AntD's default lightening, which would drop the label back under AA.
    primarySolid: "#1f69e0",
    primarySolidHover: "#1a5fcf",
    primarySolidActive: "#164fb0",
    // 5.05:1 on panel/overlay, 4.63:1 on the app ground — opaque, so it cannot
    // wash out against the surface it sits on.
    focusRing: "#1f69e0",
    info: "#2f7cff",
    success: "#12855a",
    warning: "#b26b00",
    danger: "#d1333b",
    suspend: "#7a52d6",
    neutral: "#6b7280",
    accent: "#0d8f8f",
    orange: "#d0601a",
    cyan: "#0e86b0",
    highlight: "rgba(255, 255, 255, .78)",
    hoverSubtle: "rgba(47, 124, 255, .06)",
    insetShadow: "rgba(22, 25, 29, .08)",
    shadow: "rgba(22, 25, 29, .10)",
    ...lightElevation,
  },
  dark: {
    ...darkSurfaces,
    app: darkSurfaces.surfaceApp,
    panel: darkSurfaces.surfacePanel,
    panelRaised: darkSurfaces.surfaceRaised,
    field: "#16171a",
    canvas: "#151617",
    canvasDot: "#2c2f34",
    borderSubtle: "#26292d",
    border: "#2e3136",
    borderStrong: "#41454c",
    hover: "rgba(238, 240, 243, .06)",
    text: "#eef0f3",
    textSecondary: "#b9bfc8",
    textTertiary: "#8f97a3",
    primary: "#4c8dff",
    onPrimary: "#ffffff",
    // White on #2d6fdd measures 4.74:1 (the dark brand value is 3.20:1). AntD
    // re-derives a seed through its dark palette generator, which only deepens
    // this fill (#2961bf, 5.90:1). Hover and active deepen rather than brighten
    // for the same reason as in light.
    primarySolid: "#2d6fdd",
    primarySolidHover: "#2a66cc",
    primarySolidActive: "#255fb8",
    // 8.16:1 on panel, 6.96:1 on overlay, 8.98:1 on the app ground.
    focusRing: "#8ab4ff",
    info: "#5b9bff",
    success: "#35c98b",
    warning: "#f2b44c",
    danger: "#ff6a62",
    suspend: "#a98bff",
    neutral: "#8f97a3",
    accent: "#2cc4c4",
    orange: "#ff9a52",
    cyan: "#45bde6",
    highlight: "rgba(255, 255, 255, .035)",
    hoverSubtle: "rgba(255, 255, 255, .03)",
    insetShadow: "rgba(0, 0, 0, .18)",
    shadow: "rgba(0, 0, 0, .18)",
    ...darkElevation,
  },
};

const fontSans = 'Inter, "PingFang SC", "Microsoft YaHei", system-ui, sans-serif';
const fontMono = '"JetBrains Mono", "SF Mono", Menlo, monospace';
const fontXs = "11px";
const fontSm = "12px";
const fontMd = "13px";
const fontLg = "14px";
const fontXl = "16px";
const lineHeightXs = "16px";
const lineHeightSm = "18px";
const lineHeightMd = "20px";
const lineHeightLg = "22px";
const lineHeightXl = "24px";

/** A complete `font` shorthand, so a composite token is one declaration. */
const type = (weight: string, size: string, lineHeight: string, family: string) =>
  `${weight} ${size}/${lineHeight} ${family}`;

/**
 * Appearance-independent foundation values (spec §3.1, §3.2). Stage 1c adds
 * the weight scale, composite type styles and the full-radius rename; the
 * values are built from the same size/line-height constants they pair with.
 */
export const foundation = {
  fontSans,
  fontMono,
  fontXs,
  fontSm,
  fontMd,
  fontLg,
  fontXl,
  // Weight scale (stage 1c). The sheets used 400/500/600 literals only.
  fontWeightRegular: "400",
  fontWeightMedium: "500",
  fontWeightSemibold: "600",
  // Paired line heights (spec ED-16): xs 11/16, sm 12/18, md 13/20, lg 14/22,
  // xl 16/24.
  lineHeightXs,
  lineHeightSm,
  lineHeightMd,
  lineHeightLg,
  lineHeightXl,
  // The design baseline's body rhythm (13px/20px); hosts opt in explicitly.
  lineHeight: lineHeightMd,
  // Composite type styles: weight + size/line-height + family in one value, so
  // a label or heading cannot drift across those three dimensions.
  textCaption: type("400", fontXs, lineHeightXs, fontSans),
  textBody: type("400", fontMd, lineHeightMd, fontSans),
  textLabel: type("500", fontSm, lineHeightSm, fontSans),
  textTitle: type("600", fontLg, lineHeightLg, fontSans),
  textHeading: type("600", fontXl, lineHeightXl, fontSans),
  textMono: type("400", fontXs, lineHeightXs, fontMono),
  radiusXs: "4px",
  radiusSm: "6px",
  radiusMd: "8px",
  radiusLg: "12px",
  radiusFull: "9999px",
  /** @deprecated Alias of `radiusFull`; use `radiusFull` for pills and circles. */
  radiusPill: "9999px",
  // Spacing scale (spec §4): four 4px-based steps (1/2/3/4) plus the two
  // de-facto half steps the sheets already used (1.5 = 6px, 2.5 = 10px).
  // 0.5 is the micro step for icon/text nudges and chip vertical padding.
  space0: "0px",
  space0_5: "2px",
  space1: "4px",
  space1_5: "6px",
  space2: "8px",
  space2_5: "10px",
  space3: "12px",
  space4: "16px",
  space5: "20px",
  space6: "24px",
  space8: "32px",
  space10: "40px",
  space12: "48px",
  // Motion (spec §5). Durations are the only dimension `prefers-reduced-motion`
  // overrides; easings keep their shape because a 0ms transition never plays.
  motionDurationInstant: "80ms",
  motionDurationFast: "120ms",
  motionDurationBase: "180ms",
  motionDurationSlow: "260ms",
  motionEaseStandard: "cubic-bezier(.2, 0, 0, 1)",
  // Entrance / drag-settle: settles onto the target without overshoot.
  motionEaseEmphasized: "cubic-bezier(.3, 0, 0, 1)",
  // Exit / collapse: leaves immediately and decelerates into the end state.
  motionEaseExit: "cubic-bezier(.4, 0, 1, 1)",
  controlHeight: "32px",
  controlHeightSm: "24px",
  // Every canvas floating bar (add node, run bar, zoom bar, reopen chip) is
  // this tall: 1px border + 4px padding + 24px small control + 4px + 1px.
  floatHeight: "34px",
  nodeWidth: "264px",
  ruler: "20px",
} as const;

/** The raw records, for hosts that print or inspect values (token gallery, AntD mapping). */
export const paletteTokens = palettes;
export const foundationTokens = foundation;

/**
 * The ten colours that carry a derived family. Each one yields `-fg`,
 * `-bg-subtle` and `-border` (spec §3.3), generated with `color-mix()` from the
 * palette value so a tint can never be hand-picked out of step with its base.
 */
export const semanticColors = [
  "primary",
  "info",
  "success",
  "warning",
  "danger",
  "suspend",
  "neutral",
  "accent",
  "orange",
  "cyan",
] as const;

export type SemanticColor = (typeof semanticColors)[number];

/**
 * Share of the base colour in `-fg`; the rest is `text`, which darkens it on
 * light grounds and lightens it on dark ones. 70% keeps the hue recognisable
 * while the worst of the ten stays above 6:1 on `panel` in both appearances
 * (asserted in tokens.test.ts).
 *
 * `-fg` is for standalone tone text on a panel (errors, hints, links). Status
 * chips keep the design's `--t-fg: var(--t)` instead: their 12% tint is their
 * own surface, and their foreground is the raw semantic colour.
 */
export const FG_MIX_PERCENT = 70;
const BG_SUBTLE_PERCENT = 12;
const BORDER_PERCENT = 40;

const kebab = (key: string) =>
  key
    .replace(/_/g, "-")
    .replace(/[A-Z0-9]+/g, (part) => `-${part.toLowerCase()}`)
    // An underscore separator can sit between two numeric parts
    // (`space0_5` → `space-0--5`); collapse the double hyphen it leaves.
    .replace(/-+/g, "-");

/** `fontXs` → `--xflow-font-xs`: the custom property a token renders as. */
export function cssVarName(key: string): string {
  return `--xflow-${kebab(key)}`;
}

const mixOf = (name: string, percent: number, other: string) =>
  `color-mix(in srgb, var(${cssVarName(name)}) ${percent}%, ${other})`;

/**
 * Derived custom properties, in declaration order. Values reference the
 * palette variables declared on the same element, so one set of expressions
 * serves both appearances.
 */
export const derivedTokens: ReadonlyArray<readonly [name: string, value: string]> = [
  ...semanticColors.flatMap((color) => [
    [`${kebab(color)}-fg`, mixOf(color, FG_MIX_PERCENT, `var(${cssVarName("text")})`)],
    [`${kebab(color)}-bg-subtle`, mixOf(color, BG_SUBTLE_PERCENT, "transparent")],
    [`${kebab(color)}-border`, mixOf(color, BORDER_PERCENT, "transparent")],
  ] as const),
  ["primary-border-strong", mixOf("primary", 70, "transparent")],
  ["primary-ring", mixOf("primary", 25, "transparent")],
  ["overlay", mixOf("panel", 92, "transparent")],
  ["overlay-subtle", mixOf("panel", 84, "transparent")],
  // Stage 1b: hover/pressed are text-tinted washes, so the same declaration
  // reads on panel, raised and overlay instead of only on an opaque fill.
  ["surface-hover", mixOf("text", 6, "transparent")],
  ["surface-active", mixOf("text", 10, "transparent")],
  ["minimap-mask", mixOf("app", 88, "transparent")],
  ["grid-minor", mixOf("text", 5, "transparent")],
  ["grid-major", mixOf("text", 13, "transparent")],
  ["grid-dot", mixOf("text", 12, "transparent")],
];

/**
 * Elements that carry the token scope: the editor workbench, a standalone
 * preview host, and the admin console shell. All three read the same palette,
 * so their AntD ConfigProviders and `--xflow-*` properties cannot drift.
 */
const tokenRoots = ":where(.xflow-editor, .xflow-preview-root, .xflow-admin)";

const declarations = (entries: ReadonlyArray<readonly [string, string]>) =>
  entries.map(([name, value]) => `    --xflow-${name}: ${value};`).join("\n");

function themeBlock(appearance: Appearance): string {
  const palette = Object.entries(palettes[appearance]).map(([key, value]) => [kebab(key), value] as const);
  return [
    `  ${tokenRoots}[data-theme="${appearance}"] {`,
    `    color-scheme: ${appearance};`,
    declarations(palette),
    declarations(derivedTokens),
    "  }",
  ].join("\n");
}

/**
 * The full text of `tokens.css`. The committed file is exactly this string
 * (tokens.test.ts compares them byte for byte); regenerate it with
 * `pnpm --filter @xflow/core tokens:generate` after editing anything above.
 */
export function renderTokensCss(): string {
  const base = Object.entries(foundation).map(([key, value]) => [kebab(key), value] as const);
  return [
    "/* Generated from src/tokens.ts by `pnpm --filter @xflow/core tokens:generate`. Do not edit by hand. */",
    "@layer xflow.tokens {",
    `  ${tokenRoots} {`,
    declarations(base),
    "  }",
    "",
    themeBlock("light"),
    "",
    themeBlock("dark"),
    "",
    reducedMotionBlock(),
    "}",
    "",
  ].join("\n");
}

/**
 * `prefers-reduced-motion` zeroes every duration token, so token-driven CSS
 * collapses to its end state instead of playing; the selectors carry the same
 * zero specificity as the foundation block but come later, so they win without
 * `!important`. No wildcard fallback is needed: the editor, preview and admin
 * sheets must express every duration through these tokens (pinned by their
 * own stylesheet checks). Hosts also flip AntD's own `token.motion` off.
 */
function reducedMotionBlock(): string {
  const durations = Object.keys(foundation).filter((key) => key.startsWith("motionDuration"));
  return [
    "  @media (prefers-reduced-motion: reduce) {",
    `    ${tokenRoots} {`,
    ...durations.map((key) => `      ${cssVarName(key)}: 0ms;`),
    "    }",
    "  }",
  ].join("\n");
}

