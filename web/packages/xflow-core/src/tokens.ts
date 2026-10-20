export type Appearance = "light" | "dark";

/**
 * Semantic palette (spec §3.3). This is the only place a colour value is
 * written: `renderTokensCss` turns it into the `--xflow-*` custom properties
 * every XFlow surface reads, and hosts feed the same record to AntD's
 * ConfigProvider, so the two cannot drift apart.
 */
export interface Palette {
  app: string;
  panel: string;
  panelRaised: string;
  field: string;
  canvas: string;
  canvasDot: string;
  border: string;
  borderStrong: string;
  hover: string;
  text: string;
  textSecondary: string;
  textTertiary: string;
  primary: string;
  onPrimary: string;
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
  /** Colour of the drop shadow under floating surfaces (menus, popovers). */
  shadow: string;
  elev1: string;
  elev2: string;
  elev3: string;
}

export const palettes: Record<Appearance, Palette> = {
  light: {
    app: "#f4f5f7",
    panel: "#ffffff",
    panelRaised: "#f8f9fb",
    field: "#ffffff",
    canvas: "#f7f8fa",
    canvasDot: "#d4d8df",
    border: "#e2e5ea",
    borderStrong: "#c9ced6",
    hover: "#f0f2f5",
    text: "#16191d",
    textSecondary: "#4a5260",
    // Spec §3.3 asks for 4.5:1 on both `panel` and `app`; the preview's
    // #6b7280 measures 4.43:1 on the `app` ground, so this is the next step
    // down the same slate ramp (4.56:1 / 4.97:1).
    textTertiary: "#667085",
    primary: "#2f7cff",
    onPrimary: "#ffffff",
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
    elev1: "0 1px 2px rgba(16,24,40,.06), 0 1px 3px rgba(16,24,40,.08)",
    elev2: "0 4px 12px rgba(16,24,40,.10), 0 1px 3px rgba(16,24,40,.06)",
    elev3: "0 16px 40px rgba(16,24,40,.18)",
  },
  dark: {
    app: "#111213",
    panel: "#1b1c1f",
    panelRaised: "#222428",
    field: "#16171a",
    canvas: "#151617",
    canvasDot: "#2c2f34",
    border: "#2e3136",
    borderStrong: "#41454c",
    hover: "#26292e",
    text: "#eef0f3",
    textSecondary: "#b9bfc8",
    textTertiary: "#8f97a3",
    primary: "#4c8dff",
    onPrimary: "#ffffff",
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
    elev1: "0 1px 2px rgba(0,0,0,.4)",
    elev2: "0 6px 16px rgba(0,0,0,.45)",
    elev3: "0 18px 48px rgba(0,0,0,.55)",
  },
};

/** Appearance-independent foundation values (spec §3.1, §3.2). */
export const foundation = {
  fontSans: 'Inter, "PingFang SC", "Microsoft YaHei", system-ui, sans-serif',
  fontMono: '"JetBrains Mono", "SF Mono", Menlo, monospace',
  fontXs: "11px",
  fontSm: "12px",
  fontMd: "13px",
  fontLg: "14px",
  fontXl: "16px",
  // The design baseline's body rhythm (13px/20px); hosts opt in explicitly.
  lineHeight: "20px",
  radiusXs: "4px",
  radiusSm: "6px",
  radiusMd: "8px",
  radiusLg: "12px",
  radiusPill: "999px",
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
 */
export const FG_MIX_PERCENT = 70;
const BG_SUBTLE_PERCENT = 12;
const BORDER_PERCENT = 40;

const kebab = (key: string) => key.replace(/[A-Z0-9]+/g, (part) => `-${part.toLowerCase()}`);

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
    "}",
    "",
  ].join("\n");
}
