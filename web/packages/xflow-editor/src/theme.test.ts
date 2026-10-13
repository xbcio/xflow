import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  editorThemePalettes,
  type EditorThemePalette,
  type ResolvedXFlowEditorAppearance,
  type XFlowEditorThemeVariant
} from "./theme";

/**
 * The palette feeds AntD's ConfigProvider while styles.css feeds every
 * editor-owned surface, so a color edited in only one of them splits the UI
 * into two shades. Each palette key names the --xflow-* variable it mirrors.
 */
const cssVarByPaletteKey: Record<keyof EditorThemePalette, string> = {
  app: "--xflow-surface-app",
  toolbar: "--xflow-surface-toolbar",
  panel: "--xflow-surface-panel",
  panelRaised: "--xflow-surface-panel-raised",
  field: "--xflow-surface-field",
  hover: "--xflow-surface-hover",
  text: "--xflow-text",
  textSecondary: "--xflow-text-secondary",
  textTertiary: "--xflow-text-tertiary",
  border: "--xflow-border",
  borderStrong: "--xflow-border-strong",
  primary: "--xflow-focus",
  success: "--xflow-status-success",
  warning: "--xflow-status-warning",
  error: "--xflow-status-error"
};

// Read from disk: Vitest's css handling turns `?raw` stylesheet imports into
// empty strings. `import.meta.dirname` is the real source directory (jsdom
// replaces `import.meta.url`'s file scheme).
const stylesheet = readFileSync(`${import.meta.dirname}/styles.css`, "utf8").replace(/\/\*[\s\S]*?\*\//g, "");

/** Custom properties declared by the rule whose selector list names this theme. */
function themeDeclarations(
  variant: XFlowEditorThemeVariant,
  appearance: ResolvedXFlowEditorAppearance
): Map<string, string> {
  const selector = `.xflow-editor[data-theme-variant="${variant}"][data-theme="${appearance}"]`;
  const blocks = [...stylesheet.matchAll(/([^{}]+)\{([^{}]*)\}/g)]
    .filter(([, selectors]) => selectors.split(",").some((part) => part.trim() === selector));
  expect(blocks, `exactly one rule for ${selector}`).toHaveLength(1);
  const declarations = new Map<string, string>();
  for (const [, name, value] of blocks[0][2].matchAll(/(--xflow-[\w-]+)\s*:\s*([^;]+);/g)) {
    declarations.set(name, value.trim().toLowerCase());
  }
  return declarations;
}

describe("editor theme palette", () => {
  for (const [variant, appearances] of Object.entries(editorThemePalettes)) {
    for (const [appearance, palette] of Object.entries(appearances)) {
      it(`matches the ${variant} ${appearance} CSS variables`, () => {
        const declarations = themeDeclarations(
          variant as XFlowEditorThemeVariant,
          appearance as ResolvedXFlowEditorAppearance
        );
        const mismatches = Object.entries(cssVarByPaletteKey)
          .map(([key, cssVar]) => ({
            key,
            cssVar,
            palette: palette[key as keyof EditorThemePalette].toLowerCase(),
            css: declarations.get(cssVar)
          }))
          .filter((entry) => entry.palette !== entry.css);
        expect(mismatches).toEqual([]);
      });
    }
  }

  it("maps every palette key to a CSS variable", () => {
    expect(Object.keys(cssVarByPaletteKey).sort()).toEqual(Object.keys(editorThemePalettes.graphite.dark).sort());
  });
});
