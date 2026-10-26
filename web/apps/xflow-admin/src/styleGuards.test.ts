import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

// jsdom applies no stylesheet, so a bare px font size or a loose z-index in the
// Admin sheets is invisible to every component test. This reads each
// `src/**/*.css` from disk (generated `.umi`, `dist` and `node_modules` are not
// source) and scans declarations after the comments are blanked out, because
// the headers name the very patterns the checks below ban.

type Declaration = { file: string; line: number; property: string; value: string };

const SRC = import.meta.dirname;
const SKIPPED_DIRS = new Set(["node_modules", "dist", ".umi"]);

const stripComments = (source: string) => source.replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, " "));

// Comments are blanked rather than removed so line numbers stay true.
const DECLARATION = /(?:^|[{;}])\s*(font-size|font|z-index)\s*:\s*([^;{}]*)/g;

function declarationsOf(file: string, source: string): Declaration[] {
  const css = stripComments(source);
  return [...css.matchAll(DECLARATION)].map((match) => {
    const start = match.index + match[0].indexOf(match[1]);
    return {
      file,
      line: css.slice(0, start).split("\n").length,
      property: match[1],
      value: match[2].replace(/!important/i, "").replace(/\s+/g, " ").trim()
    };
  });
}

function cssFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    if (entry.isDirectory()) return SKIPPED_DIRS.has(entry.name) ? [] : cssFiles(path.join(dir, entry.name));
    return entry.name.endsWith(".css") ? [path.join(dir, entry.name)] : [];
  });
}

const allDeclarations = cssFiles(SRC).flatMap((abs) =>
  declarationsOf(path.relative(SRC, abs).split(path.sep).join("/"), readFileSync(abs, "utf8"))
);

/** Splits a value on top-level whitespace and `/`, keeping `calc(...)` and `var(...)` whole. */
function splitTopLevel(value: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let current = "";
  for (const ch of value) {
    if (ch === "(") depth += 1;
    if (ch === ")") depth -= 1;
    if (depth === 0 && (/\s/.test(ch) || ch === "/")) {
      if (current) parts.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  if (current) parts.push(current);
  return parts;
}

// A size is either inherited/relative, or derived from an --xflow-* token with
// no length literal mixed in (`calc(var(--xflow-font-xl) * 1.5)` passes,
// `calc(12px / var(--xflow-pv-lod))` does not).
const RELATIVE_SIZES = new Set(["inherit", "initial", "unset", "100%", "1em"]);
const LENGTH_LITERAL = /\d(?:px|rem|em|pt|vw|vh)\b/;
const isTokenSize = (value: string) =>
  RELATIVE_SIZES.has(value) || (/var\(--xflow-[\w-]+\)/.test(value) && !LENGTH_LITERAL.test(value));

const FONT_STYLE_OR_WEIGHT = /^(?:normal|italic|oblique|bold|bolder|lighter|small-caps|[1-9]00|var\(--xflow-font-weight-[\w-]+\))$/;
const FONT_WHOLE_VALUE = /^(?:inherit|initial|unset|var\(--xflow-text-[\w-]+\))$/;

/** True when a `font-size` / `font` declaration does not take its size from a token. */
function breaksFontScale({ property, value }: Declaration): boolean {
  if (property === "font-size") return !isTokenSize(value);
  if (property !== "font" || FONT_WHOLE_VALUE.test(value)) return false;
  const size = splitTopLevel(value).find((part) => !FONT_STYLE_OR_WEIGHT.test(part));
  return size === undefined || !isTokenSize(size);
}

/** Local stacking is a single-digit integer; anything wider must come from a layer token. */
function breaksLayerScale({ property, value }: Declaration): boolean {
  if (property !== "z-index") return false;
  if (/^var\(--xflow-z-[\w-]+\)$/.test(value) || value === "auto") return false;
  return !(/^-?\d+$/.test(value) && Math.abs(Number(value)) < 10);
}

const where = (d: Declaration) => `${d.file}:${d.line}  ${d.property}: ${d.value}`;
const keyOf = (d: Pick<Declaration, "file" | "property" | "value">) => `${d.file} | ${d.property}: ${d.value}`;

type Allowance = { file: string; declaration: string; count: number };

/**
 * Compares offending declarations with an allowance list. `unexpected` are hits
 * the list does not cover (new, or more of the same declaration than allowed);
 * `stale` are allowances the sheet no longer needs in full. Keyed by file and
 * declaration text, not line, so unrelated edits above a hit do not move it.
 */
function ratchet(hits: Declaration[], allowed: Allowance[]) {
  const budget = new Map(allowed.map((a) => [`${a.file} | ${a.declaration}`, a.count]));
  const seen = new Map<string, number>();
  const unexpected: string[] = [];
  for (const hit of hits) {
    const key = keyOf(hit);
    const used = (seen.get(key) ?? 0) + 1;
    seen.set(key, used);
    if (used > (budget.get(key) ?? 0)) unexpected.push(where(hit));
  }
  const stale = [...budget]
    .filter(([key, count]) => (seen.get(key) ?? 0) < count)
    .map(([key, count]) => `${key} (allowed ${count}, found ${seen.get(key) ?? 0})`);
  return { unexpected, stale };
}

// Font sizes that still bypass the --xflow-font-{xs,sm,md,lg,xl} scale. Every
// entry except the first is in the ui-preview sheet, which mirrors the design
// baseline's literal sizes (icon glyph sizes 14/16/18, the 10/11px badges and
// mono captions) that the 11/12/13/14/16 scale has no token for (10, 15, 18) or
// that sit on a glyph rather than text. The list may only shrink: when a
// declaration moves onto a token its entry is deleted, never added to.
const FONT_BASELINE: Allowance[] = [
  { file: "layouts/admin.css", declaration: "font: var(--xflow-font-weight-medium) 11px var(--xflow-font-sans)", count: 1 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font-size: 11px", count: 5 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font-size: 12px", count: 3 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font-size: 13px", count: 1 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font-size: 14px", count: 3 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font-size: 15px", count: 1 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font-size: 16px", count: 7 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font-size: 18px", count: 1 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font-size: calc(12px / var(--xflow-pv-lod))", count: 1 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font-size: calc(16px / var(--xflow-pv-lod))", count: 1 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font: 10px / 1 var(--xflow-font-mono)", count: 1 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font: 11px var(--xflow-font-mono)", count: 3 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font: 500 10px / 14px var(--xflow-font-sans)", count: 1 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font: 500 11px var(--xflow-font-sans)", count: 1 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font: 600 11px / 18px var(--xflow-font-sans)", count: 1 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font: 600 12px var(--xflow-font-mono)", count: 1 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font: italic 600 10px var(--xflow-font-sans)", count: 1 },
  { file: "pages/ui-preview/ui-preview.css", declaration: "font: italic 600 12px var(--xflow-font-mono)", count: 1 }
];

// No --xflow-z-* layer variables exist yet, so the guard bans only layer-scale
// numbers (10 and up) and lists the declarations that must keep one.
const Z_INDEX_EXCEPTIONS: Allowance[] = [
  {
    file: "layouts/admin.css",
    declaration: "z-index: 1000",
    // AntD's zIndexPopupBase is 1000 and the modal mask/wrap it renders inside
    // this root stack from it; a lower value would put the dialog behind them.
    count: 1
  }
];

const hitsOf = (decls: Declaration[], breaks: (d: Declaration) => boolean) => decls.filter(breaks);
const sample = (css: string) => declarationsOf("sample.css", css);
const report = (lines: string[]) => lines.join("\n");

describe("xflow-admin style sheets", () => {
  it("reads every Admin stylesheet and finds its font and stacking declarations", () => {
    const files = new Set(allDeclarations.map((d) => d.file));
    expect(files.size, "sheets with scanned declarations").toBeGreaterThanOrEqual(4);
    expect(allDeclarations.filter((d) => d.property === "font-size").length).toBeGreaterThan(100);
    expect(allDeclarations.filter((d) => d.property === "z-index").length).toBeGreaterThan(5);
    expect(allDeclarations.every((d) => !d.file.includes("node_modules") && !d.file.startsWith("."))).toBe(true);
  });

  it("takes every font size from the --xflow-font-* scale, apart from the listed baseline", () => {
    const { unexpected, stale } = ratchet(hitsOf(allDeclarations, breaksFontScale), FONT_BASELINE);
    expect(unexpected, report(["font size bypasses the --xflow-font-* scale:", ...unexpected])).toEqual([]);
    expect(stale, report(["baseline entries no longer needed; delete them from FONT_BASELINE:", ...stale])).toEqual([]);
  });

  it("keeps stacking to local single-digit values or --xflow-z-* tokens, apart from the listed exceptions", () => {
    const { unexpected, stale } = ratchet(hitsOf(allDeclarations, breaksLayerScale), Z_INDEX_EXCEPTIONS);
    expect(unexpected, report(["z-index outside the local scale and not a --xflow-z-* token:", ...unexpected])).toEqual([]);
    expect(stale, report(["exceptions no longer needed; delete them from Z_INDEX_EXCEPTIONS:", ...stale])).toEqual([]);
  });
});

describe("style guard detection", () => {
  it("flags bare px sizes, loose font shorthands and calc with a literal", () => {
    const bad = [
      ".a { font-size: 12px; }",
      ".a { font-size: 0.75rem }",
      ".a { font-size: calc(12px / var(--xflow-pv-lod)); }",
      ".a { font: 500 11px var(--xflow-font-sans); }",
      ".a { font: italic 600 12px / 18px var(--xflow-font-mono); }",
      ".a { font: var(--xflow-font-weight-medium) 11px var(--xflow-font-sans); }",
      ".a { font: 11px Inter; }"
    ];
    for (const css of bad) {
      expect(hitsOf(sample(css), breaksFontScale), css).toHaveLength(1);
    }
  });

  it("passes token, inherited and relative font sizes", () => {
    const good = [
      ".a { font-size: var(--xflow-font-md); }",
      ".a { font-size: var(--xflow-admin-page-title-size); }",
      ".a { font-size: calc(var(--xflow-font-xl) * 1.5); }",
      ".a { font-size: calc(var(--xflow-font-md) / var(--xflow-pv-lod)); }",
      ".a { font-size: inherit; }",
      ".a { font-size: 100%; }",
      ".a { font-size: 1em; }",
      ".a { font: inherit; }",
      ".a { font: var(--xflow-text-body); }",
      ".a { font: 400 var(--xflow-font-md) / 20px var(--xflow-font-sans); }",
      ".a { font: italic var(--xflow-font-weight-semibold) var(--xflow-font-xs) / 16px var(--xflow-font-mono); }",
      ".a { --xflow-admin-page-title-size: 20px; }"
    ];
    for (const css of good) {
      expect(hitsOf(sample(css), breaksFontScale), css).toEqual([]);
    }
  });

  it("flags layer-scale z-index values and passes local ones and tokens", () => {
    for (const css of [".a { z-index: 10; }", ".a { z-index: 1000 !important; }", ".a { z-index: -10 }"]) {
      expect(hitsOf(sample(css), breaksLayerScale), css).toHaveLength(1);
    }
    for (const css of [".a { z-index: 0; }", ".a { z-index: 1; }", ".a { z-index: -1; }", ".a { z-index: 9; }", ".a { z-index: auto; }", ".a { z-index: var(--xflow-z-modal); }"]) {
      expect(hitsOf(sample(css), breaksLayerScale), css).toEqual([]);
    }
  });

  it("ignores declarations that only appear inside comments and keeps line numbers", () => {
    const css = "/* font-size: 12px;\n   z-index: 99; */\n.a {\n  font-size: 13px;\n}\n";
    const decls = sample(css);
    expect(decls).toEqual([{ file: "sample.css", line: 4, property: "font-size", value: "13px" }]);
  });

  it("reports an unlisted hit, an extra copy of a listed one, and a stale entry", () => {
    const hits = sample(".a { font-size: 12px; }\n.b { font-size: 12px; }\n.c { font-size: 15px; }");
    const result = ratchet(hits, [
      { file: "sample.css", declaration: "font-size: 12px", count: 1 },
      { file: "sample.css", declaration: "font-size: 16px", count: 1 }
    ]);
    expect(result.unexpected).toEqual(["sample.css:2  font-size: 12px", "sample.css:3  font-size: 15px"]);
    expect(result.stale).toEqual(["sample.css | font-size: 16px (allowed 1, found 0)"]);
  });
});
