import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

// jsdom applies no stylesheet, so nothing at runtime notices a composer rule that
// hard-codes a colour the token pipeline already defines, or a selector that grows
// back the doubled-class escalation `@layer xflow.composer` made unnecessary. This
// reads the sheet from disk and checks it against the token source. `import.meta.dirname`
// is this directory (packages/composer/src/form); the header comment is stripped because
// it names the very patterns (`!important`, `.ant-*`) the checks below ban.
const read = (path: string) => readFileSync(`${import.meta.dirname}/${path}`, "utf8");

const source = read("styles.css");
const css = source.replace(/\/\*[\s\S]*?\*\//g, "");

const LAYER = "@layer xflow.composer {";

/**
 * The light appearance's tokens from @xflow/core, as written in tokens.css.
 * The theme-independent base block (fonts, radii, the control scale) belongs to
 * every appearance, so it is folded in: a fallback for `--xflow-radius-md` or
 * `--xflow-font-md` must match its base value like any themed fallback.
 */
const tokensCss = read("../../../xflow-core/src/tokens.css");
const baseBlock = /:where\([^)]*\)\s*\{([\s\S]*?)\}/.exec(tokensCss)?.[1] ?? "";
const lightBlock = /\[data-theme="light"\]\s*\{([\s\S]*?)\}/.exec(tokensCss)?.[1] ?? "";
const lightTokens = new Map(
  [...`${baseBlock}\n${lightBlock}`.matchAll(/(--[\w-]+)\s*:\s*([^;]+);/g)].map((decl) => [decl[1]!, decl[2]!.trim()])
);

/** `[name, fallback]` for every `var(--xflow-…, …)` in a sheet, nested pairs included. */
const varFallbacks = (text: string): Array<[string, string]> => {
  const pairs: Array<[string, string]> = [];
  const opening = /var\(--([\w-]+)\s*,\s*/g;
  let match: RegExpExecArray | null;
  while ((match = opening.exec(text)) !== null) {
    // The fallback runs to the matching `)` — color-mix() and nested var() carry their own.
    let depth = 0;
    let index = opening.lastIndex;
    for (; index < text.length; index += 1) {
      if (text[index] === "(") depth += 1;
      else if (text[index] === ")") {
        if (depth === 0) break;
        depth -= 1;
      }
    }
    pairs.push([match[1]!, text.slice(opening.lastIndex, index).trim()]);
  }
  return pairs;
};

/** A core value with its `var()` references inlined, so a derived token compares literally. */
const resolved = (value: string) =>
  value.replace(/var\((--[\w-]+)\)/g, (_, ref: string) => lightTokens.get(ref) ?? ref);

const corePairs = () => varFallbacks(css).filter(([name]) => lightTokens.has(`--${name}`));

describe("xflow.composer cascade frame", () => {
  it("is one @layer xflow.composer block wrapping the whole sheet", () => {
    const trimmed = css.trim();
    expect(trimmed.startsWith(LAYER), "the sheet must open the layer").toBe(true);
    expect(trimmed.endsWith("}"), "the sheet must close the layer").toBe(true);
    expect(css.match(/@layer/g), "a second top-level layer would re-order the cascade").toHaveLength(1);

    let depth = 0;
    for (const char of trimmed.slice(LAYER.length, -1)) {
      if (char === "{") depth += 1;
      else if (char === "}") depth -= 1;
      expect(depth, "the layer closes before the end of the file").toBeGreaterThanOrEqual(0);
    }
    expect(depth, "the layer block is unbalanced").toBe(0);
  });

  it("leaves the header comment outside the layer", () => {
    expect(source.trimStart().startsWith("/*")).toBe(true);
    expect(source.indexOf(LAYER)).toBeGreaterThan(source.indexOf("*/"));
  });

  it("keeps the escapes the layer replaced out of the sheet", () => {
    expect(css).not.toContain("!important");
    expect(css, ".ant-* are generated names; the layer orders the semantics instead").not.toMatch(/\.ant-/);
    // `.a.a` only ever raised specificity against AntD, which the layer order now does.
    expect(css.match(/\.([\w-]+)\.\1(?![\w-])/g) ?? []).toEqual([]);
  });

  it("declares no --xflow-composer-* value of its own", () => {
    // A value declared here would shadow the host's on `.xflow-editor` for every
    // element below it; the sheet may only read these with a fallback.
    expect(css).not.toMatch(/--xflow-composer-[\w-]+\s*:/);
  });
});

describe("composer fallbacks against the core tokens", () => {
  it("reads the light tokens it is meant to compare", () => {
    expect(lightTokens.size).toBeGreaterThan(30);
    expect(lightTokens.get("--xflow-text")).toBe("#16191d");
    expect(corePairs().length).toBeGreaterThan(14);
  });

  it("gives every plain core token its light value as the literal fallback", () => {
    const mismatched = corePairs()
      .filter(([name]) => !/^(color-mix|var)\(/.test(lightTokens.get(`--${name}`)!))
      .filter(([name, fallback]) => fallback !== lightTokens.get(`--${name}`))
      .map(([name, fallback]) => `--${name}: ${fallback} !== ${lightTokens.get(`--${name}`)}`);
    expect(mismatched).toEqual([]);
  });

  it("derives every color-mix fallback from the core colours", () => {
    const derived = corePairs().filter(([name]) => lightTokens.get(`--${name}`)!.startsWith("color-mix("));
    expect(derived.length, "the derived tokens are the point of this check").toBeGreaterThanOrEqual(6);
    const mismatched = derived
      .filter(([name, fallback]) => fallback !== resolved(lightTokens.get(`--${name}`)!))
      .map(([name, fallback]) => `--${name}: ${fallback} !== ${resolved(lightTokens.get(`--${name}`)!)}`);
    expect(mismatched).toEqual([]);
  });

  it("takes every font size from a variable", () => {
    const sizes = [...css.matchAll(/font-size\s*:\s*([^;]+);/g)].map((decl) => decl[1]!.trim());
    expect(sizes.length).toBeGreaterThan(10);
    expect(sizes.filter((value) => !value.startsWith("var("))).toEqual([]);
    expect(css).not.toMatch(/font-size\s*:\s*(11|12|13)px/);
  });
});

describe("section card declarations", () => {
  it("chains the FieldGroup card radius through --xflow-radius-md", () => {
    expect(css).toMatch(
      /\.xflow-composer-field-group\s*\{[^}]*border-radius:\s*var\(--xflow-composer-section-radius,\s*var\(--xflow-radius-md,\s*8px\)\)/
    );
  });

  it("draws the hidden-error badge in the danger foreground", () => {
    expect(css).toMatch(/\.xflow-composer-section-badge\s*\{[^}]*color:\s*var\(--xflow-danger-fg,/);
  });

  it("keeps the badge chip's look on its reveal button", () => {
    // P1-c S5: the count is a <button> now; the chip styling above stands, and
    // only the native button look (cursor, font) is neutralised.
    expect(css).toMatch(/\.xflow-composer-section-badge\s*\{[^}]*cursor:\s*pointer;/);
    expect(css).toMatch(/\.xflow-composer-section-badge\s*\{[^}]*font-family:\s*inherit;/);
    expect(css).toMatch(/\.xflow-composer-section-badge:focus-visible\s*\{[^}]*outline:\s*2px solid var\(--xflow-primary,/);
  });

  it("moves the field hint to the tertiary text colour", () => {
    expect(css).toMatch(/\.xflow-composer-field-hint\s*\{[^}]*color:\s*var\(--xflow-text-tertiary,/);
  });
});

describe("control height", () => {
  it("sizes every control from the host knob, falling back to the core control height", () => {
    expect(css).toMatch(
      /\.xflow-composer-control\s*\{[^}]*min-height:\s*var\(--xflow-composer-control-height,\s*var\(--xflow-control-height,\s*32px\)\)/
    );
    // No literal control height anywhere: the host decides (P1-c S3).
    expect(css).not.toMatch(/(?:min-)?height:\s*(24|28|32)px/);
  });
});

describe("switch row (P1-c S6)", () => {
  it("chains the row height through the host knob to the small control height", () => {
    expect(css).toMatch(
      /\.xflow-composer-switch-row\s*\{[^}]*min-height:\s*var\(--xflow-composer-switch-height,\s*var\(--xflow-control-height-sm,\s*24px\)\)/
    );
  });

  it("sets the inline label in md text and greys it while effectively off", () => {
    expect(css).toMatch(
      /\.xflow-composer-switch-row\s+\.xflow-composer-field-label\s*\{[^}]*color:\s*var\(--xflow-text,\s*#16191d\)[^}]*font-size:\s*var\(--xflow-font-md,\s*13px\);[^}]*font-weight:\s*400;/
    );
    expect(css).toMatch(
      /\.xflow-composer-switch-row\.is-off\s+\.xflow-composer-field-label\s*\{[^}]*color:\s*var\(--xflow-text-tertiary,/
    );
  });
});

describe("key/value rows (P1-c S6)", () => {
  it("lays a row out as centred flex with a 6px gap", () => {
    expect(css).toMatch(
      /\.xflow-composer-key-value-row\s*\{[^}]*display:\s*flex;[^}]*align-items:\s*center;[^}]*gap:\s*6px;/
    );
  });

  it("spaces consecutive rows by margin-top and draws error lines in the danger colour", () => {
    expect(css).toMatch(/\.xflow-composer-key-value-row\s*\+\s*\.xflow-composer-key-value-row\s*\{[^}]*margin-top:\s*6px;/);
    expect(css).toMatch(
      /\.xflow-composer-row-error\s*\{[^}]*color:\s*var\(--xflow-danger-fg,\s*color-mix\(in srgb,\s*#d1333b 70%,\s*#16191d\)\)/
    );
  });

  it("leads the error lines by the baseline's 4px and adds no gap above the add button", () => {
    expect(css).toMatch(/\.xflow-composer-row-error\s*\{[^}]*margin-top:\s*4px;/);
    expect(css).not.toMatch(/\.xflow-composer-key-value-add\s*\{/);
  });
});
