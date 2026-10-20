import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

// Read from disk: Vitest's css handling turns `?raw` stylesheet imports into
// empty strings. `import.meta.dirname` is this directory (packages/xflow-editor/src).
const read = (relative: string) =>
  readFileSync(`${import.meta.dirname}/${relative}`, "utf8").replace(/\/\*[\s\S]*?\*\//g, "");

const tokensCss = read("../../xflow-core/src/tokens.css");
const stylesheets = {
  editor: read("styles.css"),
  preview: read("../../xflow-preview/src/styles.css"),
  composer: read("../../composer/src/form/styles.css")
};

const declaredIn = (css: string) => new Set([...css.matchAll(/(--xflow-[a-z0-9-]+)\s*:/g)].map((match) => match[1]));
const referencedIn = (css: string) =>
  new Set([...css.matchAll(/var\(\s*(--xflow-[a-z0-9-]+)/g)].map((match) => match[1]));

const tokens = declaredIn(tokensCss);
const componentLevel = /^--xflow-(preview|composer|editor)-/;

/**
 * Set at runtime from TSX (an inline style), so no stylesheet declares it. The
 * stylesheet reference always carries a fallback; the test below pins the
 * runtime setter so this list cannot hide a typo.
 */
const runtimeVariables: Record<string, string> = {
  "--xflow-composer-label-width": "../../composer/src/form/layout.tsx",
  "--xflow-preview-ruler-major": "../../xflow-preview/src/canvasChrome.tsx",
  "--xflow-preview-ruler-minor": "../../xflow-preview/src/canvasChrome.tsx",
  "--xflow-preview-ruler-origin": "../../xflow-preview/src/canvasChrome.tsx"
};

/**
 * Knobs a host may set on an ancestor but need not: the package reads them with a
 * fallback and must never declare them itself (that would shadow the host). The
 * editor declares only the ones whose value differs from the fallback, so these
 * may have no declaration anywhere; the test below pins each to a fallback read.
 */
const hostKnobs: Record<string, keyof typeof stylesheets> = {
  "--xflow-composer-control-height": "composer",
  "--xflow-composer-field-gap": "composer",
  "--xflow-composer-font-size": "composer",
  "--xflow-composer-section-radius": "composer",
  "--xflow-composer-section-header-padding": "composer",
  "--xflow-composer-section-body-gap": "composer",
  "--xflow-composer-section-body-padding": "composer",
  "--xflow-composer-section-gap": "composer",
  "--xflow-composer-switch-height": "composer",
  "--xflow-composer-title-size": "composer"
};

/** The variables `css` references that no core token, component-level declaration, runtime setter or host knob provides. */
const undeclaredIn = (css: string, componentDeclarations: Set<string>) =>
  [...referencedIn(css)]
    .filter(
      (name) => !tokens.has(name) && !componentDeclarations.has(name) && !(name in runtimeVariables) && !(name in hostKnobs)
    )
    .sort();

describe("--xflow-* variables in the package stylesheets", () => {
  const componentDeclarations = new Set(
    Object.values(stylesheets).flatMap((css) => [...declaredIn(css)].filter((name) => componentLevel.test(name)))
  );

  for (const [pkg, css] of Object.entries(stylesheets)) {
    it(`${pkg}: every referenced variable is a core token or a component-level variable`, () => {
      expect(undeclaredIn(css, componentDeclarations)).toEqual([]);
    });

    it(`${pkg}: declares no core token again`, () => {
      // tokens.css is the only place a shared token is written; a copy here
      // would silently win or lose depending on layer order.
      const redeclared = [...declaredIn(css)].filter((name) => tokens.has(name)).sort();
      expect(redeclared).toEqual([]);
    });

    it(`${pkg}: only declares component-level variables`, () => {
      const stray = [...declaredIn(css)].filter((name) => !componentLevel.test(name)).sort();
      expect(stray).toEqual([]);
    });
  }

  it("keeps each package's own prefix to itself", () => {
    const offenders: string[] = [];
    for (const [pkg, css] of Object.entries(stylesheets)) {
      for (const name of declaredIn(css)) {
        const owner = name.match(componentLevel)?.[1];
        if (owner && owner !== pkg && !(pkg === "editor" && (owner === "composer" || owner === "preview"))) {
          offenders.push(`${pkg} declares ${name}`);
        }
      }
    }
    expect(offenders).toEqual([]);
  });

  it("pins every runtime-set variable to the source that sets it", () => {
    for (const [name, source] of Object.entries(runtimeVariables)) {
      expect(readFileSync(`${import.meta.dirname}/${source}`, "utf8"), name).toContain(`"${name}"`);
    }
  });

  it("pins every host knob to a fallback read in its package, never a declaration there", () => {
    for (const [name, pkg] of Object.entries(hostKnobs)) {
      const css = stylesheets[pkg];
      expect(css, name).toMatch(new RegExp(`var\\(\\s*${name}\\s*,`));
      expect(css, name).not.toMatch(new RegExp(`var\\(\\s*${name}\\s*\\)`));
      expect(declaredIn(css).has(name), name).toBe(false);
    }
  });

  it("finds the declarations it is meant to guard", () => {
    // Guards the guard: an empty parse would make every check above pass.
    expect(tokens.size).toBeGreaterThan(60);
    for (const css of Object.values(stylesheets)) expect(referencedIn(css).size).toBeGreaterThan(5);
  });

  // Mutation check for the guard itself, on synthetic CSS so no real stylesheet
  // has to be broken: the same function the per-package checks use must report
  // an undeclared reference and stay quiet when every reference resolves.
  it("reports a reference that nothing declares", () => {
    const css = ".a { color: var(--xflow-text); background: var(--xflow-nope); }";
    expect(undeclaredIn(css, new Set())).toEqual(["--xflow-nope"]);
  });

  it("reports nothing when every reference is a core token, a component variable or a runtime variable", () => {
    const css = `
      .a { --xflow-editor-local: 1px; width: var(--xflow-editor-local); }
      .b { color: var(--xflow-text); border-color: var(--xflow-border); }
      .c { width: var(--xflow-composer-label-width, 120px); }
    `;
    expect(undeclaredIn(css, declaredIn(css))).toEqual([]);
  });
});
