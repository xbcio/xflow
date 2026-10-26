import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { breakpoints } from "@xflow/core";

// jsdom applies no stylesheet, so nothing at runtime notices a second top-level
// layer (which would re-order the cascade against the host's declaration), an
// `!important` escape, or a selector that grows back the doubled-class
// escalation `@layer xflow.editor` made unnecessary. This reads the sheet from
// disk. `import.meta.dirname` is this directory (packages/xflow-editor/src);
// the header comment is stripped because it names the very patterns
// (`!important`, `.ant-*`) the checks below ban.
const source = readFileSync(`${import.meta.dirname}/styles.css`, "utf8");
const css = source.replace(/\/\*[\s\S]*?\*\//g, "");

const LAYER = "@layer xflow.editor {";

/**
 * `.ant-*` selectors left in the sheet. The host declares xflow.editor after
 * antd, so these are never needed for specificity; the survivors only reach DOM
 * nodes or states AntD exposes no semantic `className`/`classNames` hook for
 * (the context-menu popup's list and its title rows, and the Select's
 * `.ant-select-focused` state). This ratchet may only be lowered when one of
 * those falls — never raised.
 */
const ANTD_SELECTOR_BUDGET = 12;

/** Every value of a declaration in the comment-stripped sheet, in source order. */
const valuesOf = (property: string) =>
  [...css.matchAll(new RegExp(`${property}\\s*:\\s*([^;]+);`, "g"))].map((match) => match[1].trim());

describe("xflow.editor cascade frame", () => {
  it("is one @layer xflow.editor block wrapping the whole sheet", () => {
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

  it("keeps the specificity escapes the layer order replaced out of the sheet", () => {
    expect(css).not.toContain("!important");
    // `.a.a` only ever raised specificity against AntD, which the layer order now does.
    expect(css.match(/\.([\w-]+)\.\1(?![\w-])/g) ?? []).toEqual([]);
  });

  it("holds every remaining .ant-* selector under the ratchet", () => {
    expect((css.match(/\.ant-/g) ?? []).length, "lower ANTD_SELECTOR_BUDGET when a selector falls; never raise it").toBeLessThanOrEqual(
      ANTD_SELECTOR_BUDGET
    );
  });

  it("keeps the retired 28px scale from returning to the sheet", () => {
    // The retired declarations reused the core names (`--xflow-control-height:
    // 28px` / `--xflow-field-height: 24px`); core tokens are only ever written
    // in tokens.css, never re-declared per package.
    expect(css).not.toMatch(/--xflow-control-height\s*:/);
    expect(css).not.toMatch(/--xflow-field-height\s*:/);
  });

  it("leaves control values and segmented labels at the shared 13px size", () => {
    for (const hook of [
      "xflow-editor-select__content",
      "xflow-editor-select__popup-item",
      "xflow-editor-variable-type",
      "xflow-editor-variable-add",
      "xflow-editor-theme-segmented__label",
      "xflow-editor-mode-segmented__label"
    ]) {
      expect(css, hook).not.toMatch(new RegExp(`\\.${hook}\\b[^{}]*\\{[^{}]*font-size:\\s*11px`));
    }
  });

  it("sizes the panel-titlebar icon buttons from the shared 24px control height", () => {
    for (const hook of [
      "xflow-editor-variable-action",
      "xflow-editor-panel-collapse"
    ]) {
      expect(css, hook).toMatch(new RegExp(`\\.${hook}\\b[^{}]*\\{[^{}]*height:\\s*var\\(--xflow-control-height-sm\\)`));
    }
    // The collapse keys are the baseline's 32×24 ghost IconButton (6px
    // radius, 8px inline padding), so the title-bar actions line up with the
    // mock's `.btn--icon.btn--sm`.
    const collapse = css.match(/\.xflow-editor button\.xflow-editor-panel-collapse\s*\{[^}]*\}/)?.[0] ?? "";
    expect(collapse).toMatch(/min-width:\s*var\(--xflow-control-height\)\s*;/);
    expect(collapse).toMatch(/border-radius:\s*var\(--xflow-radius-sm\)\s*;/);
    expect(collapse).toMatch(/padding-inline:\s*var\(--xflow-space-2\)\s*;/);
  });

  it("leaves composer's scale to core instead of re-declaring it", () => {
    expect(css).not.toMatch(/--xflow-composer-(control-height|font-size|field-gap)\s*:/);
  });
});

describe("xflow.editor panel title bar (§6.0)", () => {
  it("is a 40px bar with the baseline padding and gap; the rule is opt-in", () => {
    expect(css).toMatch(/\.xflow-editor\s*\{[^}]*--xflow-editor-panel-title-height:\s*40px\s*;/);
    expect(css).toMatch(/\.xflow-editor-panel-title\s*\{[^}]*height:\s*var\(--xflow-editor-panel-title-height\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-panel-title\s*\{[^}]*gap:\s*var\(--xflow-space-2\)\s*;/);
    expect(css).toMatch(
      /\.xflow-editor-panel-title\s*\{[^}]*padding:\s*0 var\(--xflow-space-2\) 0 var\(--xflow-space-3\)\s*;/
    );
    // The left column's two zones have no rule under their bars (mock `.panel-title`);
    // only the property panel asks for the `--divider` variant.
    expect(css).not.toMatch(/(?:^|\n)\.xflow-editor-panel-title\s*\{[^}]*border-bottom/);
    expect(css).toMatch(/\.xflow-editor-panel-title--divider\s*\{[^}]*border-bottom:\s*1px solid var\(--xflow-border-subtle\)\s*;/);
  });

  it("keeps the heading 14/600, the count 12 tertiary and the icon bare 16px", () => {
    expect(css).toMatch(
      /\.xflow-editor-panel-title__heading\s*\{[^}]*font-size:\s*var\(--xflow-font-lg\)\s*;[^}]*font-weight:\s*var\(--xflow-font-weight-semibold\)\s*;/
    );
    expect(css).toMatch(
      /\.xflow-editor-panel-title__count\s*\{[^}]*color:\s*var\(--xflow-text-tertiary\)\s*;[^}]*font-size:\s*var\(--xflow-font-sm\)\s*;/
    );
    expect(css).toMatch(/\.xflow-editor-panel-title__icon\s*\{[^}]*font-size:\s*var\(--xflow-font-xl\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-panel-title__actions\s*\{[^}]*gap:\s*var\(--xflow-space-0-5\)\s*;[^}]*margin-left:\s*auto\s*;/);
  });
});

describe("xflow.editor top bar (§6.3)", () => {
  it("is a 52px bar with the baseline padding and gap", () => {
    expect(css).toMatch(/\.xflow-editor\s*\{[^}]*--xflow-editor-toolbar-height:\s*52px\s*;/);
    expect(css).toMatch(/\.xflow-editor-toolbar\s*\{[^}]*flex:\s*0 0 var\(--xflow-editor-toolbar-height\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-toolbar\s*\{[^}]*gap:\s*var\(--xflow-space-2\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-toolbar\s*\{[^}]*padding:\s*0 var\(--xflow-space-3\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-toolbar\s*\{[^}]*border-bottom:\s*1px solid var\(--xflow-border-subtle\)\s*;/);
  });

  it("lets the title absorb the bar's negative space and keeps the right cluster fixed", () => {
    // A 1280 bar + long name must clip the title's ellipsis, never the actions.
    // Anchored to the line so the compact-layout override cannot satisfy it.
    expect(css).toMatch(/(?:^|\n)\.xflow-editor-toolbar-actions\s*\{[^}]*flex:\s*0 0 auto\s*;/);
    expect(css).toMatch(/(?:^|\n)\.xflow-editor-toolbar-actions\s*\{[^}]*flex-wrap:\s*nowrap\s*;/);
    // §6.3: name ellipsizes but keeps a 160px floor; the chip is flex:none so it
    // can never be squeezed away by that floor.
    expect(css).toMatch(/\.xflow-editor-title\s*\{[^}]*min-width:\s*160px\s*;/);
    expect(css).toMatch(/\.xflow-editor-title strong\s*\{[^}]*text-overflow:\s*ellipsis\s*;/);
    expect(css).toMatch(/\.xflow-editor-chip\s*\{[^}]*flex:\s*none\s*;/);
    expect(css).toMatch(/\.xflow-editor-history-group\s*\{[^}]*gap:\s*var\(--xflow-space-2\)\s*;/);
  });

  it("keeps one nowrap 52px row at compact width so the right cluster is never clipped", () => {
    // E0 legacy: the wrapped second row pushed 运行 outside the root at 802-925.
    // The bar never wraps again; the title pays for a long name, not the actions.
    const compact = css.match(/@container xflow-editor \(width <= 1280px\) \{([\s\S]*?)\n\}/)?.[1] ?? "";
    expect(compact).toMatch(/\.xflow-editor-toolbar\s*\{[^}]*flex:\s*0 0 var\(--xflow-editor-toolbar-height\)\s*;/);
    expect(compact).toMatch(/\.xflow-editor-toolbar\s*\{[^}]*flex-wrap:\s*nowrap\s*;/);
    expect(compact).toMatch(/\.xflow-editor-toolbar-actions\s*\{[^}]*flex:\s*0 0 auto\s*;/);
    expect(compact).toMatch(/\.xflow-editor-toolbar-actions\s*\{[^}]*flex-wrap:\s*nowrap\s*;/);
    expect(compact).not.toMatch(/\.xflow-editor-toolbar-actions\s*\{[^}]*flex:\s*1 1 100%\s*;/);
    // The band's blanket min-width:0 must not eat the title's floor.
    expect(compact).toMatch(/\.xflow-editor-toolbar > \*:not\(\.xflow-editor-title\)/);
    // Fixed cluster members keep their intrinsic width in the compact band:
    // the switch and back button used to shrink until 预览 painted under ⋯.
    expect(compact).toMatch(
      /\.xflow-editor-toolbar > \.xflow-editor-icon-button,[\s\S]*?\.xflow-editor-toolbar > \.xflow-editor-mode-segmented\s*\{[^}]*flex:\s*none\s*;/
    );
  });

  it("keeps top-bar icon buttons in the body colour and save/publish at weight 500", () => {
    expect(css).toMatch(/\.xflow-editor-icon-button\s*\{[^}]*color:\s*var\(--xflow-text\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-save-button,\s*\.xflow-editor-publish-button\s*\{[^}]*font-weight:\s*var\(--xflow-font-weight-medium\)\s*;/);
  });

  it("draws the mode switch without the border that made it 34px", () => {
    expect(css).toMatch(/\.xflow-editor-mode-segmented\s*\{[^}]*border:\s*0\s*;/);
  });

  it("chains the version chip through the tone tokens like the baseline chip", () => {
    expect(css).toMatch(/\.xflow-editor-chip\s*\{[^}]*border-radius:\s*var\(--xflow-radius-full\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-chip\s*\{[^}]*font-size:\s*var\(--xflow-font-xs\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-chip\s*\{[^}]*font-weight:\s*var\(--xflow-font-weight-medium\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-chip\s*\{[^}]*height:\s*20px\s*;/);
    expect(css).toMatch(/\.xflow-editor-chip\[data-tone="warning"\]\s*\{[^}]*--xflow-editor-chip-tone:\s*var\(--xflow-warning\)/);
    expect(css).toMatch(/\.xflow-editor-chip\[data-tone="success"\]\s*\{[^}]*--xflow-editor-chip-tone:\s*var\(--xflow-success\)/);
    expect(css).toMatch(/\.xflow-editor-chip__icon\s*\{[^}]*font-size:\s*var\(--xflow-font-sm\)\s*;/);
  });

  it("styles the ⌘S hint from tokens", () => {
    expect(css).toMatch(/\.xflow-editor-kbd\s*\{[^}]*font-size:\s*var\(--xflow-font-xs\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-kbd\s*\{[^}]*border:\s*1px solid var\(--xflow-border\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-kbd\s*\{[^}]*border-radius:\s*var\(--xflow-radius-xs\)\s*;/);
  });

  it("draws the run split seam from on-primary", () => {
    expect(css).toMatch(/\.xflow-editor-run-split__options\s*\{[^}]*border-inline-start:\s*1px solid color-mix\(in srgb, var\(--xflow-on-primary\)/);
    // The options half is the baseline's 34px key, not AntD's 32px icon button.
    expect(css).toMatch(/\.xflow-editor-run-split__options\s*\{[^}]*min-width:\s*34px\s*;/);
  });
});

describe("xflow.editor responsive rules (§7.1 breakpoints)", () => {
  it("makes the editor root the named inline-size container", () => {
    expect(css).toMatch(/\.xflow-editor\s*\{[^}]*container-name:\s*xflow-editor\s*;/);
    expect(css).toMatch(/\.xflow-editor\s*\{[^}]*container-type:\s*inline-size\s*;/);
  });

  it("uses no viewport width media queries", () => {
    // Match every prelude, including typed forms such as
    // `@media screen and (max-width: 900px)`, so a media-type prefix cannot
    // hide a viewport size query from the guard.
    const preludes = [...css.matchAll(/@media\s+([^{]+)\{/g)].map((match) => match[1].trim());
    for (const prelude of preludes) {
      expect(prelude, `@media ${prelude}`).not.toMatch(/\(\s*(?:max|min)-(?:width|height)\b/);
      // Whatever remains is a user-preference query (the theme, motion, forced
      // colours), never a size one.
      expect(prelude, `@media ${prelude}`).toMatch(/prefers-|forced-colors/);
    }
  });

  it("scopes every container query to the editor and shares the compact breakpoint", () => {
    // Name check first: it covers every condition form (min-width, ranges),
    // not only the `width < N` shape the threshold scan understands.
    const named = [...css.matchAll(/@container\s+([\w-]+)/g)];
    expect(named.length).toBeGreaterThan(0);
    for (const [, name] of named) expect(name).toBe("xflow-editor");

    const queries = [...css.matchAll(/@container\s+[\w-]+\s*\(width\s*(<=?)\s*(\d+)px\)/g)];
    expect(queries.length, "every @container is a width query on the editor").toBe(named.length);
    const thresholds = queries.map(([, , value]) => Number(value)).sort((a, b) => a - b);
    // Two compact blocks read xflow-core's breakpoints.compact (the locate
    // panel removed the third), so a single drift back to the old 1120 turns
    // this red; 560 is an editor-only density tier retained from the viewport
    // rules, and 1440 is the snug desktop tier that narrows both columns.
    expect(thresholds).toEqual([560, breakpoints.compact, breakpoints.compact, 1440]);
    // Every tier is inclusive: the JS layout tier uses `<= compact`, so 1280 is
    // compact in CSS and in JS alike. 560 keeps the `max-width` semantics it
    // replaced, and 1440 is an upper bound.
    for (const [, operator, value] of queries) {
      expect(operator, value).toBe("<=");
    }
  });
});

describe("xflow.editor type and corner scale (§3.1, §3.2)", () => {
  // The shared scale is the only source: a bare px size drifts per component
  // and the 15/17/10.5/9/8.5 values this sheet used to carry were exactly that.
  it("sizes every font from the shared --xflow-font-* scale", () => {
    const sizes = valuesOf("font-size");
    expect(sizes.length, "the sheet declares font sizes").toBeGreaterThan(100);
    for (const value of sizes) {
      expect(value, value).toMatch(/^var\(--xflow-font-(?:xs|sm|md|lg|xl)\)$/);
    }
  });

  // §3.2 bans bare px corners. A shorthand may still list var()/0 per side
  // (section-card headers use `var(--xflow-editor-section-card-radius) ... 0 0`).
  it("writes every corner as a token, zero, a circle or inherit", () => {
    const radii = valuesOf("border-radius");
    expect(radii.length, "the sheet declares radii").toBeGreaterThan(40);
    for (const value of radii) {
      for (const part of value.split(/\s+/)) {
        expect(part, value).toMatch(/^(?:var\(--xflow-[\w-]+\)|0|50%|inherit)$/);
      }
    }
  });

  // §3.1: weights are 400 / 500 / 600 only. The retired 650/700/300 values
  // were visual hierarchy carried by a font the scale never allowed.
  it("keeps every font weight on the 400/500/600 scale", () => {
    const weights = valuesOf("font-weight");
    expect(weights.length, "the sheet declares font weights").toBeGreaterThan(20);
    for (const value of weights) {
      expect(value, value).toMatch(/^(?:400|500|600|inherit|var\(--xflow-[\w-]+\))$/);
    }
  });
});

describe("xflow.editor panel collapse motion (§6.0)", () => {
  it("animates the column track on the slow motion token and drops it under reduced motion", () => {
    expect(css, "collapse/expand eases the column widths").toMatch(
      /\.xflow-editor-main\s*\{[^}]*transition:\s*grid-template-columns\s+var\(--xflow-motion-duration-slow\)\s+var\(--xflow-motion-ease-emphasized\)\s*;/
    );
    expect(css, "reduced motion removes the animation").toMatch(
      /@media\s*\(prefers-reduced-motion:\s*reduce\)\s*\{\s*\.xflow-editor-main\s*\{[^}]*transition:\s*none\s*;/
    );
  });

  it("collapses the left column to the rail and the right column to zero", () => {
    expect(css, "the collapsed left column keeps the rail width").toMatch(
      /\.xflow-editor-main\[data-left-collapsed="true"\]\s*\{[^}]*grid-template-columns:\s*var\(--xflow-editor-panel-rail-width\)/
    );
    expect(css, "the collapsed right column takes no width").toMatch(
      /\.xflow-editor-main\[data-right-collapsed="true"\]\s*\{[^}]*grid-template-columns:[^;]*\s0px;/
    );
  });
});

describe("xflow.editor inspector tabs (§6.4)", () => {
  /** The declaration block of the single `.xflow-editor <selector>` rule. */
  const ruleBody = (selector: string): string => {
    const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const match = css.match(new RegExp(`\\.xflow-editor\\s+${escaped}\\s*\\{([^}]*)\\}`));
    expect(match, `${selector} has a rule`).not.toBeNull();
    return match?.[1] ?? "";
  };

  it("styles AntD Tabs through the semantic hooks, not the retired hand-made button row", () => {
    expect(css).not.toMatch(/\.xflow-editor-inspector-tabs\s+button/);
    expect(css).not.toContain(".xflow-editor-inspector-panel");
    expect(ruleBody(".xflow-editor-inspector-tabs__header")).toMatch(/padding-inline:\s*var\(--xflow-space-3\)\s*;/);
    const body = ruleBody(".xflow-editor-inspector-tabs__body");
    expect(body).toMatch(/height:\s*100%\s*;/);
    expect(body).toMatch(/overflow:\s*auto\s*;/);
  });

  it("keeps the tab row on the panel surface and gives the content the 12px body frame", () => {
    expect(ruleBody(".xflow-editor-inspector-tabs__header")).not.toMatch(/background/);
    expect(ruleBody(".xflow-editor-inspector-tabs__header")).toMatch(/padding-bottom:\s*1px\s*;/);
    expect(ruleBody(".xflow-editor-inspector-tabs__content")).toMatch(/padding:\s*var\(--xflow-space-3\)\s*;/);
  });

  it("draws the failed-run dot as a 6px danger circle", () => {
    const dot = ruleBody(".xflow-editor-inspector-tab-dot");
    expect(dot).toMatch(/background:\s*var\(--xflow-danger\)\s*;/);
    expect(dot).toMatch(/height:\s*6px\s*;/);
    expect(dot).toMatch(/width:\s*6px\s*;/);
  });

  it("sets the tab label to 13/20/500 and keeps the active text in the main colour", () => {
    expect(css).toMatch(
      /\.xflow-editor \.xflow-editor-inspector-tabs \[role="tab"\]\s*\{[^}]*font-weight:\s*var\(--xflow-font-weight-medium\)\s*;[^}]*line-height:\s*var\(--xflow-line-height-md\)\s*;/
    );
    expect(css).toMatch(
      /\.xflow-editor \.xflow-editor-inspector-tabs \[role="tab"\]\[aria-selected="true"\]\s*\{[^}]*color:\s*var\(--xflow-text\)\s*;/
    );
  });
});

describe("xflow.editor diagnostics dock (§6.5)", () => {
  const rule = (selector: string): string => {
    const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const match = css.match(new RegExp(`(?:^|\\})\\s*${escaped}\\s*\\{([^}]*)\\}`));
    expect(match, `${selector} has a rule`).not.toBeNull();
    return match?.[1] ?? "";
  };

  it("makes the status strip its own 28px clickable row", () => {
    const strip = rule(".xflow-editor-statusbar");
    expect(strip).toMatch(/height:\s*28px\s*;/);
    expect(strip).toMatch(/flex:\s*0 0 28px\s*;/);
    expect(strip).toMatch(/cursor:\s*pointer\s*;/);
    expect(strip).toMatch(/width:\s*100%\s*;/);
  });

  it("sizes the drawer from one 280px variable over a 40px head and a 300px summary column", () => {
    expect(css).toMatch(/\.xflow-editor\s*\{[^}]*--xflow-editor-drawer-height:\s*280px\s*;/);
    const drawer = rule(".xflow-editor-drawer");
    // The variable is the ceiling; a host shorter than bar + strip + drawer wins, so
    // the open drawer never rides up over the toolbar (tier a has no viewport clamp).
    expect(drawer).toMatch(
      /height:\s*min\(var\(--xflow-editor-drawer-height\), calc\(100% - var\(--xflow-editor-toolbar-height\) - 29px\)\)\s*;/
    );
    expect(css).toMatch(
      /\.xflow-editor-bottom\[data-dock="expanded"\]\s*\{[^}]*max-height:\s*calc\(100% - var\(--xflow-editor-toolbar-height\)\)\s*;/
    );
    expect(drawer).toMatch(/grid-template-columns:\s*minmax\(0, 1fr\) 300px\s*;/);
    expect(drawer).toMatch(/grid-template-rows:\s*var\(--xflow-editor-panel-title-height\) minmax\(0, 1fr\)\s*;/);
  });

  it("clamps the drawer on the compact tier so the canvas keeps a floor", () => {
    expect(css).toMatch(
      /\.xflow-editor\[data-layout="c"\]\s*\{[^}]*--xflow-editor-drawer-height:\s*min\(280px, calc\(100dvh - var\(--xflow-editor-toolbar-height\) - 28px - 120px\)\)\s*;/
    );
  });

  it("folds the summary under the issue list at compact width by container query, not media query", () => {
    const folded = css.match(/@container xflow-editor \(width <= 1280px\)\s*\{\s*\.xflow-editor-drawer\s*\{[^}]*grid-template-columns:\s*minmax\(0, 1fr\)\s*;[^}]*\}\s*\.xflow-editor-drawer__side\s*\{([^}]*)\}/);
    expect(folded, "the single-column drawer rule").not.toBeNull();
    // Still reachable: folded, never hidden.
    expect(folded?.[1]).not.toMatch(/display:\s*none/);
    expect(folded?.[1]).toMatch(/flex-direction:\s*row\s*;/);
    expect(folded?.[1]).toMatch(/border-top:\s*1px solid var\(--xflow-border-subtle\)\s*;/);
  });

  it("sets the drawer title group 8px apart inside a 16px head", () => {
    expect(rule(".xflow-editor .xflow-editor-drawer__head")).toMatch(/gap:\s*var\(--xflow-space-4\)\s*;/);
    expect(rule(".xflow-editor-drawer__title")).toMatch(/gap:\s*var\(--xflow-space-2\)\s*;/);
  });

  it("keeps the dock's footprint equal docked and expanded, so the canvas row never moves", () => {
    expect(css).toMatch(
      /\.xflow-editor-bottom\[data-dock="docked"\],\s*\.xflow-editor-bottom\[data-dock="expanded"\]\s*\{[^}]*height:\s*calc\(1px \+ 28px \+ var\(--xflow-editor-drawer-height\)\)\s*;/
    );
  });

  it("overlays the drawer against the editor root rather than growing the grid", () => {
    const drawer = rule(".xflow-editor-drawer");
    expect(drawer).toMatch(/position:\s*absolute\s*;/);
    expect(drawer).toMatch(/bottom:\s*0\s*;/);
    expect(drawer).toMatch(/z-index:\s*var\(--xflow-editor-dock-z\)\s*;/);
    // The footer must not be a containing block, or the drawer could not reach the toolbar.
    expect(rule(".xflow-editor-bottom")).not.toMatch(/position:/);
    const expanded = rule('.xflow-editor-drawer[data-dock="expanded"]');
    expect(expanded).toMatch(/height:\s*calc\(100% - var\(--xflow-editor-toolbar-height\)\)\s*;/);
  });

  it("layers the dock above the panels and below the compact drawers and popups", () => {
    const z = Number(css.match(/--xflow-editor-dock-z:\s*(\d+)\s*;/)?.[1]);
    const compactDrawer = Number(rule(".xflow-editor-compact-drawer").match(/z-index:\s*(\d+)\s*;/)?.[1]);
    const contextMenu = Number(rule(".xflow-editor .xflow-editor-context-menu__anchor").match(/z-index:\s*(\d+)\s*;/)?.[1]);
    expect(z).toBeGreaterThan(2);
    expect(z).toBeLessThan(compactDrawer);
    expect(z).toBeLessThan(contextMenu);
  });

  it("eases only the drawer's elevation, never its height, and drops it under reduced motion", () => {
    // E-C: height is layout, so docked ↔ expanded resizes instantly; the shadow
    // is the one animated property and reads the fast token.
    expect(rule(".xflow-editor-drawer")).toMatch(
      /transition:\s*box-shadow var\(--xflow-motion-duration-fast\) var\(--xflow-motion-ease-standard\)\s*;/
    );
    expect(rule(".xflow-editor-drawer")).not.toMatch(/transition:[^;]*\bheight\b/);
    expect(css).toMatch(
      /@media \(prefers-reduced-motion: reduce\)\s*\{\s*\.xflow-editor-drawer\s*\{\s*transition:\s*none\s*;/
    );
  });

  it("no longer carries the retired English diagnostics sheet", () => {
    expect(css).not.toContain(".xflow-editor-diagnostics-meter");
    expect(css).not.toContain(".xflow-editor-diagnostics-preview");
    expect(css).not.toContain(".xflow-editor-canvas-status");
  });
});

describe("xflow.editor banners (§6.5.1 / §6.10)", () => {
  const body = (selector: string): string => {
    const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const match = css.match(new RegExp(`(?:^|\\})\\s*${escaped}\\s*\\{([^}]*)\\}`));
    expect(match, `${selector} has a rule`).not.toBeNull();
    return match?.[1] ?? "";
  };

  it("floats over the canvas top, below the preview's own floats, without taking canvas height", () => {
    const stack = body(".xflow-editor-banners");
    expect(stack).toMatch(/position:\s*absolute\s*;/);
    expect(stack).toMatch(/max-width:\s*min\(720px,/);
    expect(stack).toMatch(/top:\s*calc\(var\(--xflow-editor-banner-ruler\) \+ 12px \+ var\(--xflow-float-height\) \+ 8px\)\s*;/);
    expect(stack).toMatch(/pointer-events:\s*none\s*;/);
  });

  it("draws the baseline shell from tokens: 8/12 padding, 6px radius, panel ground, 12px secondary text", () => {
    const shell = body(".xflow-editor-banner");
    expect(shell).toMatch(/padding:\s*var\(--xflow-space-2\) var\(--xflow-space-3\)\s*;/);
    expect(shell).toMatch(/gap:\s*var\(--xflow-space-2\)\s*;/);
    expect(shell).toMatch(/border:\s*1px solid var\(--xflow-editor-banner-tone\)\s*;/);
    expect(shell).toMatch(/border-radius:\s*var\(--xflow-radius-sm\)\s*;/);
    expect(shell).toMatch(/background:\s*var\(--xflow-panel\)\s*;/);
    expect(shell).toMatch(/font-size:\s*var\(--xflow-font-sm\)\s*;/);
    expect(shell).toMatch(/color:\s*var\(--xflow-text-secondary\)\s*;/);
  });

  it("mixes warning and danger borders to 40% and draws neutral in its own tone, like the baseline", () => {
    expect(css).toMatch(/\.xflow-editor-banner\[data-tone="warning"\]\s*\{[^}]*border-color:\s*color-mix\(in srgb, var\(--xflow-warning\) 40%, transparent\)/);
    expect(css).toMatch(/\.xflow-editor-banner\[data-tone="danger"\]\s*\{[^}]*border-color:\s*color-mix\(in srgb, var\(--xflow-danger\) 40%, transparent\)/);
    // No border-color override: the shell's `1px solid <tone>` is the neutral grey.
    expect(body('.xflow-editor-banner[data-tone="neutral"]')).not.toMatch(/border/);
    expect(body(".xflow-editor-banner")).toMatch(/line-height:\s*var\(--xflow-line-height-md\)\s*;/);
  });

  it("keeps the icon on the tone, the lead at 600 and the actions 6px apart", () => {
    expect(body(".xflow-editor-banner__icon")).toMatch(/color:\s*var\(--xflow-editor-banner-tone\)\s*;/);
    expect(body(".xflow-editor-banner__strong")).toMatch(/font-weight:\s*var\(--xflow-font-weight-semibold\)\s*;/);
    expect(body(".xflow-editor-banner__text")).toMatch(/min-width:\s*0\s*;/);
    expect(body(".xflow-editor-banner__actions")).toMatch(/gap:\s*var\(--xflow-space-1-5\)\s*;/);
  });

  it("follows the preview's rulers: the top offset drops the ruler term when none are drawn", () => {
    expect(body(".xflow-editor-canvas")).toMatch(/--xflow-editor-banner-ruler:\s*var\(--xflow-ruler\)\s*;/);
    expect(body('.xflow-editor-canvas:has([data-ruler="false"])')).toMatch(/--xflow-editor-banner-ruler:\s*0px\s*;/);
  });

  it("gives the read-only / preview notice a base inset and body size at every width", () => {
    const notice = body(".xflow-editor-preview-inspector");
    expect(notice).toMatch(/padding:\s*var\(--xflow-space-4\)\s*;/);
    expect(notice).toMatch(/font-size:\s*var\(--xflow-font-md\)\s*;/);
  });
});

describe("xflow.editor left column (§6.0.1, §6.5)", () => {
  const rule = (selector: string): string => {
    const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const match = css.match(new RegExp(`(?:^|\\})\\s*${escaped}\\s*\\{([^}]*)\\}`));
    expect(match, `${selector} has a rule`).not.toBeNull();
    return match?.[1] ?? "";
  };

  it("keeps the rail glyph at 16px and sizes only the caption at 11px", () => {
    expect(rule(".xflow-editor-rail button")).toMatch(/font-size:\s*var\(--xflow-font-xl\)\s*;/);
    expect(rule(".xflow-editor-rail__text")).toMatch(/font-size:\s*var\(--xflow-font-xs\)\s*;/);
    // A `button > span` rule would also reach the AntD icon span and shrink it.
    expect(css).not.toMatch(/\.xflow-editor-rail button > span\b/);
  });

  it("draws the 1px rule between the zones on a rule that follows the panel border reset", () => {
    const divider = rule('.xflow-editor .xflow-editor-lib[data-zones="dual"] > .xflow-editor-outline');
    expect(divider).toMatch(/border-top:\s*1px solid var\(--xflow-border-subtle\)\s*;/);
    expect(css.indexOf('.xflow-editor-lib[data-zones="dual"] > .xflow-editor-outline')).toBeGreaterThan(
      css.indexOf(".xflow-editor .xflow-editor-panel {")
    );
  });

  it("sizes the outline to its tree up to half the column, then scrolls it", () => {
    // A fixed 3:2 split left the eight-node outline ~287px at 900 high and cut
    // the last section mid-row. The outline now takes its content height, capped
    // at half the column, so anything longer is reached through its own list.
    expect(rule(".xflow-editor-lib")).toMatch(
      /grid-template-rows:\s*minmax\(0, 1fr\) fit-content\(50%\)\s*;/
    );
    expect(rule(".xflow-editor-lib[data-zones=\"single\"]")).toMatch(
      /grid-template-rows:\s*minmax\(0, 1fr\)\s*;/
    );
    // The overflow chain that makes the cap a scroll cutoff rather than a clip:
    // panel body hidden → column flex → tree list auto. The library and outline
    // bodies share one rule, so this cannot use the first-rule helper.
    expect(css).toMatch(/\.xflow-editor \.xflow-editor-outline__body\s*\{[^}]*overflow:\s*hidden\s*;/);
    expect(rule(".xflow-editor-outline__list")).toMatch(/overflow:\s*auto\s*;/);
    expect(rule(".xflow-editor-outline__list")).toMatch(/min-height:\s*0\s*;/);
  });

  it("draws every key cap on a normal line box, as the baseline's font shorthand does", () => {
    // Inheriting the root's 20px line height made the cap 22.8px tall and the
    // library search 33.8px instead of 32.
    expect(rule(".xflow-editor-kbd")).toMatch(/line-height:\s*normal\s*;/);
    expect(css).not.toContain(".xflow-editor-library-search__suffix .xflow-editor-kbd");
  });

  it("weighs the library group row 600 as a whole, so its count is as heavy as its label", () => {
    // The baseline sets the weight on the row; a 400 count beside a 600 label
    // was the browser-measured difference.
    expect(rule(".xflow-editor-node-group__title")).toMatch(/font-weight:\s*var\(--xflow-font-weight-semibold\)\s*;/);
    expect(rule(".xflow-editor-node-group__count")).not.toMatch(/font-weight/);
  });

  it("anchors the compact drawer to the workbench body, which sits between the top bar and the status bar", () => {
    // The drawer is `position: absolute` inside .xflow-editor-main (getContainer
    // false), so its block extent is the body's: it cannot cover either bar.
    expect(rule(".xflow-editor-main")).toMatch(/position:\s*relative\s*;/);
  });

  it("shows the draggable library rows with the grab cursor", () => {
    expect(rule(".xflow-editor-node-tile")).toMatch(/cursor:\s*grab\s*;/);
    // The baseline row is flush: no border, 6/8 padding, 52px total.
    expect(rule(".xflow-editor-node-tile")).toMatch(/border:\s*0\s*;/);
    expect(rule(".xflow-editor-node-tile")).toMatch(/padding:\s*var\(--xflow-space-1-5\) var\(--xflow-space-2\)\s*;/);
  });

  it("draws the library row icon as the baseline 28px block with a 15px glyph", () => {
    const rule = (selector: string): string => {
      const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      return css.match(new RegExp(`(?:^|\\})\\s*${escaped}\\s*\\{([^}]*)\\}`))?.[1] ?? "";
    };
    const base = rule(".xflow-editor-category-icon");
    expect(base).toMatch(/width:\s*28px\s*;/);
    expect(base).toMatch(/height:\s*28px\s*;/);
    expect(base).toMatch(/border-radius:\s*var\(--xflow-radius-sm\)\s*;/);
    // 15px is the mock's glyph box; the type scale has no 15 step, so the icon
    // is sized by width/height instead of a bare font-size.
    const glyph = rule(".xflow-editor-category-icon:not(.xflow-editor-category-icon--sm) .xflow-icon");
    expect(glyph).toMatch(/width:\s*15px\s*;/);
    expect(glyph).toMatch(/height:\s*15px\s*;/);
    const sm = rule(".xflow-editor-category-icon--sm");
    expect(sm).toMatch(/width:\s*20px\s*;/);
    expect(sm).toMatch(/border-radius:\s*var\(--xflow-radius-xs\)\s*;/);
  });

  it("draws an outline row as the baseline 30px line: 0 8px padding, 8px gap, 13px, small radius", () => {
    const row = rule(".xflow-editor-outline__item");
    expect(row).toMatch(/height:\s*30px\s*;/);
    expect(row).toMatch(/padding:\s*0 var\(--xflow-space-2\)\s*;/);
    expect(row).toMatch(/gap:\s*var\(--xflow-space-2\)\s*;/);
    expect(row).toMatch(/font-size:\s*var\(--xflow-font-md\)\s*;/);
    expect(row).toMatch(/border-radius:\s*var\(--xflow-radius-sm\)\s*;/);
  });

  it("indents a branch row and a port heading by the one 24px step", () => {
    expect(rule(".xflow-editor-outline__port[data-indent],\n.xflow-editor-outline__item[data-indent]")).toMatch(
      /padding-inline-start:\s*var\(--xflow-space-6\)\s*;/
    );
  });

  it("marks the current row with a 12% primary ground and a 600 primary-fg name", () => {
    expect(rule('.xflow-editor-outline__item[aria-current="true"]')).toMatch(
      /background:\s*color-mix\(in srgb, var\(--xflow-primary\) 12%, transparent\)\s*;/
    );
    const name = rule('.xflow-editor-outline__item[aria-current="true"] .xflow-editor-outline__name');
    // The raw brand blue is 3.85:1 on the row's tint; the text alias is AA.
    expect(name).toMatch(/color:\s*var\(--xflow-primary-fg\)\s*;/);
    expect(name).toMatch(/font-weight:\s*var\(--xflow-font-weight-semibold\)\s*;/);
  });

  it("draws the status dot 8px from the tone token, pending as a dashed ring, skipped names tertiary", () => {
    const dot = rule(".xflow-editor-outline__dot");
    expect(dot).toMatch(/height:\s*8px\s*;/);
    expect(dot).toMatch(/width:\s*8px\s*;/);
    expect(dot).toMatch(/border-radius:\s*50%\s*;/);
    // The dot inherits the row's tone. Declaring the variable on the dot itself
    // would shadow the per-tone values the row sets and turn every dot grey.
    expect(dot).not.toMatch(/--xflow-editor-outline-tone\s*:/);
    expect(rule(".xflow-editor-outline__item")).toMatch(/--xflow-editor-outline-tone:\s*var\(--xflow-neutral\)\s*;/);
    expect(rule('.xflow-editor-outline__item[data-status="pending"] .xflow-editor-outline__dot')).toMatch(
      /border:\s*1\.5px dashed var\(--xflow-editor-outline-tone\)\s*;/
    );
    expect(rule('.xflow-editor-outline__item[data-status="skipped"] .xflow-editor-outline__name')).toMatch(
      /color:\s*var\(--xflow-text-tertiary\)\s*;/
    );
    // Each tone resolves to its palette token and nothing else.
    for (const tone of ["info", "success", "warning", "danger", "suspend"]) {
      expect(rule(`.xflow-editor-outline__item[data-tone="${tone}"]`)).toMatch(
        new RegExp(`--xflow-editor-outline-tone:\\s*var\\(--xflow-${tone}\\)\\s*;`)
      );
    }
  });

  it("draws the section heading 11/600 tertiary on the paired 16px line with a 3x10 colour bar, and the node name 11 mono", () => {
    const heading = rule(".xflow-editor-outline__heading");
    expect(heading).toMatch(/font-size:\s*var\(--xflow-font-xs\)\s*;/);
    expect(heading).toMatch(/line-height:\s*var\(--xflow-line-height-xs\)\s*;/);
    expect(rule(".xflow-editor-outline__sub")).toMatch(/font-size:\s*var\(--xflow-font-xs\)\s*;/);
    expect(heading).toMatch(/font-weight:\s*var\(--xflow-font-weight-semibold\)\s*;/);
    expect(heading).toMatch(/color:\s*var\(--xflow-text-tertiary\)\s*;/);
    const bar = rule(".xflow-editor-outline__bar");
    expect(bar).toMatch(/height:\s*10px\s*;/);
    expect(bar).toMatch(/width:\s*3px\s*;/);
  });
});

describe("xflow.editor browser-measured baseline fixes (E6)", () => {
  const rule = (selector: string): string => {
    const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const match = css.match(new RegExp(`(?:^|\\})\\s*${escaped}\\s*\\{([^}]*)\\}`));
    expect(match, `${selector} has a rule`).not.toBeNull();
    return match?.[1] ?? "";
  };

  it("keeps the 32px top-bar buttons at the 6px radius step, with a 6px gap in 保存", () => {
    expect(rule(".xflow-editor-icon-button")).toMatch(/border-radius:\s*var\(--xflow-radius-sm\)\s*;/);
    expect(rule(".xflow-editor .xflow-editor-theme-trigger")).toMatch(/border-radius:\s*var\(--xflow-radius-sm\)\s*;/);
    expect(rule(".xflow-editor-save-button")).toMatch(/gap:\s*var\(--xflow-space-1-5\)\s*;/);
  });

  it("makes the right column 360px wide", () => {
    expect(css).toMatch(/--xflow-editor-panel-right-width:\s*360px\s*;/);
  });

  it("narrows both docked columns at snug desktop widths (EL-02)", () => {
    // 1440: 360+280+rail left the canvas at ~51%; the snug tier takes 40px off
    // the left content and 40px off the right inspector. The override lives on
    // `.xflow-editor-main`: a container query cannot style its own container,
    // and the root's calc() is already resolved when it inherits.
    const snug = css.match(/@container xflow-editor \(width <= 1440px\)\s*\{\s*\.xflow-editor-main\s*\{([^}]*)\}/)?.[1] ?? "";
    expect(snug).toMatch(/--xflow-editor-panel-left-width:\s*calc\(var\(--xflow-editor-panel-rail-width\) \+ 240px\)\s*;/);
    expect(snug).toMatch(/--xflow-editor-panel-right-width:\s*320px\s*;/);
  });

  it("leaves the library rows flush: the 52px row is the whole pitch", () => {
    expect(rule(".xflow-editor-node-list")).toMatch(/gap:\s*0\s*;/);
  });

  it("draws a pressed title-bar action at 14% and the search prefix 16px tertiary", () => {
    // The diagnostics' one-button toggle keeps a pressed state; the collapse
    // keys use aria-expanded and never match this rule.
    expect(rule('.xflow-editor .xflow-editor-panel-collapse[aria-pressed="true"]')).toMatch(
      /color-mix\(in srgb, var\(--xflow-primary\) 14%, transparent\)/
    );
    // The library's search disclosure carries the same pressed fill while open.
    expect(rule('.xflow-editor .xflow-editor-panel-collapse[data-active="true"]')).toMatch(
      /color-mix\(in srgb, var\(--xflow-primary\) 14%, transparent\)/
    );
    const prefix = rule(".xflow-editor .xflow-editor-library-search__prefix");
    expect(prefix).toMatch(/color:\s*var\(--xflow-text-tertiary\)\s*;/);
    expect(prefix).toMatch(/font-size:\s*var\(--xflow-font-xl\)\s*;/);
  });

  it("draws the outline rows flush, with no border narrowing the 30px line", () => {
    expect(rule(".xflow-editor-outline__item")).not.toMatch(/border:\s*1px solid transparent\s*;/);
    expect(rule(".xflow-editor-outline__item")).toMatch(/height:\s*30px\s*;/);
    expect(rule(".xflow-editor-outline__sub")).toMatch(/margin-inline-start:\s*var\(--xflow-space-1\)\s*;/);
  });

  it("sets the canvas float colours; the add glyph is the package's 16px default", () => {
    expect(rule(".xflow-editor .xflow-editor-canvas-add")).toMatch(/color:\s*var\(--xflow-text\)\s*;/);
    const reopen = rule(".xflow-editor button.xflow-editor-canvas-reopen");
    expect(reopen).toMatch(/color:\s*var\(--xflow-text-secondary\)\s*;/);
    expect(reopen).toMatch(/font-size:\s*var\(--xflow-font-sm\)\s*;/);
    expect(reopen).toMatch(/gap:\s*var\(--xflow-space-1-5\)\s*;/);
    expect(reopen).toMatch(/padding:\s*0 var\(--xflow-space-2-5\)\s*;/);
    // The sprite glyph carries its own 16px box; no AntD-icon sizing rule remains.
    expect(css).not.toContain("xflow-editor-canvas-add .anticon");
    // The reopen chip draws the ported #i-side-right glyph, so no AntD-icon
    // rule may outlive the swap.
    expect(css).not.toContain("xflow-editor-canvas-reopen .anticon");
    // The magnifier was removed from the float (user ruling 2026-10-21); ⌘K
    // is the only locate entry, so no rule may outlive the button.
    expect(css).not.toContain("xflow-editor-canvas-locate");
  });

  it("pins the inspector footer under the scrolling tabs", () => {
    const foot = rule(".xflow-editor .xflow-editor-inspector-foot");
    expect(foot).toMatch(/border-top:\s*1px solid var\(--xflow-border-subtle\)\s*;/);
    expect(foot).toMatch(/padding:\s*var\(--xflow-space-2-5\) var\(--xflow-space-3\)\s*;/);
    expect(foot).toMatch(/flex:\s*0 0 auto\s*;/);
  });

  it("draws the inspector's run key as the baseline button: 12px padding, 6px gap, 16px glyph", () => {
    const runButton = rule(".xflow-editor .xflow-editor-inspector-run-button");
    expect(runButton).toMatch(/font-weight:\s*var\(--xflow-font-weight-medium\)\s*;/);
    expect(runButton).toMatch(/gap:\s*var\(--xflow-space-1-5\)\s*;/);
    expect(runButton).toMatch(/padding-inline:\s*var\(--xflow-space-3\)\s*;/);
    // The sprite glyph carries its own 16px box; no AntD-icon sizing rule remains.
    expect(css).not.toContain("xflow-editor-inspector-run-button .anticon");
  });

  it("keeps the 保存 / 发布 label in the body colour on hover, as the baseline top-text button does", () => {
    expect(rule(".xflow-editor-save-button:hover,\n.xflow-editor-publish-button:hover")).toMatch(/color:\s*var\(--xflow-text\)\s*;/);
  });

  it("gives the theme trigger no focus ring of its own, so it takes the AntD 3px ring", () => {
    expect(css).not.toMatch(/\.xflow-editor-theme-trigger:focus-visible/);
  });

  it("draws the library group head with the count last and a 4px side margin", () => {
    const title = rule(".xflow-editor-node-group__title");
    expect(title).toMatch(/margin:\s*0 var\(--xflow-space-1\) var\(--xflow-space-1\)\s*;/);
    expect(title).toMatch(/width:\s*calc\(100% - 8px\)\s*;/);
    expect(rule(".xflow-editor-node-group__count")).toMatch(/margin-left:\s*auto\s*;/);
  });

  it("reveals the group chevron on hover/focus and keeps it visible while collapsed", () => {
    expect(rule(".xflow-editor-node-group__chevron")).toMatch(/opacity:\s*0\s*;/);
    expect(css).toMatch(
      /\.xflow-editor-node-group__title:hover \.xflow-editor-node-group__chevron,[\s\S]*?\{[^}]*opacity:\s*1\s*;/
    );
    expect(css).toMatch(/\.xflow-editor-node-group__title:focus-visible \.xflow-editor-node-group__chevron/);
    expect(css).toMatch(/\.xflow-editor-node-group__chevron\.is-collapsed\s*\{[^}]*opacity:\s*1\s*;/);
  });

  it("lifts the compact drawers with elev-4 and a divider on the canvas side", () => {
    expect(rule(".xflow-editor .xflow-editor-compact-drawer__wrapper")).toMatch(/box-shadow:\s*var\(--xflow-elev-4\)\s*;/);
    expect(rule(".xflow-editor .xflow-editor-compact-drawer__wrapper--start")).toMatch(
      /border-right:\s*1px solid var\(--xflow-border-subtle\)\s*;/
    );
    expect(rule(".xflow-editor .xflow-editor-compact-drawer__wrapper--end")).toMatch(
      /border-left:\s*1px solid var\(--xflow-border-subtle\)\s*;/
    );
  });

  it("draws the status-bar run value in mono, in the strip's own colour", () => {
    const run = rule(".xflow-editor-statusbar__run");
    expect(run).toMatch(/color:\s*inherit\s*;/);
    expect(run).toMatch(/font-family:\s*var\(--xflow-font-mono\)\s*;/);
    // The separators match the 20px top-bar rule, not the old 14px stub.
    expect(rule(".xflow-editor-statusbar__sep")).toMatch(/height:\s*20px\s*;/);
  });

  it("draws the workflow-settings stack at the baseline figure 6 values", () => {
    const root = rule(".xflow-editor-workflow-settings");
    expect(root).toMatch(/--xflow-editor-section-card-radius:\s*var\(--xflow-radius-md\)\s*;/);
    expect(root).toMatch(/--xflow-editor-section-card-bg:\s*var\(--xflow-surface-raised\)\s*;/);
    expect(root).toMatch(/gap:\s*var\(--xflow-space-2\)\s*;/);
    const head = rule(".xflow-editor-workflow-settings .xflow-editor-section-card__header");
    // The baseline head is 35px tall at every width of content (an action key
    // or not), so the height is a floor rather than the padding around a line.
    expect(head).toMatch(/min-height:\s*35px\s*;/);
    expect(head).toMatch(/padding:\s*0 var\(--xflow-space-3\)\s*;/);
    expect(rule(".xflow-editor-workflow-settings .xflow-editor-section-card__title")).toMatch(
      /font-size:\s*var\(--xflow-font-md\)\s*;/
    );
    expect(rule(".xflow-editor-workflow-settings .xflow-editor-section-card__body")).toMatch(
      /padding:\s*var\(--xflow-space-0-5\) var\(--xflow-space-3\) var\(--xflow-space-2\)\s*;/
    );
  });

  it("stacks a workflow-settings row label-over-control with the baseline 12px/500 label", () => {
    const row = rule(".xflow-editor-workflow-settings .xflow-editor-form-row--workflow");
    expect(row).toMatch(/flex-direction:\s*column\s*;/);
    expect(row).not.toMatch(/grid-template-columns/);
    const label = rule(".xflow-editor-workflow-settings .xflow-editor-form-row > span");
    expect(label).toMatch(/font-size:\s*var\(--xflow-font-sm\)\s*;/);
    expect(label).toMatch(/font-weight:\s*var\(--xflow-font-weight-medium\)\s*;/);
    expect(label).toMatch(/color:\s*var\(--xflow-text-secondary\)\s*;/);
    expect(label).toMatch(/margin-bottom:\s*var\(--xflow-space-0-5\)\s*;/);
  });

  it("draws the inspector head tile as the shared category block, with no private tone table", () => {
    expect(css).not.toContain("xflow-editor-inspector-head__icon");
    expect(css).not.toContain("--xflow-editor-inspector-tone");
    // The block is a grid with centred content and the 16px (--xflow-font-xl) glyph.
    const block = rule(".xflow-editor-category-icon");
    expect(block).toMatch(/display:\s*grid\s*;/);
    expect(block).toMatch(/place-items:\s*center\s*;/);
    expect(block).toMatch(/font-size:\s*var\(--xflow-font-xl\)\s*;/);
  });

  it("sets the preview canvas in the shared sans stack", () => {
    const preview = readFileSync(`${import.meta.dirname}/../../xflow-preview/src/styles.css`, "utf8");
    expect(preview.replace(/\/\*[\s\S]*?\*\//g, "")).toMatch(/\.xflow-preview\s*\{[^}]*font-family:\s*var\(--xflow-font-sans\)\s*;/);
  });
});

describe("round-2 visual audit fixes", () => {
  const rule = (selector: string): string => {
    const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const match = css.match(new RegExp(`(?:^|\\})\\s*${escaped}\\s*\\{([^}]*)\\}`));
    expect(match, `${selector} has a rule`).not.toBeNull();
    return match?.[1] ?? "";
  };

  it("draws the inspector form controls as the baseline .ctl: 6px radius, 10px inline padding, 3px/18% focus ring", () => {
    expect(rule(".xflow-editor .xflow-editor-field")).toMatch(/padding-inline:\s*var\(--xflow-space-2-5\)\s*;/);
    expect(rule(".xflow-editor .xflow-editor-textarea")).toMatch(/min-height:\s*64px\s*;/);
    expect(rule(".xflow-editor .xflow-editor-textarea")).toMatch(
      /padding:\s*var\(--xflow-space-1-5\) var\(--xflow-space-2-5\)\s*;/
    );
    expect(css).toMatch(
      /\.xflow-editor \.xflow-editor-field,[\s\S]*?\.xflow-editor \.xflow-editor-select__root\s*\{[^}]*border-radius:\s*var\(--xflow-radius-sm\)\s*;/
    );
    expect(rule(".xflow-editor .xflow-editor-field:focus,\n.xflow-editor .xflow-editor-textarea:focus,\n.xflow-editor .xflow-editor-select__root.ant-select-focused")).toMatch(
      /box-shadow:\s*0 0 0 3px var\(--xflow-editor-control-ring\)\s*;/
    );
    expect(css).toMatch(/--xflow-editor-control-ring:\s*color-mix\(in srgb, var\(--xflow-primary\) 18%, transparent\)\s*;/);
  });

  it("draws the section-card stack at the baseline 12px gap on the raised surface", () => {
    expect(rule(".xflow-editor .xflow-editor-config")).toMatch(/gap:\s*var\(--xflow-space-3\)\s*;/);
    expect(css).toMatch(/--xflow-editor-section-card-bg:\s*var\(--xflow-surface-raised\)\s*;/);
  });
});

// Contrast, hit-target and forced-colours rules the browser applies but jsdom
// cannot see, so the sheet is read from disk.
describe("xflow.editor small-text tones, hit targets and forced colours", () => {
  /** The declaration block of the rule whose selector list is exactly `selector`. */
  const block = (selector: string): string => {
    const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const match = css.match(new RegExp(`(?:^|\\})\\s*${escaped}\\s*\\{([^}]*)\\}`));
    expect(match, `${selector} has a rule`).not.toBeNull();
    return match?.[1] ?? "";
  };

  it("reads the 12px status and note text from the -fg tones, not the raw colours", () => {
    expect(block('.xflow-editor-output__note[data-tone="warning"]')).toMatch(/color:\s*var\(--xflow-warning-fg\)\s*;/);
    for (const tone of ["success", "warning", "danger", "info"]) {
      expect(block(`.xflow-editor-kvlist__status[data-tone="${tone}"]`), tone).toMatch(
        new RegExp(`color:\\s*var\\(--xflow-${tone}-fg\\)\\s*;`)
      );
    }
    expect(block(".xflow-editor-error")).toMatch(/color:\s*var\(--xflow-danger-fg\)\s*;/);
  });

  it("grows the sub-24px hit targets with a transparent ::after, leaving the visual size alone", () => {
    for (const selector of [
      ".xflow-editor-connection-delete",
      ".xflow-editor .xflow-editor-hint__help"
    ]) {
      expect(block(selector), `${selector} anchors its target`).toMatch(/position:\s*relative\s*;/);
      const after = block(`${selector}::after`);
      expect(after, `${selector}::after`).toMatch(/content:\s*""\s*;/);
      expect(after, `${selector}::after`).toMatch(/position:\s*absolute\s*;/);
      expect(after, `${selector}::after`).toMatch(
        /inset:\s*calc\(\(100% - var\(--xflow-control-height-sm\)\) \/ 2\)\s*;/
      );
    }
    // The visible box stays 20px / 14px.
    expect(block(".xflow-editor-connection-delete")).toMatch(/height:\s*20px\s*;[\s\S]*width:\s*20px\s*;/);
    expect(block(".xflow-editor .xflow-editor-hint__help")).toMatch(/height:\s*14px\s*;[\s\S]*width:\s*14px\s*;/);
  });

  it("grows the library group heading to the control step through a token-sized ::after", () => {
    const heading = ".xflow-editor .xflow-editor-node-group__title";
    expect(block(`${heading},\n.xflow-editor .xflow-composer-section-toggle`)).toMatch(/position:\s*relative\s*;/);
    const after = block(`${heading}::after,\n.xflow-editor .xflow-composer-section-toggle::after`);
    expect(after).toMatch(/content:\s*""\s*;/);
    expect(after).toMatch(/position:\s*absolute\s*;/);
    // Vertical only: a side-wise inset would shrink the full-width row to 24px across, and
    // min() keeps a taller row from losing target. The step comes from the token.
    expect(after).toMatch(/inset:\s*min\(0px, calc\(\(100% - var\(--xflow-control-height-sm\)\) \/ 2\)\) 0\s*;/);
    expect(after).not.toMatch(/\b24px\b/);
    // The visible row keeps its 20px line; only the target grows.
    expect(block(".xflow-editor-node-group__title")).toMatch(/line-height:\s*var\(--xflow-line-height-md\)\s*;/);
  });

  it("marks the replayed execution row with the primary tint, ahead of the hover wash", () => {
    const button = ".xflow-editor-exec-item__button";
    expect(block(`${button}:hover`)).toMatch(/background:\s*var\(--xflow-hover\)\s*;/);
    expect(block(`${button}[aria-current="true"]`)).toMatch(/background:\s*var\(--xflow-primary-bg-subtle\)\s*;/);
    // Same specificity as :hover, so the later rule wins: the selected row keeps its tint under the pointer.
    expect(css.indexOf(`${button}[aria-current="true"]`)).toBeGreaterThan(css.indexOf(`${button}:hover`));
  });

  it("keeps an outline on every input focus state, which forced colours map to a system colour", () => {
    const focusRules = [
      ".xflow-editor .xflow-editor-field:focus,\n.xflow-editor .xflow-editor-textarea:focus,\n.xflow-editor .xflow-editor-select__root.ant-select-focused",
      ".xflow-editor .xflow-editor-library-search:focus-within",
      ".xflow-editor .xflow-editor-select__root.ant-select-focused"
    ];
    for (const selector of focusRules) {
      const rules = [...css.matchAll(new RegExp(`(?:^|\\})\\s*${selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s*\\{([^}]*)\\}`, "g"))];
      expect(rules.length, selector).toBeGreaterThan(0);
      for (const [, body] of rules) {
        if (/box-shadow:\s*0 0 0 3px/.test(body)) {
          expect(body, `${selector} draws its ring with box-shadow only`).toMatch(/outline:\s*2px solid transparent\s*;/);
        }
      }
    }
  });

  it("pins the base outline colour of those inputs to transparent, so focus cannot fade a dark ring in", () => {
    // AntD controls carry `transition: all`; with the initial outline colour
    // (currentcolor) in the base state, focusing transitions a dark outline to
    // transparent. Matching the base to the focus value removes the flash.
    for (const selector of [
      ".xflow-editor .xflow-editor-field,\n.xflow-editor .xflow-editor-textarea,\n.xflow-editor .xflow-editor-select__root",
      ".xflow-editor .xflow-editor-library-search"
    ]) {
      expect(block(selector), selector).toMatch(/outline-color:\s*transparent\s*;/);
    }
  });

  it("draws a 2px Highlight outline on those focus states under forced-colors", () => {
    const media = css.match(/@media\s*\(forced-colors:\s*active\)\s*\{([\s\S]*?\n\})\s*\n/);
    expect(media, "a forced-colors block").not.toBeNull();
    const body = media?.[1] ?? "";
    for (const selector of [
      ".xflow-editor .xflow-editor-field:focus",
      ".xflow-editor .xflow-editor-textarea:focus",
      ".xflow-editor .xflow-editor-select__root:focus-within",
      ".xflow-editor .xflow-editor-library-search:focus-within"
    ]) {
      expect(body, selector).toContain(selector);
    }
    expect(body).toMatch(/outline:\s*2px solid Highlight\s*;/);
    // The branch states a system keyword, not a colour literal.
    expect(body).not.toMatch(/#[0-9a-f]{3,8}\b|rgba?\(|hsla?\(/i);
  });
});

// E-C: once the wildcard `!important` reduced-motion fallback is gone, every
// duration must come from tokens.css. This is the grep guard over the three
// sheets that paint inside the xflow roots; token definitions and comments are
// not durations a component owns.
describe("motion durations come from tokens", () => {
  const sheets = [
    `${import.meta.dirname}/styles.css`,
    `${import.meta.dirname}/../../xflow-preview/src/styles.css`,
    `${import.meta.dirname}/../../../apps/xflow-admin/src/layouts/admin.css`,
    `${import.meta.dirname}/../../../apps/xflow-admin/src/pages/ui-preview/ui-preview.css`
  ];

  it("has no bare ms/s duration outside tokens.css", () => {
    for (const sheet of sheets) {
      const text = readFileSync(sheet, "utf8").replace(/\/\*[\s\S]*?\*\//g, "");
      expect(text.match(/(?<![\w.-])\d+(?:\.\d+)?m?s\b/g) ?? [], sheet).toEqual([]);
    }
  });
});
