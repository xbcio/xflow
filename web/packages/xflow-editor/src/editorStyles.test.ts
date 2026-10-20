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
  });

  it("leaves composer's scale to core instead of re-declaring it", () => {
    expect(css).not.toMatch(/--xflow-composer-(control-height|font-size|field-gap)\s*:/);
  });
});

describe("xflow.editor panel title bar (§6.0)", () => {
  it("is a 40px bar with the baseline padding, gap and bottom border", () => {
    expect(css).toMatch(/\.xflow-editor\s*\{[^}]*--xflow-editor-panel-title-height:\s*40px\s*;/);
    expect(css).toMatch(/\.xflow-editor-panel-title\s*\{[^}]*height:\s*var\(--xflow-editor-panel-title-height\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-panel-title\s*\{[^}]*gap:\s*8px\s*;/);
    expect(css).toMatch(/\.xflow-editor-panel-title\s*\{[^}]*padding:\s*0 8px 0 12px\s*;/);
    expect(css).toMatch(/\.xflow-editor-panel-title\s*\{[^}]*border-bottom:\s*1px solid var\(--xflow-border\)\s*;/);
  });

  it("keeps the heading 14/600, the count 12 tertiary and the icon bare 16px", () => {
    expect(css).toMatch(
      /\.xflow-editor-panel-title__heading\s*\{[^}]*font-size:\s*var\(--xflow-font-lg\)\s*;[^}]*font-weight:\s*600\s*;/
    );
    expect(css).toMatch(
      /\.xflow-editor-panel-title__count\s*\{[^}]*color:\s*var\(--xflow-text-tertiary\)\s*;[^}]*font-size:\s*var\(--xflow-font-sm\)\s*;/
    );
    expect(css).toMatch(/\.xflow-editor-panel-title__icon\s*\{[^}]*font-size:\s*var\(--xflow-font-xl\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-panel-title__actions\s*\{[^}]*gap:\s*2px\s*;[^}]*margin-left:\s*auto\s*;/);
  });
});

describe("xflow.editor top bar (§6.3)", () => {
  it("is a 52px bar with the baseline padding and gap", () => {
    expect(css).toMatch(/\.xflow-editor\s*\{[^}]*--xflow-editor-toolbar-height:\s*52px\s*;/);
    expect(css).toMatch(/\.xflow-editor-toolbar\s*\{[^}]*flex:\s*0 0 var\(--xflow-editor-toolbar-height\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-toolbar\s*\{[^}]*gap:\s*8px\s*;/);
    expect(css).toMatch(/\.xflow-editor-toolbar\s*\{[^}]*padding:\s*0 12px\s*;/);
    expect(css).toMatch(/\.xflow-editor-toolbar\s*\{[^}]*border-bottom:\s*1px solid var\(--xflow-border\)\s*;/);
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
    expect(css).toMatch(/\.xflow-editor-history-group\s*\{[^}]*gap:\s*8px\s*;/);
  });

  it("keeps one nowrap 52px row below 1280 so the right cluster is never clipped", () => {
    // E0 legacy: the wrapped second row pushed 运行 outside the root at 802-925.
    // The bar never wraps again; the title pays for a long name, not the actions.
    const compact = css.match(/@container xflow-editor \(width < 1280px\) \{([\s\S]*?)\n\}/)?.[1] ?? "";
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

  it("keeps top-bar icon buttons secondary and save/publish at weight 500", () => {
    expect(css).toMatch(/\.xflow-editor-icon-button\s*\{[^}]*color:\s*var\(--xflow-text-secondary\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-save-button,\s*\.xflow-editor-publish-button\s*\{[^}]*font-weight:\s*500\s*;/);
  });

  it("draws the mode switch without the border that made it 34px", () => {
    expect(css).toMatch(/\.xflow-editor-mode-segmented\s*\{[^}]*border:\s*0\s*;/);
  });

  it("chains the version chip through the tone tokens like the baseline chip", () => {
    expect(css).toMatch(/\.xflow-editor-chip\s*\{[^}]*border-radius:\s*var\(--xflow-radius-pill\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-chip\s*\{[^}]*font-size:\s*var\(--xflow-font-xs\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-chip\s*\{[^}]*font-weight:\s*500\s*;/);
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
      // Whatever remains is a preference query (the theme), never a size one.
      expect(prelude, `@media ${prelude}`).toMatch(/prefers-/);
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
    // rules.
    expect(thresholds).toEqual([560, breakpoints.compact, breakpoints.compact]);
    // `compact` keeps the exclusive comparison the JS tier uses; 560/801 keep
    // the inclusive `max-width` semantics they replaced.
    for (const [, operator, value] of queries) {
      expect(operator, value).toBe(Number(value) === breakpoints.compact ? "<" : "<=");
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
  it("animates the column track for 160ms and drops the transition under reduced motion", () => {
    expect(css, "collapse/expand eases the column widths").toMatch(
      /\.xflow-editor-main\s*\{[^}]*transition:\s*grid-template-columns\s+160ms\s+ease\s*;/
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
    expect(ruleBody(".xflow-editor-inspector-tabs__header")).toMatch(/padding-inline:\s*12px\s*;/);
    const body = ruleBody(".xflow-editor-inspector-tabs__body");
    expect(body).toMatch(/height:\s*100%\s*;/);
    expect(body).toMatch(/overflow:\s*auto\s*;/);
  });

  it("keeps the tab row on the panel surface and the content flush under it, as the baseline does", () => {
    expect(ruleBody(".xflow-editor-inspector-tabs__header")).not.toMatch(/background/);
    expect(ruleBody(".xflow-editor-inspector-tabs__content")).toMatch(/padding:\s*0 12px\s*;/);
  });

  it("draws the failed-run dot as a 6px danger circle", () => {
    const dot = ruleBody(".xflow-editor-inspector-tab-dot");
    expect(dot).toMatch(/background:\s*var\(--xflow-danger\)\s*;/);
    expect(dot).toMatch(/height:\s*6px\s*;/);
    expect(dot).toMatch(/width:\s*6px\s*;/);
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

  it("sizes the drawer from one 256px variable over a 40px head and a 360px summary column", () => {
    expect(css).toMatch(/\.xflow-editor\s*\{[^}]*--xflow-editor-drawer-height:\s*256px\s*;/);
    const drawer = rule(".xflow-editor-drawer");
    // The variable is the ceiling; a host shorter than bar + strip + drawer wins, so
    // the open drawer never rides up over the toolbar (tier a has no viewport clamp).
    expect(drawer).toMatch(
      /height:\s*min\(var\(--xflow-editor-drawer-height\), calc\(100% - var\(--xflow-editor-toolbar-height\) - 29px\)\)\s*;/
    );
    expect(css).toMatch(
      /\.xflow-editor-bottom\[data-dock="expanded"\]\s*\{[^}]*max-height:\s*calc\(100% - var\(--xflow-editor-toolbar-height\)\)\s*;/
    );
    expect(drawer).toMatch(/grid-template-columns:\s*minmax\(0, 1fr\) 360px\s*;/);
    expect(drawer).toMatch(/grid-template-rows:\s*var\(--xflow-editor-panel-title-height\) minmax\(0, 1fr\)\s*;/);
  });

  it("clamps the drawer on the compact tiers so the canvas keeps a floor", () => {
    expect(css).toMatch(
      /\.xflow-editor\[data-layout="b"\],\s*\.xflow-editor\[data-layout="c"\]\s*\{[^}]*--xflow-editor-drawer-height:\s*min\(256px, calc\(100dvh - var\(--xflow-editor-toolbar-height\) - 28px - 120px\)\)\s*;/
    );
  });

  it("folds the summary under the issue list below 1280 by container query, not media query", () => {
    const folded = css.match(/@container xflow-editor \(width < 1280px\)\s*\{\s*\.xflow-editor-drawer\s*\{[^}]*grid-template-columns:\s*minmax\(0, 1fr\)\s*;[^}]*\}\s*\.xflow-editor-drawer__side\s*\{([^}]*)\}/);
    expect(folded, "the single-column drawer rule").not.toBeNull();
    // Still reachable: folded, never hidden.
    expect(folded?.[1]).not.toMatch(/display:\s*none/);
    expect(folded?.[1]).toMatch(/flex-direction:\s*row\s*;/);
    expect(folded?.[1]).toMatch(/border-top:\s*1px solid var\(--xflow-border\)\s*;/);
  });

  it("sets the drawer title group 8px apart inside a 16px head", () => {
    expect(rule(".xflow-editor .xflow-editor-drawer__head")).toMatch(/gap:\s*16px\s*;/);
    expect(rule(".xflow-editor-drawer__title")).toMatch(/gap:\s*8px\s*;/);
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

  it("animates the drawer height for 160ms and switches it off under reduced motion", () => {
    expect(rule(".xflow-editor-drawer")).toMatch(/transition:\s*height 160ms ease, box-shadow 160ms ease\s*;/);
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
    expect(shell).toMatch(/padding:\s*8px 12px\s*;/);
    expect(shell).toMatch(/gap:\s*8px\s*;/);
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
    expect(body(".xflow-editor-banner")).toMatch(/line-height:\s*20px\s*;/);
  });

  it("keeps the icon on the tone, the lead at 600 and the actions 6px apart", () => {
    expect(body(".xflow-editor-banner__icon")).toMatch(/color:\s*var\(--xflow-editor-banner-tone\)\s*;/);
    expect(body(".xflow-editor-banner__strong")).toMatch(/font-weight:\s*600\s*;/);
    expect(body(".xflow-editor-banner__text")).toMatch(/min-width:\s*0\s*;/);
    expect(body(".xflow-editor-banner__actions")).toMatch(/gap:\s*6px\s*;/);
  });

  it("follows the preview's rulers: the top offset drops the ruler term when none are drawn", () => {
    expect(body(".xflow-editor-canvas")).toMatch(/--xflow-editor-banner-ruler:\s*var\(--xflow-ruler\)\s*;/);
    expect(body('.xflow-editor-canvas:has([data-ruler="false"])')).toMatch(/--xflow-editor-banner-ruler:\s*0px\s*;/);
  });

  it("gives the read-only / preview notice a base inset and body size at every width", () => {
    const notice = body(".xflow-editor-preview-inspector");
    expect(notice).toMatch(/padding:\s*16px\s*;/);
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
    expect(divider).toMatch(/border-top:\s*1px solid var\(--xflow-border\)\s*;/);
    expect(css.indexOf('.xflow-editor-lib[data-zones="dual"] > .xflow-editor-outline')).toBeGreaterThan(
      css.indexOf(".xflow-editor .xflow-editor-panel {")
    );
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
    expect(rule(".xflow-editor-node-group__title")).toMatch(/font-weight:\s*600\s*;/);
    expect(rule(".xflow-editor-node-group__count")).not.toMatch(/font-weight/);
  });

  it("anchors the compact drawer to the workbench body, which sits between the top bar and the status bar", () => {
    // The drawer is `position: absolute` inside .xflow-editor-main (getContainer
    // false), so its block extent is the body's: it cannot cover either bar.
    expect(rule(".xflow-editor-main")).toMatch(/position:\s*relative\s*;/);
  });

  it("shows the draggable library rows with the grab cursor", () => {
    expect(rule(".xflow-editor-node-tile")).toMatch(/cursor:\s*grab\s*;/);
    // The baseline row is a text Button: its 1px transparent border makes the row 58px.
    expect(rule(".xflow-editor-node-tile")).toMatch(/border:\s*1px solid transparent\s*;/);
  });

  it("draws the library row icon as the baseline 28px block, the outline one as the 20px --sm", () => {
    const rule = (selector: string): string => {
      const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      return css.match(new RegExp(`(?:^|\\})\\s*${escaped}\\s*\\{([^}]*)\\}`))?.[1] ?? "";
    };
    const base = rule(".xflow-editor-category-icon");
    expect(base).toMatch(/width:\s*28px\s*;/);
    expect(base).toMatch(/height:\s*28px\s*;/);
    expect(base).toMatch(/border-radius:\s*var\(--xflow-radius-sm\)\s*;/);
    const sm = rule(".xflow-editor-category-icon--sm");
    expect(sm).toMatch(/width:\s*20px\s*;/);
    expect(sm).toMatch(/border-radius:\s*var\(--xflow-radius-xs\)\s*;/);
  });

  it("draws an outline row as the baseline 30px line: 0 8px padding, 8px gap, 13px, small radius", () => {
    const row = rule(".xflow-editor-outline__item");
    expect(row).toMatch(/height:\s*30px\s*;/);
    expect(row).toMatch(/padding:\s*0 8px\s*;/);
    expect(row).toMatch(/gap:\s*8px\s*;/);
    expect(row).toMatch(/font-size:\s*var\(--xflow-font-md\)\s*;/);
    expect(row).toMatch(/border-radius:\s*var\(--xflow-radius-sm\)\s*;/);
  });

  it("indents a branch row and a port heading by the one 26px step", () => {
    expect(rule(".xflow-editor-outline__port[data-indent],\n.xflow-editor-outline__item[data-indent]")).toMatch(
      /padding-inline-start:\s*26px\s*;/
    );
  });

  it("marks the current row with a 12% primary ground and a 600 primary name", () => {
    expect(rule('.xflow-editor-outline__item[aria-current="true"]')).toMatch(
      /background:\s*color-mix\(in srgb, var\(--xflow-primary\) 12%, transparent\)\s*;/
    );
    const name = rule('.xflow-editor-outline__item[aria-current="true"] .xflow-editor-outline__name');
    expect(name).toMatch(/color:\s*var\(--xflow-primary\)\s*;/);
    expect(name).toMatch(/font-weight:\s*600\s*;/);
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

  it("draws the section heading 11/600 tertiary on a 20px line with a 3x10 colour bar, and the node name 11 mono", () => {
    const heading = rule(".xflow-editor-outline__heading");
    expect(heading).toMatch(/font-size:\s*var\(--xflow-font-xs\)\s*;/);
    expect(heading).toMatch(/line-height:\s*20px\s*;/);
    expect(rule(".xflow-editor-outline__sub")).toMatch(/font-size:\s*var\(--xflow-font-xs\)\s*;/);
    expect(heading).toMatch(/font-weight:\s*600\s*;/);
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

  it("keeps the 32px top-bar buttons at the 6px radius step, with an 8px gap in 保存", () => {
    expect(rule(".xflow-editor-icon-button")).toMatch(/border-radius:\s*var\(--xflow-radius-sm\)\s*;/);
    expect(rule(".xflow-editor .xflow-editor-theme-trigger")).toMatch(/border-radius:\s*var\(--xflow-radius-sm\)\s*;/);
    expect(rule(".xflow-editor-save-button")).toMatch(/gap:\s*8px\s*;/);
  });

  it("makes the right column 360px wide", () => {
    expect(css).toMatch(/--xflow-editor-panel-right-width:\s*360px\s*;/);
  });

  it("leaves the library rows flush: the 58px row is the whole pitch", () => {
    expect(rule(".xflow-editor-node-list")).toMatch(/gap:\s*0\s*;/);
  });

  it("draws a pressed title-bar action at 14% and the search prefix in the text colour", () => {
    expect(rule('.xflow-editor .xflow-editor-panel-collapse[aria-pressed="true"]')).toMatch(
      /color-mix\(in srgb, var\(--xflow-primary\) 14%, transparent\)/
    );
    expect(rule(".xflow-editor .xflow-editor-library-search__prefix")).toMatch(/color:\s*var\(--xflow-text\)\s*;/);
  });

  it("gives the outline rows the 1px transparent border and a 4px sub gap", () => {
    expect(rule(".xflow-editor-outline__item")).toMatch(/border:\s*1px solid transparent\s*;/);
    expect(rule(".xflow-editor-outline__sub")).toMatch(/margin-inline-start:\s*4px\s*;/);
  });

  it("sets the canvas float colours and the 16px locate glyph", () => {
    expect(rule(".xflow-editor-canvas-add")).toMatch(/color:\s*var\(--xflow-text\)\s*;/);
    expect(rule(".xflow-editor-canvas-reopen")).toMatch(/color:\s*var\(--xflow-text\)\s*;/);
    expect(css).toMatch(/\.xflow-editor-canvas-locate\s*\{[^}]*font-size:\s*var\(--xflow-font-xl\)\s*;/);
  });

  it("pins the inspector footer under the scrolling tabs", () => {
    const foot = rule(".xflow-editor .xflow-editor-inspector-foot");
    expect(foot).toMatch(/border-top:\s*1px solid var\(--xflow-border\)\s*;/);
    expect(foot).toMatch(/padding:\s*22px 12px 12px\s*;/);
    expect(foot).toMatch(/flex:\s*0 0 auto\s*;/);
  });

  it("keeps the 保存 / 发布 label in the body colour on hover, as the baseline top-text button does", () => {
    expect(rule(".xflow-editor-save-button:hover,\n.xflow-editor-publish-button:hover")).toMatch(/color:\s*var\(--xflow-text\)\s*;/);
  });

  it("gives the theme trigger no focus ring of its own, so it takes the AntD 3px ring", () => {
    expect(css).not.toMatch(/\.xflow-editor-theme-trigger:focus-visible/);
  });

  it("draws the library group head with the count last and a 4px side margin", () => {
    const title = rule(".xflow-editor-node-group__title");
    expect(title).toMatch(/margin:\s*0 4px 4px\s*;/);
    expect(title).toMatch(/width:\s*calc\(100% - 8px\)\s*;/);
    expect(rule(".xflow-editor-node-group__count")).toMatch(/margin-left:\s*auto\s*;/);
  });

  it("lifts the compact drawers with elev-3 and a divider on the canvas side", () => {
    expect(rule(".xflow-editor .xflow-editor-compact-drawer__wrapper")).toMatch(/box-shadow:\s*var\(--xflow-elev-3\)\s*;/);
    expect(rule(".xflow-editor .xflow-editor-compact-drawer__wrapper--start")).toMatch(
      /border-right:\s*1px solid var\(--xflow-border\)\s*;/
    );
    expect(rule(".xflow-editor .xflow-editor-compact-drawer__wrapper--end")).toMatch(
      /border-left:\s*1px solid var\(--xflow-border\)\s*;/
    );
  });

  it("draws the status-bar run value in mono, in the strip's own colour", () => {
    const run = rule(".xflow-editor-statusbar__run");
    expect(run).toMatch(/color:\s*inherit\s*;/);
    expect(run).toMatch(/font-family:\s*var\(--xflow-font-mono\)\s*;/);
  });

  it("draws the workflow-settings stack at the baseline figure 6 values", () => {
    const root = rule(".xflow-editor-workflow-settings");
    expect(root).toMatch(/--xflow-editor-section-card-radius:\s*var\(--xflow-radius-md\)\s*;/);
    expect(root).toMatch(/--xflow-editor-section-card-bg:\s*var\(--xflow-panel-raised\)\s*;/);
    expect(root).toMatch(/gap:\s*8px\s*;/);
    const head = rule(".xflow-editor-workflow-settings .xflow-editor-section-card__header");
    // The baseline head is 35px tall at every width of content (an action key
    // or not), so the height is a floor rather than the padding around a line.
    expect(head).toMatch(/min-height:\s*35px\s*;/);
    expect(head).toMatch(/padding:\s*0 12px\s*;/);
    expect(rule(".xflow-editor-workflow-settings .xflow-editor-section-card__title")).toMatch(
      /font-size:\s*var\(--xflow-font-md\)\s*;/
    );
    expect(rule(".xflow-editor-workflow-settings .xflow-editor-section-card__body")).toMatch(/padding:\s*2px 12px 8px\s*;/);
  });

  it("stacks a workflow-settings row label-over-control with the baseline 12px/500 label", () => {
    const row = rule(".xflow-editor-workflow-settings .xflow-editor-form-row--workflow");
    expect(row).toMatch(/flex-direction:\s*column\s*;/);
    expect(row).not.toMatch(/grid-template-columns/);
    const label = rule(".xflow-editor-workflow-settings .xflow-editor-form-row > span");
    expect(label).toMatch(/font-size:\s*var\(--xflow-font-sm\)\s*;/);
    expect(label).toMatch(/font-weight:\s*500\s*;/);
    expect(label).toMatch(/color:\s*var\(--xflow-text-secondary\)\s*;/);
    expect(label).toMatch(/margin-bottom:\s*2px\s*;/);
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
