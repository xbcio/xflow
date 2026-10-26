import { readFileSync } from "node:fs";
import { edgeVariant, nodeStatusMeta, type NodeStatusKey } from "@xflow/core";
import { describe, expect, it } from "vitest";

// jsdom does not load stylesheets, so no render test notices a card class that
// index.tsx emits but styles.css never mentions. This reads both files from disk
// (Vitest turns `?raw` stylesheet imports into empty strings) and compares them.
// `import.meta.dirname` is this directory (packages/xflow-preview/src).
const read = (name: string) => readFileSync(`${import.meta.dirname}/${name}`, "utf8");

const source = read("index.tsx");
const css = read("styles.css").replace(/\/\*[\s\S]*?\*\//g, "");

/**
 * Classes index.tsx emits that styles.css deliberately has no rule for. Each is
 * listed by its exact name, with the reason; this is not a place for wildcards.
 */
const withoutRule: Record<string, string> = {
  // Pure hooks: they mark a handle's role for tests and host overrides, and the
  // shared `.xflow-preview-node__handle` rule already draws every handle.
  "xflow-preview-node__handle--port": "hook only, styled by the shared handle rule",
  "xflow-preview-node__handle--spread": "hook only, styled by the shared handle rule",
  // `default` is a status the edge table maps to the resting stroke that
  // `.react-flow__edge-path` already carries; it is the baseline, not a variant.
  "xflow-preview-edge--default": "resting stroke, no rule of its own",
  // `none` means the edge is not drawn at all, so the class is never emitted.
  "xflow-preview-edge--none": "edge is skipped, class never reaches the DOM"
};

/** The `xflow-preview-node*` classes index.tsx writes, as whole names. */
const nodeClasses = (): Set<string> =>
  // The lookbehind keeps `--xflow-preview-node-category` (a custom property) out.
  new Set(source.match(/(?<![\w-])xflow-preview-node(?:__[a-z-]+?)?(?:--[a-z-]+)?(?![\w-])/g) ?? []);

/**
 * The `xflow-preview-edge--*` classes. Two are written literally; the rest come
 * from the template `xflow-preview-edge--${variant}`, so they are expanded from
 * the same table the component reads rather than re-listed here.
 */
const edgeClasses = (): Set<string> => {
  const found = new Set(source.match(/(?<![\w-])xflow-preview-edge--[a-z-]+(?![\w-])/g) ?? []);
  if (source.includes("`xflow-preview-edge--${variant}`")) {
    for (const status of Object.keys(nodeStatusMeta) as NodeStatusKey[]) {
      found.add(`xflow-preview-edge--${edgeVariant(status)}`);
    }
  }
  return found;
};

const hasRule = (name: string) => new RegExp(`\\.${name}(?![\\w-])`).test(css);

describe("xflow-preview class names against styles.css", () => {
  it("finds the classes it is meant to guard", () => {
    // A rename in index.tsx must not silently empty the lists below.
    expect(nodeClasses()).toContain("xflow-preview-node__header");
    expect(nodeClasses()).toContain("xflow-preview-node__status");
    expect(edgeClasses()).toContain("xflow-preview-edge--flow");
    expect(edgeClasses()).toContain("xflow-preview-edge--dangling");
  });

  it("styles every node-card class that index.tsx emits", () => {
    const unstyled = [...nodeClasses()].filter((name) => !hasRule(name) && !(name in withoutRule)).sort();
    expect(unstyled).toEqual([]);
  });

  it("styles every incoming-edge variant that index.tsx can emit", () => {
    const unstyled = [...edgeClasses()].filter((name) => !hasRule(name) && !(name in withoutRule)).sort();
    expect(unstyled).toEqual([]);
  });

  it("keeps the allow-list honest", () => {
    const emitted = new Set([...nodeClasses(), ...edgeClasses()]);
    for (const name of Object.keys(withoutRule)) {
      // An entry nobody emits is stale; one that now has a rule no longer needs it.
      expect(emitted.has(name), `${name} is no longer emitted by index.tsx`).toBe(true);
      expect(hasRule(name), `${name} has a rule now, drop it from the allow-list`).toBe(false);
    }
  });
});

/** Declarations of the first rule whose selector list is exactly `selector`, as a property map. */
const declarationsOf = (selector: string): Record<string, string> => {
  const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/\s+/g, "\\s+");
  // The selector must open a rule (after `{`, `}` or the file start); a line inside a
  // comma list would otherwise match the tail of a different, shared rule.
  const match = new RegExp(`(?:^|[{}])\\s*${escaped}\\s*\\{([^}]*)\\}`).exec(css);
  expect(match, `no rule for ${selector}`).not.toBeNull();
  return Object.fromEntries(
    [...match![1]!.matchAll(/([a-z-]+)\s*:\s*([^;]+);/g)].map((decl) => [decl[1]!, decl[2]!.trim()])
  );
};

// jsdom computes no cascade from styles.css, so the shapes the baseline fixes are
// pinned against the stylesheet text (ui-preview.css is the reference).
describe("xflow-preview typography tokens (ED-16 / ED-17)", () => {
  it("sizes every font from the shared --xflow-font-* scale", () => {
    const sizes = [...css.matchAll(/font-size:\s*([^;]+);/g)].map((match) => match[1].trim());
    expect(sizes.length).toBeGreaterThan(10);
    for (const value of sizes) {
      // A glyph may scale a token with calc(); a literal px size may not appear.
      expect(value, value).toMatch(/^(?:calc\()?var\(--xflow-font-(?:xs|sm|md|lg|xl)\)/);
    }
  });

  it("keeps every font weight on the 400/500/600 scale", () => {
    const weights = [...css.matchAll(/font-weight:\s*([^;]+);/g)].map((match) => match[1].trim());
    expect(weights.length).toBeGreaterThan(5);
    expect([...new Set(weights)].sort()).toEqual([
      "var(--xflow-font-weight-medium)",
      "var(--xflow-font-weight-regular)",
      "var(--xflow-font-weight-semibold)",
    ]);
  });
});

describe("xflow-preview shapes against the baseline", () => {
  it("draws a read-only handle as the same hollow ring, differing only by cursor", () => {
    expect(declarationsOf('.xflow-preview-node__handle[aria-disabled="true"]')).toEqual({ cursor: "not-allowed" });
    expect(css).not.toContain("--xflow-preview-port-disabled");
  });

  it("draws the selected edge 2px wide, as the baseline's selected sample", () => {
    const selected = declarationsOf(
      ".xflow-preview-canvas .react-flow__edge:focus-visible .react-flow__edge-path,\n  .xflow-preview-canvas .react-flow__edge.selected .react-flow__edge-path"
    );
    expect(selected["stroke-width"]).toBe("2");
  });

  it("keeps port-semantic strokes in CSS, so hover and selection can outrank them", () => {
    expect(
      declarationsOf(".xflow-preview-canvas .xflow-preview-edge--dangling .react-flow__edge-path")
    ).toEqual({
      stroke: "var(--xflow-preview-edge-dangling)",
      "stroke-dasharray": "2 4",
      "stroke-width": "1.8"
    });
    expect(declarationsOf(".xflow-preview-canvas .xflow-preview-edge--error .react-flow__edge-path")).toEqual({
      stroke: "var(--xflow-preview-edge-error)",
      "stroke-dasharray": "5 4"
    });
    // No edge object may carry an inline stroke again, and nothing in the sheet
    // should need !important once it is gone.
    expect(source).not.toContain("strokeDasharray");
    expect(source).not.toContain("style: isDangling");
    expect(css).not.toContain("!important");
  });

  it("draws the waiting edge as the flow stroke at 2.5x the period, and stills both under reduced motion", () => {
    const slow = declarationsOf(".xflow-preview-canvas .xflow-preview-edge--flow-slow .react-flow__edge-path");
    expect(slow.animation).toContain("calc(var(--xflow-preview-flow-period) * var(--xflow-preview-flow-slow-factor))");
    // `.xflow-preview-canvas` has more than one rule; the period variables sit in the edge section.
    expect(css).toMatch(/--xflow-preview-flow-period:\s*calc\(var\(--xflow-motion-duration-instant\) \* 10\);/);
    expect(css).toMatch(/--xflow-preview-flow-slow-factor:\s*2\.5;/);
    // Same stroke and dash pattern as `flow`: the two share one rule.
    const shared = declarationsOf(
      ".xflow-preview-canvas .xflow-preview-edge--flow .react-flow__edge-path,\n  .xflow-preview-canvas .xflow-preview-edge--flow-slow .react-flow__edge-path"
    );
    expect(shared["stroke-dasharray"]).toBe("6 4");
    const reduced = /@media \(prefers-reduced-motion: reduce\)\s*\{[^@]*?edge--flow-slow[^{]*\{\s*animation:\s*none/.exec(css);
    expect(reduced, "flow-slow is not stilled under prefers-reduced-motion").not.toBeNull();
  });

  it("draws the error port border as a fixed danger mix, whatever the card tone", () => {
    const port = declarationsOf(".xflow-preview-node__port--error");
    expect(port["border-color"]).toBe("color-mix(in srgb, var(--xflow-danger) 45%, transparent)");
    expect(port["border-color"]).not.toContain("preview-t-border");
  });

  it("draws the skipped edge with the baseline 3/4 dash, apart from the dangling 2/4", () => {
    expect(declarationsOf(".xflow-preview-canvas .xflow-preview-edge--skipped .react-flow__edge-path")).toMatchObject({
      opacity: "0.5",
      "stroke-dasharray": "3 4"
    });
  });

  it("draws the edge label as the baseline pill: panel fill, 1px border, 6px corner, 11/400", () => {
    expect(css).toMatch(/--xflow-preview-edge-label-bg:\s*var\(--xflow-surface-panel\)\s*;/);
    expect(declarationsOf(".xflow-preview-canvas .react-flow__edge-textbg")).toMatchObject({
      fill: "var(--xflow-preview-edge-label-bg)",
      stroke: "var(--xflow-border)",
      "stroke-width": "1"
    });
    expect(declarationsOf(".xflow-preview-canvas .react-flow__edge-text")).toMatchObject({
      fill: "var(--xflow-preview-edge-label)",
      "font-size": "var(--xflow-font-xs)",
      "font-weight": "var(--xflow-font-weight-regular)"
    });
    // React Flow owns the pill corner and padding; both are per-edge props.
    expect(source).toContain("labelBgPadding: [6, 1.75]");
    expect(source).toContain("labelBgBorderRadius: 6");
  });

  it("reads the tone foreground from the raw semantic colour, one tone at a time", () => {
    for (const tone of ["info", "success", "warning", "danger", "suspend", "neutral"]) {
      const rule = declarationsOf(
        `:is(.xflow-preview-node, .xflow-preview-node__status, .xflow-preview-status)[data-tone="${tone}"]`
      );
      expect(rule["--xflow-preview-t-fg"], tone).toBe(`var(--xflow-${tone})`);
    }
  });

  it("sets the card's small tone text in the -fg tone, keeping the raw colour for glyphs", () => {
    // 11-12px text on the raised surface: the raw danger/warning/success colours
    // miss 4.5:1 in the light appearance, the derived -fg tones clear it.
    expect(declarationsOf('.xflow-preview-node__summary dd[data-kind="missing"]').color).toBe("var(--xflow-danger-fg)");
    expect(declarationsOf(".xflow-preview-node__pin").color).toBe("var(--xflow-success-fg)");
    expect(declarationsOf('.xflow-preview-node__issues[data-tone="danger"]').color).toBe("var(--xflow-danger-fg)");
    expect(declarationsOf('.xflow-preview-node__issues[data-tone="warning"]').color).toBe("var(--xflow-warning-fg)");
    // The active zoom-bar key is a 16px glyph, which the 3:1 non-text floor covers.
    expect(declarationsOf('.xflow-preview-zoombar__button[data-active="true"]').color).toBe("var(--xflow-primary)");
  });

  it("shapes the toolbar status pill like the baseline chip", () => {
    expect(declarationsOf(".xflow-preview-status")).toMatchObject({
      "border-radius": "var(--xflow-radius-full)",
      "font-size": "var(--xflow-font-xs)",
      height: "20px",
      padding: "0 var(--xflow-space-2)"
    });
  });

  it("dims a skipped or disabled card by fill, border and icon, never by whole-card opacity", () => {
    const dimmed = declarationsOf(
      '.xflow-preview-node[data-status="skipped"],\n  .xflow-preview-node[data-disabled]'
    );
    // The fill mixes toward the canvas and stays opaque: a translucent fill would
    // let the edges the card covers show through.
    expect(dimmed["background-color"]).toContain("var(--xflow-preview-node-bg) 55%");
    expect(dimmed["background-color"]).toContain("var(--xflow-canvas)");
    // A disabled node has no tone, so a bare `var(--xflow-preview-t-border)` would
    // make the whole declaration invalid; the fallback is what keeps that card dimmed.
    expect(dimmed["border-color"]).toContain(
      "var(--xflow-preview-t-border, var(--xflow-preview-node-border))"
    );
    expect(dimmed.opacity, "the card no longer carries a whole-card opacity").toBeUndefined();
    expect(css).not.toContain("--xflow-preview-node-disabled-opacity");
    // Not just in that rule: no rule may dim the whole card for these two statuses,
    // however it is written (a second selector list would slip past the map above).
    expect(css).not.toMatch(/\.xflow-preview-node\[data-(?:status="skipped"|disabled)\](?:\s*,[^{]*)?\s*\{[^}]*opacity/);

    const icon = declarationsOf(
      '.xflow-preview-node[data-status="skipped"] .xflow-preview-node__icon,\n  .xflow-preview-node[data-disabled] .xflow-preview-node__icon'
    );
    expect(icon).toEqual({ opacity: "0.55" });

    // The shapes the dimming sits on top of are unchanged.
    expect(
      declarationsOf(
        '.xflow-preview-node[data-status="pending"][data-tone]:not([data-disabled]),\n  .xflow-preview-node[data-status="skipped"]'
      )
    ).toEqual({ "border-style": "dashed" });
    expect(css).toMatch(/\[data-disabled\]\s*\{[^}]*repeating-linear-gradient/);
  });

  it("draws a hovered edge 2px primary, as the baseline's hover sample", () => {
    expect(declarationsOf(".xflow-preview-canvas .react-flow__edge:hover .react-flow__edge-path")).toEqual({
      stroke: "var(--xflow-preview-edge-selected)",
      "stroke-width": "2"
    });
  });

  it("turns the selected edge's label primary, from CSS rather than an inline fill", () => {
    expect(source).not.toContain("labelStyle");
    expect(
      declarationsOf(
        ".xflow-preview-canvas .react-flow__edge:focus-visible .react-flow__edge-text,\n  .xflow-preview-canvas .react-flow__edge.selected .react-flow__edge-text"
      )
    ).toEqual({ fill: "var(--xflow-preview-edge-selected)" });
    // The port-semantic labels moved out of index.tsx along with it.
    expect(declarationsOf(".xflow-preview-canvas .xflow-preview-edge--dangling .react-flow__edge-text"))
      .toEqual({ fill: "var(--xflow-preview-edge-dangling-label)" });
    expect(declarationsOf(".xflow-preview-canvas .xflow-preview-edge--error .react-flow__edge-text"))
      .toEqual({ fill: "var(--xflow-preview-edge-error)" });
  });

  it("draws the canvas dots from core's token, with no local grid override left", () => {
    expect(source).toMatch(/<Background\b[^>]*color="var\(--xflow-canvas-dot\)"[^>]*gap=\{16\}/);
    expect(css).not.toContain("--xflow-preview-canvas-grid");
    expect(css).not.toContain("--xy-background-pattern-color");
  });

  it("shapes the §6.2 minimap 200x124 at the container radius", () => {
    expect(declarationsOf(".xflow-preview-canvas .react-flow__minimap")).toMatchObject({
      "border-radius": "var(--xflow-radius-md)",
      height: "124px",
      width: "200px"
    });
  });
});

// The §6.2 canvas chrome lives in canvasChrome.tsx; its classes get the same guard.
describe("xflow-preview canvas chrome against styles.css", () => {
  const chrome = read("canvasChrome.tsx");
  const chromeClasses = new Set(chrome.match(/(?<![\w-])xflow-preview-(?:ruler|zoombar)(?:__[a-z-]+)?(?:--[a-z]+)?(?![\w-])/g) ?? []);
  // `xflow-preview-ruler--${axis}` is a template; both axes are emitted.
  if (chrome.includes("xflow-preview-ruler--${axis}")) {
    chromeClasses.add("xflow-preview-ruler--x");
    chromeClasses.add("xflow-preview-ruler--y");
  }

  it("styles every chrome class canvasChrome.tsx emits", () => {
    expect(chromeClasses).toContain("xflow-preview-zoombar__slider");
    expect(chromeClasses).toContain("xflow-preview-ruler__highlight");
    expect([...chromeClasses].filter((name) => !hasRule(name)).sort()).toEqual([]);
  });

  it("draws the rulers at the core ruler width, above the pane, never under the pointer", () => {
    expect(declarationsOf(".xflow-preview-ruler--x")).toMatchObject({ height: "var(--xflow-ruler)", left: "var(--xflow-ruler)", top: "0" });
    expect(declarationsOf(".xflow-preview-ruler--y")).toMatchObject({ width: "var(--xflow-ruler)", top: "var(--xflow-ruler)", left: "0" });
    expect(declarationsOf(".xflow-preview-ruler,\n  .xflow-preview-ruler__corner")).toMatchObject({
      "pointer-events": "none",
      position: "absolute",
      "z-index": "5"
    });
  });

  it("shapes the zoom bar like the shared floating bar, inside the ruler", () => {
    expect(declarationsOf(".xflow-preview-zoombar")).toMatchObject({
      height: "var(--xflow-float-height)",
      border: "1px solid var(--xflow-border)",
      "border-radius": "var(--xflow-radius-md)",
      "box-shadow": "var(--xflow-elev-2)",
      padding: "var(--xflow-space-1)",
      left: "calc(var(--xflow-preview-ruler-inset) + var(--xflow-preview-float-inset))",
      bottom: "var(--xflow-preview-float-inset)"
    });
    // The inset follows the rulers, so the bar moves to the edge when they are off.
    expect(declarationsOf('.xflow-preview-canvas[data-ruler="true"]')).toEqual({
      "--xflow-preview-ruler-inset": "var(--xflow-ruler)"
    });
    expect(declarationsOf(".xflow-preview-zoombar__slider")).toMatchObject({ width: "96px" });
  });

  it("puts the owner's top-left float in the same chrome, below and right of the rulers", () => {
    expect(declarationsOf(".xflow-preview-canvas-float")).toMatchObject({
      height: "var(--xflow-float-height)",
      border: "1px solid var(--xflow-border)",
      "border-radius": "var(--xflow-radius-md)",
      "box-shadow": "var(--xflow-elev-2)",
      padding: "var(--xflow-space-1)",
      left: "calc(var(--xflow-preview-ruler-inset) + var(--xflow-preview-float-inset))",
      top: "calc(var(--xflow-preview-ruler-inset) + 12px)",
      "z-index": "5"
    });
  });

  it("parks the right-hand float one float inset (16px) inside the canvas edge", () => {
    // The inspector reopen chip: the same chrome, mirrored to the top right, 16px in
    // like the baseline's `.xflow-pv-reopen` and the minimap (E6 measured 12 vs 16).
    // Rulers are top/left only, so the right side never adds the ruler inset.
    expect(declarationsOf(".xflow-preview-canvas-float--end")).toMatchObject({
      left: "auto",
      right: "var(--xflow-preview-float-inset)"
    });
  });

  it("parks the minimap at the same float inset, off xyflow's 15px panel margin", () => {
    expect(declarationsOf(".xflow-preview-canvas .react-flow__minimap")).toMatchObject({
      bottom: "var(--xflow-preview-float-inset)",
      right: "var(--xflow-preview-float-inset)",
      margin: "0"
    });
  });

  it("keeps no xyflow Controls styling now that the zoom bar replaced them", () => {
    expect(source).not.toMatch(/\bControls\b|ControlButton/);
    expect(css).not.toContain("react-flow__controls");
  });

  it("writes no colour literal in the chrome, so both appearances come from tokens", () => {
    const chromeRules = css.slice(css.indexOf(".xflow-preview-ruler,"), css.indexOf(".xflow-preview-canvas .react-flow__minimap"));
    expect(chromeRules.length).toBeGreaterThan(0);
    expect(chromeRules).not.toMatch(/#[0-9a-f]{3,8}\b|rgba?\(|hsla?\(/i);
  });
});

// EL-03: the card's hover/selection feedback is a transition, not an instant
// repaint, and every duration comes from the shared stage-1 motion tokens.
describe("xflow-preview node card motion (EL-03)", () => {
  it("reads the shared motion tokens instead of a local scale", () => {
    expect(css).not.toMatch(/--xflow-preview-motion-/);
    expect(css).toMatch(/--xflow-motion-duration-fast/);
    expect(css).toMatch(/--xflow-motion-duration-base/);
    expect(css).toMatch(/--xflow-motion-ease-standard/);
  });

  it("transitions only shadow, border and the selection ring colour", () => {
    const node = declarationsOf(".xflow-preview-node");
    expect(node.transition).toContain("box-shadow var(--xflow-motion-duration-fast) var(--xflow-motion-ease-standard)");
    expect(node.transition).toContain("border-color var(--xflow-motion-duration-fast) var(--xflow-motion-ease-standard)");
    expect(node.transition).toContain("outline-color var(--xflow-motion-duration-base) var(--xflow-motion-ease-standard)");
    // `all` would animate geometry on every re-render, not just feedback.
    expect(node.transition).not.toMatch(/\ball\b/);
    // The ring fades in from transparent instead of popping at 2px.
    expect(node.outline).toBe("2px solid transparent");
  });

  it("lifts the card one elevation step on hover and keeps a toned status border", () => {
    expect(declarationsOf(".xflow-preview-node:hover")).toEqual({ "box-shadow": "var(--xflow-elev-2)" });
    expect(declarationsOf(".xflow-preview-node:not([data-tone]):hover")).toEqual({
      "border-color": "var(--xflow-border-strong)"
    });
  });

  it("still renders the selected ring and turns the motion off under reduced motion", () => {
    expect(declarationsOf('.xflow-preview-node[data-selected="true"]')).toMatchObject({
      outline: "2px solid var(--xflow-preview-node-selected-border)"
    });
    expect(css).toMatch(
      /@media \(prefers-reduced-motion: reduce\)\s*\{\s*\.xflow-preview-node\s*\{\s*transition:\s*none;\s*\}\s*\.xflow-preview-node\[data-status="running"\]::after\s*\{\s*animation:\s*none;/
    );
  });
});

// E-A: the card's information architecture is three fixed levels. jsdom measures
// no layout, so the reserved sizes are pinned against the stylesheet text.
describe("xflow-preview card information architecture (E-A)", () => {
  it("fixes the card width and one height per summary-row count", () => {
    expect(declarationsOf(".xflow-preview-node")["width"]).toBe("var(--xflow-preview-node-width)");
    expect(css).toMatch(/--xflow-preview-node-width:\s*var\(--xflow-node-width\);/);
    // Header 56 + bottom strip 26 + card border 2 = 84. A full card then adds
    // exactly the rows it draws (data-summary-rows, written by index.tsx): one
    // 30px row plus its 10px bottom padding, two or three rows plus 4px gaps.
    // 30 flow px is the worst case at the 1.25 glyph cap and still 24 screen px
    // at the 0.8 full-detail floor; it never multiplies the inverse zoom.
    expect(declarationsOf('.xflow-preview-node[data-lod="full"]')).toMatchObject({
      "--xflow-preview-summary-row-h": "30px",
      "min-height": "84px"
    });
    expect(declarationsOf('.xflow-preview-node[data-lod="full"][data-summary-rows="1"]')["min-height"]).toBe(
      "calc(84px + var(--xflow-preview-summary-row-h) + var(--xflow-space-2-5))"
    );
    expect(declarationsOf('.xflow-preview-node[data-lod="full"][data-summary-rows="2"]')["min-height"]).toBe(
      "calc(84px + 2 * var(--xflow-preview-summary-row-h) + var(--xflow-space-1) + var(--xflow-space-2-5))"
    );
    expect(declarationsOf('.xflow-preview-node[data-lod="full"][data-summary-rows="3"]')["min-height"]).toBe(
      "calc(84px + 3 * var(--xflow-preview-summary-row-h) + 2 * var(--xflow-space-1) + var(--xflow-space-2-5))"
    );
    // 56 header + 26 bottom strip + 2 border: compact keeps the type line and
    // badges but drops the summary.
    expect(declarationsOf('.xflow-preview-node[data-lod="compact"]')).toEqual({ "min-height": "84px" });
  });

  it("keeps the low-detail spread handles inside the card's bottom edge (P3-a)", () => {
    expect(declarationsOf(".xflow-preview-node__ports--handles")).toMatchObject({
      top: "100%",
      height: "0"
    });
    // The dot's lower edge sits on the padding-box line instead of half a dot
    // below it, so nothing protrudes and the card has no scroll overflow.
    expect(declarationsOf(".xflow-preview-node__ports--handles .xflow-preview-node__handle")).toEqual({
      bottom: "0",
      transform: "translate(-50%, 0)"
    });
    // The hit target stays 24 screen px and is pinned to that same edge, so it
    // cannot make the card scroll.
    expect(declarationsOf(".xflow-preview-node__ports--handles .xflow-preview-node__handle::after")).toEqual({
      height: "calc(24px * var(--xflow-preview-inverse-zoom, 1))",
      inset: "auto auto 0 50%",
      transform: "translateX(-50%)",
      width: "calc(24px * var(--xflow-preview-inverse-zoom, 1))"
    });
  });

  it("gives every summary row the same height and ellipsizes a long value", () => {
    expect(declarationsOf(".xflow-preview-node__summary")).toMatchObject({
      "grid-template-columns": "auto minmax(0, 1fr)",
      "grid-auto-rows": "var(--xflow-preview-summary-row-h, 30px)"
    });
    // The old 70px three-line reserve is gone: a one-row node no longer carries
    // two empty line boxes.
    expect(declarationsOf(".xflow-preview-node__summary")["min-height"]).toBeUndefined();
    expect(declarationsOf(".xflow-preview-node__summary dd")).toMatchObject({
      "min-width": "0",
      overflow: "hidden",
      "text-overflow": "ellipsis",
      "white-space": "nowrap"
    });
    expect(declarationsOf(".xflow-preview-node__type")).toMatchObject({
      overflow: "hidden",
      "text-overflow": "ellipsis",
      "white-space": "nowrap"
    });
  });

  it("keeps the card's measured geometry independent of the zoom (P2-2)", () => {
    // A row measured as 24px * 1/zoom grew on every step the canvas zoomed out,
    // so xyflow re-measured all nodes per wheel notch. The row height is now one
    // flow value; only the glyphs follow the inverse zoom.
    const button = declarationsOf('.xflow-preview-node__summary dd[role="button"]');
    for (const value of [
      button["line-height"],
      button["min-height"],
      declarationsOf(".xflow-preview-node__summary")["grid-auto-rows"]
    ]) {
      expect(value).toBe("var(--xflow-preview-summary-row-h, 30px)");
    }
  });

  it("scales the card's small type by the inverse zoom instead of shrinking it", () => {
    // At 80% the 11px rows used to render at 8.8 screen px. Full and compact
    // cards multiply their glyphs by the canvas's inverse zoom, capped at 1.25
    // so 75% still lands at 10.3 screen px (see lod.test.ts for the arithmetic).
    expect(css).toContain("--xflow-preview-card-text-scale: clamp(1, var(--xflow-preview-inverse-zoom, 1), 1.25);");
    const scale = "var(--xflow-preview-card-text-scale, 1)";
    for (const selector of [
      ".xflow-preview-node__title strong",
      ".xflow-preview-node__type",
      ".xflow-preview-node__summary",
      ".xflow-preview-node__badge",
      ".xflow-preview-node__status",
      ".xflow-preview-node__port-text"
    ]) {
      expect(declarationsOf(selector)["font-size"], `${selector} scales its type`).toContain(scale);
    }
  });

  it("draws every card glyph from the @xflow/icons set at 12 or 14px", () => {
    // Badge and issue glyphs are 12px; the category block is 14px; the status
    // chip takes the same 12px face. No local pixel size outside those two.
    expect(declarationsOf(".xflow-preview-node__badge-icon svg")).toEqual({
      display: "block",
      height: "12px",
      width: "12px"
    });
    expect(declarationsOf(".xflow-preview-node__issues svg")).toMatchObject({ height: "12px", width: "12px" });
    expect(declarationsOf(".xflow-preview-node__status-icon")["font-size"]).toBe("var(--xflow-font-sm)");
    expect(declarationsOf(".xflow-preview-node__icon")["font-size"]).toBe("var(--xflow-font-lg)");
  });

  it("desaturates the category colour on dark cards (EL-21)", () => {
    // The raw category colour never fills a dark icon: it tints the raised
    // surface and mixes into the glyph, so orange/teal/purple do not clash.
    expect(declarationsOf('[data-theme="dark"] .xflow-preview-node__icon')).toEqual({
      background: "color-mix(in srgb, var(--xflow-preview-node-category) 10%, var(--xflow-panel-raised))",
      color: "color-mix(in srgb, var(--xflow-preview-node-category) 72%, var(--xflow-text-secondary))"
    });
  });

  it("puts the branch port labels and their handles in the bottom strip", () => {
    expect(declarationsOf(".xflow-preview-node__meta")).toMatchObject({
      "justify-content": "space-between",
      "margin-top": "auto",
      "min-height": "26px"
    });
    expect(declarationsOf(".xflow-preview-node__ports")).toMatchObject({
      "margin-left": "auto",
      position: "relative"
    });
    // Each label is the handle's containing block, so the 10px dot hangs on the
    // label's bottom edge instead of floating in the card body.
    expect(declarationsOf(".xflow-preview-node__port")).toMatchObject({
      position: "relative",
      "font-size": "var(--xflow-font-xs)"
    });
    // P3-2: the width keeper is an unscaled hidden copy, and the painted glyphs
    // sit on top at the card's inverse scale. The pill (and its centred handle)
    // therefore keep their layout position while the text reaches 11 screen px.
    expect(declarationsOf(".xflow-preview-node__port::before")).toMatchObject({
      content: "attr(data-port)",
      "font-size": "var(--xflow-font-xs)",
      visibility: "hidden"
    });
    expect(declarationsOf(".xflow-preview-node__port-text")).toMatchObject({
      position: "absolute",
      "pointer-events": "none",
      "font-size": "calc(var(--xflow-font-xs) * var(--xflow-preview-card-text-scale, 1))"
    });
    // P2-3: the target is pinned inside the card instead of centred on a dot
    // that hangs 6px above the card's bottom edge.
    expect(declarationsOf(".xflow-preview-node__port .xflow-preview-node__handle::after")).toMatchObject({
      inset: "auto auto -4px 50%",
      transform: "translateX(-50%)",
      height: "calc(24px * var(--xflow-preview-inverse-zoom, 1))",
      width: "calc(24px * var(--xflow-preview-inverse-zoom, 1))"
    });
  });
});

// §6.2: the visual section boxes are decoration. They must sit behind the
// cards and never take a gesture away from the canvas.
describe("xflow-preview canvas sections against styles.css", () => {
  it("puts the section layer behind the cards and outside the pointer's reach", () => {
    expect(declarationsOf(".xflow-preview-section-layer")).toEqual({
      inset: "0",
      "pointer-events": "none",
      position: "absolute",
      "z-index": "-1"
    });
  });

  it("draws the baseline frame: dashed category-coloured border, md corner, title chip", () => {
    expect(declarationsOf(".xflow-preview-section")).toMatchObject({
      "border-radius": "var(--xflow-radius-md)",
      position: "absolute"
    });
    const frame = declarationsOf(".xflow-preview-section");
    expect(frame.border).toContain("dashed");
    expect(frame.border).toContain("var(--xflow-preview-section-category)");
    expect(frame.background).toContain("var(--xflow-preview-section-category) 5%");

    expect(declarationsOf(".xflow-preview-section__title")).toMatchObject({
      "border-radius": "var(--xflow-radius-xs)",
      "font-size": "var(--xflow-font-md)",
      "font-weight": "var(--xflow-font-weight-semibold)",
      height: "22px",
      left: "12px",
      position: "absolute",
      top: "-11px"
    });
  });

  it("states no colour literal for the section palette", () => {
    const start = css.indexOf(".xflow-preview-section-layer");
    const end = css.indexOf(".xflow-preview-ruler,", start);
    expect(start).toBeGreaterThanOrEqual(0);
    expect(end).toBeGreaterThan(start);
    const rules = css.slice(start, end);
    expect(rules.length).toBeGreaterThan(0);
    expect(rules).not.toMatch(/#[0-9a-f]{3,8}\b|rgba?\(|hsla?\(/i);
  });
});

// WCAG 2.5.8: a canvas control keeps its visual size and carries a transparent
// 24 screen-px target. jsdom has no layout, so the rules are pinned as text;
// the pixel extents were measured in a real browser.
describe("xflow-preview section and zoom bar hit targets (WCAG 2.5.8)", () => {
  const target = "calc(24px * var(--xflow-preview-inverse-zoom, 1))";
  const halfGap = "var(--xflow-space-1-5) / 2";

  it("grows the rename button to 24 screen px, ending where the delete button's target begins", () => {
    expect(declarationsOf(".xflow-preview-section__title-action")).toMatchObject({ position: "relative" });
    const after = declarationsOf(".xflow-preview-section__title-action::after");
    expect(after).toMatchObject({
      content: '""',
      position: "absolute",
      height: `max(100%, ${target})`,
      width: `max(calc(100% + ${halfGap}), ${target})`,
      // Anchored on the side facing the delete button; a short title grows away from it.
      right: "calc(var(--xflow-space-1-5) / -2)"
    });
    expect(after).not.toHaveProperty("left");
  });

  it("grows the delete button to 24 screen px, starting where the rename button's target ends", () => {
    expect(declarationsOf(".xflow-preview-section__delete")).toMatchObject({ position: "relative" });
    const after = declarationsOf(".xflow-preview-section__delete::after");
    expect(after).toMatchObject({
      content: '""',
      position: "absolute",
      height: `max(100%, ${target})`,
      width: `max(calc(100% + ${halfGap}), ${target})`,
      left: "calc(var(--xflow-space-1-5) / -2)"
    });
    // A symmetric inset would reach into the rename button's target.
    expect(after).not.toHaveProperty("inset");
    expect(after).not.toHaveProperty("right");
  });

  it("splits the chip's own gap between the two targets, so they never overlap", () => {
    expect(declarationsOf(".xflow-preview-section__title").gap).toBe("var(--xflow-space-1-5)");
    // Each side takes half the gap: rename ends at +gap/2, delete starts at -gap/2.
    expect(declarationsOf(".xflow-preview-section__title-action::after").right).toBe("calc(var(--xflow-space-1-5) / -2)");
    expect(declarationsOf(".xflow-preview-section__delete::after").left).toBe("calc(var(--xflow-space-1-5) / -2)");
  });

  it("makes the zoom slider 24px tall without a pseudo-element, which a range cannot have", () => {
    const slider = declarationsOf(".xflow-preview-zoombar__slider");
    expect(slider.height).toBe("24px");
    // The UA paints the track and thumb centred in the taller box, so no vertical margin is needed.
    expect(slider.margin).toBe("0 var(--xflow-space-1)");
    expect(css).not.toMatch(/\.xflow-preview-zoombar__slider(?::[\w-]+)*::(?:after|before)/);
  });

  it("fits the 24px slider inside the zoom bar's content box, so the bar does not grow", () => {
    const tokens = readFileSync(`${import.meta.dirname}/../../xflow-core/src/tokens.css`, "utf8");
    const token = (name: string) => Number(new RegExp(`--xflow-${name}:\\s*(\\d+)px`).exec(tokens)?.[1]);
    const bar = declarationsOf(".xflow-preview-zoombar");
    expect(bar.height).toBe("var(--xflow-float-height)");
    expect(bar.border).toBe("1px solid var(--xflow-border)");
    expect(bar.padding).toBe("var(--xflow-space-1)");
    expect(token("float-height") - 2 * 1 - 2 * token("space-1")).toBeGreaterThanOrEqual(24);
  });
});
