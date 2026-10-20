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
    expect(css).toMatch(/--xflow-preview-flow-period:\s*0\.8s;/);
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
    expect(port["border-color"]).toBe("color-mix(in srgb, var(--xflow-danger) 55%, transparent)");
    expect(port["border-color"]).not.toContain("preview-t-border");
  });

  it("reads the tone foreground from core's -fg, one tone at a time", () => {
    for (const tone of ["info", "success", "warning", "danger", "suspend", "neutral"]) {
      const rule = declarationsOf(
        `:is(.xflow-preview-node, .xflow-preview-node__status, .xflow-preview-status)[data-tone="${tone}"]`
      );
      expect(rule["--xflow-preview-t-fg"], tone).toBe(`var(--xflow-${tone}-fg)`);
    }
  });

  it("shapes the toolbar status pill like the baseline chip", () => {
    expect(declarationsOf(".xflow-preview-status")).toMatchObject({
      "border-radius": "var(--xflow-radius-pill)",
      "font-size": "var(--xflow-font-xs)",
      height: "20px",
      padding: "0 8px"
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

  it("shapes the §6.2 minimap 200x100 at the container radius", () => {
    expect(declarationsOf(".xflow-preview-canvas .react-flow__minimap")).toMatchObject({
      "border-radius": "var(--xflow-radius-md)",
      height: "100px",
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
      padding: "4px",
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
      padding: "4px",
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
