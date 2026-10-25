import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type * as React from "react";
import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import {
  CloseCircleIcon,
  SideRightIcon,
  XIcon,
  xIconAntdFallbacks,
  xIconRegistry,
  X_ICON_SIZE,
  X_ICON_STROKE_WIDTH,
  X_ICON_VIEW_BOX,
  type XIconName
} from "./index";

const here = path.dirname(fileURLToPath(import.meta.url));
const fixturePath = path.join(here, "__fixtures__", "sprite.html");
const mockupPath = path.resolve(here, "../../../../.claude/mockups/xflow-ui-v1-preview.html");

/** The four ids with no sprite symbol; drawn for round 2 of the visual audit. */
const DRAWN_FOR_ROUND_2 = ["edit", "eye", "format-painter", "node-index"];
/** Registry entries backed by `@ant-design/icons` instead of sprite geometry. */
const ANTD_FALLBACKS = Object.keys(xIconAntdFallbacks);
/** Everything that must follow the sprite baseline contract (svg attributes). */
const SPRITE_ICON_NAMES = (Object.keys(xIconRegistry) as XIconName[]).filter(
  (name) => !(name in xIconAntdFallbacks)
);

interface ElementSignature {
  tag: string;
  attrs: Record<string, string>;
  children: ElementSignature[];
}

function signature(element: Element): ElementSignature {
  return {
    tag: element.tagName.toLowerCase(),
    attrs: Object.fromEntries(
      [...element.attributes].map((attribute) => [attribute.name, attribute.value])
    ),
    children: [...element.children].map(signature)
  };
}

function parseSymbols(html: string): Map<string, ElementSignature> {
  const document = new DOMParser().parseFromString(html, "text/html");
  const symbols = new Map<string, ElementSignature>();
  for (const symbol of document.querySelectorAll("symbol")) {
    const id = symbol.getAttribute("id");
    if (!id?.startsWith("i-")) continue;
    symbols.set(id.slice(2), {
      tag: "symbol",
      attrs: { viewBox: symbol.getAttribute("viewBox") ?? "" },
      children: [...symbol.children].map(signature)
    });
  }
  return symbols;
}

function renderIcon(
  name: XIconName,
  props: React.SVGProps<SVGSVGElement> = {}
): SVGSVGElement {
  const { container } = render(<XIcon name={name} {...props} />);
  const svg = container.querySelector("svg");
  if (!svg) throw new Error(`XIcon "${name}" did not render an <svg>`);
  return svg;
}

const sprite = parseSymbols(readFileSync(fixturePath, "utf8"));

describe("xflow icon package registry", () => {
  it("covers every sprite symbol plus the four round-2 additions and the AntD fallbacks", () => {
    const ids = Object.keys(xIconRegistry);
    expect(ids).toHaveLength(71);
    expect(new Set(ids).size).toBe(71);
    expect([...sprite.keys()]).toHaveLength(66);
    for (const id of sprite.keys()) expect(ids).toContain(id);
    for (const id of DRAWN_FOR_ROUND_2) {
      expect(ids).toContain(id);
      expect(sprite.has(id)).toBe(false);
    }
    for (const id of ANTD_FALLBACKS) {
      expect(ids).toContain(id);
      expect(sprite.has(id)).toBe(false);
    }
    expect(ids.filter((id) => !sprite.has(id)).sort()).toEqual([...DRAWN_FOR_ROUND_2, ...ANTD_FALLBACKS].sort());
  });

  it.each(SPRITE_ICON_NAMES)("renders %s on the shared baseline", (name) => {
    const svg = renderIcon(name as XIconName);
    expect(svg.getAttribute("viewBox")).toBe(X_ICON_VIEW_BOX);
    expect(svg.getAttribute("fill")).toBe("none");
    expect(svg.getAttribute("stroke")).toBe("currentColor");
    expect(svg.getAttribute("stroke-width")).toBe(String(X_ICON_STROKE_WIDTH));
    expect(svg.getAttribute("stroke-linecap")).toBe("round");
    expect(svg.getAttribute("stroke-linejoin")).toBe("round");
    expect(svg.getAttribute("aria-hidden")).toBe("true");
    expect(svg.getAttribute("focusable")).toBe("false");
    expect(svg.getAttribute("width")).toBe(String(X_ICON_SIZE));
    expect(svg.getAttribute("height")).toBe(String(X_ICON_SIZE));
    expect(svg.getAttribute("data-xflow-icon")).toBe(name);
    expect(svg.children.length).toBeGreaterThan(0);
  });

  it("lets callers override the default size", () => {
    const svg = renderIcon("check", { width: 24, height: 24 });
    expect(svg.getAttribute("width")).toBe("24");
    expect(svg.getAttribute("height")).toBe("24");
  });

  it("renders the same markup through the look-up form and a named component", () => {
    const lookup = renderIcon("side-right").outerHTML;
    const { container } = render(<SideRightIcon />);
    expect(container.querySelector("svg")?.outerHTML).toBe(lookup);
  });

  it("registers every AntD fallback in the shared registry", () => {
    for (const [name, component] of Object.entries(xIconAntdFallbacks)) {
      expect(xIconRegistry[name as XIconName]).toBe(component);
      expect(sprite.has(name)).toBe(false);
    }
  });

  it("gives the AntD fallback the xflow-icon and aria-hidden contract", () => {
    const { container } = render(<CloseCircleIcon />);
    const span = container.querySelector("span");
    if (!span) throw new Error("CloseCircleIcon did not render a <span>");
    expect(span.classList.contains("xflow-icon")).toBe(true);
    expect(span.getAttribute("data-xflow-icon")).toBe("close-circle");
    expect(span.getAttribute("data-xflow-icon-source")).toBe("antd");
    expect(span.getAttribute("aria-hidden")).toBe("true");
    expect(span.querySelector("svg")).not.toBeNull();
  });

  it("keeps a caller className and accepts a fixed size", () => {
    const { container } = render(<CloseCircleIcon className="probe" size={20} />);
    const span = container.querySelector("span");
    if (!span) throw new Error("CloseCircleIcon did not render a <span>");
    expect(span.classList.contains("xflow-icon")).toBe(true);
    expect(span.classList.contains("probe")).toBe(true);
    expect(span.style.fontSize).toBe("20px");
  });

  it("inherits the surrounding font size when no size is given", () => {
    const { container } = render(<CloseCircleIcon />);
    const span = container.querySelector("span");
    if (!span) throw new Error("CloseCircleIcon did not render a <span>");
    expect(span.style.fontSize).toBe("");
  });

  it("exposes a named fallback as the control's accessible name", () => {
    const { container } = render(<CloseCircleIcon aria-label="清除" />);
    const span = container.querySelector("span");
    if (!span) throw new Error("CloseCircleIcon did not render a <span>");
    expect(span.getAttribute("aria-hidden")).toBeNull();
    expect(span.getAttribute("aria-label")).toBe("清除");
  });

  it("reaches the fallback through the look-up form", () => {
    const { container } = render(<XIcon name="close-circle" />);
    expect(container.querySelector('[data-xflow-icon="close-circle"]')).not.toBeNull();
  });
});

describe("sprite fidelity", () => {
  it.each([...sprite.keys()])("matches the sprite geometry of #i-%s", (name) => {
    const svg = renderIcon(name as XIconName);
    const rendered: ElementSignature = {
      tag: "symbol",
      attrs: { viewBox: svg.getAttribute("viewBox") ?? "" },
      children: [...svg.children].map(signature)
    };
    expect(rendered).toEqual(sprite.get(name));
  });

  it.runIf(existsSync(mockupPath))("stays in sync with the design mockup sprite", () => {
    const fromMockup = parseSymbols(readFileSync(mockupPath, "utf8"));
    expect(fromMockup.size).toBe(sprite.size);
    for (const [id, element] of fromMockup) expect(sprite.get(id)).toEqual(element);
  });
});
