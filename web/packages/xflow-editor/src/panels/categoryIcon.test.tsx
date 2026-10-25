import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { NodeCategoryIcon, nodeTypeGlyph } from "./categoryIcon";

const glyphOf = (container: HTMLElement): string | null | undefined =>
  container.querySelector("svg[data-xflow-icon]")?.getAttribute("data-xflow-icon");

/** The types the design names, plus the semantic fallbacks chosen for it. */
const designTypes: Array<[type: string, icon: string, category: string]> = [
  ["xflow.trigger.webhook", "link", "trigger"],
  ["xflow.trigger.cron", "clock", "trigger"],
  ["xflow.http", "globe", "action"],
  ["xflow.database", "db", "action"],
  ["xflow.if", "branch", "control"],
  ["xflow.approval", "user", "human"],
  ["xflow.notification", "bell", "action"],
  ["xflow.function", "code", "code"]
];

describe("NodeCategoryIcon", () => {
  for (const [type, icon, category] of designTypes) {
    it(`draws ${type} with its own ${icon} glyph`, () => {
      const { container } = render(<NodeCategoryIcon category={category} type={type} />);
      expect(glyphOf(container)).toBe(icon);
      // The category tint and its data hook are unchanged by the type glyph.
      expect(container.querySelector(".xflow-editor-category-icon")?.getAttribute("data-cat")).toBe(category);
    });
  }

  it("falls back to the category glyph for a type the table does not know", () => {
    const { container } = render(<NodeCategoryIcon category="action" type="xflow.custom.thing" />);
    expect(glyphOf(container)).toBe("globe");
    expect(container.querySelector(".xflow-editor-category-icon")?.getAttribute("data-cat")).toBe("action");
  });

  it("falls back to the unknown category's grid glyph when no category is declared", () => {
    const { container } = render(<NodeCategoryIcon type="xflow.custom.thing" />);
    expect(glyphOf(container)).toBe("grid");
    expect(container.querySelector(".xflow-editor-category-icon")?.getAttribute("data-cat")).toBe("unknown");
  });

  it("keeps the type lookup independent of the size variant", () => {
    const { container } = render(<NodeCategoryIcon category="trigger" size="sm" type="xflow.trigger.webhook" />);
    expect(glyphOf(container)).toBe("link");
    expect(container.querySelector(".xflow-editor-category-icon--sm")).not.toBeNull();
  });

  it("exposes the registry used by the node library and outline", () => {
    expect(nodeTypeGlyph("xflow.trigger.webhook")).toBeDefined();
    expect(nodeTypeGlyph("xflow.custom.thing")).toBeUndefined();
    expect(nodeTypeGlyph(undefined)).toBeUndefined();
  });
});
