import * as React from "react";
import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { EditorPanel } from "./shared";

describe("EditorPanel (§6.0 title bar)", () => {
  it("makes the section a region named by its visible heading", () => {
    render(
      <EditorPanel icon={<span />} title="节点">
        <p>body</p>
      </EditorPanel>
    );

    const region = screen.getByRole("region", { name: "节点" });
    const heading = within(region).getByRole("heading", { level: 3, name: "节点" });
    expect(heading.tagName).toBe("H3");
    expect(region.getAttribute("aria-labelledby")).toBe(heading.id);
    expect(heading.id).not.toBe("");
  });

  it("renders the count beside the heading without adding it to the region name", () => {
    render(
      <EditorPanel count={12} icon={<span />} title="变量">
        <p>body</p>
      </EditorPanel>
    );

    const region = screen.getByRole("region", { name: "变量" });
    expect(region.querySelector(".xflow-editor-panel-title__count")?.textContent).toBe("12");
  });

  it("hides the decorative icon from assistive technology", () => {
    render(
      <EditorPanel count={3} icon={<span aria-label="装饰图标" />} title="属性">
        <p>body</p>
      </EditorPanel>
    );

    const region = screen.getByRole("region", { name: /^(属性|工作流设置)$/ });
    expect(region.querySelector(".xflow-editor-panel-title__icon")?.getAttribute("aria-hidden")).toBe("true");
  });

  it("renders labelled actions in the title bar's trailing group", () => {
    render(
      <EditorPanel actions={<button aria-label="搜索节点" type="button">x</button>} icon={<span />} title="节点">
        <p>body</p>
      </EditorPanel>
    );

    const actions = screen.getByRole("region", { name: "节点" }).querySelector<HTMLElement>(".xflow-editor-panel-title__actions");
    expect(actions).toBeTruthy();
    expect(within(actions as HTMLElement).getByRole("button", { name: "搜索节点" })).toBeTruthy();
  });
});
