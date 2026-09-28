// Test-only helpers for the composer/form suites. `*.testkit.tsx` files are
// excluded from the build (tsconfig) and from the import-boundary scan.

import { fireEvent } from "@testing-library/react";
import type { ComposerSpec, ElementDef } from "../core";
import { jsonRenderKernel } from "../kernel-json-render";
import { nativeKernel } from "../kernel-native";

export const kernels = [nativeKernel, jsonRenderKernel];

export function formSpec(elements: Record<string, ElementDef>, children: string[] = Object.keys(elements)): ComposerSpec {
  return {
    spec: "composer/v1",
    minor: 0,
    root: "form",
    elements: { form: { type: "Form", children }, ...elements }
  };
}

export function fieldOf(container: HTMLElement, id: string): HTMLElement {
  const el = container.querySelector<HTMLElement>(`[data-composer-node="${id.replace(/["\\]/g, "\\$&")}"]`);
  if (!el) throw new Error(`no element for node ${id}`);
  return el;
}

export function one<T extends Element>(root: ParentNode, selector: string): T {
  const el = root.querySelector<T>(selector);
  if (!el) throw new Error(`no ${selector}`);
  return el;
}

export function change(el: Element, value: string): void {
  fireEvent.change(el, { target: { value } });
}

/** Opens an AntD Select inside `field` and picks the option titled `label` from its popup. */
export function pickOption(field: HTMLElement, label: string): void {
  fireEvent.mouseDown(one(field, '[role="combobox"]'));
  const popups = [...document.querySelectorAll(".xflow-composer-select-popup")];
  const option = popups.at(-1)?.querySelector(`[title="${label}"]`);
  if (!option) throw new Error(`no option ${label}`);
  fireEvent.click(option);
}

export function clickClear(field: HTMLElement): void {
  fireEvent.mouseDown(one(field, ".xflow-composer-clear-icon"));
}

export function clickButton(root: ParentNode, name: string | RegExp): void {
  const buttons = [...root.querySelectorAll("button")];
  const button = buttons.find((b) => {
    const text = b.getAttribute("aria-label") ?? b.textContent ?? "";
    return typeof name === "string" ? text === name || (b.textContent ?? "").trim() === name : name.test(text);
  });
  if (!button) throw new Error(`no button ${String(name)}`);
  fireEvent.click(button);
}

/** Visible text plus placeholders of a field (where default hints show up). */
export function shownText(field: HTMLElement): string {
  const placeholders = [...field.querySelectorAll("[placeholder]")].map((el) => el.getAttribute("placeholder"));
  return [field.textContent ?? "", ...placeholders].join(" | ");
}
